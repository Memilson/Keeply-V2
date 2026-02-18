package com.keeply.app.cli;

import com.keeply.app.blob.BlobStore;
import com.keeply.app.config.Config;
import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.database.KeeplyDao;
import com.keeply.app.inventory.Backup;
import com.keeply.app.inventory.BackupHistoryDb;
import io.github.cdimascio.dotenv.Dotenv;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public final class cli {

    private cli() {}

    public static void main(String[] args) {
        run(args);
    }

    public static void run(String[] args) {
        int exitCode = execute(args);
        if (exitCode != 0) System.exit(exitCode);
    }

    public static int execute(String[] args) {
        return executeInternal(args, true);
    }

    /**
     * Para uso embutido (ex.: API), evita shutdown global do DB ao final.
     */
    public static int executeEmbedded(String[] args) {
        return executeInternal(args, false);
    }

    private static int executeInternal(String[] args, boolean shutdownDbOnExit) {
        bootstrapEnv();

        if (args == null || args.length == 0) {
            printUsage();
            return 0;
        }

        String cmd = safeLower(args[0]);
        String[] rest = Arrays.copyOfRange(args, 1, args.length);

        int exitCode;
        try {
            exitCode = switch (cmd) {
                case "scan" -> runScan(rest);
                case "history" -> runHistory(rest);
                case "help", "-h", "--help" -> {
                    printUsage();
                    yield 0;
                }
                default -> {
                    System.err.println("Comando invalido: " + args[0]);
                    printUsage();
                    yield 2;
                }
            };
        } catch (Exception e) {
            System.err.println("Erro fatal: " + safeMsg(e));
            exitCode = 1;
        } finally {
            if (shutdownDbOnExit) {
                try { DatabaseBackup.shutdown(); } catch (Exception ignored) {}
            }
        }

        return exitCode;
    }

    // ----------------- scan -----------------

    private static int runScan(String[] args) {
        ParseResult<ScanArgs> parsed = ScanArgs.parse(args);
        if (parsed.help()) {
            printScanUsage();
            return 0;
        }
        if (parsed.error() != null) {
            System.err.println(parsed.error());
            printScanUsage();
            return 2;
        }

        ScanArgs a = parsed.value();
        Path rootPath;
        Path destPath;

        try {
            rootPath = Path.of(a.root()).toAbsolutePath().normalize();
            destPath = Path.of(a.dest()).toAbsolutePath().normalize();
        } catch (Exception e) {
            System.err.println("Caminho invalido: " + safeMsg(e));
            return 2;
        }

        if (!Files.isDirectory(rootPath)) {
            System.err.println("Pasta origem nao existe: " + rootPath);
            return 2;
        }

        if (Config.isBackupEncryptionEnabled()) {
            if (isBlank(a.password()) || !Config.verifyAndCacheBackupPassword(a.password())) {
                System.err.println("Criptografia ativa: informe --password valida.");
                return 2;
            }
        }

        // best-effort: não deixa exceção de config atrapalhar o backup
        try { Config.saveLastPath(rootPath.toString()); } catch (Exception ignored) {}
        try { Config.saveLastBackupDestination(destPath.toString()); } catch (Exception ignored) {}

        Backup.ScanMetrics metrics = new Backup.ScanMetrics();
        AtomicBoolean cancel = new AtomicBoolean(false);

        // cancelamento via Ctrl+C / kill
        Thread cancelHook = Thread.ofPlatform()
                .name("keeply-cli-cancel")
                .unstarted(() -> {
                    cancel.set(true);
                    System.err.println("Cancelamento solicitado (shutdown hook) em " + Instant.now());
                });

        try {
            Runtime.getRuntime().addShutdownHook(cancelHook);
        } catch (Exception ignored) {
            // ok: sem hook
        }

        long historyId = BackupHistoryDb.start(rootPath.toString(), destPath.toString());
        Long scanId = null;
        BlobStore.BackupResult backupResult = null;
        String status = "SUCCESS";
        String message = null;
        String backupType = null;

        try {
            DatabaseBackup.init();
            applyFastPragmas();

            Backup.ScanConfig cfg = buildScanConfig();

            scanId = Backup.runScan(
                    rootPath,
                    destPath,
                    cfg,
                    metrics,
                    cancel,
                    cli::log
            );

            Long firstId = DatabaseBackup.jdbi().withExtension(
                    KeeplyDao.class,
                    dao -> dao.fetchFirstScanIdForRoot(rootPath.toString())
            );
            backupType = (firstId != null && Objects.equals(firstId, scanId)) ? "FULL" : "INCREMENTAL";

            backupResult = BlobStore.runBackupIncremental(
                    rootPath,
                    cfg,
                    destPath,
                    scanId,
                    cancel,
                    cli::log
            );

            log("Backup concluido: scanId=" + scanId
                + ", files=" + backupResult.filesProcessed()
                + ", errors=" + backupResult.errors());

            return 0;
        } catch (Exception e) {
            status = cancel.get() ? "CANCELED" : "ERROR";
            message = safeMsg(e);
            System.err.println("Falha no backup: " + message);
            return 1;
        } finally {
            long files = backupResult == null ? 0 : backupResult.filesProcessed();
            long errors = backupResult == null ? 0 : backupResult.errors();

            try {
                BackupHistoryDb.finish(historyId, status, files, errors, scanId, message, backupType);
            } catch (Exception e) {
                System.err.println("Falha ao finalizar historico: " + safeMsg(e));
            }

            try {
                DatabaseBackup.persistEncryptedSnapshot();
            } catch (Exception e) {
                System.err.println("Falha ao persistir snapshot: " + safeMsg(e));
            }

            // best-effort: remove hook para não “sujar” em execução embutida/testes
            try {
                Runtime.getRuntime().removeShutdownHook(cancelHook);
            } catch (Exception ignored) {}
        }
    }

    // ----------------- history -----------------

    private static int runHistory(String[] args) {
        ParseResult<HistoryArgs> parsed = HistoryArgs.parse(args);
        if (parsed.help()) {
            printHistoryUsage();
            return 0;
        }
        if (parsed.error() != null) {
            System.err.println(parsed.error());
            printHistoryUsage();
            return 2;
        }

        int limit = parsed.value().limit();
        List<BackupHistoryDb.HistoryRow> rows = new ArrayList<>(BackupHistoryDb.listRecent(limit));

        if (rows.isEmpty()) {
            System.out.println("Sem historico de backups.");
            return 0;
        }

        System.out.println("id | status | tipo | inicio | fim | arquivos | erros | root");
        for (BackupHistoryDb.HistoryRow row : rows) {
            System.out.printf(
                    "%d | %s | %s | %s | %s | %d | %d | %s%n",
                    row.id(),
                    safeText(row.status()),
                    safeText(row.backupType()),
                    safeText(row.startedAt()),
                    safeText(row.finishedAt()),
                    row.filesProcessed(),
                    row.errors(),
                    safeText(row.rootPath())
            );
        }
        return 0;
    }

    // ----------------- env / config -----------------

    private static void bootstrapEnv() {
        // não sobrescreve DB_URL se já veio por -D ou env do processo
        if (isBlank(System.getProperty("DB_URL"))) {
            try {
                Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
                String dbUrl = Objects.requireNonNullElse(dotenv.get("DB_URL"), "jdbc:sqlite:keeply.db");
                System.setProperty("DB_URL", dbUrl);
            } catch (Exception ignored) {
                System.setProperty("DB_URL", "jdbc:sqlite:keeply.db");
            }
        }
    }

    private static Backup.ScanConfig buildScanConfig() {
        Backup.ScanConfig defaults = Backup.ScanConfig.defaults();

        // preserva ordem e evita duplicatas
        Set<String> excludes = new LinkedHashSet<>(defaults.excludeGlobs());

        // keeply / dev
        excludes.add("**/.keeply/**");
        excludes.add("**/*.keeply*");
        excludes.add("**/.git/**");
        excludes.add("**/node_modules/**");
        excludes.add("**/.venv/**");
        excludes.add("**/__pycache__/**");
        excludes.add("**/.DS_Store");
        excludes.add("**/Thumbs.db");

        String os = System.getProperty("os.name", "generic").toLowerCase(Locale.ROOT);
        if (os.contains("win")) {
            excludes.add("**/Windows/**");
            excludes.add("**/Program Files/**");
            excludes.add("**/Program Files (x86)/**");
            excludes.add("**/ProgramData/**");
            excludes.add("**/System Volume Information/**");
            excludes.add("**/$Recycle.Bin/**");
            excludes.add("**/AppData/**");
            excludes.add("**/ntuser.dat*");
            excludes.add("**/hiberfil.sys");
            excludes.add("**/pagefile.sys");
            excludes.add("**/swapfile.sys");
        } else if (os.contains("nux") || os.contains("linux")) {
            excludes.add("**/proc/**");
            excludes.add("**/sys/**");
            excludes.add("**/dev/**");
            excludes.add("**/run/**");
            excludes.add("**/tmp/**");
            excludes.add("**/var/tmp/**");
            excludes.add("**/var/cache/**");
            excludes.add("**/.cache/**");
            excludes.add("**/.local/share/Trash/**");
        }

        int batchSize = clamp(defaults.dbBatchSize(), 2000, 10_000);

        return new Backup.ScanConfig(batchSize, List.copyOf(excludes));
    }

    private static void applyFastPragmas() {
        try {
            DatabaseBackup.jdbi().useHandle(h -> {
                // ✅ alinhado com SQLite WAL + concorrência
                h.execute("PRAGMA foreign_keys=ON");
                h.execute("PRAGMA journal_mode=WAL");
                h.execute("PRAGMA synchronous=NORMAL");
                h.execute("PRAGMA temp_store=MEMORY");
                h.execute("PRAGMA cache_size=-20000");
                h.execute("PRAGMA busy_timeout=5000");
            });
        } catch (Exception ignored) {
            // best-effort
        }
    }

    // ----------------- usage -----------------

    private static void printUsage() {
        System.out.println("""
                Keeply CLI
                Comandos:
                  scan --root <origem> --dest <destino> [--password <senha>]
                  history [--limit <n>]
                  help
                """);
    }

    private static void printScanUsage() {
        System.out.println("""
                Uso:
                  scan --root <origem> --dest <destino> [--password <senha>]

                Exemplos:
                  scan --root /home/meus-arquivos --dest /mnt/backup
                  scan --root C:\\Users\\meuusuario --dest D:\\Backups --password 123
                """);
    }

    private static void printHistoryUsage() {
        System.out.println("""
                Uso:
                  history [--limit <n>]

                Exemplo:
                  history --limit 50
                """);
    }

    // ----------------- parsing -----------------

    private static final class ArgCursor {
        private final String[] args;
        private int i;

        ArgCursor(String[] args) {
            this.args = args == null ? new String[0] : args;
        }

        boolean hasNext() { return i < args.length; }

        String next() { return args[i++]; }

        String requireNext(String opt) {
            if (!hasNext()) throw new IllegalArgumentException("Valor ausente para " + opt);
            return next();
        }
    }

    private record ParseResult<T>(T value, boolean help, String error) {
        static <T> ParseResult<T> okResult(T v) { return new ParseResult<>(v, false, null); }
        static <T> ParseResult<T> helpResult() { return new ParseResult<>(null, true, null); }
        static <T> ParseResult<T> errorResult(String e) { return new ParseResult<>(null, false, e); }
    }

    private record ScanArgs(String root, String dest, String password) {
        static ParseResult<ScanArgs> parse(String[] args) {
            String root = null, dest = null, password = null;

            try {
                ArgCursor c = new ArgCursor(args);
                while (c.hasNext()) {
                    String t = c.next();
                    switch (t) {
                        case "-h", "--help" -> { return ParseResult.helpResult(); }
                        case "--root" -> root = c.requireNext("--root");
                        case "--dest" -> dest = c.requireNext("--dest");
                        case "--password" -> password = c.requireNext("--password");
                        default -> { return ParseResult.errorResult("Opcao invalida: " + t); }
                    }
                }
            } catch (IllegalArgumentException e) {
                return ParseResult.errorResult(safeMsg(e));
            }

            if (isBlank(root) || isBlank(dest)) {
                return ParseResult.errorResult("Parametros obrigatorios: --root e --dest");
            }
            return ParseResult.okResult(new ScanArgs(root, dest, password));
        }
    }

    private record HistoryArgs(int limit) {
        static ParseResult<HistoryArgs> parse(String[] args) {
            int limit = 20;

            try {
                ArgCursor c = new ArgCursor(args);
                while (c.hasNext()) {
                    String t = c.next();
                    switch (t) {
                        case "-h", "--help" -> { return ParseResult.helpResult(); }
                        case "--limit" -> {
                            String v = c.requireNext("--limit");
                            int n = Integer.parseInt(v.trim());
                            limit = Math.max(1, n);
                        }
                        default -> { return ParseResult.errorResult("Opcao invalida: " + t); }
                    }
                }
            } catch (NumberFormatException e) {
                return ParseResult.errorResult("Valor invalido para --limit");
            } catch (IllegalArgumentException e) {
                return ParseResult.errorResult(safeMsg(e));
            }

            return ParseResult.okResult(new HistoryArgs(limit));
        }
    }

    // ----------------- misc -----------------

    private static int clamp(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private static String safeLower(String s) {
        return s == null ? "" : s.trim().toLowerCase(Locale.ROOT);
    }

    private static String safeMsg(Throwable t) {
        String m = (t == null) ? null : t.getMessage();
        return (m == null || m.isBlank())
                ? (t == null ? "Erro" : t.getClass().getSimpleName())
                : m;
    }

    private static String safeText(String v) {
        return isBlank(v) ? "-" : v;
    }

    private static boolean isBlank(String v) {
        return v == null || v.isBlank();
    }

    private static void log(String msg) {
        if (!isBlank(msg)) System.out.println(msg);
    }
}
