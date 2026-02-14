package com.keeply.app.cli;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import com.keeply.app.blob.BlobStore;
import com.keeply.app.config.Config;
import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.database.KeeplyDao;
import com.keeply.app.inventory.Backup;
import com.keeply.app.inventory.BackupHistoryDb;

import io.github.cdimascio.dotenv.Dotenv;

public final class cli {

    private cli() {}

    public static void main(String[] args) {
        run(args);
    }

    public static void run(String[] args) {
        int exitCode = execute(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    public static int execute(String[] args) {
        bootstrapEnv();

        if (args == null || args.length == 0) {
            printUsage();
            return 0;
        }

        String cmd = args[0].trim().toLowerCase(Locale.ROOT);
        String[] rest = java.util.Arrays.copyOfRange(args, 1, args.length);

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
                    System.err.println("Comando invalido: " + cmd);
                    printUsage();
                    yield 2;
                }
            };
        } catch (Exception e) {
            System.err.println("Erro fatal: " + safeMsg(e));
            exitCode = 1;
        } finally {
            DatabaseBackup.shutdown();
        }

        return exitCode;
    }

    private static int runScan(String[] args) {
        String root = null;
        String dest = null;
        String password = null;

        for (int i = 0; i < args.length; i++) {
            String token = args[i];
            switch (token) {
                case "--root" -> root = requireValue(args, ++i, "--root");
                case "--dest" -> dest = requireValue(args, ++i, "--dest");
                case "--password" -> password = requireValue(args, ++i, "--password");
                case "-h", "--help" -> {
                    printScanUsage();
                    return 0;
                }
                default -> {
                    System.err.println("Opcao invalida: " + token);
                    printScanUsage();
                    return 2;
                }
            }
        }

        if (isBlank(root) || isBlank(dest)) {
            System.err.println("Parametros obrigatorios: --root e --dest");
            printScanUsage();
            return 2;
        }

        Path rootPath;
        Path destPath;
        try {
            rootPath = Path.of(root).toAbsolutePath().normalize();
            destPath = Path.of(dest).toAbsolutePath().normalize();
        } catch (Exception e) {
            System.err.println("Caminho invalido: " + safeMsg(e));
            return 2;
        }

        if (!java.nio.file.Files.isDirectory(rootPath)) {
            System.err.println("Pasta origem nao existe: " + rootPath);
            return 2;
        }

        if (Config.isBackupEncryptionEnabled()) {
            if (isBlank(password) || !Config.verifyAndCacheBackupPassword(password)) {
                System.err.println("Criptografia ativa: informe --password valida.");
                return 2;
            }
        }

        Config.saveLastPath(rootPath.toString());
        Config.saveLastBackupDestination(destPath.toString());

        Backup.ScanMetrics metrics = new Backup.ScanMetrics();
        AtomicBoolean cancel = new AtomicBoolean(false);

        long historyId = BackupHistoryDb.start(rootPath.toString(), destPath.toString());
        long scanId = -1;
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
            backupType = (firstId != null && firstId == scanId) ? "FULL" : "INCREMENTAL";

            backupResult = BlobStore.runBackupIncremental(
                    rootPath,
                    cfg,
                    destPath,
                    scanId,
                    cancel,
                    cli::log
            );

            log("Backup concluido: scanId=" + scanId + ", files=" + backupResult.filesProcessed() + ", errors=" + backupResult.errors());
            return 0;
        } catch (Exception e) {
            status = cancel.get() ? "CANCELED" : "ERROR";
            message = safeMsg(e);
            System.err.println("Falha no backup: " + message);
            return 1;
        } finally {
            long files = backupResult == null ? 0 : backupResult.filesProcessed();
            long errors = backupResult == null ? 0 : backupResult.errors();
            BackupHistoryDb.finish(historyId, status, files, errors, (scanId > 0 ? scanId : null), message, backupType);
            DatabaseBackup.persistEncryptedSnapshot();
        }
    }

    private static int runHistory(String[] args) {
        int limit = 20;
        for (int i = 0; i < args.length; i++) {
            String token = args[i];
            switch (token) {
                case "--limit" -> {
                    String v = requireValue(args, ++i, "--limit");
                    try {
                        limit = Math.max(1, Integer.parseInt(v));
                    } catch (NumberFormatException e) {
                        System.err.println("Valor invalido para --limit: " + v);
                        return 2;
                    }
                }
                case "-h", "--help" -> {
                    printHistoryUsage();
                    return 0;
                }
                default -> {
                    System.err.println("Opcao invalida: " + token);
                    printHistoryUsage();
                    return 2;
                }
            }
        }

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

    private static void bootstrapEnv() {
        try {
            Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
            String dbUrl = Objects.requireNonNullElse(dotenv.get("DB_URL"), "jdbc:sqlite:keeply.db");
            System.setProperty("DB_URL", dbUrl);
        } catch (Exception ignored) {
            System.setProperty("DB_URL", "jdbc:sqlite:keeply.db");
        }
    }

    private static Backup.ScanConfig buildScanConfig() {
        Backup.ScanConfig defaults = Backup.ScanConfig.defaults();
        List<String> excludes = new ArrayList<>(defaults.excludeGlobs());

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

        int batchSize = Math.max(2000, defaults.dbBatchSize());
        batchSize = Math.min(batchSize, 10000);
        return new Backup.ScanConfig(batchSize, List.copyOf(excludes));
    }

    private static void applyFastPragmas() {
        try {
            DatabaseBackup.jdbi().useHandle(h -> {
                h.execute("PRAGMA foreign_keys=ON");
                h.execute("PRAGMA synchronous=NORMAL");
                h.execute("PRAGMA temp_store=MEMORY");
                h.execute("PRAGMA cache_size=-20000");
                h.execute("PRAGMA busy_timeout=5000");
            });
        } catch (Exception ignored) {
        }
    }

    private static void printUsage() {
        System.out.println("Keeply CLI");
        System.out.println("Comandos:");
        System.out.println("  scan --root <origem> --dest <destino> [--password <senha>]");
        System.out.println("  history [--limit <n>]");
    }

    private static void printScanUsage() {
        System.out.println("Uso: scan --root <origem> --dest <destino> [--password <senha>]");
    }

    private static void printHistoryUsage() {
        System.out.println("Uso: history [--limit <n>]");
    }

    private static String requireValue(String[] args, int index, String opt) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Valor ausente para " + opt);
        }
        return args[index];
    }

    private static String safeMsg(Throwable t) {
        return t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
    }

    private static String safeText(String v) {
        return isBlank(v) ? "-" : v;
    }

    private static boolean isBlank(String v) {
        return v == null || v.isBlank();
    }

    private static void log(String msg) {
        if (!isBlank(msg)) {
            System.out.println(msg);
        }
    }
}
