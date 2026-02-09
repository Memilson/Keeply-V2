package com.keeply.app.inventory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.database.KeeplyDao;

public final class Backup {

    private Backup() {}

        public record ScanConfig(
            @SuppressWarnings("unused") int dbBatchSize,
            @SuppressWarnings("unused") List<String> excludeGlobs
        ) {
        public static ScanConfig defaults() {
            List<String> excludes = new ArrayList<>();

            // comuns
            excludes.addAll(List.of(
                "**/.keeply/**",
                "**/*.keeply*",
                "**/.git/**",
                "**/node_modules/**",
                "**/.venv/**",
                "**/__pycache__/**",
                "**/*.iso",
                "**/*.vdi"
            ));

            // Windows (se o root for perfil de usuário / disco)
            excludes.addAll(List.of(
                "**/AppData/**",
                "**/Windows/**",
                "**/ProgramData/**",
                "**/System Volume Information/**",
                "**/$Recycle.Bin/**",
                "**/hiberfil.sys",
                "**/pagefile.sys",
                "**/swapfile.sys"
            ));

            // Linux (se o root for / ou home grande)
            excludes.addAll(List.of(
                "**/proc/**",
                "**/sys/**",
                "**/dev/**",
                "**/run/**",
                "**/tmp/**",
                "**/var/tmp/**",
                "**/var/cache/**",
                "**/.cache/**",
                "**/.local/share/Trash/**"
            ));

            return new ScanConfig(10000, List.copyOf(excludes));
        }
    }

        public record FileSeen(
            @SuppressWarnings("unused") long scanId,
            @SuppressWarnings("unused") String pathRel,
            @SuppressWarnings("unused") String name,
            @SuppressWarnings("unused") long size,
            @SuppressWarnings("unused") long mtime,
            @SuppressWarnings("unused") long ctime
        ) {}

    public static final class ScanMetrics {
        public final LongAdder filesSeen = new LongAdder();
        public final LongAdder dirsSkipped = new LongAdder();
        public final LongAdder walkErrors = new LongAdder();
        public final LongAdder dbBatches = new LongAdder();
        public final AtomicBoolean running = new AtomicBoolean(false);
        public final Instant start = Instant.now();
    }

    private static final Logger logger = LoggerFactory.getLogger(Backup.class);

    // -----------------------------
    // OS helpers
    // -----------------------------
    private static boolean isWindows() {
        return System.getProperty("os.name", "generic").toLowerCase(Locale.ROOT).contains("win");
    }

    // Normaliza Path relativo pra string com "/" (pra fast checks)
    private static String relNorm(Path rel) {
        return rel.toString().replace('\\', '/');
    }

    // Fast path: evita PathMatcher (caro) na maioria dos casos
    private static boolean fastExcludePath(String rn) {
        // gerais
        if (rn.contains("/.keeply/")) return true;
        if (rn.contains("/.git/")) return true;
        if (rn.contains("/node_modules/")) return true;

        if (isWindows()) {
            if (rn.startsWith("Windows/") || rn.contains("/Windows/")) return true;
            if (rn.contains("/AppData/")) return true;
            if (rn.contains("/System Volume Information/")) return true;
            if (rn.contains("/$Recycle.Bin/")) return true;
            if (rn.contains("/ProgramData/")) return true;
        } else {
            if (rn.startsWith("proc/") || rn.contains("/proc/")) return true;
            if (rn.startsWith("sys/")  || rn.contains("/sys/"))  return true;
            if (rn.startsWith("dev/")  || rn.contains("/dev/"))  return true;
            if (rn.startsWith("run/")  || rn.contains("/run/"))  return true;
            if (rn.startsWith("tmp/")  || rn.contains("/tmp/"))  return true;
            if (rn.contains("/var/cache/")) return true;
            if (rn.contains("/var/tmp/")) return true;
            if (rn.contains("/.cache/")) return true;
            if (rn.contains("/.local/share/Trash/")) return true;
        }
        return false;
    }

    private static List<PathMatcher> compileMatchers(List<String> globs) {
        var fs = FileSystems.getDefault();
        var out = new ArrayList<PathMatcher>(globs == null ? 0 : globs.size());
        if (globs == null) return out;
        for (String g : globs) {
            if (g == null || g.isBlank()) continue;
            // Importante: na prática, "glob:**/*.java" funciona no Windows também.
            // Mantemos "/" nos globs e garantimos que o Path passado pro matcher
            // tenha separadores coerentes via Paths.get(relNormString).
            out.add(fs.getPathMatcher("glob:" + g));
        }
        return out;
    }

    private static boolean matchesAny(Path relForMatcher, List<PathMatcher> matchers) {
        for (var m : matchers) {
            if (m.matches(relForMatcher)) return true;
        }
        return false;
    }

    // -----------------------------
    // Public API
    // -----------------------------
    public static long runScan(
        Path root,
        ScanConfig cfg,
        ScanMetrics metrics,
        AtomicBoolean cancel,
        Consumer<String> uiLogger
) throws IOException {
    return runScan(root, null, cfg, metrics, cancel, uiLogger);
}

public static long runScan(
        Path root,
        Path backupDest,
        ScanConfig cfg,
        ScanMetrics metrics,
        AtomicBoolean cancel,
        Consumer<String> uiLogger
) throws IOException {

        var rootAbs = root.toAbsolutePath().normalize();
        metrics.running.set(true);

        DatabaseBackup.init();

        long scanId = DatabaseBackup.jdbi().withExtension(
                KeeplyDao.class,
                dao -> dao.startScanLog(rootAbs.toString())
        );

        uiLogger.accept(">> Fase 1: Validando metadados (Tamanho/Data/Nome)...");

        try (var writer = new DbWriter(scanId, rootAbs.toString(), cfg.dbBatchSize(), metrics, cancel)) {
            if (backupDest != null) {
                walk(rootAbs, backupDest, scanId, cfg, metrics, cancel, writer);
            } else {
                walk(rootAbs, scanId, cfg, metrics, cancel, writer);
            }
            writer.flush();
            writer.waitFinish();
        }

        if (cancel.get()) {
            try {
                DatabaseBackup.jdbi().useExtension(KeeplyDao.class, dao -> dao.cancelScanLog(scanId));
            } catch (RuntimeException e) {
                logger.error("Falha ao marcar scan como cancelado", e);
            }
            uiLogger.accept(">> Cancelado pelo usuário.");
            metrics.running.set(false);
            DatabaseBackup.persistEncryptedSnapshot();
            return scanId;
        }

        uiLogger.accept(">> Fase 2: Sincronizando banco (Limpeza)...");

        KeeplyDao.CleanupResult cleanup = DatabaseBackup.jdbi().withExtension(
            KeeplyDao.class,
            dao -> dao.deleteStaleFiles(scanId, rootAbs.toString())
        );
        if (cleanup.removedFromInventory > 0) {
            uiLogger.accept(">> Removidos " + cleanup.removedFromInventory + " arquivos que não existem mais.");
        }

        int hist = DatabaseBackup.jdbi().inTransaction(handle -> {
            KeeplyDao dao = handle.attach(KeeplyDao.class);
            int count = dao.copyToHistory(scanId);
            dao.markStable(scanId);
            dao.finishScanLog(scanId);
            return count;
        });

        if (hist > 0) logger.info("[DB] Historico: {} alteracoes detectadas.", hist);

        uiLogger.accept("[SCAN] Finalizado.");
        uiLogger.accept(">> FINALIZADO! Total de arquivos validados: " + metrics.filesSeen.sum());

        metrics.running.set(false);
        DatabaseBackup.persistEncryptedSnapshot();
        return scanId;
    }

    // -----------------------------
    // Walk (otimizado)
    // -----------------------------
    private static void walk(
        Path root,
        Path backupDest,
        long scanId,
        ScanConfig cfg,
        ScanMetrics metrics,
        AtomicBoolean cancel,
        DbWriter writer
) throws IOException {

    final Path rootAbs = root.toAbsolutePath().normalize();
    final Path destAbs = backupDest.toAbsolutePath().normalize();
    final boolean nested = destAbs.startsWith(rootAbs);

    final var matchers = compileMatchers(cfg.excludeGlobs());

    Files.walkFileTree(
            rootAbs,
            EnumSet.noneOf(FileVisitOption.class),
            Integer.MAX_VALUE,
            new SimpleFileVisitor<>() {

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    if (cancel.get() || Thread.currentThread().isInterrupted()) return FileVisitResult.TERMINATE;
                    if (dir.equals(rootAbs)) return FileVisitResult.CONTINUE;

                    if (nested) {
                        // dir já vem absoluto dentro do walk
                        Path dirAbs = dir.normalize();
                        if (dirAbs.startsWith(destAbs)) {
                            metrics.dirsSkipped.increment();
                            return FileVisitResult.SKIP_SUBTREE;
                        }
                    }

                    Path rel = rootAbs.relativize(dir);
                    String rn = relNorm(rel);

                    if (fastExcludePath(rn)) {
                        metrics.dirsSkipped.increment();
                        return FileVisitResult.SKIP_SUBTREE;
                    }

                    if (!matchers.isEmpty()) {
                        Path relForMatcher = Paths.get(rn.replace('/', java.io.File.separatorChar));
                        if (matchesAny(relForMatcher, matchers)) {
                            metrics.dirsSkipped.increment();
                            return FileVisitResult.SKIP_SUBTREE;
                        }
                    }

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    if (cancel.get() || Thread.currentThread().isInterrupted()) return FileVisitResult.TERMINATE;
                    if (!attrs.isRegularFile()) return FileVisitResult.CONTINUE;

                    if (nested) {
                        Path fileAbs = file.normalize();
                        if (fileAbs.startsWith(destAbs)) return FileVisitResult.CONTINUE;
                    }

                    Path rel = rootAbs.relativize(file);
                    String rn = relNorm(rel);

                    if (fastExcludePath(rn)) return FileVisitResult.CONTINUE;

                    if (!matchers.isEmpty()) {
                        Path relForMatcher = Paths.get(rn.replace('/', java.io.File.separatorChar));
                        if (matchesAny(relForMatcher, matchers)) return FileVisitResult.CONTINUE;
                    }

                    writer.add(new FileSeen(
                            scanId,
                            rn,
                            file.getFileName().toString(),
                            attrs.size(),
                            attrs.lastModifiedTime().toMillis(),
                            attrs.creationTime().toMillis()
                    ));

                    metrics.filesSeen.increment();
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    metrics.walkErrors.increment();
                    return FileVisitResult.CONTINUE;
                }
            }
    );
}


    private static void walk(
            Path root,
            long scanId,
            ScanConfig cfg,
            ScanMetrics metrics,
            AtomicBoolean cancel,
            DbWriter writer
    ) throws IOException {

        final Path rootAbs = root.toAbsolutePath().normalize();
        final int maxDepth = Integer.MAX_VALUE;

        final var matchers = compileMatchers(cfg.excludeGlobs());

        Files.walkFileTree(
                rootAbs,
                EnumSet.noneOf(FileVisitOption.class),
                maxDepth,
                new SimpleFileVisitor<>() {

                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                        if (cancel.get() || Thread.currentThread().isInterrupted()) return FileVisitResult.TERMINATE;
                        if (dir.equals(rootAbs)) return FileVisitResult.CONTINUE;

                        Path rel = rootAbs.relativize(dir);
                        String rn = relNorm(rel);

                        // FAST: evita PathMatcher na maioria dos casos
                        if (fastExcludePath(rn)) {
                            metrics.dirsSkipped.increment();
                            return FileVisitResult.SKIP_SUBTREE;
                        }

                        // PathMatcher: passamos um Path construído a partir do rn (com "/")
                        if (!matchers.isEmpty()) {
                            Path relForMatcher = Paths.get(rn.replace('/', java.io.File.separatorChar));
                            if (matchesAny(relForMatcher, matchers)) {
                                metrics.dirsSkipped.increment();
                                return FileVisitResult.SKIP_SUBTREE;
                            }
                        }

                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        if (cancel.get() || Thread.currentThread().isInterrupted()) return FileVisitResult.TERMINATE;
                        if (!attrs.isRegularFile()) return FileVisitResult.CONTINUE;

                        Path rel = rootAbs.relativize(file);
                        String rn = relNorm(rel);

                        // FAST também pra arquivo
                        if (fastExcludePath(rn)) return FileVisitResult.CONTINUE;

                        if (!matchers.isEmpty()) {
                            Path relForMatcher = Paths.get(rn.replace('/', java.io.File.separatorChar));
                            if (matchesAny(relForMatcher, matchers)) return FileVisitResult.CONTINUE;
                        }

                        writer.add(new FileSeen(
                                scanId,
                                rn,
                                file.getFileName().toString(),
                                attrs.size(),
                                attrs.lastModifiedTime().toMillis(),
                                attrs.creationTime().toMillis()
                        ));

                        metrics.filesSeen.increment();
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException exc) {
                        metrics.walkErrors.increment();
                        return FileVisitResult.CONTINUE;
                    }
                }
        );
    }

    // -----------------------------
    // DbWriter (otimizado)
    // -----------------------------
    private static class DbWriter implements AutoCloseable {
        private final long scanId;
        private final String rootPath;
        private final int batchSize;
        private final ScanMetrics metrics;
        private final AtomicBoolean cancel;

        private final BlockingQueue<FileSeen> queue;
        private final Thread worker;

        private volatile boolean finished = false;
        private volatile Exception workerError;

        DbWriter(long scanId,
                 String rootPath,
                 int batchSize,
                 ScanMetrics metrics,
                 AtomicBoolean cancel) {
            this.scanId = scanId;
            this.rootPath = rootPath;

            // batchSize “sadio”
            int tuned = Math.max(2000, Math.min(batchSize, 10000));
            this.batchSize = tuned;

            this.metrics = metrics;
            this.cancel = cancel;

            // fila maior = o walker não trava tanto no writer
            this.queue = new ArrayBlockingQueue<>(50_000);

            this.worker = Thread.ofVirtual().name("sqlite-writer").start(this::run);
        }

        void add(FileSeen f) {
            if (f == null) return;
            // não trava infinito se writer estiver atrás; respeita cancel/interrupção
            while (!finished && !cancel.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    if (queue.offer(f, 200, TimeUnit.MILLISECONDS)) return;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        void flush() { finished = true; }

        void waitFinish() {
            try {
                worker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (workerError != null) {
                throw new RuntimeException("DbWriter failed", workerError);
            }
        }

        @Override
        public void close() {
            flush();
            waitFinish();
        }

        private void run() {
            Connection c = null;
            try {
                c = DatabaseBackup.openSingleConnection();

                // transação real
                try { c.setAutoCommit(false); } catch (SQLException ignored) {}

                // PRAGMAs (best-effort)
                try (var st = c.createStatement()) {
                    st.execute("PRAGMA foreign_keys=ON");
                    st.execute("PRAGMA synchronous=NORMAL");
                    st.execute("PRAGMA temp_store=MEMORY");
                    st.execute("PRAGMA cache_size=-20000"); // ~20MB
                    st.execute("PRAGMA busy_timeout=5000");
                } catch (SQLException ignored) {}

                var sql = """
                    INSERT INTO file_inventory (root_path, path_rel, name, size_bytes, modified_millis, created_millis, last_scan_id, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'NEW')
                    ON CONFLICT(root_path, path_rel) DO UPDATE SET
                        last_scan_id = excluded.last_scan_id,
                        status = CASE
                                    WHEN size_bytes != excluded.size_bytes OR modified_millis != excluded.modified_millis THEN 'MODIFIED'
                                    ELSE status
                                 END,
                        size_bytes = excluded.size_bytes,
                        modified_millis = excluded.modified_millis
                """;

                try (var ps = c.prepareStatement(sql)) {

                    int pending = 0;
                    List<FileSeen> drainList = new ArrayList<>(batchSize);

                    long lastFlushNs = System.nanoTime();
                    final long MAX_LATENCY_NS = TimeUnit.MILLISECONDS.toNanos(400);

                    while ((!finished && !cancel.get()) || !queue.isEmpty()) {

                        // pega 1 com timeout curto
                        FileSeen f = queue.poll(50, TimeUnit.MILLISECONDS);
                        if (f != null) drainList.add(f);

                        // drena o resto até completar batch
                        queue.drainTo(drainList, Math.max(0, batchSize - drainList.size()));

                        for (FileSeen item : drainList) {
                            ps.setString(1, rootPath);
                            ps.setString(2, item.pathRel());
                            ps.setString(3, item.name());
                            ps.setLong(4, item.size());
                            ps.setLong(5, item.mtime());
                            ps.setLong(6, item.ctime());
                            ps.setLong(7, scanId);
                            ps.addBatch();
                            pending++;
                        }
                        drainList.clear();

                        boolean timeToFlush = pending > 0 && (System.nanoTime() - lastFlushNs) >= MAX_LATENCY_NS;
                        boolean doneAndEmpty = pending > 0 && (finished || cancel.get()) && queue.isEmpty();

                        if (pending >= batchSize || timeToFlush || doneAndEmpty) {
                            ps.executeBatch();
                            c.commit();
                            metrics.dbBatches.increment();
                            pending = 0;
                            lastFlushNs = System.nanoTime();
                        }

                        if (cancel.get() || Thread.currentThread().isInterrupted()) break;
                    }

                    // flush final
                    if (pending > 0) {
                        ps.executeBatch();
                        c.commit();
                        metrics.dbBatches.increment();
                    }
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                workerError = e;
                logger.error("DbWriter interrompido", e);
            } catch (Exception e) {
                try { c.rollback(); } catch (SQLException ignored) {}
                workerError = e;
                logger.error("DbWriter error", e);
            } finally {
                if (c != null) {
                    try { c.close(); } catch (SQLException ignored) {}
                }
            }
        }
    }
}
