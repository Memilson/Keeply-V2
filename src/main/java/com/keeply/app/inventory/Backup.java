package com.keeply.app.inventory;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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

    @SuppressWarnings("unused")
    private Backup() {}

    @SuppressWarnings("unused")
    public record ScanConfig(@SuppressWarnings("unused") int dbBatchSize,
                             @SuppressWarnings("MismatchedQueryAndUpdateOfCollection") List<String> excludeGlobs) {
        public static ScanConfig defaults() {
            List<String> excludes = new ArrayList<>();
            excludes.addAll(List.of(
                "**/.git/**",
                "**/node_modules/**",
                "**/AppData/**",
                "**/Keeply/**",
                "**/*.iso",
                "**/*.vdi"
            ));
            return new ScanConfig(10000, List.copyOf(excludes));
        }}
    @SuppressWarnings("unused")
    public record FileSeen(@SuppressWarnings("unused") long scanId,
                           @SuppressWarnings("unused") String pathRel,
                           @SuppressWarnings("unused") String name,
                           @SuppressWarnings("unused") long size,
                           @SuppressWarnings("unused") long mtime,
                           @SuppressWarnings("unused") long ctime) {}

    public static final class ScanMetrics {
        public final LongAdder filesSeen = new LongAdder();
        public final LongAdder dirsSkipped = new LongAdder();
        public final LongAdder walkErrors = new LongAdder();
        public final LongAdder dbBatches = new LongAdder();
        public final AtomicBoolean running = new AtomicBoolean(false);
        public final Instant start = Instant.now();
    }

    private static final Logger logger = LoggerFactory.getLogger(Backup.class);

        public static long runScan(
            Path root, 
            ScanConfig cfg, 
            DatabaseBackup.SimplePool pool, 
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
        try (var writer = new DbWriter(pool, scanId, rootAbs.toString(), cfg.dbBatchSize(), metrics)) {
            walk(rootAbs, scanId, cfg, metrics, cancel, writer);
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
        int deleted = DatabaseBackup.jdbi().withExtension(
                KeeplyDao.class,
                dao -> dao.deleteStaleFiles(scanId, rootAbs.toString())
        );
        if (deleted > 0) uiLogger.accept(">> Removidos " + deleted + " arquivos que não existem mais.");
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

    private static void walk(Path root, long scanId, ScanConfig cfg, ScanMetrics metrics, AtomicBoolean cancel, DbWriter writer) throws IOException {
        var matchers = cfg.excludeGlobs.stream().map(g -> FileSystems.getDefault().getPathMatcher("glob:" + g)).toList();
        
        Files.walkFileTree(root, java.util.EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if (cancel.get()) return FileVisitResult.TERMINATE;
                if (!dir.equals(root)) {
                    Path relativePath = root.relativize(dir);
                    
                    for (var m : matchers) {
                        if (m.matches(relativePath)) {
                            metrics.dirsSkipped.increment();
                            return FileVisitResult.SKIP_SUBTREE;
                        }
                    }
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (cancel.get()) return FileVisitResult.TERMINATE;
                if (attrs.isRegularFile()) {
                    Path relativePath = root.relativize(file);
                    
                    for (var m : matchers) {
                        if (m.matches(relativePath)) return FileVisitResult.CONTINUE;
                    }

                    String relString = relativePath.toString().replace('\\', '/');
                    
                    writer.add(new FileSeen(
                        scanId, 
                        relString, 
                        file.getFileName().toString(), 
                        attrs.size(), 
                        attrs.lastModifiedTime().toMillis(), 
                        attrs.creationTime().toMillis()
                    ));
                    metrics.filesSeen.increment();
                }
                return FileVisitResult.CONTINUE;
            }
            
            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                metrics.walkErrors.increment();
                return FileVisitResult.CONTINUE; 
            }
        });
    }
    private static class DbWriter implements AutoCloseable {
        private final DatabaseBackup.SimplePool pool;
        private final long scanId;
        private final String rootPath;
        private final int batchSize;
        private final ScanMetrics metrics;
        private final BlockingQueue<FileSeen> queue = new ArrayBlockingQueue<>(10000);
        private final Thread worker;
        private volatile boolean finished = false;
        private volatile Exception workerError;

        DbWriter(DatabaseBackup.SimplePool pool, long scanId, String rootPath, int batchSize, ScanMetrics metrics) {
            this.pool = pool;
            this.scanId = scanId;
            this.rootPath = rootPath;
            this.batchSize = batchSize;
            this.metrics = metrics;
            this.worker = Thread.ofVirtual().name("sqlite-writer").start(this::run);
        }

        void add(FileSeen f) { 
            try { 
                queue.put(f);
            } catch (InterruptedException e) { 
                Thread.currentThread().interrupt(); 
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
        @Override public void close() { flush(); waitFinish(); }

        private void run() {
            try (Connection c = pool.borrow()) {
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
                    
                    while (!finished || !queue.isEmpty()) {
                        FileSeen f = queue.poll(100, TimeUnit.MILLISECONDS);
                        if (f != null) drainList.add(f);
                        
                        queue.drainTo(drainList, batchSize - 1);
                        
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

                        if (pending >= batchSize || (pending > 0 && f == null)) {
                            ps.executeBatch(); 
                            c.commit(); 
                            metrics.dbBatches.increment();
                            pending = 0;
}}
                    if (pending > 0) { ps.executeBatch(); c.commit(); metrics.dbBatches.increment(); }
                }}catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                workerError = e;
                logger.error("DbWriter interrompido", e);
            } catch (java.sql.SQLException | RuntimeException e) {
                workerError = e;
                logger.error("DbWriter error", e);
            }}}}
