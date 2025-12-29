package com.keeply.app;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Scanner {

    private Scanner() {}
    // Configuração simplificada (somente metadados)
    public record ScanConfig(int dbBatchSize, List<String> excludeGlobs) {
        public static ScanConfig defaults() {
            return new ScanConfig(10000, 
                List.of("**/.git/**", "**/node_modules/**", "**/AppData/**", "**/Keeply/**", "**/*.iso", "**/*.vdi"));
        }
    }

    // Apenas metadados essenciais
    public record FileSeen(long scanId, String pathRel, String name, long size, long mtime, long ctime) {}

    public static final class ScanMetrics {
        public final LongAdder filesSeen = new LongAdder();
        public final LongAdder dirsSkipped = new LongAdder();
        public final LongAdder walkErrors = new LongAdder();
        public final LongAdder dbBatches = new LongAdder();
        public final AtomicBoolean running = new AtomicBoolean(false);
        public final Instant start = Instant.now();
    }

    private static final Logger logger = LoggerFactory.getLogger(Scanner.class);

    // --- ENGINE PRINCIPAL ---

    public static void runScan(
            Path root, 
            ScanConfig cfg, 
            Database.SimplePool pool, 
            ScanMetrics metrics, 
            AtomicBoolean cancel,
            Consumer<String> uiLogger
    ) throws Exception {
        
        var rootAbs = root.toAbsolutePath().normalize();
        metrics.running.set(true);

        // 1. Setup DB
        try (Connection c = pool.borrow()) {
            Database.ensureSchema(c);
            c.commit();
        }

        long scanId;
        try (Connection c = pool.borrow()) {
            scanId = Database.startScanLog(c, rootAbs.toString());
            c.commit();
        }
        // 2. Walk & Metadata Check (validação por metadados e SQL)
        uiLogger.accept(">> Fase 1: Validando metadados (Tamanho/Data/Nome)...");
        try (var writer = new DbWriter(pool, scanId, cfg.dbBatchSize(), metrics)) {
            walk(rootAbs, scanId, cfg, metrics, cancel, writer);
            writer.flush();
            writer.waitFinish();
        }

        if (cancel.get()) {
            try (Connection c = pool.borrow()) {
                Database.cancelScanLog(c, scanId);
                c.commit();
            } catch (Exception e) {
                logger.error("Falha ao marcar scan como cancelado", e);
            }
            uiLogger.accept(">> Cancelado pelo usuário.");
            metrics.running.set(false);
            return;
        }

        // 3. Limpeza (Detecta arquivos deletados)
        uiLogger.accept(">> Fase 2: Sincronizando banco (Limpeza)...");
        try (Connection c = pool.borrow()) {
            int deleted = Database.deleteStaleFiles(c, scanId);
            if (deleted > 0) uiLogger.accept(">> Removidos " + deleted + " arquivos que não existem mais.");
            c.commit();
        }
        // Nota: a validação ocorre inteiramente na inserção do banco.
        // 4. Histórico (Time Lapse)
        try (Connection c = pool.borrow()) {
            // Agora copiamos para o histórico baseados apenas na flag MODIFIED/NEW definida pelos metadados
            int hist = Database.snapshotToHistory(c, scanId);
            if (hist > 0) logger.info("[DB] Histórico: {} alterações detectadas.", hist);
            
            Database.finishScanLog(c, scanId);
            c.commit();
        }
        
        uiLogger.accept("[SCAN] Finalizado.");
        uiLogger.accept(">> FINALIZADO! Total de arquivos validados: " + metrics.filesSeen.sum());
        metrics.running.set(false);
    }

    private static void walk(Path root, long scanId, ScanConfig cfg, ScanMetrics metrics, AtomicBoolean cancel, DbWriter writer) throws IOException {
        // Compila os matchers apenas uma vez
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
                    
                    // Aqui está a mágica: Enviamos os metadados. 
                    // O DbWriter compara (SQL) se 'size' ou 'mtime' mudaram vs o histórico.
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
    // --- WRITER (Validação via SQL) ---
    private static class DbWriter implements AutoCloseable {
        private final Database.SimplePool pool;
        private final long scanId;
        private final int batchSize;
        private final ScanMetrics metrics;
        private final BlockingQueue<FileSeen> queue = new ArrayBlockingQueue<>(10000);
        private final Thread worker;
        private volatile boolean finished = false;

        DbWriter(Database.SimplePool pool, long scanId, int batchSize, ScanMetrics metrics) {
            this.pool = pool; this.scanId = scanId; this.batchSize = batchSize; this.metrics = metrics;
            this.worker = Thread.ofVirtual().name("sqlite-writer").start(this::run);
        }

        void add(FileSeen f) { 
            try { 
                queue.put(f); // Backpressure natural se o DB for lento
            } catch (InterruptedException e) { 
                Thread.currentThread().interrupt(); 
            } 
        }
        
        void flush() { finished = true; }
        void waitFinish() { try { worker.join(); } catch (InterruptedException e) {} }
        @Override public void close() { flush(); waitFinish(); }

        private void run() {
            try (Connection c = pool.borrow()) {
                // Validação por metadados:
                // Se o arquivo já existe (ON CONFLICT path_rel), verificamos se mudou.
                // Se size != old.size OR mtime != old.mtime -> MODIFIED
                // Senão -> mantém o status atual (ex: STABLE)
                var sql = """
                    INSERT INTO file_inventory (path_rel, name, size_bytes, modified_millis, created_millis, last_scan_id, status)
                    VALUES (?, ?, ?, ?, ?, ?, 'NEW')
                    ON CONFLICT(path_rel) DO UPDATE SET
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
                            ps.setString(1, item.pathRel()); 
                            ps.setString(2, item.name()); 
                            ps.setLong(3, item.size());
                            ps.setLong(4, item.mtime()); 
                            ps.setLong(5, item.ctime()); 
                            ps.setLong(6, scanId);
                            ps.addBatch();
                            pending++;
                        }
                        drainList.clear();

                        if (pending >= batchSize || (pending > 0 && f == null)) {
                            ps.executeBatch(); 
                            c.commit(); 
                            metrics.dbBatches.increment();
                            pending = 0;
                        }
                    }
                    if (pending > 0) { ps.executeBatch(); c.commit(); metrics.dbBatches.increment(); }
                }
            } catch (Exception e) { logger.error("DbWriter error", e); }
        }
    }
}




