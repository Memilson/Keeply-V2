package com.keeply.app;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.sql.Connection;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Scanner {

    private Scanner() {}

    public enum FileStatus { SEEN, NEW, MODIFIED, SYNCED }
    public enum LargeFileHashPolicy { SKIP, SAMPLED, FULL }

    public record ScanConfig(int dbBatchSize, int hashWorkers, boolean computeHash, long hashMaxBytes, LargeFileHashPolicy largeFileHashPolicy, int sampledChunkBytes, List<String> excludeGlobs) {
        public static ScanConfig defaults() {
            // Nota: hashWorkers pode ser aumentado para 16 ou 32 em máquinas potentes
            return new ScanConfig(5000, 32, true, 200L * 1024 * 1024, LargeFileHashPolicy.SAMPLED, 1 * 1024 * 1024, 
                List.of("**/.git/**", "**/node_modules/**", "**/AppData/**", "**/Keeply/**", "**/*.iso", "**/*.vdi"));
        }
    }

    public record FileSeen(long scanId, String pathRel, String name, long size, long mtime, long ctime) {}
    public record HashCandidate(long ignoredId, String pathRel, long sizeBytes) {} 
    public record HashUpdate(String pathRel, String hashHex) {}
    public record HashResult(HashUpdate update, Database.ScanIssue issue) {}

    public static final class ScanMetrics {
        public final LongAdder filesSeen = new LongAdder();
        public final LongAdder dirsSkipped = new LongAdder();
        public final LongAdder walkErrors = new LongAdder();
        public final LongAdder dbBatches = new LongAdder();
        public final LongAdder hashed = new LongAdder();
        public final AtomicBoolean running = new AtomicBoolean(false);
        public final Instant start = Instant.now();
    }

    private static final HexFormat HEX = HexFormat.of();
    private static final Logger logger = LoggerFactory.getLogger(Scanner.class);
    
    // ThreadLocals para evitar alocação excessiva de memória em Virtual Threads
    private static final ThreadLocal<MessageDigest> SHA256 = ThreadLocal.withInitial(() -> {
        try { return MessageDigest.getInstance("SHA-256"); } catch (Exception e) { throw new RuntimeException(e); }
    });
    private static final ThreadLocal<byte[]> BUFFER = ThreadLocal.withInitial(() -> new byte[128 * 1024]);
    private static final ThreadLocal<byte[]> SAMPLE_BUFFER = ThreadLocal.withInitial(() -> new byte[1024 * 1024]);

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
        cancel.set(false);

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

        // 2. Walk (Produtor de Metadados)
        uiLogger.accept(">> Fase 1: Mapeando arquivos no disco...");
        try (var writer = new DbWriter(pool, scanId, cfg.dbBatchSize(), metrics, uiLogger)) {
            walk(rootAbs, scanId, cfg, metrics, cancel, writer);
            writer.flush();
            writer.waitFinish();
        }

        if (cancel.get()) {
            uiLogger.accept(">> Cancelado pelo usuário.");
            return;
        }

        // 3. Limpeza
        uiLogger.accept(">> Fase 2: Sincronizando banco (Limpeza)...");
        try (Connection c = pool.borrow()) {
            int deleted = Database.deleteStaleFiles(c, scanId);
            if (deleted > 0) uiLogger.accept(">> Removidos " + deleted + " arquivos obsoletos.");
            c.commit();
        }

        // 4. Hash (Pipeline Paralelo Otimizado)
        if (cfg.computeHash()) {
            uiLogger.accept("[SCAN] Calculando Hashes (Workers: " + cfg.hashWorkers() + ")...");
            processHashesOptimized(rootAbs, pool, cfg, metrics, cancel, scanId, uiLogger);
        }

        // --- Histórico ---
        try (Connection c = pool.borrow()) {
            int hist = Database.snapshotToHistory(c, scanId);
            if (hist > 0) logger.info("[DB] Histórico: {} alterações registradas.", hist);
            
            Database.finishScanLog(c, scanId);
            c.commit();
        }
        
        uiLogger.accept("[SCAN] Finalizado.");
        uiLogger.accept(">> FINALIZADO! Total de arquivos: " + metrics.filesSeen.sum());
        metrics.running.set(false);
    }

    private static void walk(Path root, long scanId, ScanConfig cfg, ScanMetrics metrics, AtomicBoolean cancel, DbWriter writer) throws IOException {
        var matchers = cfg.excludeGlobs.stream().map(g -> FileSystems.getDefault().getPathMatcher("glob:" + g)).toList();

        Files.walkFileTree(root, java.util.EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if (cancel.get()) return FileVisitResult.TERMINATE;
                if (!dir.equals(root)) {
                    var rel = root.relativize(dir).toString().replace('\\', '/');
                    String checkPath = "/" + rel + "/";
                    for (var m : matchers) {
                        if (m.matches(Paths.get(rel)) || m.matches(Paths.get(checkPath))) {
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
                    var rel = root.relativize(file).toString().replace('\\', '/');
                    for (var m : matchers) if (m.matches(Paths.get(rel))) return FileVisitResult.CONTINUE;

                    writer.add(new FileSeen(scanId, rel, file.getFileName().toString(), attrs.size(), attrs.lastModifiedTime().toMillis(), attrs.creationTime().toMillis()));
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

    /**
     * Pipeline Otimizado: Dispatcher -> Workers (Semaphore) -> Queue -> DB Persister
     */
    private static void processHashesOptimized(Path root, Database.SimplePool pool, ScanConfig cfg, ScanMetrics metrics, AtomicBoolean cancel, long scanId, Consumer<String> uiLogger) throws Exception {
        
        // Fila de comunicação entre Workers (Hash) e Persister (Banco)
        BlockingQueue<HashUpdate> updatesQueue = new LinkedBlockingQueue<>(10000);
        BlockingQueue<Database.ScanIssue> issuesQueue = new LinkedBlockingQueue<>(2000);
        // Semáforo para controlar estritamente o número de IOs paralelos
        Semaphore concurrencyLimiter = new Semaphore(cfg.hashWorkers());
        AtomicBoolean finishedProducing = new AtomicBoolean(false);

        // 1. Thread PERSISTER (Consumidor Único para o SQLite)
        Thread dbPersister = Thread.ofVirtual().name("hash-db-writer").start(() -> {
            try (Connection c = pool.borrow()) {
                var updateList = new ArrayList<HashUpdate>(1000);
                var issueList = new ArrayList<Database.ScanIssue>(200);
                while (!finishedProducing.get() || !updatesQueue.isEmpty() || !issuesQueue.isEmpty()) {
                    HashUpdate u = updatesQueue.poll(50, TimeUnit.MILLISECONDS);
                    if (u != null) updateList.add(u);
                    Database.ScanIssue issue = issuesQueue.poll();
                    if (issue != null) issueList.add(issue);

                    boolean ready = updateList.size() >= 1000 || issueList.size() >= 200;
                    boolean flush = (u == null && issue == null) && (!updateList.isEmpty() || !issueList.isEmpty());
                    if (ready || flush) {
                        if (!updateList.isEmpty()) {
                            Database.updateHashes(c, updateList);
                        }
                        if (!issueList.isEmpty()) {
                            Database.insertScanIssues(c, scanId, issueList);
                        }
                        c.commit();
                        updateList.clear();
                        issueList.clear();
                    }
                }
            } catch (Exception e) {
                logger.error("Erro no DB Persister", e);
            }
        });

        // 2. DISPATCHER & WORKERS
        try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
            while (!cancel.get()) {
                // Fetch em batches do banco para não carregar milhões de linhas na RAM
                List<HashCandidate> batch;
                try (Connection c = pool.borrow()) {
                    batch = Database.fetchDirtyFiles(c, scanId, 5000);
                    c.commit();
                }
                
                if (batch.isEmpty()) break; // Nada mais a fazer

                for (var item : batch) {
                    if (cancel.get()) break;
                    
                    // Bloqueia se já houver 'hashWorkers' tarefas rodando
                    concurrencyLimiter.acquire();
                    
                    exec.submit(() -> {
                        try {
                            HashResult res = computeHash(root, cfg, item, metrics);
                            if (res != null && res.update() != null) {
                                try {
                                    updatesQueue.put(res.update());
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                            if (res != null && res.issue() != null) {
                                try {
                                    issuesQueue.put(res.issue());
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        } finally {
                            // Libera o slot para o próximo arquivo
                            concurrencyLimiter.release();
                        }
                    });
                }
            }
        } // O try-with-resources espera todas as threads virtuais terminarem aqui

        // Finalização
        finishedProducing.set(true);
        dbPersister.join(); // Espera o banco terminar de gravar o que sobrou
    }

    private static HashResult computeHash(Path root, ScanConfig cfg, HashCandidate item, ScanMetrics metrics) {
        try {
            var absPath = root.resolve(item.pathRel());
            if (cfg.largeFileHashPolicy() == LargeFileHashPolicy.SKIP && item.sizeBytes() > cfg.hashMaxBytes()) {
                return new HashResult(new HashUpdate(item.pathRel(), "SKIPPED_SIZE"), null);
            }

            var md = SHA256.get(); 
            md.reset();

            // Hash Amostral (Rápido)
            if (cfg.largeFileHashPolicy() == LargeFileHashPolicy.SAMPLED && item.sizeBytes() > cfg.hashMaxBytes()) {
                int chunk = Math.max(64 * 1024, cfg.sampledChunkBytes());
                var buf = SAMPLE_BUFFER.get();
                if (buf.length < chunk) buf = new byte[chunk];
                
                md.update(longToBytes(item.sizeBytes()));
                try (var ch = FileChannel.open(absPath, StandardOpenOption.READ)) {
                    readAt(ch, 0, buf, chunk, md);
                    long pos = Math.max(0, ch.size() - chunk);
                    readAt(ch, pos, buf, chunk, md);
                }
                
                String hex = HEX.formatHex(md.digest());
                metrics.hashed.increment();
                return new HashResult(new HashUpdate(item.pathRel(), hex), null);
            }

            // Hash Completo (Seguro)
            var buf = BUFFER.get();
            try (InputStream in = Files.newInputStream(absPath)) {
                int n; 
                while ((n = in.read(buf)) != -1) {
                    md.update(buf, 0, n);
                }
            }
            
            String hex = HEX.formatHex(md.digest());
            
            // TODO: Integração BlobStore
            // blobStore.store(absPath, hex); <-- Injetar aqui futuramente para backup
            
            metrics.hashed.increment();
            return new HashResult(new HashUpdate(item.pathRel(), hex), null);

        } catch (Exception e) { 
            String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            return new HashResult(null, new Database.ScanIssue(item.pathRel(), "Hash error: " + message));
        }
    }

    private static void readAt(FileChannel ch, long pos, byte[] buf, int len, MessageDigest md) throws IOException {
        var bb = ByteBuffer.wrap(buf, 0, len); 
        int read = 0;
        while (bb.hasRemaining()) { 
            int r = ch.read(bb, pos + read); 
            if (r < 0) break; 
            read += r; 
        }
        if (read > 0) md.update(buf, 0, read);
    }

    private static byte[] longToBytes(long v) { 
        return new byte[] { (byte)(v >>> 56), (byte)(v >>> 48), (byte)(v >>> 40), (byte)(v >>> 32), (byte)(v >>> 24), (byte)(v >>> 16), (byte)(v >>> 8), (byte)(v) }; 
    }

    // --- WRITER (Fase de Walk) ---
    private static class DbWriter implements AutoCloseable {
        private final Database.SimplePool pool;
        private final long scanId;
        private final int batchSize;
        private final ScanMetrics metrics;
        private final BlockingQueue<FileSeen> queue = new ArrayBlockingQueue<>(10000);
        private final Thread worker;
        @SuppressWarnings("unused")
        private final Consumer<String> uiLogger;
        private volatile boolean finished = false;

        DbWriter(Database.SimplePool pool, long scanId, int batchSize, ScanMetrics metrics, Consumer<String> uiLogger) {
            this.pool = pool; this.scanId = scanId; this.batchSize = batchSize; this.metrics = metrics; this.uiLogger = uiLogger;
            this.worker = Thread.ofVirtual().name("sqlite-writer").start(this::run);
        }

        void add(FileSeen f) { try { while (!queue.offer(f, 10, TimeUnit.MILLISECONDS)); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }
        void flush() { finished = true; }
        void waitFinish() { try { worker.join(); } catch (InterruptedException e) {} }
        @Override public void close() { flush(); waitFinish(); }

        private void run() {
            try (Connection c = pool.borrow()) {
                var sql = """
                    INSERT INTO file_inventory (path_rel, name, size_bytes, modified_millis, created_millis, last_scan_id, status)
                    VALUES (?, ?, ?, ?, ?, ?, 'NEW')
                    ON CONFLICT(path_rel) DO UPDATE SET
                        last_scan_id = excluded.last_scan_id,
                        status = CASE WHEN size_bytes != excluded.size_bytes OR modified_millis != excluded.modified_millis THEN 'MODIFIED' ELSE status END,
                        size_bytes = excluded.size_bytes, modified_millis = excluded.modified_millis
                """;
                try (var ps = c.prepareStatement(sql)) {
                    int pending = 0;
                    while (!finished || !queue.isEmpty()) {
                        var f = queue.poll(100, TimeUnit.MILLISECONDS);
                        if (f == null) continue;
                        ps.setString(1, f.pathRel()); ps.setString(2, f.name()); ps.setLong(3, f.size());
                        ps.setLong(4, f.mtime()); ps.setLong(5, f.ctime()); ps.setLong(6, scanId);
                        ps.addBatch(); pending++;
                        if (pending >= batchSize) {
                            ps.executeBatch(); c.commit(); metrics.dbBatches.increment();
                            pending = 0;
                        }
                    }
                    if (pending > 0) { ps.executeBatch(); c.commit(); metrics.dbBatches.increment(); }
                }
            } catch (Exception e) { Scanner.logger.error("DbWriter error", e); }
        }
    }
}
