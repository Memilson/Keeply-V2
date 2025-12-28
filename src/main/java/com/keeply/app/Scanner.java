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
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

public final class Scanner {

    private Scanner() {}

    public enum FileStatus { SEEN, NEW, MODIFIED, SYNCED }
    public enum LargeFileHashPolicy { SKIP, SAMPLED, FULL }

    public record ScanConfig(int dbBatchSize, int hashWorkers, boolean computeHash, long hashMaxBytes, LargeFileHashPolicy largeFileHashPolicy, int sampledChunkBytes, List<String> excludeGlobs) {
        public static ScanConfig defaults() {
            return new ScanConfig(5000, 4, true, 200L * 1024 * 1024, LargeFileHashPolicy.SAMPLED, 1 * 1024 * 1024, 
                List.of("**/.git/**", "**/node_modules/**", "**/AppData/**", "**/Keeply/**", "**/*.iso", "**/*.vdi"));
        }
    }

    public record FileSeen(long scanId, String pathRel, String name, long size, long mtime, long ctime) {}
    public record HashCandidate(long ignoredId, String pathRel, long sizeBytes) {} 
    public record HashUpdate(String pathRel, String hashHex) {}

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
            Consumer<String> logger // NOVO: Permite enviar logs para a UI
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

        // 2. Walk
        logger.accept(">> Fase 1: Mapeando arquivos no disco...");
        try (var writer = new DbWriter(pool, scanId, cfg.dbBatchSize(), metrics, logger)) {
            walk(rootAbs, scanId, cfg, metrics, cancel, writer);
            writer.flush();
            writer.waitFinish();
        }

        if (cancel.get()) {
            logger.accept(">> Cancelado pelo usuário.");
            return;
        }

        // 3. Limpeza
        logger.accept(">> Fase 2: Sincronizando banco (Upsert + Cleaning)...");
        try (Connection c = pool.borrow()) {
            int deleted = Database.deleteStaleFiles(c, scanId);
            if (deleted > 0) logger.accept(">> Removidos " + deleted + " arquivos que não existem mais.");
            c.commit();
        }

        // 4. Hash
        if (cfg.computeHash()) {
            System.out.println("[SCAN] Calculando Hashes...");
            processHashes(rootAbs, pool, cfg, metrics, cancel, scanId, logger);
        }

        // --- NOVO: GRAVAR HISTÓRICO (TIME LAPSE) ---
        // Copia as mudanças desse scan para a tabela de histórico
        try (Connection c = pool.borrow()) {
            int hist = Database.snapshotToHistory(c, scanId);
            if (hist > 0) System.out.println("[DB] Histórico: " + hist + " alterações registradas.");
            
            Database.finishScanLog(c, scanId);
            c.commit();
        }
        
        System.out.println("[SCAN] Finalizado.");
        
        logger.accept(">> FINALIZADO! Total de arquivos no inventário: " + metrics.filesSeen.sum());
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

    private static void processHashes(Path root, Database.SimplePool pool, ScanConfig cfg, ScanMetrics metrics, AtomicBoolean cancel, long scanId, Consumer<String> logger) throws Exception {
        try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
            while (!cancel.get()) {
                List<HashCandidate> batch;
                try (Connection c = pool.borrow()) {
                    batch = Database.fetchDirtyFiles(c, scanId, 2000);
                    c.commit();
                }
                if (batch.isEmpty()) break;

                // logger.accept(">> Hash em lote de " + batch.size() + " arquivos...");

                var futures = new ArrayList<Future<HashUpdate>>();
                for (var item : batch) {
                    futures.add(exec.submit(() -> computeHash(root, cfg, item, metrics)));
                }

                var updates = new ArrayList<HashUpdate>();
                for (var f : futures) {
                    try {
                        var res = f.get();
                        if (res != null) updates.add(res);
                    } catch (Exception ignored) {}
                }

                try (Connection c = pool.borrow()) {
                    Database.updateHashes(c, updates);
                    c.commit();
                }
            }
        }
    }

    private static HashUpdate computeHash(Path root, ScanConfig cfg, HashCandidate item, ScanMetrics metrics) {
        try {
            var absPath = root.resolve(item.pathRel());
            if (cfg.largeFileHashPolicy() == LargeFileHashPolicy.SKIP && item.sizeBytes() > cfg.hashMaxBytes()) {
                return new HashUpdate(item.pathRel(), "SKIPPED_SIZE");
            }

            var md = SHA256.get(); md.reset();

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
                metrics.hashed.increment();
                return new HashUpdate(item.pathRel(), HEX.formatHex(md.digest()));
            }

            var buf = BUFFER.get();
            try (InputStream in = Files.newInputStream(absPath)) {
                int n; while ((n = in.read(buf)) != -1) md.update(buf, 0, n);
            }
            metrics.hashed.increment();
            return new HashUpdate(item.pathRel(), HEX.formatHex(md.digest()));

        } catch (Exception e) { return null; }
    }

    private static void readAt(FileChannel ch, long pos, byte[] buf, int len, MessageDigest md) throws IOException {
        var bb = ByteBuffer.wrap(buf, 0, len); int read = 0;
        while (bb.hasRemaining()) { int r = ch.read(bb, pos + read); if (r < 0) break; read += r; }
        if (read > 0) md.update(buf, 0, read);
    }
    private static byte[] longToBytes(long v) { return new byte[] { (byte)(v >>> 56), (byte)(v >>> 48), (byte)(v >>> 40), (byte)(v >>> 32), (byte)(v >>> 24), (byte)(v >>> 16), (byte)(v >>>  8), (byte)(v) }; }

    // --- WRITER ---
    private static class DbWriter implements AutoCloseable {
        private final Database.SimplePool pool;
        private final long scanId;
        private final int batchSize;
        private final ScanMetrics metrics;
        private final BlockingQueue<FileSeen> queue = new ArrayBlockingQueue<>(10000);
        private final Thread worker;
        @SuppressWarnings("unused")
        private final Consumer<String> logger;
        private volatile boolean finished = false;

        DbWriter(Database.SimplePool pool, long scanId, int batchSize, ScanMetrics metrics, Consumer<String> logger) {
            this.pool = pool; this.scanId = scanId; this.batchSize = batchSize; this.metrics = metrics; this.logger = logger;
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
                            // logger.accept(">> Banco: " + metrics.filesSeen.sum() + " arquivos processados...");
                            pending = 0;
                        }
                    }
                    if (pending > 0) { ps.executeBatch(); c.commit(); metrics.dbBatches.increment(); }
                }
            } catch (Exception e) { e.printStackTrace(); }
        }
    }
}
