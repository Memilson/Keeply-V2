package com.keeply.app;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.HexFormat;

/**
 * LeitorDeArquivos (Optimized for PostgreSQL/High-Concurrency)
 * Configurado para: scan_db / keeply / reWriteBatchedInserts=true
 */
public class leitorDeArquivos {

    static {
        // Carrega driver PG apenas
        try { Class.forName("org.postgresql.Driver"); }
        catch (ClassNotFoundException e) {
            System.err.println("PostgreSQL JDBC driver not found: " + e.getMessage());
        }
    }

    // ==================================================================================
    // 0) UTILS & LOGGING
    // ==================================================================================
    static final class TeeOutputStream extends OutputStream {
        private final OutputStream a, b;
        TeeOutputStream(OutputStream a, OutputStream b) { this.a = a; this.b = b; }
        @Override public void write(int x) throws IOException { a.write(x); b.write(x); }
        @Override public void write(byte[] buf, int off, int len) throws IOException { a.write(buf, off, len); b.write(buf, off, len); }
        @Override public void flush() throws IOException { a.flush(); b.flush(); }
        @Override public void close() throws IOException { b.close(); }
    }

    static void setupLogFile(Path logDir, String prefix) throws IOException {
        Files.createDirectories(logDir);
        String ts = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(LocalDateTime.now());
        Path logFile = logDir.resolve(prefix + "-" + ts + ".log");
        PrintStream fileOut = new PrintStream(Files.newOutputStream(logFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), true, StandardCharsets.UTF_8);
        System.setOut(new PrintStream(new TeeOutputStream(System.out, fileOut), true, StandardCharsets.UTF_8));
    }

    static void logJson(String ctx, String lvl, String msg, Map<String, Object> kv) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"ts\":\"").append(Instant.now()).append("\",\"lvl\":\"").append(lvl).append("\",\"ctx\":\"").append(ctx).append("\",\"msg\":\"").append(escapeJson(msg)).append("\"");
        if (kv != null) {
            for (var e : kv.entrySet()) {
                sb.append(",\"").append(escapeJson(e.getKey())).append("\":");
                Object v = e.getValue();
                if (v instanceof Number || v instanceof Boolean) sb.append(v);
                else sb.append("\"").append(escapeJson(String.valueOf(v))).append("\"");
            }
        }
        sb.append("}");
        System.out.println(sb);
    }

    static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    // ==================================================================================
    // 1) SIMPLE CONNECTION POOL (Critical for Postgres Performance)
    // ==================================================================================
    static class SimplePool implements AutoCloseable {
        private final String url, user, pass;
        private final BlockingQueue<Connection> pool;
        private final List<Connection> allConnections = new ArrayList<>();

        SimplePool(String url, String user, String pass, int size) throws SQLException {
            this.url = url; this.user = user; this.pass = pass;
            this.pool = new ArrayBlockingQueue<>(size);
            for (int i = 0; i < size; i++) {
                Connection c = createNew();
                pool.offer(c);
                allConnections.add(c);
            }
        }

        private Connection createNew() throws SQLException {
            Connection c = DriverManager.getConnection(url, user, pass);
            c.setAutoCommit(false);
            try (Statement st = c.createStatement()) {
                // OTIMIZACAO CRITICA PARA POSTGRES:
                st.execute("SET synchronous_commit = OFF");
                st.execute("SET client_encoding = 'UTF8'");
                // O Schema já está na URL, mas garantimos aqui também
                st.execute("SET search_path TO keeply");
            }
            return c;
        }

        public Connection borrow() throws InterruptedException {
            return pool.take();
        }

        public void release(Connection c) {
            if (c != null) pool.offer(c);
        }

        @Override
        public void close() {
            for (Connection c : allConnections) {
                try { c.close(); } catch (SQLException ignored) {}
            }
        }
    }

    // ==================================================================================
    // 2) DOMAIN & CONFIG
    // ==================================================================================
    public enum Stage { WALK, HASH, DB, IGNORE }
    public enum FileStatus { NEW, MODIFIED, MOVED, UNCHANGED, HASH_FAILED, SKIPPED_SIZE, SKIPPED_DISABLED }

    public record ScanIssue(long scanId, Stage stage, String path, String identityType, String identityValue, String errorType, String message, String rule) {}

    public record FileMeta(String rootPath, String fullPath, String name, long sizeBytes, String createdAt, String modifiedAt, String fileKey, String identityType, String identityValue) {}

    public record PrevInfo(long pathId, String knownPath, long sizeBytes, String modifiedAt, String contentHash, String contentAlgo) {}

    public record FileResult(FileMeta meta, FileStatus status, String contentAlgo, String contentHash, String reason) {}

    static final class ExcludeRule {
        final String glob;
        final PathMatcher matcher;
        final LongAdder count = new LongAdder();
        ExcludeRule(String glob) {
            this.glob = glob;
            this.matcher = FileSystems.getDefault().getPathMatcher("glob:" + glob);
        }
    }

    public record ScanConfig(
            int workers, int batchLimit, boolean computeHash, long hashMaxBytes, boolean followLinks,
            List<String> excludes, long preloadIndexMaxRows, int dbPoolSize
    ) {
        public static Builder builder() { return new Builder(); }
        public static class Builder {
            private int workers = Math.max(4, Runtime.getRuntime().availableProcessors());
            private int batchLimit = 4000; // Maior batch para PG
            private boolean computeHash = true;
            private long hashMaxBytes = 200L * 1024 * 1024;
            private boolean followLinks = false;
            private final List<String> excludes = new ArrayList<>();
            private long preloadIndexMaxRows = 5_000_000;
            private int dbPoolSize = 10; // Conexoes simultaneas

            public Builder workers(int v) { this.workers = v; return this; }
            public Builder batchLimit(int v) { this.batchLimit = v; return this; }
            public Builder computeHash(boolean v) { this.computeHash = v; return this; }
            public Builder addExclude(String glob) { this.excludes.add(glob); return this; }
            public Builder dbPoolSize(int v) { this.dbPoolSize = v; return this; }

            public ScanConfig build() { return new ScanConfig(workers, batchLimit, computeHash, hashMaxBytes, followLinks, List.copyOf(excludes), preloadIndexMaxRows, dbPoolSize); }
        }
    }

    // ==================================================================================
    // 3) METRICS
    // ==================================================================================
    static class ScanMetrics {
        final LongAdder filesScanned = new LongAdder();
        final LongAdder filesIgnored = new LongAdder();
        final LongAdder filesHashed = new LongAdder();
        final LongAdder bytesScanned = new LongAdder();
        final LongAdder bytesHashed = new LongAdder();
        final LongAdder errorsWalk = new LongAdder();
        final LongAdder errorsHash = new LongAdder();
        final LongAdder dbBatches = new LongAdder();
        final Instant start = Instant.now();
        private final AtomicBoolean running = new AtomicBoolean(true);

        void startReporter(long estimate) {
            Thread t = new Thread(() -> {
                while (running.get()) {
                    printStats(estimate);
                    try { Thread.sleep(2000); } catch (InterruptedException e) { break; }
                }
                printStats(estimate);
                System.out.println();
            });
            t.setDaemon(true);
            t.start();
        }

        void stop() { running.set(false); }

        private void printStats(long estimate) {
            long count = filesScanned.sum();
            double sec = Math.max(1.0, Duration.between(start, Instant.now()).toMillis() / 1000.0);
            double rate = count / sec;
            double mb = (bytesScanned.sum() / 1024.0 / 1024.0) / sec;
            System.err.print(String.format("\r[SCAN] Files: %,d | MB/s: %.1f | Rate: %.0f/s | Hashed: %,d | Batches: %d    ", count, mb, rate, filesHashed.sum(), dbBatches.sum()));
        }
    }

    // ==================================================================================
    // 4) DB WRITER (PARALLEL & BATCHED)
    // ==================================================================================
    static final class ParallelDbWriter {
        private final SimplePool pool;
        private final long scanId;
        private final ScanConfig cfg;
        private final ScanMetrics metrics;
        
        // Cache concorrente para evitar SELECTs repetidos no Path ID
        private final ConcurrentHashMap<String, Long> pathCache = new ConcurrentHashMap<>(50_000);

        // Buffers protegidos por Lock para threads inserirem
        private final ReentrantLock lock = new ReentrantLock();
        private final List<FileResult> fileBuffer;
        private final List<ScanIssue> issueBuffer;

        ParallelDbWriter(SimplePool pool, long scanId, ScanConfig cfg, ScanMetrics metrics) {
            this.pool = pool;
            this.scanId = scanId;
            this.cfg = cfg;
            this.metrics = metrics;
            this.fileBuffer = new ArrayList<>(cfg.batchLimit * 2);
            this.issueBuffer = new ArrayList<>(cfg.batchLimit * 2);
        }

        // Chamado pelos Workers
        void queueFile(FileResult r) {
            lock.lock();
            try {
                fileBuffer.add(r);
                if (fileBuffer.size() >= cfg.batchLimit) {
                    List<FileResult> batch = new ArrayList<>(fileBuffer);
                    fileBuffer.clear();
                    flushFilesAsync(batch);
                }
            } finally {
                lock.unlock();
            }
        }

        void queueIssue(ScanIssue i) {
            lock.lock();
            try {
                issueBuffer.add(i);
                if (issueBuffer.size() >= cfg.batchLimit) {
                    List<ScanIssue> batch = new ArrayList<>(issueBuffer);
                    issueBuffer.clear();
                    flushIssuesAsync(batch);
                }
            } finally {
                lock.unlock();
            }
        }

        void flushAll() {
            lock.lock();
            try {
                if (!fileBuffer.isEmpty()) {
                    flushFilesAsync(new ArrayList<>(fileBuffer));
                    fileBuffer.clear();
                }
                if (!issueBuffer.isEmpty()) {
                    flushIssuesAsync(new ArrayList<>(issueBuffer));
                    issueBuffer.clear();
                }
            } finally {
                lock.unlock();
            }
        }

        // Dispara flush em Thread Virtual para nao bloquear o worker
        private void flushFilesAsync(List<FileResult> batch) {
            Thread.ofVirtual().start(() -> {
                Connection conn = null;
                try {
                    conn = pool.borrow();
                    executeFileBatch(conn, batch);
                    conn.commit();
                    metrics.dbBatches.increment();
                } catch (Exception e) {
                    e.printStackTrace();
                    if (conn != null) try { conn.rollback(); } catch (SQLException ignored) {}
                    // Retry logic simple here or log error
                } finally {
                    pool.release(conn);
                }
            });
        }

        private void flushIssuesAsync(List<ScanIssue> batch) {
            Thread.ofVirtual().start(() -> {
                Connection conn = null;
                try {
                    conn = pool.borrow();
                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO scan_issue(scan_id, stage, path, identity_type, identity_value, error_type, message, rule, created_at) VALUES(?,?,?,?,?,?,?,?,?)")) {
                        String now = Instant.now().toString();
                        for (ScanIssue i : batch) {
                            ps.setLong(1, scanId); ps.setString(2, i.stage().name()); ps.setString(3, i.path());
                            ps.setString(4, i.identityType()); ps.setString(5, i.identityValue());
                            ps.setString(6, i.errorType()); ps.setString(7, i.message()); ps.setString(8, i.rule());
                            ps.setString(9, now);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        conn.commit();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    pool.release(conn);
                }
            });
        }

        private void executeFileBatch(Connection conn, List<FileResult> items) throws SQLException {
            // Prepared Statements (Local var to avoid contention)
            try (PreparedStatement psPath = conn.prepareStatement("INSERT INTO path(full_path) VALUES(?) ON CONFLICT(full_path) DO NOTHING");
                 PreparedStatement psPathSel = conn.prepareStatement("SELECT id FROM path WHERE full_path=?");
                 PreparedStatement psCont = conn.prepareStatement("INSERT INTO content(algo, hash_hex, size_bytes) VALUES(?,?,?) ON CONFLICT(algo, hash_hex) DO NOTHING");
                 PreparedStatement psState = conn.prepareStatement("""
                    INSERT INTO file_state(root_path, identity_type, identity_value, path_id, name, size_bytes, 
                    created_at, modified_at, file_key, content_algo, content_hash, last_scan_id) 
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(root_path, identity_type, identity_value) DO UPDATE SET 
                    path_id=excluded.path_id, name=excluded.name, size_bytes=excluded.size_bytes, modified_at=excluded.modified_at,
                    content_hash=excluded.content_hash, last_scan_id=excluded.last_scan_id
                 """);
                 PreparedStatement psTouch = conn.prepareStatement("UPDATE file_state SET last_scan_id=? WHERE root_path=? AND identity_type=? AND identity_value=?");
                 PreparedStatement psLog = conn.prepareStatement("INSERT INTO file_change(root_path, identity_type, identity_value, scan_id, size_bytes, modified_at, content_hash, reason) VALUES(?,?,?,?,?,?,?,?)")
            ) {
                
                for (FileResult res : items) {
                    if (res.status() == FileStatus.UNCHANGED) {
                        psTouch.setLong(1, scanId);
                        psTouch.setString(2, res.meta().rootPath);
                        psTouch.setString(3, res.meta().identityType);
                        psTouch.setString(4, res.meta().identityValue);
                        psTouch.addBatch();
                        continue;
                    }

                    // Resolver Path ID (com Cache)
                    long pathId = resolvePathId(conn, psPath, psPathSel, res.meta().fullPath);

                    // Content
                    if (res.contentHash != null) {
                        psCont.setString(1, res.contentAlgo);
                        psCont.setString(2, res.contentHash);
                        psCont.setLong(3, res.meta().sizeBytes);
                        psCont.addBatch();
                    }

                    // File State
                    psState.setString(1, res.meta().rootPath);
                    psState.setString(2, res.meta().identityType);
                    psState.setString(3, res.meta().identityValue);
                    psState.setLong(4, pathId);
                    psState.setString(5, res.meta().name);
                    psState.setLong(6, res.meta().sizeBytes);
                    psState.setString(7, res.meta().createdAt);
                    psState.setString(8, res.meta().modifiedAt);
                    psState.setString(9, res.meta().fileKey);
                    psState.setString(10, res.contentAlgo);
                    psState.setString(11, res.contentHash);
                    psState.setLong(12, scanId);
                    psState.addBatch();

                    // Change Log
                    psLog.setString(1, res.meta().rootPath);
                    psLog.setString(2, res.meta().identityType);
                    psLog.setString(3, res.meta().identityValue);
                    psLog.setLong(4, scanId);
                    psLog.setLong(5, res.meta().sizeBytes);
                    psLog.setString(6, res.meta().modifiedAt);
                    psLog.setString(7, res.contentHash);
                    psLog.setString(8, res.status.name());
                    psLog.addBatch();
                }

                psPath.executeBatch(); 
                // psPathSel executa sob demanda dentro do resolvePathId
                psCont.executeBatch();
                psState.executeBatch();
                psTouch.executeBatch();
                psLog.executeBatch();
            }
        }

        private long resolvePathId(Connection conn, PreparedStatement psIns, PreparedStatement psSel, String path) throws SQLException {
            Long cached = pathCache.get(path);
            if (cached != null) return cached;

            // Tenta inserir
            psIns.setString(1, path);
            psIns.addBatch(); 
            psIns.executeBatch(); // Precisa executar na hora pra garantir que existe pro select

            psSel.setString(1, path);
            try (ResultSet rs = psSel.executeQuery()) {
                if (rs.next()) {
                    long id = rs.getLong(1);
                    pathCache.put(path, id);
                    return id;
                }
            }
            throw new SQLException("Path ID resolution failed for " + path);
        }
    }

    // ==================================================================================
    // 5) PROCESSOR LOGIC
    // ==================================================================================
    static FileResult processFile(FileMeta curr, Map<String, PrevInfo> index, ScanConfig cfg, ScanMetrics metrics) {
        String key = curr.identityType + ":" + curr.identityValue;
        PrevInfo prev = index.get(key);

        if (prev == null) {
            var hash = computeHashIfNeeded(curr, cfg, metrics);
            return new FileResult(curr, FileStatus.NEW, hash.algo, hash.hex, "NEW");
        }

        boolean metaDiff = prev.sizeBytes != curr.sizeBytes || !Objects.equals(prev.modifiedAt, curr.modifiedAt);
        if (metaDiff) {
            var hash = computeHashIfNeeded(curr, cfg, metrics);
            return new FileResult(curr, FileStatus.MODIFIED, hash.algo, hash.hex, "MODIFIED");
        }

        // UNCHANGED
        return new FileResult(curr, FileStatus.UNCHANGED, prev.contentAlgo, prev.contentHash, null);
    }

    record HashRes(String hex, String algo) {}
    static HashRes computeHashIfNeeded(FileMeta m, ScanConfig cfg, ScanMetrics metrics) {
        if (!cfg.computeHash || (cfg.hashMaxBytes > 0 && m.sizeBytes > cfg.hashMaxBytes)) return new HashRes(null, null);
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (InputStream in = new BufferedInputStream(Files.newInputStream(Path.of(m.fullPath)))) {
                byte[] b = new byte[64*1024]; int r;
                while ((r = in.read(b)) != -1) md.update(b, 0, r);
            }
            metrics.filesHashed.increment();
            metrics.bytesHashed.add(m.sizeBytes);
            return new HashRes(HexFormat.of().formatHex(md.digest()), "SHA-256");
        } catch (Exception e) {
            metrics.errorsHash.increment();
            return new HashRes(null, null);
        }
    }

    // ==================================================================================
    // 6) MAIN EXECUTION
    // ==================================================================================
    public static void runScan(String rootPath, ScanConfig cfg, String dbUrl, String dbUser, String dbPass) throws Exception {
        ScanMetrics metrics = new ScanMetrics();
        
        System.out.println("Iniciando Pool de Conexoes (" + cfg.dbPoolSize + " connections)...");
        try (SimplePool pool = new SimplePool(dbUrl, dbUser, dbPass, cfg.dbPoolSize)) {
            
            // 1. Setup e Load Index
            long scanId;
            Map<String, PrevInfo> index = new HashMap<>();
            
            try (Connection c = pool.borrow()) {
                initSchema(c);
                scanId = startScanLog(c, rootPath);
                
                System.out.println("Carregando snapshot do banco...");
                loadIndex(c, rootPath, cfg.preloadIndexMaxRows, index);
                System.out.println("Snapshot carregado: " + index.size() + " arquivos.");
                c.commit();
            }

            metrics.startReporter(index.size());
            ParallelDbWriter writer = new ParallelDbWriter(pool, scanId, cfg, metrics);
            List<ExcludeRule> rules = cfg.excludes.stream().map(ExcludeRule::new).toList();
            Path root = Paths.get(rootPath);

            // 2. Walker Thread
            BlockingQueue<FileMeta> queue = new ArrayBlockingQueue<>(10_000);
            Thread walker = Thread.ofVirtual().start(() -> {
                try {
                    Files.walkFileTree(root, EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE, new SimpleFileVisitor<>() {
                        @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                            if (!attrs.isRegularFile()) return FileVisitResult.CONTINUE;
                            
                            // Ignorar (Fast Path)
                            String rel = root.relativize(file).toString().replace('\\','/');
                            for(var r : rules) if(r.matcher.matches(Path.of(rel))) {
                                metrics.filesIgnored.increment();
                                return FileVisitResult.CONTINUE;
                            }

                            metrics.filesScanned.increment();
                            metrics.bytesScanned.add(attrs.size());
                            
                            FileMeta fm = new FileMeta(
                                rootPath, file.toAbsolutePath().toString(), file.getFileName().toString(),
                                attrs.size(), safeTime(attrs.creationTime()), safeTime(attrs.lastModifiedTime()),
                                null, "PATH", file.toAbsolutePath().toString()
                            );
                            
                            try { queue.put(fm); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                            return FileVisitResult.CONTINUE;
                        }
                        @Override public FileVisitResult visitFileFailed(Path file, IOException exc) {
                            metrics.errorsWalk.increment();
                            writer.queueIssue(new ScanIssue(scanId, Stage.WALK, file.toString(), "PATH", file.toString(), exc.getClass().getSimpleName(), exc.getMessage(), null));
                            return FileVisitResult.CONTINUE;
                        }
                    });
                } catch (IOException e) { e.printStackTrace(); }
                finally {
                     // Poison Pill
                    try { for(int i=0; i<cfg.workers; i++) queue.put(new FileMeta(null,null,null,0,null,null,null,"POISON",null)); } catch(Exception e){}
                }
            });

            // 3. Workers (Process & Hash)
            ExecutorService executor = Executors.newFixedThreadPool(cfg.workers);
            for (int i = 0; i < cfg.workers; i++) {
                executor.submit(() -> {
                    while (true) {
                        try {
                            FileMeta fm = queue.take();
                            if ("POISON".equals(fm.identityType)) break;
                            
                            FileResult res = processFile(fm, index, cfg, metrics);
                            writer.queueFile(res);
                            
                        } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
                    }
                });
            }

            walker.join();
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.DAYS);
            
            System.out.println("Processamento finalizado. Aguardando escritas pendentes...");
            writer.flushAll();

            // 4. Cleanup (Deleted files)
            try (Connection c = pool.borrow()) {
                handleDeletions(c, scanId, rootPath);
                finishScanLog(c, scanId);
                c.commit();
            }
            
            metrics.stop();
            System.out.println("Scan Completo! Tempo: " + Duration.between(metrics.start, Instant.now()).toSeconds() + "s");
        }
    }
    
    // ==================================================================================
    // 7) JDBC HELPERS
    // ==================================================================================
    static String safeTime(FileTime t) { return t == null ? null : t.toInstant().toString(); }
    
    static void initSchema(Connection c) throws SQLException {
        try(Statement s = c.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS keeply");
            s.execute("SET search_path TO keeply");
            s.execute("CREATE TABLE IF NOT EXISTS scan (id BIGSERIAL PRIMARY KEY, root_path TEXT, started_at TEXT, finished_at TEXT, status TEXT)");
            s.execute("CREATE TABLE IF NOT EXISTS path (id BIGSERIAL PRIMARY KEY, full_path TEXT UNIQUE)");
            s.execute("CREATE TABLE IF NOT EXISTS content (algo TEXT, hash_hex TEXT, size_bytes BIGINT, PRIMARY KEY(algo, hash_hex))");
            s.execute("CREATE TABLE IF NOT EXISTS file_state (root_path TEXT, identity_type TEXT, identity_value TEXT, path_id BIGINT, name TEXT, size_bytes BIGINT, created_at TEXT, modified_at TEXT, file_key TEXT, content_algo TEXT, content_hash TEXT, last_scan_id BIGINT, PRIMARY KEY (root_path, identity_type, identity_value))");
            s.execute("CREATE TABLE IF NOT EXISTS file_change (id BIGSERIAL PRIMARY KEY, root_path TEXT, identity_type TEXT, identity_value TEXT, scan_id BIGINT, size_bytes BIGINT, modified_at TEXT, content_hash TEXT, reason TEXT)");
            s.execute("CREATE TABLE IF NOT EXISTS scan_issue (id BIGSERIAL PRIMARY KEY, scan_id BIGINT, stage TEXT, path TEXT, identity_type TEXT, identity_value TEXT, error_type TEXT, message TEXT, rule TEXT, created_at TEXT)");
            
            // Indexess
            s.execute("CREATE INDEX IF NOT EXISTS idx_file_state_root_scan ON file_state(root_path, last_scan_id)");
        }
    }

    static long startScanLog(Connection c, String root) throws SQLException {
        try(PreparedStatement ps = c.prepareStatement("INSERT INTO scan(root_path, started_at, status) VALUES(?,?, 'RUNNING')", Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, root); ps.setString(2, Instant.now().toString());
            ps.executeUpdate();
            var rs = ps.getGeneratedKeys(); rs.next(); return rs.getLong(1);
        }
    }
    
    static void finishScanLog(Connection c, long id) throws SQLException {
        c.createStatement().execute("UPDATE scan SET finished_at='" + Instant.now() + "', status='SUCCESS' WHERE id=" + id);
    }
    
    static void loadIndex(Connection c, String root, long limit, Map<String, PrevInfo> map) throws SQLException {
        // Pre-load logic (simplificada)
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT fs.identity_type, fs.identity_value, fs.path_id, fs.size_bytes, fs.modified_at, fs.content_hash, fs.content_algo FROM file_state fs WHERE fs.root_path=? LIMIT ?")) {
            ps.setString(1, root); ps.setLong(2, limit);
            var rs = ps.executeQuery();
            while(rs.next()) {
                map.put(rs.getString(1)+":"+rs.getString(2), 
                    new PrevInfo(rs.getLong(3), null, rs.getLong(4), rs.getString(5), rs.getString(6), rs.getString(7)));
            }
        }
    }
    
    static void handleDeletions(Connection c, long scanId, String root) throws SQLException {
        c.createStatement().execute("INSERT INTO file_change(root_path, identity_type, identity_value, scan_id, reason) SELECT root_path, identity_type, identity_value, "+scanId+", 'DELETED' FROM file_state WHERE root_path='"+root+"' AND last_scan_id < " + scanId);
        c.createStatement().execute("DELETE FROM file_state WHERE root_path='"+root+"' AND last_scan_id < " + scanId);
    }

    public static void runCli(String[] args) throws Exception {
        main(args);
    }

    public static void main(String[] args) throws Exception {
        String root = args.length > 0 ? args[0] : "C:/Dados";
        
        // --- CREDENCIAIS HARDCODED DO USUARIO ---
        String url = "jdbc:postgresql://localhost:5432/scan_db?currentSchema=keeply&reWriteBatchedInserts=true"; 
        String user = "postgres";
        String pass = "admin";
        
        System.out.println("=== KEEPLY SCANNER ===");
        System.out.println("Alvo: " + root);
        System.out.println("DB: " + url);

        ScanConfig cfg = ScanConfig.builder()
                .workers(8)
                .dbPoolSize(12) // Aumentei um pouco para aproveitar o batch
                .addExclude("**/node_modules/**")
                .addExclude("**/.git/**")
                .addExclude("**/$Recycle.Bin/**")
                .build();
                
        setupLogFile(Paths.get("logs"), "scan-pg");
        runScan(root, cfg, url, user, pass);
    }
}