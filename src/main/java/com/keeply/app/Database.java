package com.keeply.app;

import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.nio.file.attribute.FileTime;

public class Database {

    // --- Utilitários SQL Estáticos ---
    public static void initSchema(Connection c) throws SQLException {
        try(Statement s = c.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS keeply");
            s.execute("SET search_path TO keeply");
            s.execute("CREATE TABLE IF NOT EXISTS scan (id BIGSERIAL PRIMARY KEY, root_path TEXT, started_at TEXT, finished_at TEXT, status TEXT)");
            s.execute("CREATE TABLE IF NOT EXISTS path (id BIGSERIAL PRIMARY KEY, full_path TEXT UNIQUE)");
            s.execute("CREATE TABLE IF NOT EXISTS content (algo TEXT, hash_hex TEXT, size_bytes BIGINT, PRIMARY KEY(algo, hash_hex))");
            s.execute("CREATE TABLE IF NOT EXISTS file_state (root_path TEXT, identity_type TEXT, identity_value TEXT, path_id BIGINT, name TEXT, size_bytes BIGINT, created_at TEXT, modified_at TEXT, file_key TEXT, content_algo TEXT, content_hash TEXT, last_scan_id BIGINT, PRIMARY KEY (root_path, identity_type, identity_value))");
            s.execute("CREATE INDEX IF NOT EXISTS idx_file_state_path_id ON file_state(path_id)");
            s.execute("CREATE TABLE IF NOT EXISTS file_change (id BIGSERIAL PRIMARY KEY, root_path TEXT, identity_type TEXT, identity_value TEXT, scan_id BIGINT, size_bytes BIGINT, modified_at TEXT, content_hash TEXT, reason TEXT)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_file_change_scan_id ON file_change(scan_id)");
            s.execute("CREATE TABLE IF NOT EXISTS scan_issue (id BIGSERIAL PRIMARY KEY, scan_id BIGINT, stage TEXT, path TEXT, identity_type TEXT, identity_value TEXT, error_type TEXT, message TEXT, rule TEXT, created_at TEXT)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_file_state_root_scan ON file_state(root_path, last_scan_id)");
        }
    }

    public static long startScanLog(Connection c, String root) throws SQLException {
        try(PreparedStatement ps = c.prepareStatement("INSERT INTO scan(root_path, started_at, status) VALUES(?,?, 'RUNNING')", Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, root); ps.setString(2, Instant.now().toString());
            ps.executeUpdate();
            var rs = ps.getGeneratedKeys(); rs.next(); return rs.getLong(1);
        }
    }

    public static void finishScanLog(Connection c, long id) throws SQLException {
        try (Statement st = c.createStatement()) {
             st.execute("UPDATE scan SET finished_at='" + Instant.now() + "', status='SUCCESS' WHERE id=" + id);
        }
    }

    public static void loadIndex(Connection c, String root, long limit, Map<String, Scanner.PrevInfo> map) throws SQLException {
        String sql = """
            SELECT 
                path_id,
                identity_value AS full_path,
                size_bytes,
                modified_at,
                content_hash,
                content_algo
            FROM file_state
            WHERE identity_value ILIKE ? 
            LIMIT ?
            """;

        try (PreparedStatement ps = c.prepareStatement(sql)) {
            // NORMALIZAÇÃO NA BUSCA:
            // 1. Converte qualquer barra invertida (\) para normal (/)
            // 2. Adiciona o % no final
            String normalizedRoot = root.replace('\\', '/');
            String searchPattern = normalizedRoot + "%";
            
            //System.out.println("[DEBUG DB] Buscando (Normalizado): " + searchPattern);

            ps.setString(1, searchPattern); 
            ps.setLong(2, limit);
            ps.setFetchSize(10000); 

            long count = 0;
            long start = System.currentTimeMillis();

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    count++;
                    long pathId = rs.getLong("path_id");
                    String fullPath = rs.getString("full_path");
                    long sizeBytes = rs.getLong("size_bytes");
                    String modifiedAtStr = rs.getString("modified_at");
                    String contentHash = rs.getString("content_hash");
                    String contentAlgo = rs.getString("content_algo");

                    long modifiedAtMillis = -1L;
                    if (modifiedAtStr != null) {
                         try {
                            modifiedAtMillis = Instant.parse(modifiedAtStr).toEpochMilli();
                        } catch (Exception ignored) {}
                    }

                    map.put(fullPath, new Scanner.PrevInfo(
                            pathId, fullPath, sizeBytes, modifiedAtMillis,
                            modifiedAtStr, contentHash, contentAlgo
                    ));
                }
            }
            //System.out.println("[DEBUG DB] Encontrados " + count + " arquivos no banco em " + (System.currentTimeMillis() - start) + "ms");
        }
    }

    public static void handleDeletions(Connection c, long scanId, String root) throws SQLException {
        String insertSql = "INSERT INTO file_change(root_path, identity_type, identity_value, scan_id, reason) " +
                           "SELECT root_path, identity_type, identity_value, ?, 'DELETED' " +
                           "FROM file_state WHERE root_path=? AND last_scan_id < ?";
        try (PreparedStatement ps = c.prepareStatement(insertSql)) {
            ps.setLong(1, scanId); ps.setString(2, root); ps.setLong(3, scanId);
            ps.execute();
        }

        String deleteSql = "DELETE FROM file_state WHERE root_path=? AND last_scan_id < ?";
        try (PreparedStatement ps = c.prepareStatement(deleteSql)) {
             ps.setString(1, root); ps.setLong(2, scanId);
             ps.execute();
        }
    }

    public static String safeTime(FileTime t) { return t == null ? null : t.toInstant().toString(); }

    // --- SimplePool com Graceful Shutdown ---
    public static class SimplePool implements AutoCloseable {
        private final String url, user, pass;
        private final BlockingQueue<Connection> pool;
        private final List<Connection> allConnections = new ArrayList<>();
        private final AtomicInteger activeBorrows = new AtomicInteger(0);
        private volatile boolean shuttingDown = false;
        private final int maxSize; // Armazena o tamanho máximo para uso externo se necessário

        public SimplePool(String url, String user, String pass, int size) {
            this.maxSize = size;
            try { Class.forName("org.postgresql.Driver"); } catch (ClassNotFoundException e) { System.err.println("PG Driver missing"); }
            this.url = url; this.user = user; this.pass = pass;
            this.pool = new ArrayBlockingQueue<>(size);
            for (int i = 0; i < size; i++) {
                try {
                    Connection c = createNew();
                    pool.offer(c);
                    allConnections.add(c);
                } catch (SQLException e) {
                    System.err.println("Failed to create connection: " + e.getMessage());
                }
            }
        }
        
        public int getMaxSize() { return maxSize; }

        private Connection createNew() throws SQLException {
            Connection c = DriverManager.getConnection(url, user, pass);
            c.setAutoCommit(false);
            try (Statement st = c.createStatement()) {
                st.execute("SET synchronous_commit = OFF");
                st.execute("SET client_encoding = 'UTF8'");
                st.execute("SET search_path TO keeply");
            }
            return c;
        }

        public Connection borrow() throws InterruptedException, SQLException {
            if (shuttingDown) throw new SQLException("Pool is shutting down");
            Connection c = pool.poll(10, TimeUnit.SECONDS); // Timeout aumentado para 10s
            if (c == null) throw new SQLException("Timeout waiting for connection");
            activeBorrows.incrementAndGet();
            return c;
        }

        public void release(Connection c) {
            if (c != null) {
                pool.offer(c);
                activeBorrows.decrementAndGet();
            }
        }

        public void close() {
            shuttingDown = true;
            long start = System.currentTimeMillis();
            while (activeBorrows.get() > 0) {
                if (System.currentTimeMillis() - start > 10_000) break; // Espera até 10s
                try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            }
            for (Connection c : allConnections) {
                try { c.close(); } catch (SQLException ignored) {}
            }
        }
    }

    // --- ParallelDbWriter Otimizado ---
    public static final class ParallelDbWriter {
        private final SimplePool pool;
        private final long scanId;
        private final Scanner.ScanConfig cfg;
        private final Scanner.ScanMetrics metrics;
        
        // Cache LRU para evitar estouro de memória
        private final Map<String, Long> pathCache = Collections.synchronizedMap(new LinkedHashMap<String, Long>(50_000, 0.75f, true) {
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) { return size() > 500_000; }
        });
        
        private final ReentrantLock lock = new ReentrantLock();
        private final List<Scanner.FileResult> fileBuffer;
        private final List<Scanner.ScanIssue> issueBuffer;
        private final Phaser activeWrites = new Phaser(1);
        
        // CORREÇÃO DE TIMEOUT: Semáforo para limitar escritas simultâneas
        private final Semaphore dbPermits;

        public ParallelDbWriter(SimplePool pool, long scanId, Scanner.ScanConfig cfg, Scanner.ScanMetrics metrics) {
            this.pool = pool; this.scanId = scanId; this.cfg = cfg; this.metrics = metrics;
            this.fileBuffer = new ArrayList<>(cfg.batchLimit() * 2);
            this.issueBuffer = new ArrayList<>(cfg.batchLimit() * 2);
            
            // Permite usar todas as conexões menos 1 (reserva)
            int permits = Math.max(1, pool.getMaxSize() - 1);
            this.dbPermits = new Semaphore(permits, true); 
        }
        
        public void clearCache() { pathCache.clear(); }
        public void waitForCompletion() { activeWrites.arriveAndAwaitAdvance(); }

        public void queueFile(Scanner.FileResult r) {
            lock.lock();
            try {
                fileBuffer.add(r);
                if (fileBuffer.size() >= cfg.batchLimit()) {
                    List<Scanner.FileResult> batch = new ArrayList<>(fileBuffer); fileBuffer.clear();
                    flushFilesAsync(batch);
                }
            } finally { lock.unlock(); }
        }

        public void queueIssue(Scanner.ScanIssue i) {
            lock.lock();
            try {
                issueBuffer.add(i);
                if (issueBuffer.size() >= cfg.batchLimit()) {
                    List<Scanner.ScanIssue> batch = new ArrayList<>(issueBuffer); issueBuffer.clear();
                    flushIssuesAsync(batch);
                }
            } finally { lock.unlock(); }
        }

        public void flushAll() {
            lock.lock();
            try {
                if (!fileBuffer.isEmpty()) { flushFilesAsync(new ArrayList<>(fileBuffer)); fileBuffer.clear(); }
                if (!issueBuffer.isEmpty()) { flushIssuesAsync(new ArrayList<>(issueBuffer)); issueBuffer.clear(); }
            } finally { lock.unlock(); }
        }

        private void flushFilesAsync(List<Scanner.FileResult> batch) {
            // BACKPRESSURE: Se o banco estiver cheio, ESPERA AQUI em vez de criar thread e dar timeout
            try {
                dbPermits.acquire(); 
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            activeWrites.register();
            Thread.ofVirtual().start(() -> {
                Connection conn = null;
                try {
                    conn = pool.borrow();
                    executeFileBatch(conn, batch);
                    conn.commit();
                    metrics.dbBatches.increment();
                } catch (Exception e) {
                    e.printStackTrace(); // Loga o erro, mas não para o scan inteiro
                    if (conn != null) try { conn.rollback(); } catch (SQLException ignored) {}
                } finally { 
                    pool.release(conn);
                    dbPermits.release(); // Libera o semáforo para a próxima thread
                    activeWrites.arriveAndDeregister();
                }
            });
        }

        private void flushIssuesAsync(List<Scanner.ScanIssue> batch) {
            try { dbPermits.acquire(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }

            activeWrites.register();
            Thread.ofVirtual().start(() -> {
                Connection conn = null;
                try {
                    conn = pool.borrow();
                    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO scan_issue(scan_id, stage, path, identity_type, identity_value, error_type, message, rule, created_at) VALUES(?,?,?,?,?,?,?,?,?)")) {
                        String now = Instant.now().toString();
                        for (Scanner.ScanIssue i : batch) {
                            ps.setLong(1, scanId); ps.setString(2, i.stage().name()); ps.setString(3, i.path());
                            ps.setString(4, i.identityType()); ps.setString(5, i.identityValue());
                            ps.setString(6, i.errorType()); ps.setString(7, i.message()); ps.setString(8, i.rule()); ps.setString(9, now);
                            ps.addBatch();
                        }
                        ps.executeBatch(); conn.commit();
                    }
                } catch (Exception e) { e.printStackTrace(); } finally { 
                    pool.release(conn);
                    dbPermits.release();
                    activeWrites.arriveAndDeregister();
                }
            });
        }

        private void executeFileBatch(Connection conn, List<Scanner.FileResult> items) throws SQLException {
             // 1. CORREÇÃO DE DEADLOCK: Dedup e Ordenação de Hashes
             Set<Scanner.FileResult> uniqueContent = new TreeSet<>(Comparator.comparing(Scanner.FileResult::contentHash, Comparator.nullsLast(Comparator.naturalOrder())));
             for (Scanner.FileResult res : items) {
                 if (res.contentHash() != null) uniqueContent.add(res);
             }

             if (!uniqueContent.isEmpty()) {
                 try (PreparedStatement psCont = conn.prepareStatement("INSERT INTO content(algo, hash_hex, size_bytes) VALUES(?,?,?) ON CONFLICT(algo, hash_hex) DO NOTHING")) {
                     for (Scanner.FileResult res : uniqueContent) {
                         psCont.setString(1, res.contentAlgo()); psCont.setString(2, res.contentHash()); psCont.setLong(3, res.meta().sizeBytes());
                         psCont.addBatch();
                     }
                     psCont.executeBatch();
                 }
             }

             // 2. Ordenação por Arquivos (evita deadlock na tabela file_state)
             Collections.sort(items, Comparator.comparing(f -> f.meta().rootPath() + f.meta().identityType() + f.meta().identityValue()));
             
             try (PreparedStatement psPath = conn.prepareStatement("INSERT INTO path(full_path) VALUES(?) ON CONFLICT(full_path) DO NOTHING");
                  PreparedStatement psPathSel = conn.prepareStatement("SELECT id FROM path WHERE full_path=?");
                  PreparedStatement psState = conn.prepareStatement("INSERT INTO file_state(root_path, identity_type, identity_value, path_id, name, size_bytes, created_at, modified_at, file_key, content_algo, content_hash, last_scan_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(root_path, identity_type, identity_value) DO UPDATE SET path_id=excluded.path_id, name=excluded.name, size_bytes=excluded.size_bytes, modified_at=excluded.modified_at, content_hash=excluded.content_hash, last_scan_id=excluded.last_scan_id");
                  PreparedStatement psTouch = conn.prepareStatement("UPDATE file_state SET last_scan_id=? WHERE root_path=? AND identity_type=? AND identity_value=?");
                  PreparedStatement psLog = conn.prepareStatement("INSERT INTO file_change(root_path, identity_type, identity_value, scan_id, size_bytes, modified_at, content_hash, reason) VALUES(?,?,?,?,?,?,?,?)")) {
                
                for (Scanner.FileResult res : items) {
                    if (res.status() == Scanner.FileStatus.UNCHANGED) {
                        psTouch.setLong(1, scanId); psTouch.setString(2, res.meta().rootPath());
                        psTouch.setString(3, res.meta().identityType()); psTouch.setString(4, res.meta().identityValue());
                        psTouch.addBatch(); continue;
                    }
                    long pathId = resolvePathId(conn, psPath, psPathSel, res.meta().fullPath());
                    
                    psState.setString(1, res.meta().rootPath()); psState.setString(2, res.meta().identityType()); psState.setString(3, res.meta().identityValue());
                    psState.setLong(4, pathId); psState.setString(5, res.meta().name()); psState.setLong(6, res.meta().sizeBytes());
                    psState.setString(7, res.meta().createdAt()); psState.setString(8, res.meta().modifiedAt()); psState.setString(9, res.meta().fileKey());
                    psState.setString(10, res.contentAlgo()); psState.setString(11, res.contentHash()); psState.setLong(12, scanId); psState.addBatch();
                    
                    psLog.setString(1, res.meta().rootPath()); psLog.setString(2, res.meta().identityType()); psLog.setString(3, res.meta().identityValue());
                    psLog.setLong(4, scanId); psLog.setLong(5, res.meta().sizeBytes()); psLog.setString(6, res.meta().modifiedAt());
                    psLog.setString(7, res.contentHash()); psLog.setString(8, res.status().name()); psLog.addBatch();
                }
                psState.executeBatch(); psTouch.executeBatch(); psLog.executeBatch();
            }
        }
        
        private long resolvePathId(Connection conn, PreparedStatement psIns, PreparedStatement psSel, String path) throws SQLException {
            Long cached = pathCache.get(path); if (cached != null) return cached;
            psSel.setString(1, path);
            try (ResultSet rs = psSel.executeQuery()) {
                if (rs.next()) { long id = rs.getLong(1); pathCache.put(path, id); return id; }
            }
            psIns.setString(1, path); psIns.executeUpdate(); 
            psSel.setString(1, path);
            try (ResultSet rs = psSel.executeQuery()) {
                if (rs.next()) { long id = rs.getLong(1); pathCache.put(path, id); return id; }
            }
            throw new SQLException("Path ID resolution failed for " + path);
        }
    }
}