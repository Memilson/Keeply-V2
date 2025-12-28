package com.keeply.app;

import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
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
            s.execute("CREATE TABLE IF NOT EXISTS file_change (id BIGSERIAL PRIMARY KEY, root_path TEXT, identity_type TEXT, identity_value TEXT, scan_id BIGINT, size_bytes BIGINT, modified_at TEXT, content_hash TEXT, reason TEXT)");
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
        c.createStatement().execute("UPDATE scan SET finished_at='" + Instant.now() + "', status='SUCCESS' WHERE id=" + id);
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
        WHERE root_path = ?
        LIMIT ?
        """;

    try (PreparedStatement ps = c.prepareStatement(sql)) {
        ps.setString(1, root);
        ps.setLong(2, limit);

        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                long pathId = rs.getLong("path_id");
                String fullPath = rs.getString("full_path");
                long sizeBytes = rs.getLong("size_bytes");
                String modifiedAtStr = rs.getString("modified_at");
                String contentHash = rs.getString("content_hash");
                String contentAlgo = rs.getString("content_algo");

                long modifiedAtMillis = -1L;
                try {
                    // converte "2025-12-27T19:40:00Z" -> epoch millis (Postgres ou ISO-8601)
                    modifiedAtMillis = java.time.Instant.parse(modifiedAtStr).toEpochMilli();
                } catch (Exception ignored) {
                    // se não for ISO parseável, deixa -1 e Scanner cai no fallback textual
                }

                map.put(fullPath, new Scanner.PrevInfo(
                        pathId,
                        fullPath,
                        sizeBytes,
                        modifiedAtMillis,
                        modifiedAtStr,
                        contentHash,
                        contentAlgo
                ));
            }
        }
    }
}


    public static void handleDeletions(Connection c, long scanId, String root) throws SQLException {
        c.createStatement().execute("INSERT INTO file_change(root_path, identity_type, identity_value, scan_id, reason) SELECT root_path, identity_type, identity_value, "+scanId+", 'DELETED' FROM file_state WHERE root_path='"+root+"' AND last_scan_id < " + scanId);
        c.createStatement().execute("DELETE FROM file_state WHERE root_path='"+root+"' AND last_scan_id < " + scanId);
    }

    public static String safeTime(FileTime t) { return t == null ? null : t.toInstant().toString(); }

    // --- SimplePool ---
    public static class SimplePool implements AutoCloseable {
        private final String url, user, pass;
        private final BlockingQueue<Connection> pool;
        private final List<Connection> allConnections = new ArrayList<>();

        public SimplePool(String url, String user, String pass, int size) throws SQLException {
            try { Class.forName("org.postgresql.Driver"); } catch (ClassNotFoundException e) { System.err.println("PG Driver missing"); }
            this.url = url; this.user = user; this.pass = pass;
            this.pool = new ArrayBlockingQueue<>(size);
            for (int i = 0; i < size; i++) { Connection c = createNew(); pool.offer(c); allConnections.add(c); }
        }

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

        public Connection borrow() throws InterruptedException { return pool.take(); }
        public void release(Connection c) { if (c != null) pool.offer(c); }
        public void close() { for (Connection c : allConnections) try { c.close(); } catch (SQLException ignored) {} }
    }

    // --- ParallelDbWriter ---
    public static final class ParallelDbWriter {
        private final SimplePool pool;
        private final long scanId;
        private final Scanner.ScanConfig cfg;
        private final Scanner.ScanMetrics metrics;
        private final ConcurrentHashMap<String, Long> pathCache = new ConcurrentHashMap<>(50_000);
        private final ReentrantLock lock = new ReentrantLock();
        private final List<Scanner.FileResult> fileBuffer;
        private final List<Scanner.ScanIssue> issueBuffer;
        private final Phaser activeWrites = new Phaser(1);

        public ParallelDbWriter(SimplePool pool, long scanId, Scanner.ScanConfig cfg, Scanner.ScanMetrics metrics) {
            this.pool = pool; this.scanId = scanId; this.cfg = cfg; this.metrics = metrics;
            this.fileBuffer = new ArrayList<>(cfg.batchLimit() * 2);
            this.issueBuffer = new ArrayList<>(cfg.batchLimit() * 2);
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
            activeWrites.register();
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
                } finally { 
                    pool.release(conn);
                    activeWrites.arriveAndDeregister();
                }
            });
        }

        private void flushIssuesAsync(List<Scanner.ScanIssue> batch) {
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
                    activeWrites.arriveAndDeregister();
                }
            });
        }

        private void executeFileBatch(Connection conn, List<Scanner.FileResult> items) throws SQLException {
             Collections.sort(items, Comparator.comparing(f -> f.contentHash() == null ? "" : f.contentHash()));
             try (PreparedStatement psPath = conn.prepareStatement("INSERT INTO path(full_path) VALUES(?) ON CONFLICT(full_path) DO NOTHING");
                  PreparedStatement psPathSel = conn.prepareStatement("SELECT id FROM path WHERE full_path=?");
                  PreparedStatement psCont = conn.prepareStatement("INSERT INTO content(algo, hash_hex, size_bytes) VALUES(?,?,?) ON CONFLICT(algo, hash_hex) DO NOTHING");
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
                    if (res.contentHash() != null) {
                        psCont.setString(1, res.contentAlgo()); psCont.setString(2, res.contentHash()); psCont.setLong(3, res.meta().sizeBytes()); psCont.addBatch();
                    }
                    psState.setString(1, res.meta().rootPath()); psState.setString(2, res.meta().identityType()); psState.setString(3, res.meta().identityValue());
                    psState.setLong(4, pathId); psState.setString(5, res.meta().name()); psState.setLong(6, res.meta().sizeBytes());
                    psState.setString(7, res.meta().createdAt()); psState.setString(8, res.meta().modifiedAt()); psState.setString(9, res.meta().fileKey());
                    psState.setString(10, res.contentAlgo()); psState.setString(11, res.contentHash()); psState.setLong(12, scanId); psState.addBatch();
                    psLog.setString(1, res.meta().rootPath()); psLog.setString(2, res.meta().identityType()); psLog.setString(3, res.meta().identityValue());
                    psLog.setLong(4, scanId); psLog.setLong(5, res.meta().sizeBytes()); psLog.setString(6, res.meta().modifiedAt());
                    psLog.setString(7, res.contentHash()); psLog.setString(8, res.status().name()); psLog.addBatch();
                }
                psCont.executeBatch(); psState.executeBatch(); psTouch.executeBatch(); psLog.executeBatch();
            }
        }
        
        private long resolvePathId(Connection conn, PreparedStatement psIns, PreparedStatement psSel, String path) throws SQLException {
            Long cached = pathCache.get(path); if (cached != null) return cached;
            psIns.setString(1, path); psIns.executeUpdate(); 
            psSel.setString(1, path);
            try (ResultSet rs = psSel.executeQuery()) {
                if (rs.next()) { long id = rs.getLong(1); pathCache.put(path, id); return id; }
            }
            throw new SQLException("Path ID resolution failed for " + path);
        }
    }
}