package com.keeply.app;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import com.keeply.app.Scanner.HashCandidate;
import com.keeply.app.Scanner.HashUpdate;

public final class Database {

    private Database() {}

    // --- RECORDS (Modelos de Dados) ---
    // Devem ser 'public' para serem vistos pelos Controllers
    public record InventoryRow(String rootPath, String pathRel, String name, long sizeBytes, long modifiedMillis, long createdMillis, String status, String hashHex) {}
    public record ScanSummary(long scanId, String rootPath, String startedAt, String finishedAt) {}
    public record FileHistoryRow(long scanId, String rootPath, String startedAt, String finishedAt, String hashHex, long sizeBytes, String statusEvent, String createdAt) {}
    public record CapacityReport(String date, long totalBytes, long growthBytes) {}

    // --- CONNECTION POOL (HikariCP singleton) ---
    private static HikariDataSource dataSource;

    static {
        createDataSource();
    }

    private static synchronized void createDataSource() {
        if (dataSource != null) return;
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(Config.getDbUrl());
        config.addDataSourceProperty("cipher", "sqlcipher");
        config.addDataSourceProperty("key", Config.getSecretKey());
        config.addDataSourceProperty("legacy", "0");
        config.addDataSourceProperty("kdf_iter", "64000");

        // Do not force autoCommit at datasource level; callers will set connection auto-commit as needed

        // Configurações vitais para SQLite
        config.setConnectionTestQuery("SELECT 1");
        config.setMaximumPoolSize(10);
        config.setConnectionInitSql("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=10000;");

        dataSource = new HikariDataSource(config);
    }

    /**
     * Close and clear the shared datasource. Call before deleting the DB file.
     */
    public static synchronized void shutdown() {
        if (dataSource != null) {
            try { dataSource.close(); } catch (Exception ignored) {}
            dataSource = null;
        }
    }

    /**
     * Ensure the datasource is initialized (lazy init after shutdown).
     */
    public static synchronized void init() {
        if (dataSource == null) createDataSource();
    }

    public static final class SimplePool implements AutoCloseable {
        // kept for API compatibility with existing code; uses shared Hikari datasource
        public SimplePool(String jdbcUrl, int poolSize) {
            // constructor parameters are ignored — singleton datasource already configured
        }

        public Connection borrow() throws SQLException {
            createDataSource();
            Connection c = dataSource.getConnection();
            try { c.setAutoCommit(false); } catch (Exception ignored) {}
            return c;
        }

        @Override public void close() {
            // no-op: datasource is application-wide singleton
        }
    }

    private static Connection openEncryptedPhysical(String url) throws SQLException {
        try { Class.forName("org.sqlite.JDBC"); } catch (ClassNotFoundException ignored) {}
        var props = new Properties();
        props.setProperty("cipher", "sqlcipher");
        props.setProperty("key", Config.getSecretKey());
        props.setProperty("legacy", "0");
        props.setProperty("kdf_iter", "64000");

        var c = DriverManager.getConnection(url, props);
        try (var st = c.createStatement()) {
            st.execute("PRAGMA journal_mode=WAL;");
            st.execute("PRAGMA synchronous=NORMAL;");
            st.execute("PRAGMA busy_timeout=10000;");
        }
        c.setAutoCommit(false);
        return c;
    }

    public static Connection openSingleConnection() throws SQLException {
        // Ensure datasource is initialized (may have been shutdown earlier)
        init();
        if (dataSource == null) throw new SQLException("Datasource not initialized");
        Connection c = dataSource.getConnection();
        try { c.setAutoCommit(false); } catch (Exception ignored) {}
        return c;
    }

    // --- SCHEMA ---
    public static void ensureSchema(Connection c) throws SQLException {
        try (var st = c.createStatement()) {
            try { st.execute("ALTER TABLE scans ADD COLUMN total_usage INTEGER"); } catch (SQLException ignored) {}

            st.execute("CREATE TABLE IF NOT EXISTS scans (scan_id INTEGER PRIMARY KEY AUTOINCREMENT, root_path TEXT, started_at TEXT, finished_at TEXT, total_usage INTEGER)");
            
            st.execute("""
                CREATE TABLE IF NOT EXISTS file_inventory (
                    path_rel TEXT PRIMARY KEY, name TEXT, size_bytes INTEGER, modified_millis INTEGER, created_millis INTEGER,
                    file_key TEXT, hash_hex TEXT, last_scan_id INTEGER, status TEXT
                )
            """);

            st.execute("""
                CREATE TABLE IF NOT EXISTS file_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id INTEGER, path_rel TEXT, hash_hex TEXT, size_bytes INTEGER, status_event TEXT, created_at TEXT
                )
            """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_history_path ON file_history(path_rel)");
            st.execute("CREATE TABLE IF NOT EXISTS scan_issues (id INTEGER PRIMARY KEY AUTOINCREMENT, scan_id INTEGER, path TEXT, message TEXT, created_at TEXT)");
        }
    }

    // --- LOGS E SCAN ---
    public static long startScanLog(Connection c, String rootPath) throws SQLException {
        try (var ps = c.prepareStatement("INSERT INTO scans(root_path, started_at) VALUES(?, datetime('now'))", Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, rootPath);
            ps.executeUpdate();
            try (var rs = ps.getGeneratedKeys()) { if (rs.next()) return rs.getLong(1); }
        }
        return 0;
    }

    public static void finishScanLog(Connection c, long scanId) throws SQLException {
        long totalBytes = 0;
        try (var rs = c.createStatement().executeQuery("SELECT SUM(size_bytes) FROM file_inventory")) {
            if (rs.next()) totalBytes = rs.getLong(1);
        }
        try (var ps = c.prepareStatement("UPDATE scans SET finished_at=datetime('now'), total_usage=? WHERE scan_id=?")) {
            ps.setLong(1, totalBytes);
            ps.setLong(2, scanId);
            ps.executeUpdate();
        }
    }

    // --- CORE LOGIC ---
    public static int deleteStaleFiles(Connection c, long currentScanId) throws SQLException {
        try (var ps = c.prepareStatement("DELETE FROM file_inventory WHERE last_scan_id < ?")) {
            ps.setLong(1, currentScanId);
            return ps.executeUpdate();
        }
    }

    public static List<HashCandidate> fetchDirtyFiles(Connection c, long scanId, int limit) throws SQLException {
        var out = new ArrayList<HashCandidate>();
        try (var ps = c.prepareStatement("SELECT path_rel, size_bytes FROM file_inventory WHERE last_scan_id = ? AND (status = 'NEW' OR status = 'MODIFIED' OR hash_hex IS NULL) LIMIT ?")) {
            ps.setLong(1, scanId); ps.setInt(2, limit);
            try (var rs = ps.executeQuery()) { while (rs.next()) out.add(new HashCandidate(0, rs.getString(1), rs.getLong(2))); }
        }
        return out;
    }

    public static void updateHashes(Connection c, List<HashUpdate> updates) throws SQLException {
        try (var ps = c.prepareStatement("UPDATE file_inventory SET hash_hex=?, status='HASHED' WHERE path_rel=?")) {
            for (var u : updates) {
                ps.setString(1, u.hashHex());
                ps.setString(2, u.pathRel());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    public static int snapshotToHistory(Connection c, long scanId) throws SQLException {
        int count = 0;
        String sqlCopy = """
                INSERT INTO file_history (scan_id, path_rel, hash_hex, size_bytes, status_event, created_at)
                SELECT last_scan_id, path_rel, hash_hex, size_bytes, status, datetime('now')
                FROM file_inventory
                WHERE last_scan_id = ?
                  AND status IN ('NEW', 'HASHED', 'MODIFIED')
            """;
        String sqlUpdate = "UPDATE file_inventory SET status='STABLE' WHERE last_scan_id = ?";
        try (var psCopy = c.prepareStatement(sqlCopy);
             var psUpdate = c.prepareStatement(sqlUpdate)) {
            psCopy.setLong(1, scanId);
            count = psCopy.executeUpdate();
            psUpdate.setLong(1, scanId);
            psUpdate.executeUpdate();
        }
        return count;
    }

    // --- FETCH DATA ---
    public static List<InventoryRow> fetchInventory(Connection c) throws SQLException {
        ensureSchema(c);
        var out = new ArrayList<InventoryRow>();
        var sql = """
            SELECT fi.path_rel, fi.name, fi.size_bytes, fi.modified_millis, fi.created_millis, fi.status, fi.hash_hex, COALESCE(s.root_path, '') AS root_path
            FROM file_inventory fi
            LEFT JOIN scans s ON s.scan_id = fi.last_scan_id
            ORDER BY fi.path_rel
        """;
        try (var rs = c.prepareStatement(sql).executeQuery()) {
            while (rs.next()) out.add(mapInventoryRow(rs));
        }
        return out;
    }

    public static List<InventoryRow> fetchSnapshotFiles(Connection c, long targetScanId) throws SQLException {
        ensureSchema(c);
        var out = new ArrayList<InventoryRow>();
        var sql = """
            WITH LatestState AS (
                SELECT path_rel, MAX(scan_id) as max_scan
                FROM file_history
                WHERE scan_id <= ?
                GROUP BY path_rel
            )
            SELECT fh.path_rel, fh.size_bytes, fh.hash_hex, fh.status_event, fh.created_at, fh.scan_id
            FROM file_history fh
            JOIN LatestState ls ON fh.path_rel = ls.path_rel AND fh.scan_id = ls.max_scan
            ORDER BY fh.path_rel
        """;
        try (var ps = c.prepareStatement(sql)) {
            ps.setLong(1, targetScanId);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    String path = rs.getString("path_rel");
                    String name = path.contains("/") ? path.substring(path.lastIndexOf('/') + 1) : path;
                    out.add(new InventoryRow("", path, name, rs.getLong("size_bytes"), 0, 0, rs.getString("status_event"), rs.getString("hash_hex")));
                }
            }
        }
        return out;
    }

    public static List<ScanSummary> fetchAllScans(Connection c) throws SQLException {
        var list = new ArrayList<ScanSummary>();
        try (var rs = c.createStatement().executeQuery("SELECT scan_id, root_path, started_at, finished_at FROM scans ORDER BY scan_id DESC")) {
            while (rs.next()) list.add(new ScanSummary(rs.getLong("scan_id"), rs.getString("root_path"), rs.getString("started_at"), rs.getString("finished_at")));
        }
        return list;
    }

    public static ScanSummary fetchLastScan(Connection c) throws SQLException {
        var list = fetchAllScans(c);
        return list.isEmpty() ? null : list.get(0);
    }

    public static List<FileHistoryRow> fetchFileHistory(Connection c, String pathRel) throws SQLException {
        ensureSchema(c);
        var out = new ArrayList<FileHistoryRow>();
        var sql = """
            SELECT fh.scan_id, fh.hash_hex, fh.size_bytes, fh.status_event, fh.created_at, s.root_path, s.started_at, s.finished_at
            FROM file_history fh
            LEFT JOIN scans s ON s.scan_id = fh.scan_id
            WHERE fh.path_rel = ?
            ORDER BY fh.scan_id DESC
        """;
        try (var ps = c.prepareStatement(sql)) {
            ps.setString(1, pathRel);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) out.add(new FileHistoryRow(rs.getLong("scan_id"), rs.getString("root_path"), rs.getString("started_at"), rs.getString("finished_at"), rs.getString("hash_hex"), rs.getLong("size_bytes"), rs.getString("status_event"), rs.getString("created_at")));
            }
        }
        return out;
    }

    public static List<CapacityReport> predictGrowth(Connection c) throws SQLException {
        var list = new ArrayList<CapacityReport>();
        var sql = """
            SELECT started_at, total_usage, (total_usage - LAG(total_usage, 1, 0) OVER (ORDER BY scan_id)) as crescimento
            FROM scans WHERE total_usage IS NOT NULL ORDER BY scan_id DESC LIMIT 10
        """;
        try (var rs = c.createStatement().executeQuery(sql)) {
            while (rs.next()) list.add(new CapacityReport(rs.getString("started_at"), rs.getLong("total_usage"), rs.getLong("crescimento")));
        }
        return list;
    }

    private static InventoryRow mapInventoryRow(ResultSet rs) throws SQLException {
        return new InventoryRow(rs.getString("root_path"), rs.getString("path_rel"), rs.getString("name"), rs.getLong("size_bytes"), rs.getLong("modified_millis"), rs.getLong("created_millis"), rs.getString("status"), rs.getString("hash_hex"));
    }
}
