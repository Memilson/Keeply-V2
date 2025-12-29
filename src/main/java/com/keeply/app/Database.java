package com.keeply.app;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public final class Database {

    private Database() {}

    // --- RECORDS (Modelos de Dados) ---
    public record InventoryRow(String rootPath, String pathRel, String name, long sizeBytes, long modifiedMillis, long createdMillis, String status, String hashHex) {}
    public record ScanSummary(long scanId, String rootPath, String startedAt, String finishedAt) {}
    public record FileHistoryRow(long scanId, String rootPath, String startedAt, String finishedAt, String hashHex, long sizeBytes, String statusEvent, String createdAt) {}
    public record CapacityReport(String date, long totalBytes, long growthBytes) {}
    public record ScanIssue(String pathRel, String message) {}

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

        config.setConnectionTestQuery("SELECT 1");
        config.setMaximumPoolSize(10);
        // WAL e Synchronous NORMAL são essenciais para velocidade de escrita do DbWriter
        config.setConnectionInitSql("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=10000;");

        dataSource = new HikariDataSource(config);
    }

    public static synchronized void shutdown() {
        if (dataSource != null) {
            try { dataSource.close(); } catch (Exception ignored) {}
            dataSource = null;
        }
    }

    public static synchronized void init() {
        if (dataSource == null) createDataSource();
    }

    public static final class SimplePool implements AutoCloseable {
        public SimplePool(String jdbcUrl, int poolSize) {
            // Pool size gerido pelo Hikari globalmente
        }

        public Connection borrow() throws SQLException {
            createDataSource();
            Connection c = dataSource.getConnection();
            try { c.setAutoCommit(false); } catch (Exception ignored) {}
            return c;
        }

        @Override public void close() { }
    }

    public static Connection openSingleConnection() throws SQLException {
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
            
            // Note: hash_hex mantido para compatibilidade, mas será NULL na maioria dos casos agora
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

    // --- CORE LOGIC (Removido fetchDirtyFiles/updateHashes) ---
    
    public static int deleteStaleFiles(Connection c, long currentScanId) throws SQLException {
        try (var ps = c.prepareStatement("DELETE FROM file_inventory WHERE last_scan_id < ?")) {
            ps.setLong(1, currentScanId);
            return ps.executeUpdate();
        }
    }

    // Apenas para erros de permissão ou IO no Walk
    public static void insertScanIssues(Connection c, long scanId, List<ScanIssue> issues) throws SQLException {
        // Query removida: O Scanner novo não usa batch de issues ainda, mas se usar no futuro, reativamos.
        // O código do Scanner Metadata-Only não está chamando isso, mas mantemos para compatibilidade.
    }

    // Copia o estado atual (NEW/MODIFIED) para o histórico
    public static int snapshotToHistory(Connection c, long scanId) throws SQLException {
        int count = 0;
        String sqlCopy = """
                INSERT INTO file_history (scan_id, path_rel, hash_hex, size_bytes, status_event, created_at)
                SELECT last_scan_id, path_rel, hash_hex, size_bytes, status, datetime('now')
                FROM file_inventory
                WHERE last_scan_id = ?
                  AND status IN ('NEW', 'HASHED', 'MODIFIED')
            """;
        // Marca como STABLE após copiar para o histórico
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

    // --- FETCH DATA (Relatórios e UI) ---
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
        // Pega a versão do arquivo naquele ponto do tempo (scan_id)
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