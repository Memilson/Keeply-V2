package com.keeply.app;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public final class Database {

    private Database() {}

    // --- RECORDS (Modelos de Dados) ---
    public record InventoryRow(String rootPath, String pathRel, String name, long sizeBytes, long modifiedMillis, long createdMillis, String status) {}
    public record ScanSummary(long scanId, String rootPath, String startedAt, String finishedAt) {}
    public record FileHistoryRow(long scanId, String rootPath, String startedAt, String finishedAt, long sizeBytes, String statusEvent, String createdAt) {}
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

    public static void safeClose(Connection c) {
        if (c == null) return;
        try { c.close(); } catch (Exception ignored) {}
    }

    // --- SCHEMA ---
    public static void ensureSchema(Connection c) throws SQLException {
        try (var st = c.createStatement()) {
            try { st.execute("ALTER TABLE scans ADD COLUMN total_usage INTEGER"); } catch (SQLException ignored) {}
            try { st.execute("ALTER TABLE scans ADD COLUMN status TEXT"); } catch (SQLException ignored) {}

            st.execute("CREATE TABLE IF NOT EXISTS scans (scan_id INTEGER PRIMARY KEY AUTOINCREMENT, root_path TEXT, started_at TEXT, finished_at TEXT, total_usage INTEGER, status TEXT)");
            // Nota: schema sem colunas legadas
            st.execute("""
                CREATE TABLE IF NOT EXISTS file_inventory (
                    root_path TEXT NOT NULL,
                    path_rel TEXT NOT NULL,
                    name TEXT,
                    size_bytes INTEGER,
                    modified_millis INTEGER,
                    created_millis INTEGER,
                    last_scan_id INTEGER,
                    status TEXT,
                    PRIMARY KEY (root_path, path_rel)
                )
            """);

            st.execute("""
                CREATE TABLE IF NOT EXISTS file_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id INTEGER,
                    root_path TEXT,
                    path_rel TEXT,
                    size_bytes INTEGER,
                    status_event TEXT,
                    created_at TEXT,
                    created_millis INTEGER,
                    modified_millis INTEGER
                )
            """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_history_path ON file_history(root_path, path_rel)");
            st.execute("CREATE TABLE IF NOT EXISTS scan_issues (id INTEGER PRIMARY KEY AUTOINCREMENT, scan_id INTEGER, path TEXT, message TEXT, created_at TEXT)");
        }
        migrateLegacySchema(c);
    }

    // --- LOGS E SCAN ---
    public static long startScanLog(Connection c, String rootPath) throws SQLException {
        try (var ps = c.prepareStatement("INSERT INTO scans(root_path, started_at, status) VALUES(?, datetime('now'), 'RUNNING')", Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, rootPath);
            ps.executeUpdate();
            try (var rs = ps.getGeneratedKeys()) { if (rs.next()) return rs.getLong(1); }
        }
        return 0;
    }

    public static void finishScanLog(Connection c, long scanId) throws SQLException {
        long totalBytes = 0;
        String rootPath = null;
        try (var ps = c.prepareStatement("SELECT root_path FROM scans WHERE scan_id = ?")) {
            ps.setLong(1, scanId);
            try (var rs = ps.executeQuery()) {
                if (rs.next()) rootPath = rs.getString(1);
            }
        }
        if (rootPath == null) rootPath = "";
        try (var ps = c.prepareStatement("SELECT SUM(size_bytes) FROM file_inventory WHERE root_path = ?")) {
            ps.setString(1, rootPath);
            try (var rs = ps.executeQuery()) {
                if (rs.next()) totalBytes = rs.getLong(1);
            }
        }
        try (var ps = c.prepareStatement("UPDATE scans SET finished_at=datetime('now'), total_usage=?, status='DONE' WHERE scan_id=?")) {
            ps.setLong(1, totalBytes);
            ps.setLong(2, scanId);
            ps.executeUpdate();
        }
    }

    public static void cancelScanLog(Connection c, long scanId) throws SQLException {
        try (var ps = c.prepareStatement("UPDATE scans SET finished_at=datetime('now'), total_usage=NULL, status='CANCELED' WHERE scan_id=?")) {
            ps.setLong(1, scanId);
            ps.executeUpdate();
        }
    }
    // --- CORE LOGIC (Removido fetchDirtyFiles/validação antiga) ---
    
    public static int deleteStaleFiles(Connection c, long currentScanId, String rootPath) throws SQLException {
        try (var ps = c.prepareStatement("DELETE FROM file_inventory WHERE root_path = ? AND last_scan_id < ?")) {
            ps.setString(1, rootPath);
            ps.setLong(2, currentScanId);
            return ps.executeUpdate();
        }
    }


    // Copia o estado atual (NEW/MODIFIED) para o histórico
    public static int snapshotToHistory(Connection c, long scanId) throws SQLException {
        int count = 0;
        String sqlCopy = """
                INSERT INTO file_history (scan_id, root_path, path_rel, size_bytes, status_event, created_at, created_millis, modified_millis)
                SELECT last_scan_id, root_path, path_rel, size_bytes, status, datetime('now'), created_millis, modified_millis
                FROM file_inventory
                WHERE last_scan_id = ?
                  AND status IN ('NEW', 'MODIFIED')
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
            SELECT fi.root_path, fi.path_rel, fi.name, fi.size_bytes, fi.modified_millis, fi.created_millis, fi.status
            FROM file_inventory fi
            ORDER BY fi.root_path, fi.path_rel
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
            WITH Target AS (
                SELECT root_path AS root
                FROM scans
                WHERE scan_id = ?
            ),
            LatestState AS (
                SELECT root_path, path_rel, MAX(scan_id) as max_scan
                FROM file_history
                WHERE scan_id <= ?
                  AND root_path = (SELECT root FROM Target)
                GROUP BY root_path, path_rel
            )
            SELECT fh.root_path, fh.path_rel, fh.size_bytes, fh.status_event, fh.created_millis, fh.modified_millis, fh.scan_id
            FROM file_history fh
            JOIN LatestState ls
              ON fh.root_path = ls.root_path
             AND fh.path_rel = ls.path_rel
             AND fh.scan_id = ls.max_scan
            ORDER BY fh.path_rel
        """;
        try (var ps = c.prepareStatement(sql)) {
            ps.setLong(1, targetScanId);
            ps.setLong(2, targetScanId);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    String path = rs.getString("path_rel");
                    String name = path.contains("/") ? path.substring(path.lastIndexOf('/') + 1) : path;
                    out.add(new InventoryRow(
                            rs.getString("root_path"),
                            path,
                            name,
                            rs.getLong("size_bytes"),
                            rs.getLong("modified_millis"),
                            rs.getLong("created_millis"),
                            rs.getString("status_event")));
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
            SELECT fh.scan_id, fh.size_bytes, fh.status_event, fh.created_at, s.root_path, s.started_at, s.finished_at
            FROM file_history fh
            LEFT JOIN scans s ON s.scan_id = fh.scan_id
            WHERE fh.path_rel = ?
            ORDER BY fh.scan_id DESC
        """;
        try (var ps = c.prepareStatement(sql)) {
            ps.setString(1, pathRel);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) out.add(new FileHistoryRow(rs.getLong("scan_id"), rs.getString("root_path"), rs.getString("started_at"), rs.getString("finished_at"), rs.getLong("size_bytes"), rs.getString("status_event"), rs.getString("created_at")));
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

    private static void migrateLegacySchema(Connection c) throws SQLException {
        migrateFileInventory(c);
        migrateFileHistory(c);
        normalizeLegacyStatuses(c);
        backfillScanStatus(c);
    }

    private static void migrateFileInventory(Connection c) throws SQLException {
        boolean hasRootPath = hasColumn(c, "file_inventory", "root_path");
        boolean hasLegacyHash = hasColumn(c, "file_inventory", "hash_hex") || hasColumn(c, "file_inventory", "file_key");
        if (hasRootPath && !hasLegacyHash) return;
        try (var st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS file_inventory_old");
            st.execute("ALTER TABLE file_inventory RENAME TO file_inventory_old");
            st.execute("""
                CREATE TABLE file_inventory (
                    root_path TEXT NOT NULL,
                    path_rel TEXT NOT NULL,
                    name TEXT,
                    size_bytes INTEGER,
                    modified_millis INTEGER,
                    created_millis INTEGER,
                    last_scan_id INTEGER,
                    status TEXT,
                    PRIMARY KEY (root_path, path_rel)
                )
            """);
            st.execute("""
                INSERT INTO file_inventory (root_path, path_rel, name, size_bytes, modified_millis, created_millis, last_scan_id, status)
                SELECT COALESCE(s.root_path, ''), fi.path_rel, fi.name, fi.size_bytes, fi.modified_millis, fi.created_millis, fi.last_scan_id, fi.status
                FROM file_inventory_old fi
                LEFT JOIN scans s ON s.scan_id = fi.last_scan_id
            """);
            st.execute("DROP TABLE file_inventory_old");
        }
    }

    private static void migrateFileHistory(Connection c) throws SQLException {
        boolean hasRootPath = hasColumn(c, "file_history", "root_path");
        boolean hasCreatedMillis = hasColumn(c, "file_history", "created_millis");
        boolean hasModifiedMillis = hasColumn(c, "file_history", "modified_millis");
        boolean hasLegacyHash = hasColumn(c, "file_history", "hash_hex");
        if (hasRootPath && hasCreatedMillis && hasModifiedMillis && !hasLegacyHash) return;
        try (var st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS file_history_old");
            st.execute("ALTER TABLE file_history RENAME TO file_history_old");
            st.execute("""
                CREATE TABLE file_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id INTEGER,
                    root_path TEXT,
                    path_rel TEXT,
                    size_bytes INTEGER,
                    status_event TEXT,
                    created_at TEXT,
                    created_millis INTEGER,
                    modified_millis INTEGER
                )
            """);
            st.execute("""
                INSERT INTO file_history (id, scan_id, root_path, path_rel, size_bytes, status_event, created_at, created_millis, modified_millis)
                SELECT fh.id, fh.scan_id, COALESCE(s.root_path, ''), fh.path_rel, fh.size_bytes, fh.status_event, fh.created_at, 0, 0
                FROM file_history_old fh
                LEFT JOIN scans s ON s.scan_id = fh.scan_id
            """);
            st.execute("DROP TABLE file_history_old");
            st.execute("CREATE INDEX IF NOT EXISTS idx_history_path ON file_history(root_path, path_rel)");
        }
    }

    private static void normalizeLegacyStatuses(Connection c) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute("UPDATE file_inventory SET status='STABLE' WHERE status='HASHED'");
            st.execute("UPDATE file_history SET status_event='STABLE' WHERE status_event='HASHED'");
        }
    }

    private static void backfillScanStatus(Connection c) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute("UPDATE scans SET status='DONE' WHERE status IS NULL AND finished_at IS NOT NULL");
            st.execute("UPDATE scans SET status='RUNNING' WHERE status IS NULL AND finished_at IS NULL");
        }
    }

    private static boolean hasColumn(Connection c, String table, String column) throws SQLException {
        try (var rs = c.createStatement().executeQuery("PRAGMA table_info(" + table + ")")) {
            while (rs.next()) {
                if (column.equalsIgnoreCase(rs.getString("name"))) return true;
            }
        }
        return false;
    }

    private static InventoryRow mapInventoryRow(ResultSet rs) throws SQLException {
        return new InventoryRow(rs.getString("root_path"), rs.getString("path_rel"), rs.getString("name"), rs.getLong("size_bytes"), rs.getLong("modified_millis"), rs.getLong("created_millis"), rs.getString("status"));
    }
}




