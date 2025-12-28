package com.keeply.app;

import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.keeply.app.Scanner.HashCandidate;
import com.keeply.app.Scanner.HashUpdate;

public final class Database {

    private Database() {
    }

    public record InventoryRow(String rootPath, String pathRel, String name, long sizeBytes, long modifiedMillis,
            long createdMillis, String status, String hashHex) {
    }

    public record ScanSummary(long scanId, String rootPath, String startedAt, String finishedAt) {
    }

    public record FileHistoryRow(long scanId, String rootPath, String startedAt, String finishedAt, String hashHex,
            long sizeBytes, String statusEvent, String createdAt) {
    }

    public static final class SimplePool implements AutoCloseable {
        private final BlockingQueue<Connection> idle;

        public SimplePool(String jdbcUrl, int poolSize) throws SQLException {
            this.idle = new ArrayBlockingQueue<>(poolSize);
            for (int i = 0; i < poolSize; i++)
                idle.add(openEncryptedPhysical(jdbcUrl));
        }

        public Connection borrow() throws InterruptedException {
            var physical = idle.take();
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class<?>[] { Connection.class }, (p, m, a) -> {
                        if (m.getName().equals("close")) {
                            release(physical);
                            return null;
                        }
                        return m.invoke(physical, a);
                    });
        }

        private void release(Connection c) {
            try {
                if (!c.getAutoCommit())
                    c.rollback();
            } catch (Exception ignored) {
            }
            if (!idle.offer(c))
                try {
                    c.close();
                } catch (Exception ignored) {
                }
        }

        @Override
        public void close() {
            Connection c;
            while ((c = idle.poll()) != null)
                try {
                    c.close();
                } catch (Exception ignored) {
                }
        }
    }

    private static Connection openEncryptedPhysical(String url) throws SQLException {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException ignored) {
        }

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
        return openEncryptedPhysical(Config.getDbUrl());
    }

    public static void ensureSchema(Connection c) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute(
                    "CREATE TABLE IF NOT EXISTS scans (scan_id INTEGER PRIMARY KEY AUTOINCREMENT, root_path TEXT, started_at TEXT, finished_at TEXT)");

            // Tabela de Inventário (Estado Atual)
            st.execute(
                    """
                                CREATE TABLE IF NOT EXISTS file_inventory (
                                    path_rel TEXT PRIMARY KEY, name TEXT, size_bytes INTEGER, modified_millis INTEGER, created_millis INTEGER,
                                    file_key TEXT, hash_hex TEXT, last_scan_id INTEGER, status TEXT
                                )
                            """);

            // --- NOVO: Tabela de Histórico (Time Lapse) ---
            st.execute("""
                        CREATE TABLE IF NOT EXISTS file_history (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            scan_id INTEGER,       -- Liga ao scan específico
                            path_rel TEXT,         -- O arquivo
                            hash_hex TEXT,         -- O hash naquele momento
                            size_bytes INTEGER,    -- O tamanho naquele momento
                            status_event TEXT,     -- O que aconteceu: 'CREATED', 'MODIFIED'
                            created_at TEXT        -- Data real do registro
                        )
                    """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_history_path ON file_history(path_rel)");

            st.execute(
                    "CREATE TABLE IF NOT EXISTS scan_issues (id INTEGER PRIMARY KEY AUTOINCREMENT, scan_id INTEGER, path TEXT, message TEXT, created_at TEXT)");
        }
    }

    // ... (startScanLog, finishScanLog, deleteStaleFiles e fetchDirtyFiles
    // permanecem iguais) ...

    public static void updateHashes(Connection c, List<HashUpdate> updates) throws SQLException {
        // MUDANÇA: Status agora é 'HASHED' (temporário) em vez de 'Synced'
        // Isso nos ajuda a identificar quem mudou neste scan exato.
        try (var ps = c.prepareStatement("UPDATE file_inventory SET hash_hex=?, status='HASHED' WHERE path_rel=?")) {
            for (var u : updates) {
                ps.setString(1, u.hashHex());
                ps.setString(2, u.pathRel());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /**
     * --- A MÁGICA DO TIME LAPSE ---
     * Copia para o histórico apenas os arquivos que foram criados (NEW)
     * ou modificados (HASHED/MODIFIED) neste scan.
     */
    public static int snapshotToHistory(Connection c, long scanId) throws SQLException {
        int count = 0;
        try (var st = c.createStatement()) {
            // 1. Copia para o histórico tudo que é relevante deste scan
            String sqlCopy = """
                    INSERT INTO file_history (scan_id, path_rel, hash_hex, size_bytes, status_event, created_at)
                    SELECT last_scan_id, path_rel, hash_hex, size_bytes, status, datetime('now')
                    FROM file_inventory
                    WHERE last_scan_id = """ + scanId + """
                          AND status IN ('NEW', 'HASHED', 'MODIFIED')
                    """;
            count = st.executeUpdate(sqlCopy);

            // 2. Reseta o status para 'STABLE' para que no próximo scan a gente saiba que
            // nada mudou
            st.execute("UPDATE file_inventory SET status='STABLE' WHERE last_scan_id = " + scanId);
        }
        return count;
    }

    public static long startScanLog(Connection c, String rootPath) throws SQLException {
        try (var ps = c.prepareStatement("INSERT INTO scans(root_path, started_at) VALUES(?, datetime('now'))",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, rootPath);
            ps.executeUpdate();
            try (var rs = ps.getGeneratedKeys()) {
                if (rs.next())
                    return rs.getLong(1);
            }
        }
        return 0;
    }

    public static void finishScanLog(Connection c, long scanId) throws SQLException {
        try (var ps = c.prepareStatement("UPDATE scans SET finished_at=datetime('now') WHERE scan_id=?")) {
            ps.setLong(1, scanId);
            ps.executeUpdate();
        }
    }

    public static int deleteStaleFiles(Connection c, long currentScanId) throws SQLException {
        try (var ps = c.prepareStatement("DELETE FROM file_inventory WHERE last_scan_id < ?")) {
            ps.setLong(1, currentScanId);
            return ps.executeUpdate();
        }
    }

    public static List<HashCandidate> fetchDirtyFiles(Connection c, long scanId, int limit) throws SQLException {
        var out = new ArrayList<HashCandidate>();
        try (var ps = c.prepareStatement(
                "SELECT path_rel, size_bytes FROM file_inventory WHERE last_scan_id = ? AND (status = 'NEW' OR status = 'MODIFIED' OR hash_hex IS NULL) LIMIT ?")) {
            ps.setLong(1, scanId);
            ps.setInt(2, limit);
            try (var rs = ps.executeQuery()) {
                while (rs.next())
                    out.add(new HashCandidate(0, rs.getString(1), rs.getLong(2)));
            }
        }
        return out;
    }

    public static List<InventoryRow> fetchInventory(Connection c) throws SQLException {
        ensureSchema(c);
        var out = new ArrayList<InventoryRow>();
        var sql = """
                    SELECT fi.path_rel, fi.name, fi.size_bytes, fi.modified_millis, fi.created_millis, fi.status, fi.hash_hex,
                           COALESCE(s.root_path, '') AS root_path
                    FROM file_inventory fi
                    LEFT JOIN scans s ON s.scan_id = fi.last_scan_id
                    ORDER BY fi.path_rel
                """;
        try (var ps = c.prepareStatement(sql); var rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(new InventoryRow(
                        rs.getString("root_path"),
                        rs.getString("path_rel"),
                        rs.getString("name"),
                        rs.getLong("size_bytes"),
                        rs.getLong("modified_millis"),
                        rs.getLong("created_millis"),
                        rs.getString("status"),
                        rs.getString("hash_hex")));
            }
        }
        return out;
    }

    public static ScanSummary fetchLastScan(Connection c) throws SQLException {
        try (var ps = c.prepareStatement("""
                    SELECT scan_id, root_path, started_at, finished_at
                    FROM scans
                    ORDER BY scan_id DESC
                    LIMIT 1
                """); var rs = ps.executeQuery()) {
            if (rs.next()) {
                return new ScanSummary(
                        rs.getLong("scan_id"),
                        rs.getString("root_path"),
                        rs.getString("started_at"),
                        rs.getString("finished_at"));
            }
        }
        return null;
    }

    public static List<FileHistoryRow> fetchFileHistory(Connection c, String pathRel) throws SQLException {
        ensureSchema(c);
        var out = new ArrayList<FileHistoryRow>();
        var sql = """
                SELECT fh.scan_id, fh.hash_hex, fh.size_bytes, fh.status_event, fh.created_at,
                       s.root_path, s.started_at, s.finished_at
                FROM file_history fh
                LEFT JOIN scans s ON s.scan_id = fh.scan_id
                WHERE fh.path_rel = ?
                ORDER BY fh.scan_id DESC, fh.created_at DESC
                """;
        try (var ps = c.prepareStatement(sql)) {
            ps.setString(1, pathRel);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new FileHistoryRow(
                            rs.getLong("scan_id"),
                            rs.getString("root_path"),
                            rs.getString("started_at"),
                            rs.getString("finished_at"),
                            rs.getString("hash_hex"),
                            rs.getLong("size_bytes"),
                            rs.getString("status_event"),
                            rs.getString("created_at")));
                }
            }
        }
        return out;
    }
}
