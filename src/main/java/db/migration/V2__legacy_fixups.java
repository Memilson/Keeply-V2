package db.migration;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class V2__legacy_fixups extends BaseJavaMigration {

    @Override
    public void migrate(Context context) throws Exception {
        Connection conn = context.getConnection();
        ensureSchema(conn);
        if (tableExists(conn, "scans")) {
            addColumnIfMissing(conn, "scans", "total_usage", "INTEGER");
            addColumnIfMissing(conn, "scans", "status", "TEXT");
        }

        if (tableExists(conn, "file_inventory")) {
            addColumnIfMissing(conn, "file_inventory", "root_path", "TEXT");
            addColumnIfMissing(conn, "file_inventory", "last_scan_id", "INTEGER");
        }

        if (tableExists(conn, "file_history")) {
            addColumnIfMissing(conn, "file_history", "root_path", "TEXT");
            addColumnIfMissing(conn, "file_history", "created_millis", "INTEGER");
            addColumnIfMissing(conn, "file_history", "modified_millis", "INTEGER");
        }

        backfillRootPaths(conn);
        normalizeStatuses(conn);
        backfillScanStatus(conn);
    }

    private void ensureSchema(Connection conn) throws SQLException {
        try (var st = conn.createStatement()) {
            st.execute("""
                CREATE TABLE IF NOT EXISTS scans (
                    scan_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    root_path TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    total_usage INTEGER,
                    status TEXT
                )
                """);

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

            st.execute("""
                CREATE TABLE IF NOT EXISTS scan_issues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id INTEGER,
                    path TEXT,
                    message TEXT,
                    created_at TEXT
                )
                """);
        }
    }

    private void addColumnIfMissing(Connection conn, String table, String column, String type) throws SQLException {
        if (!columnExists(conn, table, column)) {
            try (var st = conn.createStatement()) {
                st.execute("ALTER TABLE " + table + " ADD COLUMN " + column + " " + type);
            }
        }
    }

    private void backfillRootPaths(Connection conn) throws SQLException {
        if (columnExists(conn, "file_inventory", "root_path")
                && columnExists(conn, "file_inventory", "last_scan_id")
                && columnExists(conn, "scans", "root_path")) {
            try (var st = conn.createStatement()) {
                st.execute("""
                    UPDATE file_inventory
                       SET root_path = COALESCE(root_path, (
                           SELECT root_path
                             FROM scans s
                            WHERE s.scan_id = file_inventory.last_scan_id
                       ), '')
                     WHERE root_path IS NULL OR root_path = ''
                    """);
            }
        }

        if (columnExists(conn, "file_history", "root_path")
                && columnExists(conn, "file_history", "scan_id")
                && columnExists(conn, "scans", "root_path")) {
            try (var st = conn.createStatement()) {
                st.execute("""
                    UPDATE file_history
                       SET root_path = COALESCE(root_path, (
                           SELECT root_path
                             FROM scans s
                            WHERE s.scan_id = file_history.scan_id
                       ), '')
                     WHERE root_path IS NULL OR root_path = ''
                    """);
            }
        }
    }

    private void normalizeStatuses(Connection conn) throws SQLException {
        if (columnExists(conn, "file_inventory", "status")) {
            try (var st = conn.createStatement()) {
                st.execute("UPDATE file_inventory SET status = 'STABLE' WHERE status = 'HASHED'");
            }
        }
        if (columnExists(conn, "file_history", "status_event")) {
            try (var st = conn.createStatement()) {
                st.execute("UPDATE file_history SET status_event = 'STABLE' WHERE status_event = 'HASHED'");
            }
        }
    }

    private void backfillScanStatus(Connection conn) throws SQLException {
        if (columnExists(conn, "scans", "status") && columnExists(conn, "scans", "finished_at")) {
            try (var st = conn.createStatement()) {
                st.execute("UPDATE scans SET status = 'DONE' WHERE status IS NULL AND finished_at IS NOT NULL");
                st.execute("UPDATE scans SET status = 'RUNNING' WHERE status IS NULL AND finished_at IS NULL");
            }
        }
    }

    private boolean tableExists(Connection conn, String table) throws SQLException {
        String sql = "SELECT name FROM sqlite_master WHERE type='table' AND name = ?";
        try (var ps = conn.prepareStatement(sql)) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean columnExists(Connection conn, String table, String column) throws SQLException {
        if (!tableExists(conn, table)) return false;
        try (var st = conn.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA table_info(" + table + ")")) {
            while (rs.next()) {
                if (column.equalsIgnoreCase(rs.getString("name"))) return true;
            }
        }
        return false;
    }
}
