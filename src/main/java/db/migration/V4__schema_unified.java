package db.migration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

public final class V4__schema_unified extends BaseJavaMigration {

    // Tipos permitidos pra não virar "ALTER TABLE ... ADD COLUMN foo BLAH BLAH"
    private static final Set<String> ALLOWED_TYPES =
            Set.of("INTEGER", "TEXT", "BLOB", "REAL", "NUMERIC");

    @Override
    public void migrate(Context context) throws Exception {
        Connection conn = context.getConnection();

        try (Statement st = conn.createStatement()) {
            st.execute("PRAGMA foreign_keys=ON");
            st.execute("PRAGMA busy_timeout=5000");
        }

        // Flyway roda migration versionada 1x, mas manter idempotente ajuda em DBs “tortos”.
        ensureSchema(conn);

        // cache de schema pra evitar bater em sqlite_master/pragma mil vezes
        SchemaInfo schema = SchemaInfo.load(conn);

        // scans
        addColumnIfMissing(conn, schema, "scans", "total_usage", "INTEGER");
        addColumnIfMissing(conn, schema, "scans", "status", "TEXT");

        // file_inventory
        addColumnIfMissing(conn, schema, "file_inventory", "root_path", "TEXT");
        addColumnIfMissing(conn, schema, "file_inventory", "last_scan_id", "INTEGER");
        addColumnIfMissing(conn, schema, "file_inventory", "status", "TEXT");
        addColumnIfMissing(conn, schema, "file_inventory", "created_millis", "INTEGER");
        addColumnIfMissing(conn, schema, "file_inventory", "modified_millis", "INTEGER");

        // file_history
        addColumnIfMissing(conn, schema, "file_history", "root_path", "TEXT");
        addColumnIfMissing(conn, schema, "file_history", "created_millis", "INTEGER");
        addColumnIfMissing(conn, schema, "file_history", "modified_millis", "INTEGER");
        addColumnIfMissing(conn, schema, "file_history", "content_hash", "TEXT");

        // índices alinhados com KeeplyDao
        ensureIndexes(conn);

        // higiene de dados (melhorada)
        backfillRootPaths(conn, schema);
        normalizeStatuses(conn, schema);
        backfillScanStatus(conn, schema);
    }

    private void ensureSchema(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
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
                    modified_millis INTEGER,
                    content_hash TEXT
                )
                """);

            st.execute("""
                CREATE TABLE IF NOT EXISTS scan_issues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id INTEGER,
                    path TEXT,
                    message TEXT,
                    created_at TEXT
                )
                """);

                        st.execute("""
                                CREATE TABLE IF NOT EXISTS journal_cursors (
                                    root_path  TEXT PRIMARY KEY,
                                    backend    TEXT NOT NULL,              -- WATCHSERVICE | USN
                                    cursor_a   TEXT,                       -- ex: last_usn
                                    cursor_b   TEXT,                       -- ex: journal_id
                                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                                )
                                """);

                        st.execute("""
                                CREATE TABLE IF NOT EXISTS fs_events (
                                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                                    root_path     TEXT NOT NULL,
                                    path_rel      TEXT,
                                    kind          TEXT NOT NULL,           -- CREATE|MODIFY|DELETE|MOVE|OVERFLOW
                                    old_path_rel  TEXT,
                                    event_time    TEXT NOT NULL DEFAULT (datetime('now'))
                                )
                                """);
        }
    }

    private void ensureIndexes(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            // Snapshot eficiente (root + path + scan) => bate direto no que seu DAO precisa
            st.execute("""
                CREATE INDEX IF NOT EXISTS idx_file_history_root_path_scan
                ON file_history(root_path, path_rel, scan_id)
                """);

            // Cleanup “stale” do inventory
            st.execute("""
                CREATE INDEX IF NOT EXISTS idx_file_inventory_root_lastscan
                ON file_inventory(root_path, last_scan_id)
                """);

            // Mudanças por scan (NEW/MODIFIED)
            st.execute("""
                CREATE INDEX IF NOT EXISTS idx_file_history_scan_status_path
                ON file_history(scan_id, status_event, path_rel)
                """);

            // Blobs/hash por scan (ajuda fetchChangedBlobsForScan e setHistoryContentHash)
            st.execute("""
                CREATE INDEX IF NOT EXISTS idx_file_history_scan_path_hash
                ON file_history(scan_id, path_rel, content_hash)
                """);

            // scans por root (ajuda target/root e relatórios)
            st.execute("""
                CREATE INDEX IF NOT EXISTS idx_scans_root_scanid
                ON scans(root_path, scan_id)
                """);
        }
    }

    private void addColumnIfMissing(Connection conn, SchemaInfo schema, String table, String column, String type)
            throws SQLException {

        if (!ALLOWED_TYPES.contains(type)) {
            throw new IllegalArgumentException("Tipo SQL não permitido: " + type);
        }
        if (!schema.tableExists(table)) return;
        if (schema.columnExists(table, column)) return;

        String sql = "ALTER TABLE " + q(table) + " ADD COLUMN " + q(column) + " " + type;
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }

        schema.addColumn(table, column); // mantém cache consistente
    }

    private void backfillRootPaths(Connection conn, SchemaInfo schema) throws SQLException {
        // file_inventory.root_path a partir de scans.root_path via last_scan_id
        if (schema.hasColumns("file_inventory", "root_path", "last_scan_id")
                && schema.hasColumns("scans", "scan_id", "root_path")) {

            try (Statement st = conn.createStatement()) {
                // Não coloca '' — se não der pra inferir, deixa NULL.
                st.execute("""
                    UPDATE file_inventory
                       SET root_path = (
                           SELECT s.root_path
                             FROM scans s
                            WHERE s.scan_id = file_inventory.last_scan_id
                       )
                     WHERE (root_path IS NULL OR root_path = '')
                       AND last_scan_id IS NOT NULL
                       AND EXISTS (
                           SELECT 1
                             FROM scans s
                            WHERE s.scan_id = file_inventory.last_scan_id
                              AND s.root_path IS NOT NULL
                              AND s.root_path != ''
                       )
                    """);
            }
        }

        // file_history.root_path a partir de scans.root_path via scan_id
        if (schema.hasColumns("file_history", "root_path", "scan_id")
                && schema.hasColumns("scans", "scan_id", "root_path")) {

            try (Statement st = conn.createStatement()) {
                st.execute("""
                    UPDATE file_history
                       SET root_path = (
                           SELECT s.root_path
                             FROM scans s
                            WHERE s.scan_id = file_history.scan_id
                       )
                     WHERE (root_path IS NULL OR root_path = '')
                       AND scan_id IS NOT NULL
                       AND EXISTS (
                           SELECT 1
                             FROM scans s
                            WHERE s.scan_id = file_history.scan_id
                              AND s.root_path IS NOT NULL
                              AND s.root_path != ''
                       )
                    """);
            }
        }
    }

    private void normalizeStatuses(Connection conn, SchemaInfo schema) throws SQLException {
        try (Statement st = conn.createStatement()) {
            if (schema.columnExists("file_inventory", "status")) {
                st.execute("UPDATE file_inventory SET status = 'STABLE' WHERE status = 'HASHED'");
            }
            if (schema.columnExists("file_history", "status_event")) {
                st.execute("UPDATE file_history SET status_event = 'STABLE' WHERE status_event = 'HASHED'");
            }
        }
    }

    private void backfillScanStatus(Connection conn, SchemaInfo schema) throws SQLException {
        if (!schema.hasColumns("scans", "status", "finished_at")) return;
        try (Statement st = conn.createStatement()) {
            st.execute("UPDATE scans SET status = 'DONE' WHERE status IS NULL AND finished_at IS NOT NULL");
            st.execute("UPDATE scans SET status = 'RUNNING' WHERE status IS NULL AND finished_at IS NULL");
        }
    }

    // ---------- helpers ----------

    private static String q(String ident) {
        // quoting simples do SQLite. Como tudo aqui é constante do app,
        // isso é mais pra robustez do que segurança.
        if (ident == null || ident.isBlank()) throw new IllegalArgumentException("Identificador vazio");
        // evita aspas internas quebrando a query
        if (ident.contains("\"")) throw new IllegalArgumentException("Identificador inválido: " + ident);
        return "\"" + ident + "\"";
    }

    private static final class SchemaInfo {
        private final Set<String> tables = new HashSet<>();
        private final Map<String, Set<String>> columnsByTable = new HashMap<>();

        static SchemaInfo load(Connection conn) throws SQLException {
            SchemaInfo s = new SchemaInfo();
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT name FROM sqlite_master WHERE type='table'")) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String t = rs.getString(1);
                        s.tables.add(t);
                    }
                }
            }
            for (String t : s.tables) {
                s.columnsByTable.put(t, readColumns(conn, t));
            }
            return s;
        }

        boolean tableExists(String table) {
            return tables.contains(table);
        }

        boolean columnExists(String table, String column) {
            return columnsByTable.getOrDefault(table, Set.of()).contains(column.toLowerCase(Locale.ROOT));
        }

        boolean hasColumns(String table, String... cols) {
            for (String c : cols) {
                if (!columnExists(table, c)) return false;
            }
            return true;
        }

        void addColumn(String table, String column) {
            columnsByTable.computeIfAbsent(table, k -> new HashSet<>())
                    .add(column.toLowerCase(Locale.ROOT));
        }

        private static Set<String> readColumns(Connection conn, String table) throws SQLException {
            Set<String> cols = new HashSet<>();
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("PRAGMA table_info(" + q(table) + ")")) {
                while (rs.next()) {
                    cols.add(rs.getString("name").toLowerCase(Locale.ROOT));
                }
            }
            return cols;
        }
    }
}
