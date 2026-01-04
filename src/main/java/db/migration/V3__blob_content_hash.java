package db.migration;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import java.sql.Connection;
import java.sql.ResultSet;

public final class V3__blob_content_hash extends BaseJavaMigration {

    @Override
    public void migrate(Context context) throws Exception {
        Connection conn = context.getConnection();
        addColumnIfMissing(conn, "file_history", "content_hash", "TEXT");
    }

    private void addColumnIfMissing(Connection conn, String table, String column, String type) throws Exception {
        if (!tableExists(conn, table) || columnExists(conn, table, column)) return;
        try (var st = conn.createStatement()) {
            st.execute("ALTER TABLE " + table + " ADD COLUMN " + column + " " + type);
        }
    }

    private boolean tableExists(Connection conn, String table) throws Exception {
        String sql = "SELECT name FROM sqlite_master WHERE type='table' AND name = ?";
        try (var ps = conn.prepareStatement(sql)) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean columnExists(Connection conn, String table, String column) throws Exception {
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
