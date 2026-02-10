package db.migration;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

public final class V5__backup_settings extends BaseJavaMigration {
    @Override
    public void migrate(Context context) throws Exception {
        Connection conn = context.getConnection();
        ensureSchema(conn);
    }

    private void ensureSchema(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("""
                CREATE TABLE IF NOT EXISTS backup_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """);
        }
    }
}
