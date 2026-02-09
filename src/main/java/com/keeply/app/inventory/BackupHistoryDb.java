package com.keeply.app.inventory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.keeply.app.config.Config;
import com.keeply.app.database.DatabaseBackup;

public final class BackupHistoryDb {

    private BackupHistoryDb() {}

    public record HistoryRow(
            long id,
            String startedAt,
            String finishedAt,
            String status,
            String backupType,
            String rootPath,
            String destPath,
            long filesProcessed,
            long errors,
            Long scanId,
            String message
    ) {}

    public static void init() {
        DatabaseBackup.init();
        try (Connection c = open();
             PreparedStatement ps = c.prepareStatement(
                     "CREATE TABLE IF NOT EXISTS backup_history (" +
                             "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                             "started_at TEXT NOT NULL," +
                             "finished_at TEXT," +
                             "status TEXT NOT NULL," +
                             "backup_type TEXT," +
                             "root_path TEXT," +
                             "dest_path TEXT," +
                             "files_processed INTEGER DEFAULT 0," +
                             "errors INTEGER DEFAULT 0," +
                             "scan_id INTEGER," +
                             "message TEXT" +
                             ")"
             )) {
            ps.execute();
            ensureColumn(c, "backup_history", "backup_type", "ALTER TABLE backup_history ADD COLUMN backup_type TEXT");
        } catch (Exception ignored) {
        }
    }

    public static long start(String rootPath, String destPath) {
        init();
        String now = Instant.now().toString();
        try (Connection c = open();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO backup_history(started_at, status, root_path, dest_path, files_processed, errors) VALUES(?,?,?,?,?,?)",
                     PreparedStatement.RETURN_GENERATED_KEYS
             )) {
            ps.setString(1, now);
            ps.setString(2, "RUNNING");
            ps.setString(3, rootPath);
            ps.setString(4, destPath);
            ps.setLong(5, 0);
            ps.setLong(6, 0);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) return rs.getLong(1);
            }
        } catch (Exception ignored) {
        }
        return -1;
    }

    public static void finish(long id, String status, long files, long errors, Long scanId, String message, String backupType) {
        if (id <= 0) return;
        String now = Instant.now().toString();
        try (Connection c = open();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE backup_history SET finished_at=?, status=?, files_processed=?, errors=?, scan_id=?, message=?, backup_type=? WHERE id=?"
             )) {
            ps.setString(1, now);
            ps.setString(2, status);
            ps.setLong(3, files);
            ps.setLong(4, errors);
            if (scanId == null) ps.setNull(5, java.sql.Types.INTEGER);
            else ps.setLong(5, scanId);
            ps.setString(6, message);
            ps.setString(7, backupType);
            ps.setLong(8, id);
            ps.executeUpdate();
        } catch (Exception ignored) {
        }
    }

    public static List<HistoryRow> listRecent(int limit) {
        init();
        List<HistoryRow> out = new ArrayList<>();
        try (Connection c = open();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT id, started_at, finished_at, status, backup_type, root_path, dest_path, files_processed, errors, scan_id, message " +
                             "FROM backup_history ORDER BY id DESC LIMIT ?"
             )) {
            ps.setInt(1, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new HistoryRow(
                            rs.getLong(1),
                            rs.getString(2),
                            rs.getString(3),
                            rs.getString(4),
                            rs.getString(5),
                            rs.getString(6),
                            rs.getString(7),
                            rs.getLong(8),
                            rs.getLong(9),
                            (rs.getObject(10) == null) ? null : rs.getLong(10),
                            rs.getString(11)
                    ));
                }
            }
        } catch (Exception ignored) {
        }
        return out;
    }

    private static Connection open() throws Exception {
        return DriverManager.getConnection(Config.getDbUrl());
    }

    private static void ensureColumn(Connection c, String table, String column, String ddl) {
        try (PreparedStatement ps = c.prepareStatement("PRAGMA table_info(" + table + ")")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("name");
                    if (column.equalsIgnoreCase(name)) return;
                }
            }
            try (PreparedStatement alter = c.prepareStatement(ddl)) {
                alter.execute();
            }
        } catch (Exception ignored) {
        }
    }
}
