package com.keeply.app.database;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

import com.keeply.app.database.Database.CapacityReport;
import com.keeply.app.database.Database.FileHistoryRow;
import com.keeply.app.database.Database.InventoryRow;
import com.keeply.app.database.Database.ScanSummary;
import com.keeply.app.database.Database.SnapshotBlobRow;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public interface KeeplyDao {

    // --- Scan log ---
    @SqlUpdate("INSERT INTO scans(root_path, started_at, status) VALUES(:rootPath, datetime('now'), 'RUNNING')")
    @GetGeneratedKeys("scan_id")
    long startScanLog(@Bind("rootPath") String rootPath);

    @SqlUpdate("""
        UPDATE scans
           SET finished_at = datetime('now'),
               total_usage = COALESCE((
                   SELECT SUM(size_bytes)
                     FROM file_inventory
                    WHERE root_path = scans.root_path
               ), 0),
               status = 'DONE'
         WHERE scan_id = :scanId
        """)
    void finishScanLog(@Bind("scanId") long scanId);

    @SqlUpdate("""
        UPDATE scans
           SET finished_at = datetime('now'),
               total_usage = NULL,
               status = 'CANCELED'
         WHERE scan_id = :scanId
        """)
    void cancelScanLog(@Bind("scanId") long scanId);

    @SqlQuery("""
        SELECT scan_id AS scanId,
               root_path AS rootPath,
               started_at AS startedAt,
               finished_at AS finishedAt
          FROM scans
         ORDER BY scan_id DESC
        """)
    @RegisterConstructorMapper(ScanSummary.class)
    List<ScanSummary> fetchAllScans();

    @SqlQuery("""
        SELECT scan_id AS scanId,
               root_path AS rootPath,
               started_at AS startedAt,
               finished_at AS finishedAt
          FROM scans
         ORDER BY scan_id DESC
         LIMIT 1
        """)
    @RegisterConstructorMapper(ScanSummary.class)
    Optional<ScanSummary> fetchLastScan();

    @SqlQuery("""
        SELECT started_at AS date,
               total_usage AS totalBytes,
               (total_usage - LAG(total_usage, 1, 0) OVER (ORDER BY scan_id)) AS growthBytes
          FROM scans
         WHERE total_usage IS NOT NULL
         ORDER BY scan_id DESC
         LIMIT 10
        """)
    @RegisterConstructorMapper(CapacityReport.class)
    List<CapacityReport> predictGrowth();

    // --- Inventory ---
    @SqlQuery("""
        SELECT fi.root_path AS rootPath,
               fi.path_rel AS pathRel,
               fi.name AS name,
               fi.size_bytes AS sizeBytes,
               fi.modified_millis AS modifiedMillis,
               fi.created_millis AS createdMillis,
               fi.status AS status
          FROM file_inventory fi
         ORDER BY fi.root_path, fi.path_rel
        """)
    @RegisterConstructorMapper(InventoryRow.class)
    List<InventoryRow> fetchInventory();

    @SqlQuery("""
        WITH Target AS (
            SELECT root_path AS root
              FROM scans
             WHERE scan_id = :scanId
        ),
        LatestState AS (
            SELECT root_path, path_rel, MAX(scan_id) AS max_scan
              FROM file_history
             WHERE scan_id <= :scanId
               AND root_path = (SELECT root FROM Target)
          GROUP BY root_path, path_rel
        )
        SELECT fh.root_path,
               fh.path_rel,
               fh.size_bytes,
               fh.status_event,
               fh.created_millis,
               fh.modified_millis
          FROM file_history fh
          JOIN LatestState ls
            ON fh.root_path = ls.root_path
           AND fh.path_rel = ls.path_rel
           AND fh.scan_id = ls.max_scan
                 WHERE fh.status_event != 'DELETED'
         ORDER BY fh.path_rel
        """)
    @RegisterRowMapper(InventorySnapshotMapper.class)
    List<InventoryRow> fetchSnapshotFiles(@Bind("scanId") long scanId);

        @SqlUpdate("""
                INSERT INTO file_history (scan_id, root_path, path_rel, size_bytes, status_event, created_at, created_millis, modified_millis)
                SELECT :scanId,
                             root_path,
                             path_rel,
                             size_bytes,
                             'DELETED',
                             datetime('now'),
                             created_millis,
                             modified_millis
                    FROM file_inventory
                 WHERE root_path = :rootPath
                     AND last_scan_id < :scanId
                """)
        int copyStaleFilesToHistoryAsDeleted(@Bind("scanId") long scanId, @Bind("rootPath") String rootPath);

        @SqlUpdate("DELETE FROM file_inventory WHERE root_path = :rootPath AND last_scan_id < :scanId")
        int deleteStaleFilesRaw(@Bind("scanId") long scanId, @Bind("rootPath") String rootPath);

        @Transaction
        default int deleteStaleFiles(long scanId, String rootPath) {
                copyStaleFilesToHistoryAsDeleted(scanId, rootPath);
                return deleteStaleFilesRaw(scanId, rootPath);
        }

    // --- History ---
    @Transaction
    default int snapshotToHistory(long scanId) {
        int count = copyToHistory(scanId);
        markStable(scanId);
        return count;
    }

    @SqlUpdate("""
        INSERT INTO file_history (scan_id, root_path, path_rel, size_bytes, status_event, created_at, created_millis, modified_millis)
        SELECT last_scan_id, root_path, path_rel, size_bytes, status, datetime('now'), created_millis, modified_millis
          FROM file_inventory
         WHERE last_scan_id = :scanId
           AND status IN ('NEW', 'MODIFIED')
        """)
    int copyToHistory(@Bind("scanId") long scanId);

    @SqlUpdate("UPDATE file_inventory SET status = 'STABLE' WHERE last_scan_id = :scanId")
    void markStable(@Bind("scanId") long scanId);

    @SqlQuery("""
        SELECT fh.scan_id AS scanId,
               s.root_path AS rootPath,
               s.started_at AS startedAt,
               s.finished_at AS finishedAt,
               fh.size_bytes AS sizeBytes,
               fh.status_event AS statusEvent,
               fh.created_at AS createdAt
          FROM file_history fh
          LEFT JOIN scans s ON s.scan_id = fh.scan_id
         WHERE fh.path_rel = :pathRel
         ORDER BY fh.scan_id DESC
        """)
    @RegisterConstructorMapper(FileHistoryRow.class)
    List<FileHistoryRow> fetchFileHistory(@Bind("pathRel") String pathRel);

        // --- Blob mapping (per-scan) ---
        @SqlQuery("""
                SELECT path_rel
                    FROM file_history
                 WHERE scan_id = :scanId
                     AND status_event IN ('NEW', 'MODIFIED')
                 ORDER BY path_rel
                """)
        List<String> fetchChangedFilesForScan(@Bind("scanId") long scanId);

        @SqlUpdate("""
                UPDATE file_history
                     SET content_hash = :contentHash
                 WHERE scan_id = :scanId
                     AND path_rel = :pathRel
                """)
        void setHistoryContentHash(@Bind("scanId") long scanId, @Bind("pathRel") String pathRel, @Bind("contentHash") String contentHash);

        @SqlQuery("""
                SELECT path_rel AS pathRel,
                             content_hash AS contentHash
                    FROM file_history
                 WHERE scan_id = :scanId
                     AND status_event IN ('NEW', 'MODIFIED')
                     AND content_hash IS NOT NULL
                 ORDER BY path_rel
                """)
        @RegisterConstructorMapper(SnapshotBlobRow.class)
        List<SnapshotBlobRow> fetchChangedBlobsForScan(@Bind("scanId") long scanId);

        @SqlQuery("""
                WITH Target AS (
                        SELECT root_path AS root
                            FROM scans
                         WHERE scan_id = :scanId
                ),
                LatestState AS (
                        SELECT root_path, path_rel, MAX(scan_id) AS max_scan
                            FROM file_history
                         WHERE scan_id <= :scanId
                             AND root_path = (SELECT root FROM Target)
                    GROUP BY root_path, path_rel
                )
                SELECT fh.path_rel AS pathRel,
                             fh.content_hash AS contentHash
                    FROM file_history fh
                    JOIN LatestState ls
                        ON fh.root_path = ls.root_path
                     AND fh.path_rel = ls.path_rel
                     AND fh.scan_id = ls.max_scan
                                 WHERE fh.content_hash IS NOT NULL
                                     AND fh.status_event != 'DELETED'
                 ORDER BY fh.path_rel
                """)
        @RegisterConstructorMapper(SnapshotBlobRow.class)
        List<SnapshotBlobRow> fetchSnapshotBlobs(@Bind("scanId") long scanId);

    final class InventorySnapshotMapper implements RowMapper<InventoryRow> {
        @Override
        public InventoryRow map(ResultSet rs, StatementContext ctx) throws SQLException {
            String rootPath = rs.getString("root_path");
            String pathRel = rs.getString("path_rel");
            String name = "";
            if (pathRel != null) {
                int idx = pathRel.lastIndexOf('/');
                name = (idx >= 0 && idx < pathRel.length() - 1) ? pathRel.substring(idx + 1) : pathRel;
            }
            long sizeBytes = rs.getLong("size_bytes");
            long modifiedMillis = rs.getLong("modified_millis");
            long createdMillis = rs.getLong("created_millis");
            String status = rs.getString("status_event");
            return new InventoryRow(rootPath, pathRel, name, sizeBytes, modifiedMillis, createdMillis, status);
        }
    }
}
