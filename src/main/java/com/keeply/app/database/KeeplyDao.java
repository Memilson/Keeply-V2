package com.keeply.app.database;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

import com.keeply.app.database.DatabaseBackup.CapacityReport;
import com.keeply.app.database.DatabaseBackup.FileHistoryRow;
import com.keeply.app.database.DatabaseBackup.InventoryRow;
import com.keeply.app.database.DatabaseBackup.ScanSummary;
import com.keeply.app.database.DatabaseBackup.SnapshotBlobRow;

public interface KeeplyDao {

    // --- Scan log ------------------------------------------------------------

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
           AND status = 'RUNNING'
        """)
    int finishScanLog(@Bind("scanId") long scanId);

    @SqlUpdate("""
        UPDATE scans
           SET finished_at = datetime('now'),
               total_usage = NULL,
               status = 'CANCELED'
         WHERE scan_id = :scanId
           AND status = 'RUNNING'
        """)
    int cancelScanLog(@Bind("scanId") long scanId);

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
        SELECT MIN(scan_id)
          FROM scans
         WHERE root_path = :rootPath
        """)
    Long fetchFirstScanIdForRoot(@Bind("rootPath") String rootPath);

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

    // --- Inventory -----------------------------------------------------------

    @SqlQuery("""
        SELECT fi.root_path AS rootPath,
               fi.path_rel  AS pathRel,
               fi.name      AS name,
               fi.size_bytes AS sizeBytes,
               fi.modified_millis AS modifiedMillis,
               fi.created_millis  AS createdMillis,
               fi.status    AS status
          FROM file_inventory fi
         ORDER BY fi.root_path, fi.path_rel
        """)
    @RegisterConstructorMapper(InventoryRow.class)
    List<InventoryRow> fetchInventory();

    /**
     * Snapshot “estado atual até scanId” (último evento por arquivo), ignorando DELETED.
     * Mantém a regra correta: escolhe o evento mais recente e SÓ DEPOIS filtra DELETED.
     */
    @SqlQuery("""
        WITH target AS (
            SELECT root_path AS root
              FROM scans
             WHERE scan_id = :scanId
        ),
        ranked AS (
            SELECT
                fh.root_path   AS root_path,
                fh.path_rel    AS path_rel,
                fh.size_bytes  AS size_bytes,
                fh.status_event AS status_event,
                fh.created_millis AS created_millis,
                fh.modified_millis AS modified_millis,
                ROW_NUMBER() OVER (
                    PARTITION BY fh.root_path, fh.path_rel
                    ORDER BY fh.scan_id DESC
                ) AS rn
              FROM file_history fh
              JOIN target t ON t.root = fh.root_path
             WHERE fh.scan_id <= :scanId
        )
        SELECT root_path, path_rel, size_bytes, status_event, created_millis, modified_millis
          FROM ranked
         WHERE rn = 1
           AND status_event != 'DELETED'
         ORDER BY path_rel
        """)
    @RegisterRowMapper(InventorySnapshotMapper.class)
    List<InventoryRow> fetchSnapshotFiles(@Bind("scanId") long scanId);

    // --- Stale cleanup -------------------------------------------------------

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

    @SqlUpdate("""
        DELETE FROM file_inventory
         WHERE root_path = :rootPath
           AND last_scan_id < :scanId
        """)
    int deleteStaleFilesRaw(@Bind("scanId") long scanId, @Bind("rootPath") String rootPath);

    @Transaction
    default CleanupResult deleteStaleFiles(long scanId, String rootPath) {
        int archived = copyStaleFilesToHistoryAsDeleted(scanId, rootPath);
        int deleted  = deleteStaleFilesRaw(scanId, rootPath);
        return new CleanupResult(archived, deleted);
    }

    final class CleanupResult {
        public final int archivedAsDeleted;
        public final int removedFromInventory;
        public CleanupResult(int archivedAsDeleted, int removedFromInventory) {
            this.archivedAsDeleted = archivedAsDeleted;
            this.removedFromInventory = removedFromInventory;
        }
    }

    // --- History -------------------------------------------------------------

    @Transaction
    default int snapshotToHistory(long scanId) {
        int inserted = copyToHistory(scanId);
        markStable(scanId);
        return inserted;
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
    int markStable(@Bind("scanId") long scanId);

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

    // --- Blob mapping (per-scan) --------------------------------------------

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
    int setHistoryContentHash(@Bind("scanId") long scanId, @Bind("pathRel") String pathRel, @Bind("contentHash") String contentHash);

    // Batch: muito mais eficiente quando você seta hash de muitos arquivos
    @SqlBatch("""
        UPDATE file_history
           SET content_hash = :contentHash
         WHERE scan_id = :scanId
           AND path_rel = :pathRel
        """)
    int[] setHistoryContentHashes(@BindBean List<HashUpdate> updates);

    final class HashUpdate {
        public final long scanId;
        public final String pathRel;
        public final String contentHash;
        public HashUpdate(long scanId, String pathRel, String contentHash) {
            this.scanId = scanId;
            this.pathRel = pathRel;
            this.contentHash = contentHash;
        }
    }

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
        WITH target AS (
            SELECT root_path AS root
              FROM scans
             WHERE scan_id = :scanId
        ),
        ranked AS (
            SELECT
                fh.path_rel AS pathRel,
                fh.content_hash AS contentHash,
                fh.status_event AS status_event,
                ROW_NUMBER() OVER (
                    PARTITION BY fh.root_path, fh.path_rel
                    ORDER BY fh.scan_id DESC
                ) AS rn
              FROM file_history fh
              JOIN target t ON t.root = fh.root_path
             WHERE fh.scan_id <= :scanId
               AND fh.content_hash IS NOT NULL
        )
        SELECT pathRel, contentHash
          FROM ranked
         WHERE rn = 1
           AND status_event != 'DELETED'
         ORDER BY pathRel
        """)
    @RegisterConstructorMapper(SnapshotBlobRow.class)
    List<SnapshotBlobRow> fetchSnapshotBlobs(@Bind("scanId") long scanId);

    // --- Settings (backup) -------------------------------------------------

    @SqlUpdate("""
        INSERT INTO backup_settings(key, value, updated_at)
        VALUES(:key, :value, datetime('now'))
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        """)
    void upsertSetting(@Bind("key") String key, @Bind("value") String value);

    @SqlQuery("SELECT value FROM backup_settings WHERE key = :key")
    String fetchSetting(@Bind("key") String key);

    // --- Mapper --------------------------------------------------------------

    final class InventorySnapshotMapper implements RowMapper<InventoryRow> {
        @Override
        public InventoryRow map(ResultSet rs, StatementContext ctx) throws SQLException {
            String rootPath = rs.getString("root_path");
            String pathRel  = rs.getString("path_rel");

            String name = basename(pathRel);

            long sizeBytes      = rs.getLong("size_bytes");
            long modifiedMillis = rs.getLong("modified_millis");
            long createdMillis  = rs.getLong("created_millis");
            String status       = rs.getString("status_event");

            return new InventoryRow(rootPath, pathRel, name, sizeBytes, modifiedMillis, createdMillis, status);
        }

        private static String basename(String pathRel) {
            if (pathRel == null || pathRel.isBlank()) return "";
            int idx = pathRel.lastIndexOf('/');
            return (idx >= 0 && idx < pathRel.length() - 1) ? pathRel.substring(idx + 1) : pathRel;
        }
    }
}
