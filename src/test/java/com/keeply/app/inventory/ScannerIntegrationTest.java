package com.keeply.app.inventory;

import com.keeply.app.config.Config;
import com.keeply.app.database.Database;
import com.keeply.app.database.KeeplyDao;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class ScannerIntegrationTest {

    @AfterEach
    void tearDown() {
        Database.shutdown();
    }

    @Test
    void runScan_persistsInventory_andDetectsModificationInHistory() throws Exception {
        assertTrue(Config.isDbEncryptionEnabled(), "Tests run with file encryption enabled via Surefire properties");

        // Unique file names to avoid collisions with other tests (history query uses only pathRel).
        String fileAName = "keeply-test-" + UUID.randomUUID() + "-a.txt";
        String fileBName = "keeply-test-" + UUID.randomUUID() + "-b.txt";

        Path root = Files.createTempDirectory("keeply-scan-root-");
        Files.writeString(root.resolve(fileAName), "hello", StandardCharsets.UTF_8);
        Files.writeString(root.resolve(fileBName), "world", StandardCharsets.UTF_8);

        Backup.ScanConfig cfg = Backup.ScanConfig.defaults();
        Backup.ScanMetrics metrics = new Backup.ScanMetrics();
        AtomicBoolean cancel = new AtomicBoolean(false);

        Database.init();
        Database.SimplePool pool = new Database.SimplePool(Config.getDbUrl(), 4);

        Backup.runScan(root, cfg, pool, metrics, cancel, s -> { /* no-op */ });

        assertTrue(metrics.filesSeen.sum() >= 2, "Expected scanner to see at least 2 files");

        String rootAbs = root.toAbsolutePath().normalize().toString();

        List<Database.InventoryRow> inventory = Database.jdbi().withExtension(KeeplyDao.class, KeeplyDao::fetchInventory);
        List<Database.InventoryRow> ours = inventory.stream().filter(r -> r.rootPath() != null && r.rootPath().equalsIgnoreCase(rootAbs)).toList();

        assertTrue(ours.size() >= 2, "Expected at least 2 inventory rows for the test root");
        assertTrue(ours.stream().allMatch(r -> "STABLE".equals(r.status())), "After scan, statuses should be STABLE");

        var lastScan = Database.jdbi().withExtension(KeeplyDao.class, KeeplyDao::fetchLastScan).orElseThrow();
        assertTrue(lastScan.rootPath() != null && lastScan.rootPath().equalsIgnoreCase(rootAbs));
        assertNotNull(lastScan.finishedAt(), "Scan should be marked finished");

        // Modify one file and rescan.
        Files.writeString(root.resolve(fileAName), "hello!!", StandardCharsets.UTF_8);

        Backup.ScanMetrics metrics2 = new Backup.ScanMetrics();
        Backup.runScan(root, cfg, pool, metrics2, cancel, s -> { /* no-op */ });

        String relA = fileAName;
        var history = Database.jdbi().withExtension(KeeplyDao.class, dao -> dao.fetchFileHistory(relA));

        // Expect at least one NEW (first scan) and one MODIFIED (second scan) for this pathRel.
        assertTrue(history.size() >= 2, "Expected at least 2 history events for modified file");
        assertTrue(history.stream().anyMatch(h -> "NEW".equals(h.statusEvent())), "Expected a NEW event in history");
        assertTrue(history.stream().anyMatch(h -> "MODIFIED".equals(h.statusEvent())), "Expected a MODIFIED event in history");
    }
}
