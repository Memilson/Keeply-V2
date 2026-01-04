package com.keeply.app.database;

import com.keeply.app.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class DatabaseLifecycleTest {

    @AfterEach
    void tearDown() {
        // Make sure connections are closed between tests.
        Database.shutdown();
    }

    @Test
    void initCreatesRuntime_andShutdownPersistsEncrypted_andCleansRuntime() throws Exception {
        assertTrue(Config.isDbEncryptionEnabled(), "This lifecycle test expects file encryption enabled");

        Path encrypted = Config.getEncryptedDbFilePath();
        Path runtime = Config.getRuntimeDbFilePath();
        Path runtimeWal = runtime.resolveSibling(runtime.getFileName().toString() + "-wal");
        Path runtimeShm = runtime.resolveSibling(runtime.getFileName().toString() + "-shm");

        // Start from a clean slate for this test.
        Database.shutdown();
        Files.deleteIfExists(runtime);
        Files.deleteIfExists(runtimeWal);
        Files.deleteIfExists(runtimeShm);
        Files.deleteIfExists(encrypted);

        Database.init();
        // Touch the DB to ensure the runtime file is created.
        Database.jdbi().withHandle(h -> h.createQuery("SELECT 1").mapTo(int.class).one());

        assertTrue(Files.exists(runtime), "Runtime plaintext sqlite file should exist while DB is initialized");
        assertFalse(DbFileCrypto.looksLikeKeeplyEncrypted(runtime), "Runtime file must not have KEEPLYENC header");

        Database.shutdown();

        assertFalse(Files.exists(runtime), "Runtime plaintext sqlite file should be removed on shutdown");
        assertFalse(Files.exists(runtimeWal), "Runtime WAL should be removed on shutdown");
        assertFalse(Files.exists(runtimeShm), "Runtime SHM should be removed on shutdown");

        assertTrue(Files.exists(encrypted), "Encrypted at-rest file should exist after shutdown");
        assertTrue(DbFileCrypto.looksLikeKeeplyEncrypted(encrypted), "Encrypted file must have KEEPLYENC header");
        assertFalse(DbFileCrypto.looksLikePlainSqlite(encrypted), "Encrypted file must not look like plain SQLite");

        Database.DbEncryptionStatus status = Database.getEncryptionStatus();
        assertTrue(status.encryptedFileExists());
        assertTrue(status.encryptedLooksEncrypted());
        assertFalse(status.runtimePlainExists());
    }
}
