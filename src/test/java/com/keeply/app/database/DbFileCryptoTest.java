package com.keeply.app.database;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class DbFileCryptoTest {

    @Test
    void encryptDecrypt_roundTrip_preservesBytes() throws Exception {
        Path dir = Files.createTempDirectory("keeply-crypto-test-");
        Path runtime = dir.resolve("db.runtime.sqlite");
        Path enc = dir.resolve("db.keeply.enc");
        Path restored = dir.resolve("db.restored.sqlite");

        byte[] original = "hello-keeply".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Files.write(runtime, original);

        DbFileCrypto.encryptFromRuntime(runtime, enc, "pass-123");
        assertTrue(Files.exists(enc));
        assertTrue(DbFileCrypto.looksLikeKeeplyEncrypted(enc));
        assertFalse(DbFileCrypto.looksLikePlainSqlite(enc));

        DbFileCrypto.decryptToRuntime(enc, restored, "pass-123");
        assertArrayEquals(original, Files.readAllBytes(restored));
    }

    @Test
    void decrypt_withWrongPassphrase_fails() throws Exception {
        Path dir = Files.createTempDirectory("keeply-crypto-test-");
        Path runtime = dir.resolve("db.runtime.sqlite");
        Path enc = dir.resolve("db.keeply.enc");
        Path restored = dir.resolve("db.restored.sqlite");

        Files.write(runtime, "secret".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        DbFileCrypto.encryptFromRuntime(runtime, enc, "correct");

        assertThrows(Exception.class, () -> DbFileCrypto.decryptToRuntime(enc, restored, "wrong"));
    }

    @Test
    void looksLikePlainSqlite_detectsHeader() throws Exception {
        Path dir = Files.createTempDirectory("keeply-crypto-test-");
        Path sqlite = dir.resolve("plain.sqlite");

        // SQLite header: "SQLite format 3\0" (16 bytes total prefix)
        byte[] header = new byte[16];
        byte[] h = "SQLite format 3\u0000".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        System.arraycopy(h, 0, header, 0, Math.min(h.length, header.length));

        Files.write(sqlite, header);
        assertTrue(DbFileCrypto.looksLikePlainSqlite(sqlite));
    }
}
