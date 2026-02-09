package com.keeply.app.database;

import com.keeply.app.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.exception.FlywayValidateException;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * Gerencia banco de dados, migrações e criptografia.
 */
public final class DatabaseBackup {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseBackup.class);

    private DatabaseBackup() {}

    public record InventoryRow(String rootPath, String pathRel, String name, long sizeBytes, long modifiedMillis,
                               long createdMillis, String status) {}
    public record ScanSummary(long scanId, String rootPath, String startedAt, String finishedAt) {}
    public record FileHistoryRow(long scanId, String rootPath, String startedAt, String finishedAt, long sizeBytes,
                                 String statusEvent, String createdAt) {}
    public record SnapshotBlobRow(String pathRel, String contentHash) {}
    public record CapacityReport(String date, long totalBytes, long growthBytes) {}

    private static HikariDataSource dataSource;
    private static Jdbi jdbi;
    private static boolean migrated = false;
    private static volatile boolean shutdownHookRegistered = false;

    public record DbEncryptionStatus(
            boolean encryptionEnabled,
            java.nio.file.Path encryptedFile,
            boolean encryptedFileExists,
            boolean encryptedLooksEncrypted,
            boolean encryptedLooksPlainSqlite,
            java.nio.file.Path legacyPlainFile,
            boolean legacyPlainExists,
            boolean legacyPlainLooksPlainSqlite,
            boolean legacyPlainWalExists,
            boolean legacyPlainShmExists,
            java.nio.file.Path runtimePlainFile,
            boolean runtimePlainExists,
            boolean runtimePlainWalExists,
            boolean runtimePlainShmExists
    ) {}

    public static DbEncryptionStatus getEncryptionStatus() {
        boolean enabled = Config.isDbEncryptionEnabled();

        var encrypted = Config.getEncryptedDbFilePath();
        var legacyPlain = enabled ? encrypted.resolveSibling(removeSuffix(encrypted.getFileName().toString(), ".enc")) : encrypted;
        var runtime = Config.getRuntimeDbFilePath();

        boolean encryptedExists = Files.exists(encrypted);
        boolean encLooksEncrypted = DbFileCrypto.looksLikeKeeplyEncrypted(encrypted);
        boolean encLooksPlain = DbFileCrypto.looksLikePlainSqlite(encrypted);

        boolean legacyExists = Files.exists(legacyPlain);
        boolean legacyLooksPlain = DbFileCrypto.looksLikePlainSqlite(legacyPlain);

        boolean legacyWal = Files.exists(legacyPlain.resolveSibling(legacyPlain.getFileName().toString() + "-wal"));
        boolean legacyShm = Files.exists(legacyPlain.resolveSibling(legacyPlain.getFileName().toString() + "-shm"));

        boolean runtimeExists = Files.exists(runtime);
        boolean runtimeWal = Files.exists(runtime.resolveSibling(runtime.getFileName().toString() + "-wal"));
        boolean runtimeShm = Files.exists(runtime.resolveSibling(runtime.getFileName().toString() + "-shm"));

        return new DbEncryptionStatus(
                enabled,
                encrypted,
                encryptedExists,
                encLooksEncrypted,
                encLooksPlain,
                legacyPlain,
                legacyExists,
                legacyLooksPlain,
                legacyWal,
                legacyShm,
                runtime,
                runtimeExists,
                runtimeWal,
                runtimeShm
        );
    }

    public record SelfTestResult(boolean ok, String report) {}

    public static SelfTestResult runBasicSelfTests() {
        StringBuilder r = new StringBuilder();
        boolean ok = true;

        try {
            DbEncryptionStatus s = getEncryptionStatus();
            r.append("Criptografia habilitada: ").append(s.encryptionEnabled()).append('\n');
            r.append(".enc existe: ").append(s.encryptedFileExists()).append('\n');
            r.append(".enc header KEEPLYENC: ").append(s.encryptedLooksEncrypted()).append('\n');
            r.append("Plaintext legado existe: ").append(s.legacyPlainExists()).append('\n');
            r.append("Runtime existe agora: ").append(s.runtimePlainExists()).append('\n');
            r.append('\n');

            if (s.encryptionEnabled()) {
                if (!s.encryptedFileExists() || !s.encryptedLooksEncrypted() || s.encryptedLooksPlainSqlite()) {
                    ok = false;
                    r.append("[FAIL] Arquivo .enc não parece criptografado corretamente.\n");
                }
            }
        } catch (Exception e) {
            ok = false;
            r.append("[FAIL] Status do DB falhou: ").append(e.getMessage()).append('\n');
        }

        try {
            Path dir = Files.createTempDirectory("keeply-selftest-");
            Path runtime = dir.resolve("self.runtime.sqlite");
            Path enc = dir.resolve("self.keeply.enc");
            Path restored = dir.resolve("self.restored.sqlite");

            byte[] payload = "keeply-selftest".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            Files.write(runtime, payload);

            DbFileCrypto.encryptFromRuntime(runtime, enc, "test-pass");
            if (!DbFileCrypto.looksLikeKeeplyEncrypted(enc)) {
                ok = false;
                r.append("[FAIL] Roundtrip: .enc não tem header KEEPLYENC\n");
            }

            DbFileCrypto.decryptToRuntime(enc, restored, "test-pass");
            byte[] out = Files.readAllBytes(restored);
            if (!Arrays.equals(payload, out)) {
                ok = false;
                r.append("[FAIL] Roundtrip: bytes não batem\n");
            } else {
                r.append("[OK] Roundtrip AES-GCM passou\n");
            }

            try {
                DbFileCrypto.decryptToRuntime(enc, dir.resolve("wrong.sqlite"), "wrong");
                ok = false;
                r.append("[FAIL] Roundtrip: senha errada não falhou\n");
            } catch (Exception expected) {
                r.append("[OK] Senha errada falha (esperado)\n");
            }

        } catch (Exception e) {
            ok = false;
            r.append("[FAIL] Roundtrip AES-GCM falhou: ").append(e.getMessage()).append('\n');
        }

        return new SelfTestResult(ok, r.toString());
    }

    private static String removeSuffix(String s, String suffix) {
        if (s == null || suffix == null) return s;
        if (s.endsWith(suffix)) return s.substring(0, s.length() - suffix.length());
        return s;
    }

    private static synchronized void ensureShutdownHook() {
        if (shutdownHookRegistered) return;
        shutdownHookRegistered = true;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                DatabaseBackup.shutdown();
            } catch (Exception ignored) {
            }
        }, "keeply-db-shutdown"));
    }

    private static synchronized void createDataSource() {
        if (dataSource != null) return;
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(Config.getDbUrl());

        config.setConnectionTestQuery("SELECT 1");
        config.setMaximumPoolSize(10);
        if (Config.isDbEncryptionEnabled()) {
            config.setConnectionInitSql("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA busy_timeout=10000;");
        } else {
            config.setConnectionInitSql("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=10000;");
        }

        dataSource = new HikariDataSource(config);
    }

    public static synchronized void init() {
        if (dataSource == null) {
            if (Config.isDbEncryptionEnabled()) {
                ensureShutdownHook();
                prepareRuntimeDbFile();
            }
            createDataSource();
        }
        if (jdbi == null) {
            jdbi = Jdbi.create(dataSource);
            jdbi.installPlugin(new SqlObjectPlugin());
        }
        migrateIfNeeded();
    }

    private static void prepareRuntimeDbFile() {
        try {
            var encrypted = Config.getEncryptedDbFilePath();
            var runtime = Config.getRuntimeDbFilePath();

            var runtimeWal = runtime.resolveSibling(runtime.getFileName().toString() + "-wal");
            var runtimeShm = runtime.resolveSibling(runtime.getFileName().toString() + "-shm");

            try { Files.deleteIfExists(runtime); } catch (java.io.IOException ignored) {}
            try { Files.deleteIfExists(runtimeWal); } catch (java.io.IOException ignored) {}
            try { Files.deleteIfExists(runtimeShm); } catch (java.io.IOException ignored) {}

            if (Files.exists(encrypted)) {
                if (DbFileCrypto.looksLikePlainSqlite(encrypted)) {
                    throw new IllegalStateException(
                            "DB encryption habilitada, mas o arquivo persistido parece SQLite plaintext: " + encrypted +
                            ". Como você pediu sem migração, apague esse arquivo ou use o sufixo .enc (Config.getEncryptedDbFilePath)."
                    );
                }
                DbFileCrypto.decryptToRuntime(encrypted, runtime, Config.getSecretKey());
            }
        } catch (java.io.IOException | RuntimeException e) {
            throw new IllegalStateException("Falha ao preparar DB criptografado (arquivo runtime)", e);
        }
    }

    private static void checkpointSqliteIfPossible() {
        if (dataSource == null) return;
        try (Connection c = dataSource.getConnection()) {
            try (PreparedStatement ps = c.prepareStatement("PRAGMA wal_checkpoint(TRUNCATE);");) {
                ps.execute();
            } catch (java.sql.SQLException ignored) {
            }
        } catch (java.sql.SQLException ignored) {
        }
    }

    private static synchronized void migrateIfNeeded() {
        if (migrated) return;
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load();
        try {
            try {
                flyway.migrate();
            } catch (FlywayValidateException e) {
                logger.warn("Flyway validation failed; attempting repair.", e);
                flyway.repair();
                flyway.migrate();
            }
            migrated = true;
        } catch (RuntimeException e) {
            logger.error("Flyway migration failed", e);
            throw new IllegalStateException("Flyway migration failed", e);
        }
    }

    public static synchronized void shutdown() {
        if (Config.isDbEncryptionEnabled()) {
            checkpointSqliteIfPossible();
        }

        if (dataSource != null) {
            try { dataSource.close(); } catch (RuntimeException ignored) {}
            dataSource = null;
        }
        jdbi = null;
        migrated = false;

        if (Config.isDbEncryptionEnabled()) {
            try {
                var runtime = Config.getRuntimeDbFilePath();
                var encrypted = Config.getEncryptedDbFilePath();

                var runtimeWal = runtime.resolveSibling(runtime.getFileName().toString() + "-wal");
                var runtimeShm = runtime.resolveSibling(runtime.getFileName().toString() + "-shm");

                DbFileCrypto.encryptFromRuntime(runtime, encrypted, Config.getSecretKey());
                try { Files.deleteIfExists(runtime); } catch (java.io.IOException ignored) {}
                try { Files.deleteIfExists(runtimeWal); } catch (java.io.IOException ignored) {}
                try { Files.deleteIfExists(runtimeShm); } catch (java.io.IOException ignored) {}
            } catch (java.io.IOException | RuntimeException e) {
                logger.error("Falha ao salvar/encriptar o DB no shutdown", e);
            }
        }
    }

    public static synchronized void persistEncryptedSnapshot() {
        if (!Config.isDbEncryptionEnabled()) return;
        try {
            checkpointSqliteIfPossible();
            var runtime = Config.getRuntimeDbFilePath();
            var encrypted = Config.getEncryptedDbFilePath();
            DbFileCrypto.encryptFromRuntime(runtime, encrypted, Config.getSecretKey());
        } catch (java.io.IOException | RuntimeException e) {
            logger.warn("Falha ao persistir snapshot criptografado", e);
        }
    }

    public static Jdbi jdbi() {
        init();
        return jdbi;
    }

    public static final class SimplePool implements AutoCloseable {
        public SimplePool(String jdbcUrl, int poolSize) {
        }

        public Connection borrow() throws SQLException {
            init();
            Connection c = dataSource.getConnection();
            try { c.setAutoCommit(false); } catch (java.sql.SQLException ignored) {}
            return c;
        }

        @Override public void close() { }
    }

    public static Connection openSingleConnection() throws SQLException {
        init();
        if (dataSource == null) throw new SQLException("Datasource not initialized");
        Connection c = dataSource.getConnection();
        try { c.setAutoCommit(false); } catch (java.sql.SQLException ignored) {}
        return c;
    }

    public static void safeClose(Connection c) {
        if (c == null) return;
        try { c.close(); } catch (java.sql.SQLException ignored) {}
    }
}

final class DbFileCrypto {

    private static final byte[] MAGIC = "KEEPLYENC".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    private static final byte VERSION = 1;

    private static final int SALT_LEN = 16;
    private static final int NONCE_LEN = 12;
    private static final int PBKDF2_ITERS = 120_000;
    private static final int KEY_BITS = 256;

    private static final int GCM_TAG_BITS = 128;

    private DbFileCrypto() {}

    static boolean looksLikePlainSqlite(Path file) {
        if (file == null || !Files.exists(file)) return false;
        try (java.io.InputStream in = Files.newInputStream(file)) {
            byte[] header = in.readNBytes(16);
            String s = new String(header, java.nio.charset.StandardCharsets.US_ASCII);
            return s.startsWith("SQLite format 3");
        } catch (Exception e) {
            return false;
        }
    }

    static boolean looksLikeKeeplyEncrypted(Path file) {
        if (file == null || !Files.exists(file)) return false;
        try (java.io.InputStream in = Files.newInputStream(file)) {
            byte[] m = in.readNBytes(MAGIC.length);
            if (m.length != MAGIC.length) return false;
            for (int i = 0; i < MAGIC.length; i++) {
                if (m[i] != MAGIC[i]) return false;
            }
            int v = in.read();
            return v == VERSION;
        } catch (Exception e) {
            return false;
        }
    }

    static void decryptToRuntime(Path encryptedFile, Path runtimeSqliteFile, String passphrase) throws java.io.IOException {
        if (!Files.exists(encryptedFile)) {
            return; // nada pra restaurar
        }

        try (java.io.InputStream in = Files.newInputStream(encryptedFile)) {
            byte[] magic = in.readNBytes(MAGIC.length);
            if (magic.length != MAGIC.length) {
                throw new IllegalStateException("Arquivo de DB criptografado inválido (magic curto)");
            }
            for (int i = 0; i < MAGIC.length; i++) {
                if (magic[i] != MAGIC[i]) {
                    throw new IllegalStateException("Arquivo de DB criptografado inválido (magic mismatch)");
                }
            }
            int version = in.read();
            if (version != VERSION) {
                throw new IllegalStateException("Versão de criptografia não suportada: " + version);
            }

            byte[] salt = in.readNBytes(SALT_LEN);
            byte[] nonce = in.readNBytes(NONCE_LEN);
            if (salt.length != SALT_LEN || nonce.length != NONCE_LEN) {
                throw new IllegalStateException("Arquivo de DB criptografado inválido (salt/nonce)");
            }

            try {
                javax.crypto.SecretKey key = deriveKey(passphrase, salt);
                javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("AES/GCM/NoPadding");
                cipher.init(javax.crypto.Cipher.DECRYPT_MODE, key, new javax.crypto.spec.GCMParameterSpec(GCM_TAG_BITS, nonce));

                Files.createDirectories(runtimeSqliteFile.toAbsolutePath().getParent());
                try (java.io.OutputStream out = Files.newOutputStream(runtimeSqliteFile)) {
                    // streaming decrypt
                    byte[] buf = new byte[64 * 1024];
                    int read;
                    while ((read = in.read(buf)) != -1) {
                        byte[] pt = cipher.update(buf, 0, read);
                        if (pt != null && pt.length > 0) out.write(pt);
                    }
                    byte[] finalPt = cipher.doFinal();
                    if (finalPt != null && finalPt.length > 0) out.write(finalPt);
                }
            } catch (java.security.GeneralSecurityException e) {
                throw new java.io.IOException("Falha ao descriptografar DB.", e);
            }
        }
    }

    static void encryptFromRuntime(Path runtimeSqliteFile, Path encryptedFile, String passphrase) throws java.io.IOException {
        if (!Files.exists(runtimeSqliteFile)) {
            return; // nada pra persistir
        }

        try {
            java.security.SecureRandom rng = new java.security.SecureRandom();
            byte[] salt = new byte[SALT_LEN];
            byte[] nonce = new byte[NONCE_LEN];
            rng.nextBytes(salt);
            rng.nextBytes(nonce);

            javax.crypto.SecretKey key = deriveKey(passphrase, salt);
            javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(javax.crypto.Cipher.ENCRYPT_MODE, key, new javax.crypto.spec.GCMParameterSpec(GCM_TAG_BITS, nonce));

            Files.createDirectories(encryptedFile.toAbsolutePath().getParent());
            try (java.io.OutputStream out = Files.newOutputStream(encryptedFile)) {
                out.write(MAGIC);
                out.write(VERSION);
                out.write(salt);
                out.write(nonce);

                try (java.io.InputStream in = Files.newInputStream(runtimeSqliteFile)) {
                    byte[] buf = new byte[64 * 1024];
                    int read;
                    while ((read = in.read(buf)) != -1) {
                        byte[] ct = cipher.update(buf, 0, read);
                        if (ct != null && ct.length > 0) out.write(ct);
                    }
                    byte[] finalCt = cipher.doFinal();
                    if (finalCt != null && finalCt.length > 0) out.write(finalCt);
                }
            }
        } catch (java.security.GeneralSecurityException e) {
            throw new java.io.IOException("Falha ao criptografar DB.", e);
        }
    }

    private static javax.crypto.SecretKey deriveKey(String passphrase, byte[] salt) throws java.security.GeneralSecurityException {
        javax.crypto.spec.PBEKeySpec spec = new javax.crypto.spec.PBEKeySpec(passphrase.toCharArray(), salt, PBKDF2_ITERS, KEY_BITS);
        javax.crypto.SecretKeyFactory skf = javax.crypto.SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        byte[] keyBytes = skf.generateSecret(spec).getEncoded();
        return new javax.crypto.spec.SecretKeySpec(keyBytes, "AES");
    }
}
