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
