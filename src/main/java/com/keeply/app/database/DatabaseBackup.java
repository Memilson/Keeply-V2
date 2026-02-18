package com.keeply.app.database;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.exception.FlywayValidateException;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.keeply.app.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
public final class DatabaseBackup {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseBackup.class);
    private DatabaseBackup() {}
    public record InventoryRow(String rootPath, String pathRel, String name, long sizeBytes, long modifiedMillis, long createdMillis, String status) {}
    public record ScanSummary(long scanId, String rootPath, String startedAt, String finishedAt) {}
    public record FileHistoryRow(long scanId, String rootPath, String startedAt, String finishedAt, long sizeBytes, String statusEvent, String createdAt) {}
    public record SnapshotBlobRow(String pathRel, String contentHash) {}
    public record CapacityReport(String date, long totalBytes, long growthBytes) {}
    private static volatile HikariDataSource dataSource;
    private static volatile Jdbi jdbi;
    private static volatile boolean migrated = false;
    private static volatile boolean shutdownHookRegistered = false;
    public record DbEncryptionStatus(boolean encryptionEnabled, Path encryptedFile, boolean encryptedFileExists, boolean encryptedLooksEncrypted, boolean encryptedLooksPlainSqlite, Path legacyPlainFile, boolean legacyPlainExists, boolean legacyPlainLooksPlainSqlite, boolean legacyPlainWalExists, boolean legacyPlainShmExists, Path runtimePlainFile, boolean runtimePlainExists, boolean runtimePlainWalExists, boolean runtimePlainShmExists) {}
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
        return new DbEncryptionStatus(enabled, encrypted, encryptedExists, encLooksEncrypted, encLooksPlain, legacyPlain, legacyExists, legacyLooksPlain, legacyWal, legacyShm, runtime, runtimeExists, runtimeWal, runtimeShm);
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
            byte[] payload = "keeply-selftest".getBytes(StandardCharsets.UTF_8);
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
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Driver SQLite nao encontrado no classpath", e);
        }
        HikariConfig hc = new HikariConfig();
        hc.setDriverClassName("org.sqlite.JDBC");
        hc.setJdbcUrl(Config.getDbUrl());
        hc.setMaximumPoolSize(2);
        hc.setMinimumIdle(1);
        hc.setConnectionTimeout(10_000);
        hc.setValidationTimeout(5_000);
        hc.setPoolName("keeply-sqlite");
        hc.setConnectionTestQuery("SELECT 1");
        hc.setConnectionInitSql("""
            PRAGMA foreign_keys=ON;
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=2000;
            PRAGMA synchronous=NORMAL;
            PRAGMA busy_timeout=10000;
            PRAGMA temp_store=MEMORY;
            PRAGMA cache_size=-20000;
            """);
        try {
            dataSource = new HikariDataSource(hc);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Falha ao iniciar pool SQLite para " + Config.getDbUrl() + ": " + e.getMessage(), e);
        }
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
            Files.createDirectories(runtime.toAbsolutePath().getParent());
            Files.createDirectories(encrypted.toAbsolutePath().getParent());
            var runtimeWal = runtime.resolveSibling(runtime.getFileName().toString() + "-wal");
            var runtimeShm = runtime.resolveSibling(runtime.getFileName().toString() + "-shm");
            try { Files.deleteIfExists(runtime); } catch (Exception ignored) {}
            try { Files.deleteIfExists(runtimeWal); } catch (Exception ignored) {}
            try { Files.deleteIfExists(runtimeShm); } catch (Exception ignored) {}
            if (Files.exists(encrypted)) {
                if (DbFileCrypto.looksLikePlainSqlite(encrypted)) {
                    throw new IllegalStateException("DB encryption habilitada, mas o arquivo persistido parece SQLite plaintext: " + encrypted + ". Apague esse arquivo ou garanta o sufixo .enc (Config.getEncryptedDbFilePath).");
                }
                DbFileCrypto.decryptToRuntime(encrypted, runtime, Config.getSecretKey());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao preparar DB criptografado (arquivo runtime)", e);
        }
    }
    private static void checkpointSqliteIfPossible() {
        if (dataSource == null) return;
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            try { st.execute("PRAGMA wal_checkpoint(FULL);"); } catch (SQLException ignored) {}
            try { st.execute("PRAGMA wal_checkpoint(TRUNCATE);"); } catch (SQLException ignored) {}
            try { st.execute("PRAGMA optimize;"); } catch (SQLException ignored) {}
        } catch (SQLException ignored) {
        }
    }
    private static synchronized void migrateIfNeeded() {
        if (migrated) return;
        Flyway flyway = Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").baselineOnMigrate(true).baselineVersion("0").load();
        try {
            try {
                flyway.migrate();
            } catch (FlywayValidateException e) {
                logger.warn("Flyway validation failed; attempting repair.", e);
                flyway.repair();
                flyway.migrate();
            }
            ensureCoreSchemaFallback();
            migrated = true;
        } catch (RuntimeException e) {
            logger.error("Flyway migration failed", e);
            throw new IllegalStateException("Flyway migration failed", e);
        }
    }

    private static void ensureCoreSchemaFallback() {
        if (dataSource == null) return;
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("""
                CREATE TABLE IF NOT EXISTS scans (
                    scan_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    root_path TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    total_usage INTEGER,
                    status TEXT
                )
                """);
            st.execute("""
                CREATE TABLE IF NOT EXISTS file_inventory (
                    root_path TEXT NOT NULL,
                    path_rel TEXT NOT NULL,
                    name TEXT,
                    size_bytes INTEGER,
                    modified_millis INTEGER,
                    created_millis INTEGER,
                    last_scan_id INTEGER,
                    status TEXT,
                    PRIMARY KEY (root_path, path_rel)
                )
                """);
            st.execute("""
                CREATE TABLE IF NOT EXISTS file_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id INTEGER,
                    root_path TEXT,
                    path_rel TEXT,
                    size_bytes INTEGER,
                    status_event TEXT,
                    created_at TEXT,
                    created_millis INTEGER,
                    modified_millis INTEGER,
                    content_hash TEXT
                )
                """);
            st.execute("""
                CREATE TABLE IF NOT EXISTS scan_issues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id INTEGER,
                    path TEXT,
                    message TEXT,
                    created_at TEXT
                )
                """);
            st.execute("""
                CREATE TABLE IF NOT EXISTS backup_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_scans_root_scanid ON scans(root_path, scan_id)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_file_inventory_root_lastscan ON file_inventory(root_path, last_scan_id)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_file_history_root_path_scan ON file_history(root_path, path_rel, scan_id)");
        } catch (SQLException e) {
            throw new IllegalStateException("Falha ao garantir schema base do SQLite", e);
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
                Files.createDirectories(encrypted.toAbsolutePath().getParent());
                var runtimeWal = runtime.resolveSibling(runtime.getFileName().toString() + "-wal");
                var runtimeShm = runtime.resolveSibling(runtime.getFileName().toString() + "-shm");
                DbFileCrypto.encryptFromRuntime(runtime, encrypted, Config.getSecretKey());
                try { Files.deleteIfExists(runtime); } catch (Exception ignored) {}
                try { Files.deleteIfExists(runtimeWal); } catch (Exception ignored) {}
                try { Files.deleteIfExists(runtimeShm); } catch (Exception ignored) {}
            } catch (Exception e) {
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
            Files.createDirectories(encrypted.toAbsolutePath().getParent());
            DbFileCrypto.encryptFromRuntime(runtime, encrypted, Config.getSecretKey());
        } catch (Exception e) {
            logger.warn("Falha ao persistir snapshot criptografado", e);
        }
    }
    public static Jdbi jdbi() {
        init();
        return jdbi;
    }
    public static Connection openSingleConnection() throws SQLException {
        init();
        if (dataSource == null) throw new SQLException("Datasource not initialized");
        return dataSource.getConnection();
    }
    public static void safeClose(Connection c) {
        if (c == null) return;
        try { c.close(); } catch (SQLException ignored) {}
    }
}
final class DbFileCrypto {
    private static final byte[] MAGIC = "KEEPLYENC".getBytes(StandardCharsets.US_ASCII);
    private static final byte VERSION = 1;
    private static final int SALT_LEN = 16;
    private static final int NONCE_LEN = 12;
    private static final int PBKDF2_ITERS = 250_000;
    private static final int KEY_BITS = 256;
    private static final int GCM_TAG_BITS = 128;
    private DbFileCrypto() {}
    static boolean looksLikePlainSqlite(Path file) {
        if (file == null || !Files.exists(file)) return false;
        try (InputStream in = Files.newInputStream(file)) {
            byte[] header = in.readNBytes(16);
            String s = new String(header, StandardCharsets.US_ASCII);
            return s.startsWith("SQLite format 3");
        } catch (Exception e) {
            return false;
        }
    }
    static boolean looksLikeKeeplyEncrypted(Path file) {
        if (file == null || !Files.exists(file)) return false;
        try (InputStream in = Files.newInputStream(file)) {
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
        if (!Files.exists(encryptedFile)) return;
        Files.createDirectories(runtimeSqliteFile.toAbsolutePath().getParent());
        Path tmp = runtimeSqliteFile.resolveSibling(runtimeSqliteFile.getFileName().toString() + ".tmp");
        try (InputStream in = Files.newInputStream(encryptedFile)) {
            byte[] magic = in.readNBytes(MAGIC.length);
            if (magic.length != MAGIC.length) throw new IllegalStateException("Arquivo de DB criptografado inválido (magic curto)");
            for (int i = 0; i < MAGIC.length; i++) {
                if (magic[i] != MAGIC[i]) throw new IllegalStateException("Arquivo de DB criptografado inválido (magic mismatch)");
            }
            int version = in.read();
            if (version != VERSION) throw new IllegalStateException("Versão de criptografia não suportada: " + version);
            byte[] salt = in.readNBytes(SALT_LEN);
            byte[] nonce = in.readNBytes(NONCE_LEN);
            if (salt.length != SALT_LEN || nonce.length != NONCE_LEN) throw new IllegalStateException("Arquivo de DB criptografado inválido (salt/nonce)");
            try {
                SecretKey key = deriveKey(passphrase, salt);
                Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
                cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, nonce));
                try (FileChannel channel = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE); OutputStream out = Channels.newOutputStream(channel)) {
                    byte[] buf = new byte[64 * 1024];
                    int read;
                    while ((read = in.read(buf)) != -1) {
                        byte[] pt = cipher.update(buf, 0, read);
                        if (pt != null && pt.length > 0) out.write(pt);
                    }
                    byte[] finalPt = cipher.doFinal();
                    if (finalPt != null && finalPt.length > 0) out.write(finalPt);
                    channel.force(true);
                }
                try {
                    Files.move(tmp, runtimeSqliteFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (Exception atomicFail) {
                    Files.move(tmp, runtimeSqliteFile, StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (GeneralSecurityException e) {
                try { Files.deleteIfExists(tmp); } catch (Exception ignored) {}
                throw new java.io.IOException("Falha ao descriptografar DB.", e);
            }
        }
    }
    static void encryptFromRuntime(Path runtimeSqliteFile, Path encryptedFile, String passphrase) throws java.io.IOException {
        if (!Files.exists(runtimeSqliteFile)) return;
        Files.createDirectories(encryptedFile.toAbsolutePath().getParent());
        Path tmp = encryptedFile.resolveSibling(encryptedFile.getFileName().toString() + ".tmp");
        try {
            SecureRandom rng = new SecureRandom();
            byte[] salt = new byte[SALT_LEN];
            byte[] nonce = new byte[NONCE_LEN];
            rng.nextBytes(salt);
            rng.nextBytes(nonce);
            SecretKey key = deriveKey(passphrase, salt);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, nonce));
            try (FileChannel channel = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE); OutputStream out = Channels.newOutputStream(channel)) {
                out.write(MAGIC);
                out.write(VERSION);
                out.write(salt);
                out.write(nonce);
                try (InputStream in = Files.newInputStream(runtimeSqliteFile)) {
                    byte[] buf = new byte[64 * 1024];
                    int read;
                    while ((read = in.read(buf)) != -1) {
                        byte[] ct = cipher.update(buf, 0, read);
                        if (ct != null && ct.length > 0) out.write(ct);
                    }
                    byte[] finalCt = cipher.doFinal();
                    if (finalCt != null && finalCt.length > 0) out.write(finalCt);
                }
                channel.force(true);
            }
            try {
                Files.move(tmp, encryptedFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception atomicFail) {
                Files.move(tmp, encryptedFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (GeneralSecurityException e) {
            try { Files.deleteIfExists(tmp); } catch (Exception ignored) {}
            throw new java.io.IOException("Falha ao criptografar DB.", e);
        }
    }
    private static SecretKey deriveKey(String passphrase, byte[] salt) throws GeneralSecurityException {
        PBEKeySpec spec = new PBEKeySpec(passphrase.toCharArray(), salt, PBKDF2_ITERS, KEY_BITS);
        try {
            SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            byte[] keyBytes = skf.generateSecret(spec).getEncoded();
            return new SecretKeySpec(keyBytes, "AES");
        } finally {
            try { spec.clearPassword(); } catch (Exception ignored) {}
        }
    }
}
