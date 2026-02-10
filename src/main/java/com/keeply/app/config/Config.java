package com.keeply.app.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;


import io.github.cdimascio.dotenv.Dotenv;

/**
 * Configuração central do Keeply.
 */
public final class Config {
    private static final String APP_NAME = "Keeply";
    private static final String DEFAULT_DB_NAME = "data.keeply";
    private static final String ENV_DB_NAME = "KEEPLY_DB_NAME";
    private static final String ENV_DB_ENCRYPTION = "KEEPLY_DB_ENCRYPTION";
    private static final String ENV_KEY_PRIMARY = "KEEPLY_SECRET_KEY";
    private static final String ENV_KEY_SECONDARY = "SECRET_KEY";
    private static final String ENV_DATA_DIR = "KEEPLY_DATA_DIR";
    private static final String PROP_DB_NAME = "keeply.dbName";
    private static final String PROP_DB_ENCRYPTION = "keeply.dbEncryption";
    private static final String PROP_SECRET_KEY = "keeply.secretKey";
    private static final String PROP_DATA_DIR = "keeply.dataDir";
    private static final String PREF_SCHEDULE_ENABLED = "backup_schedule_enabled";
    private static final String PREF_SCHEDULE_MODE = "backup_schedule_mode"; // DAILY | INTERVAL
    private static final String PREF_SCHEDULE_TIME = "backup_schedule_time"; // HH:mm
    private static final String PREF_SCHEDULE_INTERVAL_MIN = "backup_schedule_interval_min"; // minutes
    private static final String PREF_BACKUP_MODE = "backup_mode"; // INCREMENTAL | FULL
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Config.class);
    private static final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
    private static volatile String cachedDbPathKey;
    private static volatile Path cachedDbPath;
    private static volatile String sessionBackupPassword;
    private static volatile boolean sessionDbInitialized = false;
    private Config() {}

    public static String getDbUrl() {
        return "jdbc:sqlite:" + getRuntimeDbFilePath().toAbsolutePath();
    }

    public static Path getDbFilePath() {
        return getEncryptedDbFilePath();
    }

    public static Path getSessionDbFilePath() {
        Path dbPath = getResolvedDbPath();
        Path dir = dbPath.getParent();
        if (dir == null) return Path.of("session.keeply");
        return dir.resolve("session.keeply");
    }

    public static Path getEncryptedDbFilePath() {
        Path dbPath = getResolvedDbPath();
        if (!isDbEncryptionEnabled()) return dbPath;
        return dbPath.resolveSibling(dbPath.getFileName().toString() + ".enc");
    }

    public static Path getRuntimeDbFilePath() {
        Path dbPath = getResolvedDbPath();
        if (!isDbEncryptionEnabled()) return dbPath;
        return dbPath.resolveSibling(dbPath.getFileName().toString() + ".runtime.sqlite");
    }

    public static boolean isDbEncryptionEnabled() {
        return resolveDbEncryptionEnabled();
    }

    public static String getSecretKey() {
        if (!isDbEncryptionEnabled()) return "";
        return resolveSecretKeyInternal();
    }
    private static Path getResolvedDbPath() {
        String dbFileName = resolveDbFileName();
        String overrideDir = getEnvOrDotenv(ENV_DATA_DIR);
        String key = (overrideDir == null ? "" : overrideDir.trim()) + "|" + dbFileName;
        Path current = cachedDbPath;
        if (current != null && key.equals(cachedDbPathKey)) {
            return current;
        }
        synchronized (Config.class) {
            current = cachedDbPath;
            if (current != null && key.equals(cachedDbPathKey)) {
                return current;
            }
            Path resolved = resolveDbPath(dbFileName);
            cachedDbPathKey = key;
            cachedDbPath = resolved;
            return resolved;
        }
    }

    public static String getDbUser() {
        return "";
    }

    public static String getDbPass() {
        return "";
    }

    public static void saveLastPath(String path) {
        prefsPut("last_scan_path", path);
    }

    public static String getLastPath() {
        return prefsGet("last_scan_path", System.getProperty("user.home"));
    }

    public static void saveLastBackupDestination(String path) {
        prefsPut("last_backup_dest", path);
    }
    public static String getLastBackupDestination() {
        String home = System.getProperty("user.home");
        String fallback = (home == null || home.isBlank())
                ? "."
                : Paths.get(home, "Documents", APP_NAME, "Backup").toString();
        return prefsGet("last_backup_dest", fallback);
    }

    public static void saveBackupEncryptionEnabled(boolean enabled) {
        prefsPutBoolean("backup_encryption_enabled", enabled);
        if (!enabled) {
            prefsPutBoolean("backup_password_active", false);
        }
    }

    // --- Agendamento (preferências) ---
    public static boolean isScheduleEnabled() {
        return prefsGetBoolean(PREF_SCHEDULE_ENABLED, false);
    }

    public static void setScheduleEnabled(boolean enabled) {
        prefsPutBoolean(PREF_SCHEDULE_ENABLED, enabled);
    }

    public static String getScheduleMode() {
        String v = prefsGet(PREF_SCHEDULE_MODE, "DAILY");
        return (v == null || v.isBlank()) ? "DAILY" : v;
    }

    public static void setScheduleMode(String mode) {
        if (mode == null || mode.isBlank()) mode = "DAILY";
        prefsPut(PREF_SCHEDULE_MODE, mode);
    }

    public static String getScheduleTime() {
        String v = prefsGet(PREF_SCHEDULE_TIME, "22:00");
        return (v == null || v.isBlank()) ? "22:00" : v;
    }

    public static void setScheduleTime(String time) {
        if (time == null || time.isBlank()) time = "22:00";
        prefsPut(PREF_SCHEDULE_TIME, time);
    }

    public static int getScheduleIntervalMinutes() {
        String v = prefsGet(PREF_SCHEDULE_INTERVAL_MIN, "120");
        try {
            return Integer.parseInt(v);
        } catch (Exception ignored) {
            return 120;
        }
    }

    public static void setScheduleIntervalMinutes(int minutes) {
        int safe = Math.max(15, Math.min(minutes, 1440));
        prefsPut(PREF_SCHEDULE_INTERVAL_MIN, Integer.toString(safe));
    }

    // --- Modo de backup ---
    public static String getBackupMode() {
        String v = prefsGet(PREF_BACKUP_MODE, "INCREMENTAL");
        return (v == null || v.isBlank()) ? "INCREMENTAL" : v;
    }

    public static void setBackupMode(String mode) {
        if (mode == null || mode.isBlank()) mode = "INCREMENTAL";
        prefsPut(PREF_BACKUP_MODE, mode);
    }

    public static boolean isBackupEncryptionEnabled() {
        return prefsGetBoolean("backup_encryption_enabled", false);
    }

    public static void setBackupEncryptionPassword(String password) {
        sessionBackupPassword = (password == null) ? null : password;
    }

    public static String getBackupEncryptionPassword() {
        return sessionBackupPassword;
    }

    public static String getSessionValue(String key) {
        return sessionGet(key);
    }

    public static void putSessionValue(String key, String value) {
        sessionPut(key, value);
    }

    public static String getBackupPasswordHash() {
        return prefsGet("backup_password_hash", null);
    }

    public static String getBackupPasswordSetAt() {
        return prefsGet("backup_password_set_at", null);
    }

    public static boolean hasBackupPasswordHash() {
        String hash = getBackupPasswordHash();
        return hash != null && !hash.isBlank();
    }

    public static boolean verifyAndCacheBackupPassword(String password) {
        if (password == null || password.isBlank()) return false;
        try {
            String hash = sha256Hex(password);
            String stored = prefsGet("backup_password_hash", null);
            if (stored == null || stored.isBlank()) {
                prefsPut("backup_password_hash", hash);
                prefsPut("backup_password_set_at", Instant.now().toString());
                prefsPutBoolean("backup_password_active", true);
                sessionBackupPassword = password;
                return true;
            }
            if (stored.equals(hash)) {
                prefsPutBoolean("backup_password_active", true);
                sessionBackupPassword = password;
                return true;
            }
            sessionBackupPassword = null;
            return false;
        } catch (NoSuchAlgorithmException e) {
            sessionBackupPassword = null;
            return false;
        }
    }

    private static String resolveDbFileName() {
        String name = getEnvOrDotenv(ENV_DB_NAME);
        if (name == null) {
            name = getEnvOrDotenv("DB_NAME");
        }
        return name == null || name.isBlank() ? DEFAULT_DB_NAME : name.trim();
    }

    private static String resolveSecretKeyInternal() {
        String key = getEnvOrDotenv(ENV_KEY_PRIMARY);
        if (key == null || key.isBlank()) {
            key = getEnvOrDotenv(ENV_KEY_SECONDARY);
        }

        if (key == null || key.isBlank()) {
            throw new IllegalStateException(
                "CRÍTICO: Secret Key não encontrada. Configure " + ENV_KEY_PRIMARY + " no ambiente ou no arquivo .env"
            );
        }
        return key;
    }

    public static String requireBackupPassword() {
        String pass = getBackupEncryptionPassword();
        if (pass == null || pass.isBlank()) {
            throw new IllegalStateException("Senha do backup não configurada.");
        }
        return pass;
    }

    public static String requireBackupPasswordForEncryption() {
        if (!isBackupEncryptionEnabled()) {
            return "";
        }
        return requireBackupPassword();
    }

    public static void clearBackupPassword() {
        sessionBackupPassword = null;
        prefsPut("backup_password_hash", "");
        prefsPut("backup_password_set_at", "");
        prefsPutBoolean("backup_password_active", false);
    }

    private static boolean resolveDbEncryptionEnabled() {
        String v = getEnvOrDotenv(ENV_DB_ENCRYPTION);
        if (v == null || v.isBlank()) return false;
        String norm = v.trim().toLowerCase();
        return norm.equals("1") || norm.equals("true") || norm.equals("yes") || norm.equals("on") || norm.equals("file");
    }

    private static String getEnvOrDotenv(String key) {
        String propKey = mapToSystemPropertyKey(key);
        if (propKey != null) {
            String propVal = System.getProperty(propKey);
            if (propVal != null && !propVal.isBlank()) {
                return propVal.trim();
            }
        }

        String envVal = System.getenv(key);
        if (envVal != null && !envVal.isBlank()) {
            return envVal.trim();
        }

        String fileVal = dotenv.get(key);
        if (fileVal == null || fileVal.isBlank()) {
            return null;
        }
        return fileVal.trim();
    }

    private static String mapToSystemPropertyKey(String envKey) {
        if (envKey == null) return null;
        return switch (envKey) {
            case ENV_DB_NAME -> PROP_DB_NAME;
            case ENV_DB_ENCRYPTION -> PROP_DB_ENCRYPTION;
            case ENV_KEY_PRIMARY -> PROP_SECRET_KEY;
            case ENV_KEY_SECONDARY -> PROP_SECRET_KEY;
            case ENV_DATA_DIR -> PROP_DATA_DIR;
            default -> null;
        };
    }

    private static Path resolveDbPath(String dbFileName) {
        String overrideDir = getEnvOrDotenv(ENV_DATA_DIR);
        if (overrideDir != null && !overrideDir.isBlank()) {
            Path p = Paths.get(overrideDir.trim());
            try {
                if (!Files.exists(p)) {
                    Files.createDirectories(p);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Não foi possível criar diretório de dados: " + p, e);
            }
            logger.info("Banco de dados localizado em (override): {}", p.toAbsolutePath());
            return p.resolve(dbFileName);
        }

        String os = System.getProperty("os.name").toLowerCase();
        String userHome = System.getProperty("user.home");
        Path appDataDir;

        if (os.contains("win")) {
            String appDataEnv = System.getenv("APPDATA");
            if (appDataEnv != null && !appDataEnv.isBlank()) {
                appDataDir = Paths.get(appDataEnv, APP_NAME);
            } else {
                appDataDir = Paths.get(userHome, "AppData", "Roaming", APP_NAME);
            }
        } else if (os.contains("mac")) {
            appDataDir = Paths.get(userHome, "Library", "Application Support", APP_NAME);
        } else {
            String xdgData = System.getenv("XDG_DATA_HOME");
            if (xdgData != null && !xdgData.isBlank()) {
                appDataDir = Paths.get(xdgData, APP_NAME);
            } else {
                appDataDir = Paths.get(userHome, ".local", "share", APP_NAME);
            }
        }

        try {
            if (!Files.exists(appDataDir)) {
                Files.createDirectories(appDataDir);
            }
            logger.info("Banco de dados localizado em: {}", appDataDir.toAbsolutePath());
            return appDataDir.resolve(dbFileName);
        } catch (IOException e) {
            Path localPath = Paths.get(dbFileName).toAbsolutePath();
            logger.warn("ERRO PERMISSÃO: Não foi possível usar {}. Usando diretório local como fallback: {}", appDataDir, localPath);
            return localPath;
        }
    }


    private static String sessionGet(String key) {
        if (key == null || key.isBlank()) return null;
        try {
            sessionInit();
            try (Connection c = sessionOpen();
                 PreparedStatement ps = c.prepareStatement("SELECT value FROM session_kv WHERE key = ?")) {
                ps.setString(1, key);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) return rs.getString(1);
                }
            }
        } catch (SQLException | RuntimeException ignored) {
        }
        return null;
    }

    private static void sessionPut(String key, String value) {
        if (key == null || key.isBlank()) return;
        try {
            sessionInit();
            try (Connection c = sessionOpen();
                 PreparedStatement ps = c.prepareStatement(
                         "INSERT INTO session_kv(key, value, updated_at) VALUES(?,?,?) " +
                                 "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at"
                 )) {
                ps.setString(1, key);
                ps.setString(2, value);
                ps.setString(3, Instant.now().toString());
                ps.executeUpdate();
            }
        } catch (SQLException | RuntimeException ignored) {
        }
    }

    private static synchronized void sessionInit() {
        if (sessionDbInitialized) return;
        try (Connection c = sessionOpen();
             PreparedStatement ps = c.prepareStatement(
                     "CREATE TABLE IF NOT EXISTS session_kv (" +
                             "key TEXT PRIMARY KEY, " +
                             "value TEXT, " +
                             "updated_at TEXT" +
                             ")"
             )) {
            ps.execute();
            try (PreparedStatement ps2 = c.prepareStatement(
                    "CREATE TABLE IF NOT EXISTS prefs_kv (" +
                            "key TEXT PRIMARY KEY, " +
                            "value TEXT, " +
                            "updated_at TEXT" +
                            ")"
            )) {
                ps2.execute();
            }
            sessionDbInitialized = true;
        } catch (SQLException | RuntimeException ignored) {
        }
    }

    private static Connection sessionOpen() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + getSessionDbFilePath().toAbsolutePath());
    }

    private static String prefsGet(String key, String fallback) {
        if (key == null || key.isBlank()) return fallback;
        try {
            sessionInit();
            try (Connection c = sessionOpen();
                 PreparedStatement ps = c.prepareStatement("SELECT value FROM prefs_kv WHERE key = ?")) {
                ps.setString(1, key);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String v = rs.getString(1);
                        return (v == null || v.isBlank()) ? fallback : v;
                    }
                }
            }
        } catch (SQLException | RuntimeException ignored) {
        }
        return fallback;
    }

    private static void prefsPut(String key, String value) {
        if (key == null || key.isBlank()) return;
        try {
            sessionInit();
            try (Connection c = sessionOpen();
                 PreparedStatement ps = c.prepareStatement(
                         "INSERT INTO prefs_kv(key, value, updated_at) VALUES(?,?,?) " +
                                 "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at"
                 )) {
                ps.setString(1, key);
                ps.setString(2, value);
                ps.setString(3, Instant.now().toString());
                ps.executeUpdate();
            }
        } catch (SQLException | RuntimeException ignored) {
        }
    }

    private static boolean prefsGetBoolean(String key, boolean fallback) {
        String v = prefsGet(key, null);
        if (v == null || v.isBlank()) return fallback;
        return v.equalsIgnoreCase("true") || v.equalsIgnoreCase("1") || v.equalsIgnoreCase("yes");
    }

    private static void prefsPutBoolean(String key, boolean value) {
        prefsPut(key, value ? "true" : "false");
    }

    private static String sha256Hex(String input) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] out = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder(out.length * 2);
        for (byte b : out) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
