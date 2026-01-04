package com.keeply.app.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.prefs.Preferences;

import com.keeply.app.Main;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Configuração Central do Keeply.
 * Responsável por resolver caminhos de sistema operacional e
 * fornecer credenciais de segurança para o banco de dados.
 */
public final class Config {

    private static final String APP_NAME = "Keeply";
    
    // Extensão personalizada para "disfarçar" o arquivo
    private static final String DEFAULT_DB_NAME = "data.keeply";
    
    // Variáveis de ambiente a serem checadas (ordem de prioridade)
    private static final String ENV_DB_NAME = "KEEPLY_DB_NAME";
    private static final String ENV_DB_ENCRYPTION = "KEEPLY_DB_ENCRYPTION";
    private static final String ENV_KEY_PRIMARY = "KEEPLY_SECRET_KEY";
    private static final String ENV_KEY_SECONDARY = "SECRET_KEY";
    private static final String ENV_DATA_DIR = "KEEPLY_DATA_DIR";

    // System property overrides (useful for tests/CI)
    private static final String PROP_DB_NAME = "keeply.dbName";
    private static final String PROP_DB_ENCRYPTION = "keeply.dbEncryption";
    private static final String PROP_SECRET_KEY = "keeply.secretKey";
    private static final String PROP_DATA_DIR = "keeply.dataDir";

    // O caminho e a chave são calculados estaticamente na inicialização da classe
    // Logger must be initialized before any static initializer that may use it
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Config.class);
    private static final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

    private static final Preferences prefs = Preferences.userNodeForPackage(Main.class);

    private static volatile String cachedDbPathKey;
    private static volatile Path cachedDbPath;

    // Construtor privado para impedir instanciação (Utility Class)
    private Config() {}

    /**
     * Retorna a URL de Conexão JDBC.
     */
    public static String getDbUrl() {
        // IMPORTANTE:
        // - Sem criptografia: conecta direto no arquivo base.
        // - Com criptografia de arquivo (AES): conecta no arquivo runtime (plaintext) e persiste cifrado em *.enc.
        return "jdbc:sqlite:" + getRuntimeDbFilePath().toAbsolutePath();
    }

    /**
     * Caminho do arquivo de banco (usado para operações de limpeza/backup).
     */
    public static Path getDbFilePath() {
        return getEncryptedDbFilePath();
    }

    public static Path getEncryptedDbFilePath() {
        Path dbPath = getResolvedDbPath();
        if (!isDbEncryptionEnabled()) return dbPath;
        // Não sobrescreve o banco atual (data.keeply). Cria um novo arquivo cifrado ao lado.
        return dbPath.resolveSibling(dbPath.getFileName().toString() + ".enc");
    }

    public static Path getRuntimeDbFilePath() {
        Path dbPath = getResolvedDbPath();
        if (!isDbEncryptionEnabled()) return dbPath;
        // Arquivo plaintext temporário usado somente durante execução.
        return dbPath.resolveSibling(dbPath.getFileName().toString() + ".runtime.sqlite");
    }

    public static boolean isDbEncryptionEnabled() {
        return resolveDbEncryptionEnabled();
    }

    /**
     * Retorna a chave para desbloquear o arquivo data.keeply.
     */
    public static String getSecretKey() {
        if (!isDbEncryptionEnabled()) return "";
        return resolveSecretKey();
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

    // Métodos de compatibilidade
    public static String getDbUser() { return ""; }
    public static String getDbPass() { return ""; }

    public static void saveLastPath(String path) {
        prefs.put("last_scan_path", path);
    }

    public static String getLastPath() {
        return prefs.get("last_scan_path", System.getProperty("user.home"));
    }

    public static void saveLastBackupDestination(String path) {
        prefs.put("last_backup_dest", path);
    }

    public static String getLastBackupDestination() {
        String fallback;
        try {
            Path p = getEncryptedDbFilePath().toAbsolutePath().getParent();
            fallback = (p != null) ? p.toString() : System.getProperty("user.home");
        } catch (Exception e) {
            fallback = System.getProperty("user.home");
        }
        return prefs.get("last_backup_dest", fallback);
    }

    // --- Lógica de Resolução ---

    private static String resolveDbFileName() {
        // Tenta pegar do ambiente ou .env, se não, usa o padrão
        String name = getEnvOrDotenv(ENV_DB_NAME);
        if (name == null) {
            name = getEnvOrDotenv("DB_NAME");
        }
        return name == null || name.isBlank() ? DEFAULT_DB_NAME : name.trim();
    }

    private static String resolveSecretKey() {
        // Tenta KEEPLY_SECRET_KEY, depois SECRET_KEY
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

    private static boolean resolveDbEncryptionEnabled() {
        String v = getEnvOrDotenv(ENV_DB_ENCRYPTION);
        if (v == null || v.isBlank()) return false;
        String norm = v.trim().toLowerCase();
        return norm.equals("1") || norm.equals("true") || norm.equals("yes") || norm.equals("on") || norm.equals("file");
    }

    /**
     * Tenta obter o valor de uma variável de ambiente.
     * Se não existir, tenta ler do arquivo .env local via java-dotenv.
     */
    private static String getEnvOrDotenv(String key) {
        // 0. System properties override (tests/CI)
        String propKey = mapToSystemPropertyKey(key);
        if (propKey != null) {
            String propVal = System.getProperty(propKey);
            if (propVal != null && !propVal.isBlank()) {
                return propVal.trim();
            }
        }

        // 1. Tenta Variável de Ambiente do SO
        String envVal = System.getenv(key);
        if (envVal != null && !envVal.isBlank()) {
            return envVal.trim();
        }

        // 2. Tenta ler do arquivo .env
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

    /**
     * Lógica resiliente para determinar o local de armazenamento.
     */
    private static Path resolveDbPath(String dbFileName) {
        // Test/CI override: allow forcing a specific data dir.
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
            // Linux/Unix: Padrão XDG (~/.local/share/Keeply)
            String xdgData = System.getenv("XDG_DATA_HOME");
            if (xdgData != null && !xdgData.isBlank()) {
                appDataDir = Paths.get(xdgData, APP_NAME);
            } else {
                appDataDir = Paths.get(userHome, ".local", "share", APP_NAME);
            }
        }

        // Tenta criar o diretório
        try {
            if (!Files.exists(appDataDir)) {
                Files.createDirectories(appDataDir);
            }
            // Sucesso: retorna caminho no AppData
            logger.info("Banco de dados localizado em: {}", appDataDir.toAbsolutePath());
            return appDataDir.resolve(dbFileName);
        } catch (IOException e) {
            // Falha: Fallback para diretório local (onde o jar está)
            Path localPath = Paths.get(dbFileName).toAbsolutePath();
            logger.warn("ERRO PERMISSÃO: Não foi possível usar {}. Usando diretório local como fallback: {}", appDataDir, localPath);
            return localPath;
        }
    }
}



