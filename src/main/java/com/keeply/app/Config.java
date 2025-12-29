package com.keeply.app;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.prefs.Preferences;

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
    private static final String ENV_KEY_PRIMARY = "KEEPLY_SECRET_KEY";
    private static final String ENV_KEY_SECONDARY = "SECRET_KEY";

    // O caminho e a chave são calculados estaticamente na inicialização da classe
    // Logger must be initialized before any static initializer that may use it
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Config.class);
    private static final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

    private static final Preferences prefs = Preferences.userNodeForPackage(Main.class);

    private static final String DB_FILENAME = resolveDbFileName();
    private static final Path DB_PATH = resolveDbPath(DB_FILENAME);
    private static final String SECRET_KEY = resolveSecretKey();

    // Construtor privado para impedir instanciação (Utility Class)
    private Config() {}

    /**
     * Retorna a URL de Conexão JDBC.
     */
    public static String getDbUrl() {
        return "jdbc:sqlite:" + DB_PATH.toAbsolutePath().toString();
    }

    /**
     * Caminho do arquivo de banco (usado para operações de limpeza/backup).
     */
    public static Path getDbFilePath() {
        return DB_PATH;
    }

    /**
     * Retorna a chave para desbloquear o arquivo data.keeply.
     */
    public static String getSecretKey() {
        return SECRET_KEY;
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

    /**
     * Tenta obter o valor de uma variável de ambiente.
     * Se não existir, tenta ler do arquivo .env local via java-dotenv.
     */
    private static String getEnvOrDotenv(String key) {
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

    /**
     * Lógica resiliente para determinar o local de armazenamento.
     */
    private static Path resolveDbPath(String dbFileName) {
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



