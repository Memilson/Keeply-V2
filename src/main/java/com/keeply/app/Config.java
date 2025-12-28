package com.keeply.app;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final String DB_FILENAME = resolveDbFileName();
    private static final Path DB_PATH = resolveDbPath(DB_FILENAME);
    private static final String SECRET_KEY = resolveSecretKey();

    // Construtor privado para impedir instanciação (Utility Class)
    private Config() {}

    private static final Logger logger = LoggerFactory.getLogger(Config.class);

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

    // --- Lógica de Resolução ---

    private static String resolveDbFileName() {
        // Tenta pegar do ambiente ou .env, se não, usa o padrão
        return getEnvOrFile(ENV_DB_NAME).orElse(getEnvOrFile("DB_NAME").orElse(DEFAULT_DB_NAME));
    }

    private static String resolveSecretKey() {
        // Tenta KEEPLY_SECRET_KEY, depois SECRET_KEY
        String key = getEnvOrFile(ENV_KEY_PRIMARY)
                .orElseGet(() -> getEnvOrFile(ENV_KEY_SECONDARY).orElse(null));

        if (key == null || key.isBlank()) {
            throw new IllegalStateException(
                "CRÍTICO: Secret Key não encontrada. Configure " + ENV_KEY_PRIMARY + " no ambiente ou no arquivo .env"
            );
        }
        return key;
    }

    /**
     * Tenta obter o valor de uma variável de ambiente.
     * Se não existir, tenta ler do arquivo .env local.
     */
    private static Optional<String> getEnvOrFile(String key) {
        // 1. Tenta Variável de Ambiente do SO
        String envVal = System.getenv(key);
        if (envVal != null && !envVal.isBlank()) {
            return Optional.of(envVal.trim());
        }

        // 2. Tenta ler do arquivo .env
        return readFromDotEnv(key);
    }

    /**
     * Lê a chave do arquivo .env no diretório atual de forma eficiente (Stream).
     */
    private static Optional<String> readFromDotEnv(String key) {
        Path envFile = Paths.get(".env");
        if (!Files.exists(envFile)) return Optional.empty();

        try (Stream<String> lines = Files.lines(envFile)) {
            return lines
                .map(String::trim)
                .filter(line -> !line.startsWith("#") && !line.isBlank()) // Ignora comentários e vazios
                .filter(line -> line.toUpperCase().startsWith(key.toUpperCase() + "=")) // Case-insensitive para a chave
                .findFirst()
                .map(line -> {
                    String[] parts = line.split("=", 2);
                    if (parts.length < 2) return null;
                    String value = parts[1].trim();
                    // Remove aspas se houver (ex: KEY="valor")
                    if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
                        return value.substring(1, value.length() - 1);
                    }
                    return value;
                });
        } catch (IOException e) {
            // Log discreto, não queremos travar a aplicação se o .env estiver ilegível
            logger.warn("Não foi possível ler o arquivo .env: {}", e.getMessage());
            return Optional.empty();
        }
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