package com.keeply.app;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Configuração Central do Keeply.
 * Responsável por resolver caminhos de sistema operacional e
 * fornecer credenciais de segurança para o banco de dados.
 */
public class Config {

    private static final String APP_NAME = "Keeply";
    
    // Extensão personalizada para "disfarçar" o arquivo (Security through obscurity)
    // O SQLite lê normalmente, mas o usuário não associa a um DB comum.
    private static final String DEFAULT_DB_NAME = "data.keeply";
    private static final String DB_NAME = resolveDbFileName();
    
    // CHAVE MESTRA DE CRIPTOGRAFIA (AES-256 via SQLCipher)
    // Obrigatória via variáveis de ambiente (sem fallback hardcoded).
    private static final String SECRET_KEY = resolveSecretKey();

    // O caminho é calculado estaticamente apenas uma vez na inicialização da classe
    private static final Path DB_PATH = resolveDbPath(DB_NAME);

    /**
     * Retorna a URL de Conexão JDBC.
     * O prefixo jdbc:sqlite: é padrão, a mágica da criptografia
     * acontece nas Properties passadas ao DriverManager no Database.java.
     */
    public static String getDbUrl() {
        return "jdbc:sqlite:" + DB_PATH.toAbsolutePath().toString();
    }

    /**
     * Retorna a chave para desbloquear o arquivo data.keeply
     */
    public static String getSecretKey() {
        return SECRET_KEY;
    }

    // Métodos de compatibilidade (mantidos vazios pois usamos chave nas Properties)
    public static String getDbUser() { return ""; }
    public static String getDbPass() { return ""; }

    private static String resolveSecretKey() {
        String env = System.getenv("KEEPLY_SECRET_KEY");
        if (env == null || env.isBlank()) {
            env = System.getenv("SECRET_KEY");
        }
        if (env == null || env.isBlank()) {
            env = readFromDotEnv("KEEPLY_SECRET_KEY");
        }
        if (env == null || env.isBlank()) {
            env = readFromDotEnv("SECRET_KEY");
        }
        if (env == null || env.isBlank()) {
            throw new IllegalStateException("Secret key não configurada. Defina KEEPLY_SECRET_KEY ou SECRET_KEY no ambiente.");
        }
        return env;
    }

    /**
     * Lógica resiliente para determinar o local de armazenamento
     * baseada no Sistema Operacional do usuário.
     */
    private static Path resolveDbPath(String dbFileName) {
        String os = System.getProperty("os.name").toLowerCase();
        String userHome = System.getProperty("user.home");
        Path appDataDir;

        // 1. Definição do Caminho Base
        if (os.contains("win")) {
            // Windows: Tenta usar a variável %APPDATA% (Roaming)
            String appDataEnv = System.getenv("APPDATA");
            if (appDataEnv != null && !appDataEnv.isBlank()) {
                appDataDir = Paths.get(appDataEnv, APP_NAME);
            } else {
                // Fallback para C:\Users\Nome\AppData\Roaming\Keeply
                appDataDir = Paths.get(userHome, "AppData", "Roaming", APP_NAME);
            }
        } else if (os.contains("mac")) {
            // MacOS: ~/Library/Application Support/Keeply
            appDataDir = Paths.get(userHome, "Library", "Application Support", APP_NAME);
        } else {
            // Linux/Unix: Padrão XDG Base Directory (~/.local/share/Keeply)
            appDataDir = Paths.get(userHome, ".local", "share", APP_NAME);
        }

        // 2. Garantia de Existência (Autocriação)
        try {
            if (!Files.exists(appDataDir)) {
                Files.createDirectories(appDataDir);
                // Opcional: Em Linux/Mac, poderia setar permissões 700 aqui para segurança extra
            }
            System.out.println(">> Banco de dados localizado em: " + appDataDir.toAbsolutePath());
        } catch (IOException e) {
            // 3. Fallback de Emergência (Se falhar permissão de escrita no sistema)
            System.err.println("ERRO CRÍTICO: Não foi possível criar pasta de dados em: " + appDataDir);
            System.err.println(">> Usando diretório local como fallback.");
            e.printStackTrace();
            return Paths.get(dbFileName); // Salva na pasta onde o .jar/exe está rodando
        }

        return appDataDir.resolve(dbFileName);
    }

    private static String resolveDbFileName() {
        String env = System.getenv("KEEPLY_DB_NAME");
        if (env == null || env.isBlank()) {
            env = System.getenv("DB_NAME");
        }
        if (env == null || env.isBlank()) {
            return DEFAULT_DB_NAME;
        }
        return env;
    }

    /**
     * Lê a chave do arquivo .env no diretório atual (compatível com dev).
     * Formato: KEY=VALUE (ignora comentários e linhas vazias).
     */
    private static String readFromDotEnv(String key) {
        try {
            Path envFile = Paths.get(".env");
            if (!Files.exists(envFile)) return null;

            List<String> lines = Files.readAllLines(envFile);
            for (String line : lines) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) continue;
                int idx = trimmed.indexOf('=');
                if (idx <= 0) continue;
                String k = trimmed.substring(0, idx).trim();
                if (!k.equalsIgnoreCase(key)) continue;
                return trimmed.substring(idx + 1).trim();
            }
        } catch (Exception ignored) {}
        return null;
    }
}
