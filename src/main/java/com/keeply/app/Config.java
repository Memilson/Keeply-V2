package com.keeply.app;

import io.github.cdimascio.dotenv.Dotenv;
import java.util.Objects;

public class Config {
    // Carrega o .env. Agora removemos o ignoreIfMissing() para garantir que o arquivo exista
    // Se não achar o arquivo .env, ele já lança erro aqui.
    private static final Dotenv dotenv = Dotenv.load();

    public static String getDbUrl() {
        // Objects.requireNonNull lança erro se a chave não existir no arquivo
        return Objects.requireNonNull(dotenv.get("DB_URL"), "A variável DB_URL não foi encontrada no .env");
    }

    public static String getDbUser() {
        return Objects.requireNonNull(dotenv.get("DB_USER"), "A variável DB_USER não foi encontrada no .env");
    }

    public static String getDbPass() {
        return Objects.requireNonNull(dotenv.get("DB_PASS"), "A variável DB_PASS não foi encontrada no .env");
    }
}