package com.keeply.app.config;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class ConfigTest {

    @Test
    void testEncryptionToggleAndPaths() {
        // These are set by Surefire system properties in pom.xml.
        assertTrue(Config.isDbEncryptionEnabled(), "Expected encryption enabled for tests via system properties");

        Path encrypted = Config.getEncryptedDbFilePath();
        Path runtime = Config.getRuntimeDbFilePath();

        assertNotNull(encrypted);
        assertNotNull(runtime);
        assertTrue(encrypted.getFileName().toString().endsWith(".enc"), "Encrypted DB should end with .enc");
        assertTrue(runtime.getFileName().toString().endsWith(".runtime.sqlite"), "Runtime DB should end with .runtime.sqlite");

        assertNotNull(Config.getSecretKey());
        assertFalse(Config.getSecretKey().isBlank(), "Secret key must be present when encryption is enabled");

        // URL should always point to runtime when encryption enabled.
        assertTrue(Config.getDbUrl().contains(runtime.toAbsolutePath().toString()));
    }
}
