package com.keeply.app.database;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

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
        try (InputStream in = Files.newInputStream(file)) {
            byte[] header = in.readNBytes(16);
            String s = new String(header, java.nio.charset.StandardCharsets.US_ASCII);
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
        if (!Files.exists(encryptedFile)) {
            return; // nada pra restaurar
        }

        try (InputStream in = Files.newInputStream(encryptedFile)) {
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
                SecretKey key = deriveKey(passphrase, salt);
                Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
                cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, nonce));

                Files.createDirectories(runtimeSqliteFile.toAbsolutePath().getParent());
                try (OutputStream out = Files.newOutputStream(runtimeSqliteFile)) {
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
            } catch (GeneralSecurityException e) {
                throw new java.io.IOException("Falha ao descriptografar DB.", e);
            }
        }
    }

    static void encryptFromRuntime(Path runtimeSqliteFile, Path encryptedFile, String passphrase) throws java.io.IOException {
        if (!Files.exists(runtimeSqliteFile)) {
            return; // nada pra persistir
        }

        try {
            SecureRandom rng = new SecureRandom();
            byte[] salt = new byte[SALT_LEN];
            byte[] nonce = new byte[NONCE_LEN];
            rng.nextBytes(salt);
            rng.nextBytes(nonce);

            SecretKey key = deriveKey(passphrase, salt);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, nonce));

            Files.createDirectories(encryptedFile.toAbsolutePath().getParent());
            try (OutputStream out = Files.newOutputStream(encryptedFile)) {
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
            }
        } catch (GeneralSecurityException e) {
            throw new java.io.IOException("Falha ao criptografar DB.", e);
        }
    }

    private static SecretKey deriveKey(String passphrase, byte[] salt) throws GeneralSecurityException {
        PBEKeySpec spec = new PBEKeySpec(passphrase.toCharArray(), salt, PBKDF2_ITERS, KEY_BITS);
        SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        byte[] keyBytes = skf.generateSecret(spec).getEncoded();
        return new SecretKeySpec(keyBytes, "AES");
    }
}
