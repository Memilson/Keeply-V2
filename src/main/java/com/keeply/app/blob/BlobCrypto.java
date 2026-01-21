package com.keeply.app.blob;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;

final class BlobCrypto {

    private static final byte[] MAGIC = "KEEPLYBLOB".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    private static final byte VERSION = 1;

    private static final int SALT_LEN = 16;
    private static final int NONCE_LEN = 12;
    private static final int PBKDF2_ITERS = 120_000;
    private static final int KEY_BITS = 256;
    private static final int GCM_TAG_BITS = 128;

    private BlobCrypto() {}

    static boolean looksEncrypted(Path file) {
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

    static OutputStream openEncryptingStream(OutputStream out, String passphrase) throws Exception {
        SecureRandom rng = new SecureRandom();
        byte[] salt = new byte[SALT_LEN];
        byte[] nonce = new byte[NONCE_LEN];
        rng.nextBytes(salt);
        rng.nextBytes(nonce);

        SecretKey key = deriveKey(passphrase, salt);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, nonce));

        out.write(MAGIC);
        out.write(VERSION);
        out.write(salt);
        out.write(nonce);
        return new CipherOutputStream(out, cipher);
    }

    static InputStream openDecryptingStream(InputStream in, String passphrase) throws Exception {
        byte[] magic = in.readNBytes(MAGIC.length);
        if (magic.length != MAGIC.length) {
            throw new IllegalStateException("Backup criptografado inválido (magic curto)");
        }
        for (int i = 0; i < MAGIC.length; i++) {
            if (magic[i] != MAGIC[i]) {
                throw new IllegalStateException("Backup criptografado inválido (magic mismatch)");
            }
        }
        int version = in.read();
        if (version != VERSION) {
            throw new IllegalStateException("Versão de criptografia não suportada: " + version);
        }

        byte[] salt = in.readNBytes(SALT_LEN);
        byte[] nonce = in.readNBytes(NONCE_LEN);
        if (salt.length != SALT_LEN || nonce.length != NONCE_LEN) {
            throw new IllegalStateException("Backup criptografado inválido (salt/nonce)");
        }

        SecretKey key = deriveKey(passphrase, salt);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, nonce));
        return new CipherInputStream(in, cipher);
    }

    private static SecretKey deriveKey(String passphrase, byte[] salt) throws Exception {
        PBEKeySpec spec = new PBEKeySpec(passphrase.toCharArray(), salt, PBKDF2_ITERS, KEY_BITS);
        SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        byte[] keyBytes = skf.generateSecret(spec).getEncoded();
        return new SecretKeySpec(keyBytes, "AES");
    }
}
