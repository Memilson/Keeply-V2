package com.keeply.app.blob;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.keeply.app.config.Config;
import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.database.KeeplyDao;
import com.keeply.app.inventory.Backup;

/**
 * Armazena blobs deduplicados e gerencia backup/restore.
 */
public class BlobStore {

    private final Path rootDir;
    private static final Logger logger = LoggerFactory.getLogger(BlobStore.class);
    private static final String ENCRYPTED_EXT = ".kply";

    public BlobStore(Path baseDir) throws IOException {
        this.rootDir = baseDir.resolve(".keeply").resolve("storage");
        Files.createDirectories(this.rootDir);
    }

    public String put(Path sourceFile) throws IOException {
        String hash = calculateHash(sourceFile);

        String prefix = hash.substring(0, 2);
        Path bucketDir = rootDir.resolve(prefix);
        boolean encrypt = Config.isBackupEncryptionEnabled();
        String fileName = encrypt ? (hash + ENCRYPTED_EXT) : hash;
        Path targetBlob = bucketDir.resolve(fileName);

        if (Files.exists(targetBlob)) {
            return hash; 
        }

        Files.createDirectories(bucketDir);
        
        Path tempFile = rootDir.resolve(fileName + ".tmp");

        String passphrase = encrypt ? Config.requireBackupPasswordForEncryption() : null;

        try (InputStream is = Files.newInputStream(sourceFile);
             OutputStream base = Files.newOutputStream(tempFile);
             OutputStream out = encrypt ? BlobCrypto.openEncryptingStream(base, passphrase) : base;
             ZstdOutputStream zos = new ZstdOutputStream(out, 3)) {
            is.transferTo(zos);
        }

        Files.move(tempFile, targetBlob, StandardCopyOption.ATOMIC_MOVE);
        if (encrypt && !BlobCrypto.looksEncrypted(targetBlob)) {
            throw new IOException("Falha ao verificar criptografia do backup: " + targetBlob);
        }

        return hash;
    }

    public void get(String hash, Path destination) throws IOException {
        String prefix = hash.substring(0, 2);
        Path blobPath = resolveBlobPath(prefix, hash);

        if (!Files.exists(blobPath)) {
            throw new IOException("Arquivo corrompido ou ausente no cofre: " + hash);
        }

        boolean encrypted = BlobCrypto.looksEncrypted(blobPath);
        String passphrase = encrypted ? Config.requireBackupPassword() : null;

        try (InputStream fis = Files.newInputStream(blobPath);
             InputStream in = encrypted ? safeDecryptStream(fis, passphrase) : fis;
             ZstdInputStream zis = new ZstdInputStream(in);
             OutputStream fos = Files.newOutputStream(destination)) {
            zis.transferTo(fos);
        }
    }

    private String calculateHash(Path file) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("Algoritmo SHA-256 indisponível.", e);
        }
        try (InputStream fis = Files.newInputStream(file)) {
            byte[] buffer = new byte[8192];
            int bytesCount;
            while ((bytesCount = fis.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesCount);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    @SuppressWarnings("unused")
    public record BackupResult(@SuppressWarnings("unused") long filesProcessed, @SuppressWarnings("unused") long errors) {}

    @SuppressWarnings("unused")
    public record RestoreResult(@SuppressWarnings("unused") long filesRestored, @SuppressWarnings("unused") long errors) {}

    public enum RestoreMode {
        ORIGINAL_PATH,
        DEST_WITH_STRUCTURE,
        DEST_FLAT
    }

    public static boolean verifyBackupPassword(String passphrase) {
        if (passphrase == null || passphrase.isBlank()) return false;
        Path tmpDir = null;
        Path enc = null;
        Path dec = null;
        try {
            tmpDir = Files.createTempDirectory("keeply-crypto-test");
            enc = tmpDir.resolve("test.kply");
            dec = tmpDir.resolve("test.bin");

            byte[] payload = "keeply-crypto-test".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            try (OutputStream out = Files.newOutputStream(enc);
                 OutputStream encOut = BlobCrypto.openEncryptingStream(out, passphrase)) {
                encOut.write(payload);
            }

            try (InputStream in = Files.newInputStream(enc);
                 InputStream decIn = BlobCrypto.openDecryptingStream(in, passphrase);
                 OutputStream out = Files.newOutputStream(dec)) {
                decIn.transferTo(out);
            }

            byte[] restored = Files.readAllBytes(dec);
            if (restored.length != payload.length) return false;
            for (int i = 0; i < payload.length; i++) {
                if (payload[i] != restored[i]) return false;
            }
            return true;
        } catch (IOException | RuntimeException e) {
            logger.warn("Falha ao validar senha de backup (teste de criptografia).", e);
            return false;
        } finally {
            try { if (dec != null) Files.deleteIfExists(dec); } catch (IOException ignored) {}
            try { if (enc != null) Files.deleteIfExists(enc); } catch (IOException ignored) {}
            try { if (tmpDir != null) Files.deleteIfExists(tmpDir); } catch (IOException ignored) {}
        }
    }

    public static BackupResult runBackup(Path root, Backup.ScanConfig cfg, AtomicBoolean cancel, Consumer<String> uiLogger) throws IOException {
        Path baseDir = Config.getEncryptedDbFilePath().toAbsolutePath().getParent();
        if (baseDir == null) throw new IllegalStateException("Base dir not resolved for BlobStore");
        return runBackup(root, cfg, baseDir, cancel, uiLogger);
    }

    public static BackupResult runBackup(Path root, Backup.ScanConfig cfg, Path baseDir, AtomicBoolean cancel, Consumer<String> uiLogger) throws IOException {
        Path rootAbs = root.toAbsolutePath().normalize();

        if (baseDir == null) {
            throw new IllegalArgumentException("baseDir is required");
        }
        ensureWritableDir(baseDir);

        BlobStore store = new BlobStore(baseDir);

        List<PathMatcher> matchers = cfg.excludeGlobs().stream()
                .map(g -> FileSystems.getDefault().getPathMatcher("glob:" + g))
                .toList();

        uiLogger.accept(">> Backup: iniciando (BlobStore com Zstd) ...");
        uiLogger.accept(">> Backup: destino = " + baseDir.resolve(".keeply").resolve("storage").toAbsolutePath());

        final long[] files = {0};
        final long[] errors = {0};

        Files.walkFileTree(rootAbs, EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE, new java.nio.file.SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if (cancel.get()) return java.nio.file.FileVisitResult.TERMINATE;
                if (!dir.equals(rootAbs)) {
                    Path relativePath = rootAbs.relativize(dir);
                    for (var m : matchers) {
                        if (m.matches(relativePath)) {
                            return java.nio.file.FileVisitResult.SKIP_SUBTREE;
                        }
                    }
                }
                return java.nio.file.FileVisitResult.CONTINUE;
            }

            @Override
            public java.nio.file.FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (cancel.get()) return java.nio.file.FileVisitResult.TERMINATE;
                if (!attrs.isRegularFile()) return java.nio.file.FileVisitResult.CONTINUE;

                Path relativePath = rootAbs.relativize(file);
                for (var m : matchers) {
                    if (m.matches(relativePath)) return java.nio.file.FileVisitResult.CONTINUE;
                }

                try {
                    store.put(file);
                    files[0]++;
                } catch (IOException e) {
                    errors[0]++;
                    logger.warn("Falha ao armazenar arquivo no backup: {}", file, e);
                }

                return java.nio.file.FileVisitResult.CONTINUE;
            }

            @Override
            public java.nio.file.FileVisitResult visitFileFailed(Path file, java.io.IOException exc) {
                errors[0]++;
                logger.warn("Falha ao ler arquivo durante backup: {}", file, exc);
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });

        uiLogger.accept(">> Backup: finalizado. Arquivos processados=" + files[0] + ", erros=" + errors[0]);
        return new BackupResult(files[0], errors[0]);
    }

    public static BackupResult runBackupIncremental(Path root, Backup.ScanConfig cfg, Path baseDir, long scanId, AtomicBoolean cancel, Consumer<String> uiLogger) throws IOException {
        return runBackupIncremental(root, cfg, baseDir, scanId, cancel, uiLogger, null);
    }

    public static BackupResult runBackupIncremental(
            Path root,
            Backup.ScanConfig cfg,
            Path baseDir,
            long scanId,
            AtomicBoolean cancel,
            Consumer<String> uiLogger,
            BiConsumer<Long, Long> progress
    ) throws IOException {
        Path rootAbs = root.toAbsolutePath().normalize();

        if (baseDir == null) {
            throw new IllegalArgumentException("baseDir is required");
        }
        ensureWritableDir(baseDir);

        DatabaseBackup.init();
        List<String> changed = DatabaseBackup.jdbi().withExtension(KeeplyDao.class, dao -> dao.fetchChangedFilesForScan(scanId));

        long total = changed.size();
        if (progress != null) progress.accept(0L, total);

        uiLogger.accept(">> Backup: destino = " + baseDir.resolve(".keeply").resolve("storage").toAbsolutePath());

        BlobStore store = new BlobStore(baseDir);

        final long[] files = {0};
        final long[] errors = {0};

        long done = 0;

        for (String pathRel : changed) {
            if (cancel.get()) break;

            Path source = rootAbs.resolve(pathRel);
            try {
                if (!Files.exists(source) || !Files.isRegularFile(source)) {
                    errors[0]++;
                    logger.debug("Arquivo não encontrado para backup incremental: {}", source);
                    continue;
                }

                String hash = store.put(source);
                DatabaseBackup.jdbi().useExtension(KeeplyDao.class, dao -> dao.setHistoryContentHash(scanId, pathRel, hash));
                files[0]++;
                done++;
                if (progress != null) progress.accept(done, total);
            } catch (IOException e) {
                errors[0]++;
                logger.warn("Falha ao armazenar arquivo no backup incremental: {}", source, e);
                done++;
                if (progress != null) progress.accept(done, total);
            }
        }

        return new BackupResult(files[0], errors[0]);
    }

    public static RestoreResult restoreChangedFilesFromScan(
            long scanId,
            Path baseDir,
            Path destinationDir,
            Path originalRoot,
            RestoreMode mode,
            AtomicBoolean cancel,
            Consumer<String> uiLogger
    ) throws IOException {
        if (baseDir == null) {
            throw new IllegalArgumentException("baseDir is required");
        }
        DatabaseBackup.init();
        List<DatabaseBackup.SnapshotBlobRow> blobs = DatabaseBackup.jdbi().withExtension(
                KeeplyDao.class,
                dao -> dao.fetchChangedBlobsForScan(scanId)
        );
        return restoreBlobs(blobs, baseDir, destinationDir, originalRoot, mode, cancel, uiLogger);
    }

    public static RestoreResult restoreSelectionFromSnapshot(
            long scanId,
            List<String> filePaths,
            List<String> dirPrefixes,
            Path baseDir,
            Path destinationDir,
            Path originalRoot,
            RestoreMode mode,
            AtomicBoolean cancel,
            Consumer<String> uiLogger
    ) throws IOException {
        if (baseDir == null) {
            throw new IllegalArgumentException("baseDir is required");
        }
        filePaths = (filePaths == null) ? List.of() : filePaths;
        dirPrefixes = (dirPrefixes == null) ? List.of() : dirPrefixes;

        DatabaseBackup.init();
        List<DatabaseBackup.SnapshotBlobRow> snapshot = DatabaseBackup.jdbi().withExtension(
                KeeplyDao.class,
                dao -> dao.fetchSnapshotBlobs(scanId)
        );

        Set<String> wanted = new HashSet<>();
        for (String p : filePaths) {
            if (p != null && !p.isBlank()) wanted.add(p);
        }

        Set<DatabaseBackup.SnapshotBlobRow> toRestore = new java.util.LinkedHashSet<>();
        for (DatabaseBackup.SnapshotBlobRow row : snapshot) {
            String p = row.pathRel();
            if (p == null || p.isBlank()) continue;
            if (wanted.contains(p)) {
                toRestore.add(row);
                continue;
            }
            for (String dir : dirPrefixes) {
                if (dir == null || dir.isBlank()) continue;
                String prefix = dir.endsWith("/") ? dir : (dir + "/");
                if (p.startsWith(prefix)) {
                    toRestore.add(row);
                    break;
                }
            }
        }

        return restoreBlobs(List.copyOf(toRestore), baseDir, destinationDir, originalRoot, mode, cancel, uiLogger);
    }

    private static RestoreResult restoreBlobs(
            List<DatabaseBackup.SnapshotBlobRow> blobs,
            Path baseDir,
            Path destinationDir,
            Path originalRoot,
            RestoreMode mode,
            AtomicBoolean cancel,
            Consumer<String> uiLogger
    ) throws IOException {
        List<DatabaseBackup.SnapshotBlobRow> safe = (blobs == null) ? List.of() : blobs;

        uiLogger.accept(">> Restore: arquivos encontrados=" + safe.size());
        uiLogger.accept(">> Restore: origem (cofre)=" + baseDir.resolve(".keeply").resolve("storage").toAbsolutePath());
        uiLogger.accept(">> Restore: modo=" + mode);
        if (mode == RestoreMode.ORIGINAL_PATH) {
            uiLogger.accept(">> Restore: destino(original)=" + originalRoot.toAbsolutePath());
        } else {
            uiLogger.accept(">> Restore: destino=" + destinationDir.toAbsolutePath());
        }

        if (mode == RestoreMode.ORIGINAL_PATH) {
            if (originalRoot == null) {
                throw new IllegalArgumentException("originalRoot is required for ORIGINAL_PATH");
            }
            Files.createDirectories(originalRoot);
        } else {
            if (destinationDir == null) {
                throw new IllegalArgumentException("destinationDir is required for restore");
            }
            Files.createDirectories(destinationDir);
        }
        BlobStore store = new BlobStore(baseDir);

        long restored = 0;
        long errors = 0;

        for (DatabaseBackup.SnapshotBlobRow row : safe) {
            if (cancel != null && cancel.get()) break;

            String pathRel = row.pathRel();
            String hash = row.contentHash();
            if (pathRel == null || pathRel.isBlank() || hash == null || hash.isBlank()) {
                errors++;
                continue;
            }

            Path out = resolveOutputPath(pathRel, destinationDir, originalRoot, mode);
            try {
                Path parent = out.getParent();
                if (parent != null) Files.createDirectories(parent);
                uiLogger.accept(">> Restore: " + pathRel);
                store.get(hash, out);
                restored++;
            } catch (IOException e) {
                errors++;
                uiLogger.accept(">> Restore: ERRO em " + pathRel + " (" + e.getClass().getSimpleName() + ")");
            }
        }

        uiLogger.accept(">> Restore: finalizado. arquivos=" + restored + ", erros=" + errors);
        return new RestoreResult(restored, errors);
    }

    private static void ensureWritableDir(Path baseDir) throws IOException {
        if (!Files.exists(baseDir)) {
            Files.createDirectories(baseDir);
        }
        if (!Files.isDirectory(baseDir)) {
            throw new IOException("Destino inválido para backup: " + baseDir);
        }
        if (!Files.isWritable(baseDir)) {
            throw new IOException("Sem permissão de escrita no destino do backup: " + baseDir);
        }
    }

    private Path resolveBlobPath(String prefix, String hash) {
        Path encrypted = rootDir.resolve(prefix).resolve(hash + ENCRYPTED_EXT);
        if (Files.exists(encrypted)) return encrypted;
        return rootDir.resolve(prefix).resolve(hash);
    }

    private static InputStream safeDecryptStream(InputStream in, String passphrase) throws IOException {
        return BlobCrypto.openDecryptingStream(in, passphrase);
    }

    private static final class BlobCrypto {

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

        static OutputStream openEncryptingStream(OutputStream out, String passphrase) throws java.io.IOException {
            try {
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
            } catch (GeneralSecurityException e) {
                throw new java.io.IOException("Falha ao inicializar criptografia.", e);
            }
        }

        static InputStream openDecryptingStream(InputStream in, String passphrase) throws java.io.IOException {
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
            try {
                SecretKey key = deriveKey(passphrase, salt);
                Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
                cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, nonce));
                return new CipherInputStream(in, cipher);
            } catch (GeneralSecurityException e) {
                throw new java.io.IOException("Falha ao inicializar descriptografia.", e);
            }
        }

        private static SecretKey deriveKey(String passphrase, byte[] salt) throws GeneralSecurityException {
            PBEKeySpec spec = new PBEKeySpec(passphrase.toCharArray(), salt, PBKDF2_ITERS, KEY_BITS);
            SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            byte[] keyBytes = skf.generateSecret(spec).getEncoded();
            return new SecretKeySpec(keyBytes, "AES");
        }
    }

    private static Path resolveOutputPath(String pathRel, Path destinationDir, Path originalRoot, RestoreMode mode) throws IOException {
        return switch (mode) {
            case ORIGINAL_PATH -> {
                if (originalRoot == null) throw new IOException("Destino original não resolvido");
                yield originalRoot.resolve(pathRel);
            }
            case DEST_WITH_STRUCTURE -> destinationDir.resolve(pathRel);
            case DEST_FLAT -> resolveFlatPath(destinationDir, pathRel);
        };
    }

    private static Path resolveFlatPath(Path destinationDir, String pathRel) throws IOException {
        if (destinationDir == null) throw new IOException("Destino não resolvido");
        String name = Path.of(pathRel).getFileName().toString();
        Path out = destinationDir.resolve(name);
        if (!Files.exists(out)) return out;

        String base = name;
        String ext = "";
        int idx = name.lastIndexOf('.');
        if (idx > 0 && idx < name.length() - 1) {
            base = name.substring(0, idx);
            ext = name.substring(idx);
        }

        int i = 1;
        while (true) {
            Path candidate = destinationDir.resolve(base + " (" + i + ")" + ext);
            if (!Files.exists(candidate)) return candidate;
            i++;
        }
    }
}
