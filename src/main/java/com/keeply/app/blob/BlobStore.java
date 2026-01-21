package com.keeply.app.blob;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.keeply.app.config.Config;
import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.database.KeeplyDao;
import com.keeply.app.inventory.Backup;

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
import java.security.MessageDigest;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Armazena blobs deduplicados e gerencia backup/restore.
 */
public class BlobStore {

    private final Path rootDir;
    private static final Logger logger = LoggerFactory.getLogger(BlobStore.class);

    public BlobStore(Path baseDir) throws IOException {
        this.rootDir = baseDir.resolve(".keeply").resolve("storage");
        Files.createDirectories(this.rootDir);
    }

    public String put(Path sourceFile) throws Exception {
        String hash = calculateHash(sourceFile);

        String prefix = hash.substring(0, 2);
        Path bucketDir = rootDir.resolve(prefix);
        Path targetBlob = bucketDir.resolve(hash);

        if (Files.exists(targetBlob)) {
            return hash; 
        }

        Files.createDirectories(bucketDir);
        
        Path tempFile = rootDir.resolve(hash + ".tmp");

        try (InputStream is = Files.newInputStream(sourceFile);
             OutputStream fos = Files.newOutputStream(tempFile);
             ZstdOutputStream zos = new ZstdOutputStream(fos, 3)) {
            
            is.transferTo(zos);
        }

        Files.move(tempFile, targetBlob, StandardCopyOption.ATOMIC_MOVE);

        return hash;
    }

    public void get(String hash, Path destination) throws IOException {
        String prefix = hash.substring(0, 2);
        Path blobPath = rootDir.resolve(prefix).resolve(hash);

        if (!Files.exists(blobPath)) {
            throw new IOException("Arquivo corrompido ou ausente no cofre: " + hash);
        }

        try (InputStream fis = Files.newInputStream(blobPath);
             ZstdInputStream zis = new ZstdInputStream(fis);
             OutputStream fos = Files.newOutputStream(destination)) {
            
            zis.transferTo(fos);
        }
    }

    private String calculateHash(Path file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream fis = Files.newInputStream(file)) {
            byte[] buffer = new byte[8192];
            int bytesCount;
            while ((bytesCount = fis.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesCount);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    public record BackupResult(long filesProcessed, long errors) {}

    public record RestoreResult(long filesRestored, long errors) {}

    public static BackupResult runBackup(Path root, Backup.ScanConfig cfg, AtomicBoolean cancel, Consumer<String> uiLogger) throws Exception {
        Path baseDir = Config.getEncryptedDbFilePath().toAbsolutePath().getParent();
        if (baseDir == null) throw new IllegalStateException("Base dir not resolved for BlobStore");
        return runBackup(root, cfg, baseDir, cancel, uiLogger);
    }

    public static BackupResult runBackup(Path root, Backup.ScanConfig cfg, Path baseDir, AtomicBoolean cancel, Consumer<String> uiLogger) throws Exception {
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
                } catch (Exception e) {
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

    public static BackupResult runBackupIncremental(Path root, Backup.ScanConfig cfg, Path baseDir, long scanId, AtomicBoolean cancel, Consumer<String> uiLogger) throws Exception {
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
    ) throws Exception {
        Path rootAbs = root.toAbsolutePath().normalize();

        if (baseDir == null) {
            throw new IllegalArgumentException("baseDir is required");
        }
        ensureWritableDir(baseDir);

        DatabaseBackup.init();
        List<String> changed = DatabaseBackup.jdbi().withExtension(KeeplyDao.class, dao -> dao.fetchChangedFilesForScan(scanId));

        long total = changed.size();
        if (progress != null) progress.accept(0L, total);

        uiLogger.accept(">> Backup incremental: scanId=" + scanId + ", arquivos alterados=" + total);
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
            } catch (Exception e) {
                errors[0]++;
                logger.warn("Falha ao armazenar arquivo no backup incremental: {}", source, e);
                done++;
                if (progress != null) progress.accept(done, total);
            }
        }

        uiLogger.accept(">> Backup incremental: finalizado. Arquivos processados=" + files[0] + ", erros=" + errors[0]);
        return new BackupResult(files[0], errors[0]);
    }

    public static RestoreResult restoreChangedFilesFromScan(long scanId, Path baseDir, Path destinationDir, AtomicBoolean cancel, Consumer<String> uiLogger) throws Exception {
        if (baseDir == null || destinationDir == null) {
            throw new IllegalArgumentException("baseDir and destinationDir are required");
        }
        DatabaseBackup.init();
        List<DatabaseBackup.SnapshotBlobRow> blobs = DatabaseBackup.jdbi().withExtension(
                KeeplyDao.class,
                dao -> dao.fetchChangedBlobsForScan(scanId)
        );
        return restoreBlobs(blobs, baseDir, destinationDir, cancel, uiLogger);
    }

    public static RestoreResult restoreSelectionFromSnapshot(long scanId, List<String> filePaths, List<String> dirPrefixes, Path baseDir, Path destinationDir, AtomicBoolean cancel, Consumer<String> uiLogger) throws Exception {
        if (baseDir == null || destinationDir == null) {
            throw new IllegalArgumentException("baseDir and destinationDir are required");
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

        return restoreBlobs(List.copyOf(toRestore), baseDir, destinationDir, cancel, uiLogger);
    }

    private static RestoreResult restoreBlobs(List<DatabaseBackup.SnapshotBlobRow> blobs, Path baseDir, Path destinationDir, AtomicBoolean cancel, Consumer<String> uiLogger) throws Exception {
        List<DatabaseBackup.SnapshotBlobRow> safe = (blobs == null) ? List.of() : blobs;

        uiLogger.accept(">> Restore: arquivos encontrados=" + safe.size());
        uiLogger.accept(">> Restore: origem (cofre)=" + baseDir.resolve(".keeply").resolve("storage").toAbsolutePath());
        uiLogger.accept(">> Restore: destino=" + destinationDir.toAbsolutePath());

        Files.createDirectories(destinationDir);
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

            Path out = destinationDir.resolve(pathRel);
            try {
                Path parent = out.getParent();
                if (parent != null) Files.createDirectories(parent);
                uiLogger.accept(">> Restore: " + pathRel);
                store.get(hash, out);
                restored++;
            } catch (Exception e) {
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
}
