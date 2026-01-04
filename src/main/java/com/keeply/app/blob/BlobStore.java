package com.keeply.app.blob;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.keeply.app.config.Config;
import com.keeply.app.database.Database;
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
import java.util.function.Consumer;

public class BlobStore {

    private final Path rootDir;

    public BlobStore(Path baseDir) throws IOException {
        // Define a raiz do cofre, ex: C:/Users/Angelo/.keeply/storage
        this.rootDir = baseDir.resolve(".keeply").resolve("storage");
        Files.createDirectories(this.rootDir);
    }

    /**
     * Guarda um arquivo no cofre com compressão Zstd.
     * @param sourceFile O arquivo original no disco do usuário.
     * @return O Hash (SHA-256) do conteúdo ORIGINAL.
     */
    public String put(Path sourceFile) throws Exception {
        // 1. Calcular o Hash do conteúdo ORIGINAL (Crucial para deduplicação correta)
        String hash = calculateHash(sourceFile);

        // 2. Definir onde ele vai morar no cofre
        String prefix = hash.substring(0, 2);
        Path bucketDir = rootDir.resolve(prefix);
        Path targetBlob = bucketDir.resolve(hash);

        // 3. Verificar se já existe (Deduplicação)
        if (Files.exists(targetBlob)) {
            // Já temos este arquivo (seja comprimido ou não, o hash é a chave)
            return hash; 
        }

        // 4. Se não existe, cria a pasta e prepara a cópia
        Files.createDirectories(bucketDir);
        
        // Nome temporário para garantir atomicidade
        Path tempFile = rootDir.resolve(hash + ".tmp");

        // --- COMPRESSÃO ZSTD ---
        // Lemos o original -> Comprimimos -> Escrevemos no temporário
        try (InputStream is = Files.newInputStream(sourceFile);
             OutputStream fos = Files.newOutputStream(tempFile);
             // Nível 3 é o padrão do Zstd (bom equilíbrio velocidade/tamanho)
             ZstdOutputStream zos = new ZstdOutputStream(fos, 3)) {
            
            is.transferTo(zos);
            // O ZstdOutputStream finaliza e fecha automaticamente aqui
        }

        // 5. Move atomicamente para o local final
        Files.move(tempFile, targetBlob, StandardCopyOption.ATOMIC_MOVE);

        return hash;
    }

    /**
     * Recupera um arquivo do cofre (Restore) descomprimindo-o.
     */
    public void get(String hash, Path destination) throws IOException {
        String prefix = hash.substring(0, 2);
        Path blobPath = rootDir.resolve(prefix).resolve(hash);

        if (!Files.exists(blobPath)) {
            throw new IOException("Arquivo corrompido ou ausente no cofre: " + hash);
        }

        // --- DESCOMPRESSÃO ZSTD ---
        // Lemos do cofre -> Descomprimimos -> Escrevemos no destino
        try (InputStream fis = Files.newInputStream(blobPath);
             ZstdInputStream zis = new ZstdInputStream(fis);
             OutputStream fos = Files.newOutputStream(destination)) {
            
            zis.transferTo(fos);
        }
    }

    // Função auxiliar para calcular SHA-256 de forma eficiente (lê o arquivo cru)
    private String calculateHash(Path file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream fis = Files.newInputStream(file)) {
            byte[] buffer = new byte[8192]; // Lê em pedaços de 8KB
            int bytesCount;
            while ((bytesCount = fis.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesCount);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    // -----------------------------
    // Backup runner (post-scan)
    // -----------------------------

    public record BackupResult(long filesProcessed, long errors) {}

    public record RestoreResult(long filesRestored, long errors) {}

    /**
     * Runs a "backup" by copying scanned files into the local BlobStore.
     *
     * This does not change BlobStore format/logic; it only calls put() for each file.
     */
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
                }

                return java.nio.file.FileVisitResult.CONTINUE;
            }

            @Override
            public java.nio.file.FileVisitResult visitFileFailed(Path file, java.io.IOException exc) {
                errors[0]++;
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });

        uiLogger.accept(">> Backup: finalizado. Arquivos processados=" + files[0] + ", erros=" + errors[0]);
        return new BackupResult(files[0], errors[0]);
    }

    /**
     * Incremental backup: only uploads NEW/MODIFIED files from the given scanId.
     * Also writes the produced SHA-256 hash back into file_history.content_hash.
     */
    public static BackupResult runBackupIncremental(Path root, Backup.ScanConfig cfg, Path baseDir, long scanId, AtomicBoolean cancel, Consumer<String> uiLogger) throws Exception {
        Path rootAbs = root.toAbsolutePath().normalize();

        if (baseDir == null) {
            throw new IllegalArgumentException("baseDir is required");
        }

        Database.init();
        List<String> changed = Database.jdbi().withExtension(KeeplyDao.class, dao -> dao.fetchChangedFilesForScan(scanId));

        uiLogger.accept(">> Backup incremental: scanId=" + scanId + ", arquivos alterados=" + changed.size());
        uiLogger.accept(">> Backup: destino = " + baseDir.resolve(".keeply").resolve("storage").toAbsolutePath());

        BlobStore store = new BlobStore(baseDir);

        final long[] files = {0};
        final long[] errors = {0};

        for (String pathRel : changed) {
            if (cancel.get()) break;

            Path source = rootAbs.resolve(pathRel);
            try {
                if (!Files.exists(source) || !Files.isRegularFile(source)) {
                    errors[0]++;
                    continue;
                }

                String hash = store.put(source);
                Database.jdbi().useExtension(KeeplyDao.class, dao -> dao.setHistoryContentHash(scanId, pathRel, hash));
                files[0]++;
            } catch (Exception e) {
                errors[0]++;
            }
        }

        uiLogger.accept(">> Backup incremental: finalizado. Arquivos processados=" + files[0] + ", erros=" + errors[0]);
        return new BackupResult(files[0], errors[0]);
    }

    // -----------------------------
    // Restore (via DB + BlobStore)
    // -----------------------------

    /**
     * Restore only the NEW/MODIFIED files of a specific scanId.
     */
    public static RestoreResult restoreChangedFilesFromScan(long scanId, Path baseDir, Path destinationDir, AtomicBoolean cancel, Consumer<String> uiLogger) throws Exception {
        if (baseDir == null || destinationDir == null) {
            throw new IllegalArgumentException("baseDir and destinationDir are required");
        }
        Database.init();
        List<Database.SnapshotBlobRow> blobs = Database.jdbi().withExtension(
                KeeplyDao.class,
                dao -> dao.fetchChangedBlobsForScan(scanId)
        );
        return restoreBlobs(blobs, baseDir, destinationDir, cancel, uiLogger);
    }

    /**
     * Restore a selection (files and folders) as-of a snapshot. Folder prefixes restore all files under them.
     * Selection itself should be limited by the caller (e.g. up to 10 nodes).
     */
    public static RestoreResult restoreSelectionFromSnapshot(long scanId, List<String> filePaths, List<String> dirPrefixes, Path baseDir, Path destinationDir, AtomicBoolean cancel, Consumer<String> uiLogger) throws Exception {
        if (baseDir == null || destinationDir == null) {
            throw new IllegalArgumentException("baseDir and destinationDir are required");
        }
        filePaths = (filePaths == null) ? List.of() : filePaths;
        dirPrefixes = (dirPrefixes == null) ? List.of() : dirPrefixes;

        Database.init();
        List<Database.SnapshotBlobRow> snapshot = Database.jdbi().withExtension(
                KeeplyDao.class,
                dao -> dao.fetchSnapshotBlobs(scanId)
        );

        Set<String> wanted = new HashSet<>();
        for (String p : filePaths) {
            if (p != null && !p.isBlank()) wanted.add(p);
        }

        Set<Database.SnapshotBlobRow> toRestore = new java.util.LinkedHashSet<>();
        for (Database.SnapshotBlobRow row : snapshot) {
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

    private static RestoreResult restoreBlobs(List<Database.SnapshotBlobRow> blobs, Path baseDir, Path destinationDir, AtomicBoolean cancel, Consumer<String> uiLogger) throws Exception {
        List<Database.SnapshotBlobRow> safe = (blobs == null) ? List.of() : blobs;

        uiLogger.accept(">> Restore: arquivos encontrados=" + safe.size());
        uiLogger.accept(">> Restore: origem (cofre)=" + baseDir.resolve(".keeply").resolve("storage").toAbsolutePath());
        uiLogger.accept(">> Restore: destino=" + destinationDir.toAbsolutePath());

        Files.createDirectories(destinationDir);
        BlobStore store = new BlobStore(baseDir);

        long restored = 0;
        long errors = 0;

        for (Database.SnapshotBlobRow row : safe) {
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
}