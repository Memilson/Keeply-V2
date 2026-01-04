package com.keeply.app.blob;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.HexFormat;

public class BlobStore {

    private final Path rootDir;

    public BlobStore(Path baseDir) throws IOException {
        // Define a raiz do cofre, ex: C:/Users/Angelo/.keeply/storage
        this.rootDir = baseDir.resolve(".keeply").resolve("storage");
        Files.createDirectories(this.rootDir);
    }

    /**
     * Guarda um arquivo no cofre.
     * @param sourceFile O arquivo original no disco do usuário.
     * @return O Hash (SHA-256) do arquivo, que servirá como ID.
     */
    public String put(Path sourceFile) throws Exception {
        // 1. Calcular o Hash (O "DNA" do arquivo)
        String hash = calculateHash(sourceFile);

        // 2. Definir onde ele vai morar no cofre
        // Ex: Hash = "a1b2c3..." -> Pasta = "storage/a1/" -> Arquivo = "storage/a1/a1b2c3..."
        String prefix = hash.substring(0, 2);
        Path bucketDir = rootDir.resolve(prefix);
        Path targetBlob = bucketDir.resolve(hash);

        // 3. Verificar se já existe (Deduplicação)
        if (Files.exists(targetBlob)) {
            // Já temos esse conteúdo! Não precisa copiar.
            // Apenas retornamos o hash para o banco de dados saber.
            return hash; 
        }

        // 4. Se não existe, cria a pasta e copia
        Files.createDirectories(bucketDir);
        
        // Copia ATOMICAMENTE (para evitar arquivos corrompidos se a luz cair)
        Path tempFile = rootDir.resolve(hash + ".tmp");
        Files.copy(sourceFile, tempFile, StandardCopyOption.REPLACE_EXISTING);
        Files.move(tempFile, targetBlob, StandardCopyOption.ATOMIC_MOVE);

        return hash;
    }

    /**
     * Recupera um arquivo do cofre (Restore).
     */
    public void get(String hash, Path destination) throws IOException {
        String prefix = hash.substring(0, 2);
        Path blobPath = rootDir.resolve(prefix).resolve(hash);

        if (!Files.exists(blobPath)) {
            throw new IOException("Arquivo corrompido ou ausente no cofre: " + hash);
        }

        Files.copy(blobPath, destination, StandardCopyOption.REPLACE_EXISTING);
    }

    // Função auxiliar para calcular SHA-256 de forma eficiente
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
}