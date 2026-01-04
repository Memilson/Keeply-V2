package com.keeply.app.blob;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class BlobStoreTest {

    @Test
    void putStoresFileAndGetRestoresSameBytes() throws Exception {
        Path baseDir = Files.createTempDirectory("keeply-blobstore-");
        BlobStore store = new BlobStore(baseDir);

        Path source = baseDir.resolve("source.txt");
        byte[] bytes = "hello keeply".getBytes(StandardCharsets.UTF_8);
        Files.write(source, bytes);

        String hash = store.put(source);
        assertNotNull(hash);
        assertEquals(64, hash.length(), "SHA-256 hex should be 64 chars");

        Path restored = baseDir.resolve("restored.txt");
        store.get(hash, restored);

        assertArrayEquals(bytes, Files.readAllBytes(restored));
    }

    @Test
    void putIsDeduplicated_sameContentSameHash_singleStoredBlob() throws Exception {
        Path baseDir = Files.createTempDirectory("keeply-blobstore-");
        BlobStore store = new BlobStore(baseDir);

        Path a = baseDir.resolve("a.txt");
        Path b = baseDir.resolve("b.txt");
        Files.writeString(a, "same", StandardCharsets.UTF_8);
        Files.writeString(b, "same", StandardCharsets.UTF_8);

        String hash1 = store.put(a);
        String hash2 = store.put(b);

        assertEquals(hash1, hash2, "Same content must produce same hash");

        Path storageRoot = baseDir.resolve(".keeply").resolve("storage");
        String prefix = hash1.substring(0, 2);
        Path bucket = storageRoot.resolve(prefix);
        Path blobPath = bucket.resolve(hash1);

        assertTrue(Files.exists(blobPath), "Blob should exist in storage");

        try (Stream<Path> s = Files.list(bucket)) {
            long count = s.filter(p -> p.getFileName().toString().equals(hash1)).count();
            assertEquals(1L, count, "Only one blob file should be stored for deduplicated content");
        }
    }

    @Test
    void getMissingHashThrows() throws Exception {
        Path baseDir = Files.createTempDirectory("keeply-blobstore-");
        BlobStore store = new BlobStore(baseDir);

        assertThrows(IOException.class, () -> store.get("00" + "a".repeat(62), baseDir.resolve("out.bin")));
    }
}
