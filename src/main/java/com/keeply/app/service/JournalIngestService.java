package com.keeply.app.service;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.keeply.app.database.DatabaseBackup;

/**
 * Serviço unificado: monitora mudanças em tempo real e ingere eventos no banco (fs_events).
 */
public final class JournalIngestService implements Closeable {

    public enum Backend { WATCHSERVICE, USN }

    public enum Kind { CREATE, MODIFY, DELETE, MOVE, OVERFLOW }

    public record FsChange(
            Kind kind,
            Path root,
            String pathRel,
            String oldPathRel,
            Instant at
    ) {}

    private static final Logger logger = LoggerFactory.getLogger(JournalIngestService.class);

    private final Path root;
    private final ExecutorService ioLoop;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Closeable source; // WatchSource hoje; USN amanhã

    public JournalIngestService(Path root) {
        this.root = Objects.requireNonNull(root, "root");
        this.ioLoop = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "keeply-journal-io");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) return;

        DatabaseBackup.init();

        WatchSource ws = new WatchSource(root, this::onEvent);
        ws.start();
        this.source = ws;
        ioLoop.submit(ws::loop);

        logger.info("JournalIngestService iniciado para {}", root.toAbsolutePath());
    }

    private void onEvent(FsChange ev) {
        try {
            DatabaseBackup.jdbi().useHandle(h -> {
                h.execute(
                    "INSERT INTO fs_events (root_path, path_rel, kind, old_path_rel, event_time) VALUES (?,?,?,?,?)",
                    ev.root().toString(),
                    ev.pathRel(),
                    ev.kind().name(),
                    ev.oldPathRel(),
                    ev.at().toString()
                );
            });
        } catch (Exception e) {
            logger.warn("Falha ao inserir fs_event: {}", e.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        running.set(false);
        ioLoop.shutdownNow();
        if (source != null) source.close();
    }

    // -------------------------------------------------------------------------
    // WatchService implementation (recursivo + auto-register em diretórios novos)
    // -------------------------------------------------------------------------
    private static final class WatchSource implements Closeable {
        private final Path root;
        private final Consumer<FsChange> sink;

        private final WatchService watcher;
        private final Map<WatchKey, Path> keyToDir = new ConcurrentHashMap<>();
        private final AtomicBoolean alive = new AtomicBoolean(true);

        WatchSource(Path root, Consumer<FsChange> sink) throws IOException {
            this.root = root;
            this.sink = sink;
            this.watcher = FileSystems.getDefault().newWatchService();
        }

        void start() throws IOException {
            registerTree(root);
        }

        void loop() {
            while (alive.get()) {
                WatchKey key;
                try {
                    key = watcher.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                Path dir = keyToDir.get(key);
                if (dir == null) {
                    key.reset();
                    continue;
                }

                for (WatchEvent<?> ev : key.pollEvents()) {
                    WatchEvent.Kind<?> k = ev.kind();

                    if (k == OVERFLOW) {
                        sink.accept(new FsChange(Kind.OVERFLOW, root, rel(dir), null, Instant.now()));
                        continue;
                    }

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> pev = (WatchEvent<Path>) ev;
                    Path name = pev.context();
                    Path full = dir.resolve(name);

                    Kind outKind =
                            (k == ENTRY_CREATE) ? Kind.CREATE :
                            (k == ENTRY_DELETE) ? Kind.DELETE :
                            Kind.MODIFY;

                    sink.accept(new FsChange(outKind, root, rel(full), null, Instant.now()));

                    if (outKind == Kind.CREATE) {
                        try {
                            if (Files.isDirectory(full, LinkOption.NOFOLLOW_LINKS)) {
                                registerTree(full);
                            }
                        } catch (IOException ignored) {
                            // best-effort
                        }
                    }
                }

                boolean valid = key.reset();
                if (!valid) {
                    keyToDir.remove(key);
                }
            }
        }

        private void registerTree(Path start) throws IOException {
            Files.walkFileTree(start, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    registerDir(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        }

        private void registerDir(Path dir) throws IOException {
            WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
            keyToDir.put(key, dir);
        }

        private String rel(Path p) {
            try {
                Path rp = root.toAbsolutePath().normalize();
                Path ap = p.toAbsolutePath().normalize();
                if (ap.startsWith(rp)) {
                    return rp.relativize(ap).toString().replace('\\', '/');
                }
            } catch (Exception ignored) {}
            return p.toString().replace('\\', '/');
        }

        @Override
        public void close() throws IOException {
            alive.set(false);
            watcher.close();
            keyToDir.clear();
        }
    }

    // -------------------------------------------------------------------------
    // FUTURO: Windows USN Journal (NTFS) - "journal real"
    // -------------------------------------------------------------------------
    @SuppressWarnings("unused")
    private static final class UsnSource implements Closeable {
        @Override public void close() {}
    }

    // -------------------------------------------------------------------------
    // FUTURO: VSS Snapshotter - consistência de leitura (não é watcher)
    // -------------------------------------------------------------------------
    @SuppressWarnings("unused")
    public interface Snapshotter extends Closeable {
        Path openSnapshotRoot(Path root) throws Exception;
    }
}
