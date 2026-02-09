package com.keeply.app.service;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.service.RealtimeJournalService.FsChange;

/**
 * Serviço simples para ingerir eventos do filesystem em tempo real (fs_events).
 */
public final class JournalIngestService implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(JournalIngestService.class);

    private final Path root;
    private final RealtimeJournalService journal;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public JournalIngestService(Path root) {
        this.root = Objects.requireNonNull(root, "root");
        this.journal = new RealtimeJournalService(this.root, this::onEvent);
    }

    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) return;
        DatabaseBackup.init();
        journal.start();
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
        journal.close();
    }
}
