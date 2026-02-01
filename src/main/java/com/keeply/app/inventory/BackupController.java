package com.keeply.app.inventory;

import com.keeply.app.config.Config;
import com.keeply.app.blob.BlobStore;
import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.history.BackupHistoryDb;
import com.keeply.app.templates.KeeplyTemplate.ScanModel;

import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.scene.control.Alert;
import javafx.util.Duration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BackupController {

    private final BackupScreen view;
    private final ScanModel model;

    private volatile ScannerTask currentTask;
    private volatile Thread scanThread;
    private volatile Timeline uiUpdater;

    private static final Logger logger = LoggerFactory.getLogger(BackupController.class);

    public BackupController(BackupScreen view, ScanModel model) {
        this.view = view;
        this.model = model;
        wireEvents();
    }

    private void wireEvents() {
        view.getScanButton().setOnAction(e -> startScan());
        view.getStopButton().setOnAction(e -> stopScan());
        view.getWipeButton().setOnAction(e -> wipeDatabase());
    }

    // -----------------------------
    // FX-thread helpers
    // -----------------------------
    private void ui(Runnable r) {
        if (Platform.isFxApplicationThread()) r.run();
        else Platform.runLater(r);
    }

    private void log(String msg) {
        ui(() -> view.appendLog(msg));
    }

    private void showError(String message) {
        ui(() -> {
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle("Erro");
            alert.setHeaderText("Não foi possível iniciar o backup");
            alert.setContentText(message);
            alert.showAndWait();
        });
    }

    private void errorAndLog(String message) {
        log(">> Erro: " + message);
        showError(message);
    }

    private void setScanningState(boolean scanning) {
        ui(() -> view.setScanningState(scanning));
    }

    // -----------------------------
    // Actions
    // -----------------------------
    private void startScan() {
        if (currentTask != null && currentTask.isRunning()) {
            log(">> Aviso: Backup anterior ainda está finalizando...");
            return;
        }

        if (view.isCloudSelected()) {
            errorAndLog("Nuvem ainda não implementado (placeholder). Selecione 'Disco local'.");
            return;
        }

        var pathText = view.getRootPathText();
        if (pathText == null || pathText.isBlank()) {
            errorAndLog("Selecione uma pasta de origem.");
            return;
        }

        var destText = view.getBackupDestinationText();
        if (destText == null || destText.isBlank()) {
            errorAndLog("Selecione uma pasta de destino para salvar os backups.");
            return;
        }

        if (view.isEncryptionEnabled()) {
            String pass = view.getBackupEncryptionPassword();
            if (pass == null || pass.isBlank()) {
                errorAndLog("Informe uma senha para criptografar os backups.");
                return;
            }
            if (!Config.verifyAndCacheBackupPassword(pass)) {
                errorAndLog("Senha incorreta. Digite a senha configurada para desbloquear.");
                return;
            }
            log(">> Criptografia ativa: senha validada.");
        }

        try {
            ui(() -> {
                view.clearConsole();
                model.reset();
                view.setScanningState(true);
                model.phaseProperty.set("Preparando...");
                model.progressProperty.set(-1);
            });

            // Cria a configuração simplificada (sem hash)
            var config = buildScanConfig();

            // Inicia a tarefa
            currentTask = new ScannerTask(pathText, destText, config);
            
            // Thread Virtual para orquestrar o scan
            scanThread = Thread.ofVirtual()
                    .name("keeply-scan-main")
                    .start(currentTask);

        } catch (Exception e) {
            log(">> ERRO AO INICIAR: " + safeMsg(e));
            logger.error("Erro ao iniciar backup", e);
            setScanningState(false);
            currentTask = null;
            scanThread = null;
        }
    }

    private void stopScan() {
        var task = currentTask;
        if (task != null && task.isRunning()) {
            log("!!! Solicitando parada... Aguarde...");
            task.cancel();
            
            var t = scanThread;
            if (t != null) t.interrupt();
            
            ui(() -> view.getStopButton().setDisable(true));
        }
    }

    private void wipeDatabase() {
        if (!view.confirmWipe()) return;

        setScanningState(true);

        Thread.ofVirtual().name("keeply-db-wipe").start(() -> {
            log("!!! APAGANDO BACKUPS (DB + COFRE) !!!");
            try {
                ui(() -> {
                    model.phaseProperty.set("Apagando backups...");
                    model.progressProperty.set(-1);
                });
                requestStopAndWait(5, TimeUnit.SECONDS);
                var t = scanThread;
                if (t != null && t.isAlive()) {
                    throw new IllegalStateException("Backup ainda está rodando. Aguarde finalizar/cancelar e tente novamente.");
                }

                // 1) Delete backup vault binaries (.keeply/storage) under the configured destination(s)
                wipeBackupStorageBestEffort();

                // 2) Delete SQLite DB files (history/metadata)
                performWipeLogic();
                log(">> SUCESSO: Backups apagados (banco + cofre). O app recria o DB no próximo backup.");
            } catch (Exception e) {
                log(">> ERRO NO WIPE: " + safeMsg(e));
                logger.error("Erro no wipe do banco", e);
            } finally {
                setScanningState(false);
                ui(() -> {
                    model.phaseProperty.set("Idle");
                    model.progressProperty.set(0);
                });
                ui(() -> view.getStopButton().setDisable(false));
            }
        });
    }

    private void requestStopAndWait(long timeout, TimeUnit unit) {
        var t = scanThread;
        if (t == null || !t.isAlive()) return;
        stopScan();
        try {
            t.join(unit.toMillis(timeout));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void performWipeLogic() throws Exception {
        // Wipe only the Keeply database files (history/metadata).
        // Backup vault deletion is handled separately.

        Path encrypted = Config.getEncryptedDbFilePath();
        Path runtime = Config.getRuntimeDbFilePath();

        if (encrypted == null || runtime == null) {
            log(">> ERRO: Caminho do banco não resolvido.");
            return;
        }

        log(">> Encerrando conexões e removendo arquivos de banco...");

        try {
            DatabaseBackup.shutdown();
        } catch (Exception ignored) {
            // Best-effort.
        }

        // Ensure the directory exists
        if (encrypted.getParent() != null) {
            Files.createDirectories(encrypted.getParent());
        }

        boolean anyFailed = false;

        // Delete runtime + WAL/SHM
        anyFailed |= !deleteIfExistsQuiet(runtime, 12, 75);
        anyFailed |= !deleteIfExistsQuiet(Path.of(runtime.toString() + "-wal"), 12, 75);
        anyFailed |= !deleteIfExistsQuiet(Path.of(runtime.toString() + "-shm"), 12, 75);

        // Delete encrypted + any accidental sqlite sidecars
        anyFailed |= !deleteIfExistsQuiet(encrypted, 6, 75);
        anyFailed |= !deleteIfExistsQuiet(Path.of(encrypted.toString() + "-wal"), 6, 75);
        anyFailed |= !deleteIfExistsQuiet(Path.of(encrypted.toString() + "-shm"), 6, 75);

        // If encryption is enabled, also try removing legacy plaintext base name (without .enc)
        if (Config.isDbEncryptionEnabled()) {
            String name = encrypted.getFileName().toString();
            if (name.endsWith(".enc")) {
                Path legacy = encrypted.resolveSibling(name.substring(0, name.length() - 4));
                anyFailed |= !deleteIfExistsQuiet(legacy, 6, 75);
                anyFailed |= !deleteIfExistsQuiet(Path.of(legacy.toString() + "-wal"), 6, 75);
                anyFailed |= !deleteIfExistsQuiet(Path.of(legacy.toString() + "-shm"), 6, 75);
            }
        }

        if (anyFailed) {
            log(">> Falha ao apagar algum arquivo. Limpando tabelas como fallback.");
            wipeTablesFallback();
        } else {
            log(">> Arquivos removidos (ou já não existiam). Um novo banco será criado no próximo backup.");
            DatabaseBackup.init();
        }
    }

    private void wipeBackupStorageBestEffort() {
        Set<Path> baseDirs = new LinkedHashSet<>();

        // 1) From UI field
        try {
            String txt = view.getBackupDestinationText();
            if (txt != null && !txt.isBlank()) baseDirs.add(Path.of(txt).toAbsolutePath().normalize());
        } catch (Exception ignored) {}

        // 2) From persisted config
        try {
            String txt = Config.getLastBackupDestination();
            if (txt != null && !txt.isBlank()) baseDirs.add(Path.of(txt).toAbsolutePath().normalize());
        } catch (Exception ignored) {}

        // 3) Legacy/default location: alongside the DB directory.
        // Older code paths may have stored the vault under %APPDATA%\Keeply\.keeply\storage.
        try {
            Path p = Config.getEncryptedDbFilePath();
            Path parent = (p == null) ? null : p.toAbsolutePath().getParent();
            if (parent != null) baseDirs.add(parent.normalize());
        } catch (Exception ignored) {}

        if (baseDirs.isEmpty()) {
            baseDirs.add(Path.of(".").toAbsolutePath().normalize());
        }

        log(">> Apagando backups locais (BlobStore/.keeply/storage)...");
        log(">> Locais candidatos: " + baseDirs);

        boolean anyDeleted = false;
        boolean anyFailed = false;

        for (Path baseDir : baseDirs) {
            Path storageDir = baseDir.resolve(".keeply").resolve("storage");
            log(">> Cofre = " + storageDir.toAbsolutePath());

            if (!Files.exists(storageDir)) {
                log(">> Nenhum cofre encontrado aqui (ok).");
                continue;
            }

            try {
                deleteDirectoryRecursive(storageDir, 8, 90);
                anyDeleted = true;
                log(">> Backups locais removidos.");
            } catch (Exception e) {
                anyFailed = true;
                log(">> Aviso: falha ao apagar cofre: " + safeMsg(e));
                logger.warn("Falha ao apagar cofre {}", storageDir, e);
            }
        }

        if (!anyDeleted && !anyFailed) {
            log(">> Nenhum cofre encontrado (ok). ");
        }
    }

    private static void deleteDirectoryRecursive(Path dir, int attempts, long delayMillis) throws Exception {
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws java.io.IOException {
                deletePathWithRetry(file, attempts, delayMillis);
                return java.nio.file.FileVisitResult.CONTINUE;
            }

            @Override
            public java.nio.file.FileVisitResult postVisitDirectory(Path d, java.io.IOException exc) throws java.io.IOException {
                if (exc != null) throw exc;
                deletePathWithRetry(d, attempts, delayMillis);
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });
    }

    private static void deletePathWithRetry(Path p, int attempts, long delayMillis) throws java.io.IOException {
        if (p == null) return;

        java.io.IOException last = null;
        int tries = Math.max(1, attempts);

        for (int i = 1; i <= tries; i++) {
            try {
                Files.deleteIfExists(p);
                return;
            } catch (java.io.IOException e) {
                last = e;

                // On Windows, read-only attribute can prevent deletion.
                try {
                    Files.setAttribute(p, "dos:readonly", false);
                } catch (Exception ignored) {}

                if (delayMillis > 0 && i < tries) {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        if (last != null) throw last;
    }

    private boolean deleteIfExistsQuiet(Path p) {
        return deleteIfExistsQuiet(p, 1, 0);
    }

    private boolean deleteIfExistsQuiet(Path p, int attempts, long delayMillis) {
        if (p == null) return true;
        Exception last = null;
        for (int i = 1; i <= Math.max(1, attempts); i++) {
            try {
                Files.deleteIfExists(p);
                return true;
            } catch (Exception e) {
                last = e;
                // Windows may keep file handles briefly after closing SQLite/Hikari.
                if (delayMillis > 0 && i < attempts) {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        logger.warn("Falha ao deletar {}", p, last);
        return false;
    }

    private void wipeTablesFallback() throws Exception {
        DatabaseBackup.init();
        DatabaseBackup.jdbi().useHandle(handle -> {
            handle.execute("PRAGMA busy_timeout = 5000");
            log(">> Apagando tabelas...");
            handle.execute("DELETE FROM scan_issues");
            handle.execute("DELETE FROM file_inventory");
            handle.execute("DELETE FROM scans");
            // Reset AUTOINCREMENT counters
            try {
                handle.execute("DELETE FROM sqlite_sequence");
            } catch (Exception ignored) {
                // sqlite_sequence may not exist on some schemas; ignore
            }
            log(">> Compactando arquivo (VACUUM)...");
            handle.execute("VACUUM");
        });
    }


    // --- CONFIGURAÇÃO (Otimizada) ---
    private Backup.ScanConfig buildScanConfig() {
        var defaults = Backup.ScanConfig.defaults();
        
        var excludes = new ArrayList<>(defaults.excludeGlobs());
        // Adiciona exclusões de sistema
        excludes.addAll(List.of(
            "**/Keeply/**", "**/*.keeply*", 
            "**/.keeply/**",
            "**/AppData/Local/Temp/**", "**/AppData/Local/Microsoft/**",
            "**/ntuser.dat*", "**/Cookies/**", "**/$Recycle.Bin/**",
            "**/System Volume Information/**", "**/Windows/**"
        ));

        // Note: removemos os parâmetros de HashWorker, pois o novo Scanner não usa
        return new Backup.ScanConfig(
                defaults.dbBatchSize(),
                List.copyOf(excludes)
        );
    }

    // --- UI UPDATER ---
    private void startUiUpdater(Backup.ScanMetrics metrics) {
        stopUiUpdater();
        uiUpdater = new Timeline(new KeyFrame(Duration.millis(250), evt -> updateModel(metrics)));
        uiUpdater.setCycleCount(Timeline.INDEFINITE);
        uiUpdater.play();
    }

    private void stopUiUpdater() {
        if (uiUpdater != null) {
            uiUpdater.stop();
            uiUpdater = null;
        }
    }

    private void updateModel(Backup.ScanMetrics m) {
        long elapsed = java.time.Duration.between(m.start, Instant.now()).toSeconds();
        double seconds = Math.max(1.0, elapsed);
        
        long scanned = m.filesSeen.sum();
        
        // UI simplificada sem métricas de Hash
        model.filesScannedProperty.set("%,d".formatted(scanned));
        model.rateProperty.set("%.0f files/s".formatted(scanned / seconds));
        model.dbBatchesProperty.set(Long.toString(m.dbBatches.sum()));
        model.errorsProperty.set(Long.toString(m.walkErrors.sum()));
        
        // Feedback visual de atividade
        String phase = m.running.get() ? "Validando metadados..." : "Idle";
        model.mbPerSecProperty.set(phase);
        model.phaseProperty.set(phase);
    }

    // --- WORKER ---
    private final class ScannerTask implements Runnable {
        private final String rootPath;
        private final String backupDest;
        private final Backup.ScanConfig config;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final AtomicBoolean cancelRequested = new AtomicBoolean(false);
        private DatabaseBackup.SimplePool pool;

        ScannerTask(String rootPath, String backupDest, Backup.ScanConfig config) {
            this.rootPath = rootPath;
            this.backupDest = backupDest;
            this.config = config;
        }

        public boolean isRunning() { return running.get(); }
        
        public void cancel() { 
            cancelRequested.set(true);
            if (scanThread != null) scanThread.interrupt();
        }

        @Override
        public void run() {
            var metrics = new Backup.ScanMetrics();
            running.set(true);
            cancelRequested.set(false);
            metrics.running.set(true);
            ui(() -> startUiUpdater(metrics));

            long historyId = BackupHistoryDb.start(rootPath, backupDest);
            long scanId = -1;
            BlobStore.BackupResult backupResult = null;
            String historyStatus = "SUCCESS";
            String historyMessage = null;
            String backupType = null;

            try {
                log(">> Conectando ao Banco de Dados...");
                ui(() -> {
                    model.phaseProperty.set("Validando metadados...");
                    model.progressProperty.set(-1);
                });
                // Pool pequeno é suficiente, pois agora só temos 1 Writer e a UI
                DatabaseBackup.init();
                pool = new DatabaseBackup.SimplePool(Config.getDbUrl(), 4);

                log(">> Iniciando motor de metadados...");
                scanId = Backup.runScan(java.nio.file.Path.of(rootPath), config, pool, metrics, cancelRequested, BackupController.this::log);
                try {
                    Long firstId = DatabaseBackup.jdbi().withExtension(
                            com.keeply.app.database.KeeplyDao.class,
                            dao -> dao.fetchFirstScanIdForRoot(rootPath)
                    );
                    if (firstId != null && firstId == scanId) {
                        backupType = "FULL";
                    } else {
                        backupType = "INCREMENTAL";
                    }
                } catch (Exception ignored) {
                }

                // Depois que o scan terminou, aciona o Backup (BlobStore).
                if (!cancelRequested.get()) {
                    ui(() -> {
                        model.phaseProperty.set("Backup: comprimindo/gravando...");
                        model.progressProperty.set(-1);
                    });
                    backupResult = BlobStore.runBackupIncremental(
                            java.nio.file.Path.of(rootPath),
                            config,
                            java.nio.file.Path.of(backupDest),
                            scanId,
                            cancelRequested,
                            BackupController.this::log,
                            (done, total) -> ui(() -> {
                                if (total == null || total <= 0) {
                                    model.progressProperty.set(1);
                                    model.phaseProperty.set("Backup: nada para enviar");
                                } else {
                                    double p = Math.min(1.0, Math.max(0.0, done.doubleValue() / total.doubleValue()));
                                    model.progressProperty.set(p);
                                    model.phaseProperty.set("Backup: " + done + "/" + total);
                                }
                            })
                    );
                } else {
                    historyStatus = "CANCELED";
                }

            } catch (InterruptedException ie) {
                log(">> Operação interrompida.");
                historyStatus = "CANCELED";
                historyMessage = "Interrompido";
            } catch (Exception e) {
                log(">> ERRO FATAL: " + safeMsg(e));
                logger.error("Erro fatal durante o scan", e);
                historyStatus = "ERROR";
                historyMessage = safeMsg(e);
            } finally {
                if (cancelRequested.get()) {
                    historyStatus = "CANCELED";
                }
                long files = (backupResult == null) ? 0 : backupResult.filesProcessed();
                long errors = (backupResult == null) ? 0 : backupResult.errors();
                BackupHistoryDb.finish(historyId, historyStatus, files, errors, (scanId <= 0) ? null : scanId, historyMessage, backupType);
                running.set(false);
                cleanup();
                ui(() -> {
                    stopUiUpdater();
                    view.setScanningState(false);
                    view.getStopButton().setDisable(false);
                    model.phaseProperty.set("Idle");
                    model.progressProperty.set(0);
                });
                currentTask = null;
            }
        }

        private void cleanup() {
            if (pool != null) try { pool.close(); } catch (Exception ignored) {}
        }
    }

    private static String safeMsg(Throwable t) {
        return (t.getMessage() != null) ? t.getMessage() : t.getClass().getSimpleName();
    }
}
