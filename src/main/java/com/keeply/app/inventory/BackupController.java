package com.keeply.app.inventory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.keeply.app.blob.BlobStore;
import com.keeply.app.config.Config;
import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.database.KeeplyDao;
import com.keeply.app.screen.BackupScreen;
import com.keeply.app.templates.KeeplyTemplate.ScanModel;

import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.scene.control.Alert;
import javafx.util.Duration;

public final class BackupController {

    private final BackupScreen view;
    private final ScanModel model;

    private volatile ScannerTask currentTask;
    private volatile Thread scanThread;
    private volatile Timeline uiUpdater;

    // Otimização crítica: NUNCA chamar Platform.runLater() por log em volume.
    // Em vez disso, enfileira logs e dá flush em lote na UI.
    private final LogBus logBus = new LogBus();

    private static final Logger logger = LoggerFactory.getLogger(BackupController.class);

    public BackupController(BackupScreen view, ScanModel model) {
        this.view = view;
        this.model = model;
        wireEvents();
        initSettings();
    }

    private void initSettings() {
        view.onScheduleConfigured(state -> {
            Config.setScheduleEnabled(state.enabled());
            Config.setScheduleMode(state.mode() == BackupScreen.ScheduleMode.INTERVAL ? "INTERVAL" : "DAILY");
            Config.setScheduleTime(state.time());
            Config.setScheduleIntervalMinutes(state.intervalMinutes());
        });

        view.onRetentionConfigured(retention -> {
            try {
                DatabaseBackup.init();
                DatabaseBackup.jdbi().useExtension(KeeplyDao.class, dao ->
                        dao.upsertSetting("retention_max", Integer.toString(retention))
                );
            } catch (Exception e) {
                logger.warn("Falha ao salvar retenção", e);
            }
        });

        BackupScreen.ScheduleState state = new BackupScreen.ScheduleState(
                Config.isScheduleEnabled(),
                "INTERVAL".equalsIgnoreCase(Config.getScheduleMode())
                        ? BackupScreen.ScheduleMode.INTERVAL
                        : BackupScreen.ScheduleMode.DAILY,
                Config.getScheduleTime(),
                Config.getScheduleIntervalMinutes()
        );
        view.setScheduleState(state);

        int retention = 10;
        try {
            DatabaseBackup.init();
            String v = DatabaseBackup.jdbi().withExtension(KeeplyDao.class, dao -> dao.fetchSetting("retention_max"));
            if (v != null && !v.isBlank()) {
                retention = Integer.parseInt(v.trim());
            }
        } catch (Exception e) {
            logger.warn("Falha ao carregar retenção", e);
        }
        view.setRetentionValue(retention);
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

    // Log "rápido": vai pra fila e é despejado em lote na UI
    private void log(String msg) {
        logBus.enqueue(msg);
    }

    // Log "importante": aparece mais rápido (ainda sem spam)
    private void logNow(String msg) {
        logBus.enqueueImportant(msg);
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
        logNow(">> Erro: " + message);
        showError(message);
    }

    private void setScanningState(boolean scanning) {
        ui(() -> view.setScanningState(scanning));
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "generic").toLowerCase(Locale.ROOT).contains("win");
    }

    private static boolean isLinux() {
        String os = System.getProperty("os.name", "generic").toLowerCase(Locale.ROOT);
        return os.contains("nux") || os.contains("linux");
    }

    // -----------------------------
    // Actions
    // -----------------------------
    private void startScan() {
        if (currentTask != null && currentTask.isRunning()) {
            logNow(">> Aviso: Backup anterior ainda está finalizando...");
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

        // Check forte: destino dentro da origem = risco de recursão/scan infinito (mesmo com excludes).
        try {
            Path root = Path.of(pathText).toAbsolutePath().normalize();
            Path dest = Path.of(destText).toAbsolutePath().normalize();
            boolean nested = dest.startsWith(root);

            if (nested) {
                logNow(">> Aviso: destino está dentro da origem — o scanner vai ignorar o destino para evitar recursão.");
                // NÃO retorna; deixa rodar
            }
        } catch (Exception ignored) { }

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
            logNow(">> Criptografia ativa: senha validada.");
        }

        try {
            ui(() -> {
                view.clearConsole();
                model.reset();
                view.setScanningState(true);
                model.phaseProperty.set("Preparando...");
                model.progressProperty.set(-1);
            });

            // Inicia log flusher (UI) e throttles
            logBus.start();

            // Config (agressiva e cross-platform)
            var config = buildScanConfig();

            // Inicia a tarefa
            currentTask = new ScannerTask(pathText, destText, config);

            scanThread = Thread.ofVirtual()
                    .name("keeply-scan-main")
                    .start(currentTask);

        } catch (Exception e) {
            logNow(">> ERRO AO INICIAR: " + safeMsg(e));
            logger.error("Erro ao iniciar backup", e);
            setScanningState(false);
            currentTask = null;
            scanThread = null;
            logBus.stop();
        }
    }

    private void stopScan() {
        var task = currentTask;
        if (task != null && task.isRunning()) {
            logNow("!!! Solicitando parada... Aguarde...");
            task.cancel();

            var t = scanThread;
            if (t != null) t.interrupt();

            ui(() -> view.getStopButton().setDisable(true));
        }
    }

    private void wipeDatabase() {
        if (!view.confirmWipe()) return;

        setScanningState(true);
        logBus.start();

        Thread.ofVirtual().name("keeply-db-wipe").start(() -> {
            logNow("!!! APAGANDO BACKUPS (DB + COFRE) !!!");
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

                // 1) Delete backup vault binaries (.keeply/storage)
                wipeBackupStorageBestEffort();

                // 2) Delete SQLite DB files (history/metadata)
                performWipeLogic();

                logNow(">> SUCESSO: Backups apagados (banco + cofre). O app recria o DB no próximo backup.");
            } catch (Exception e) {
                logNow(">> ERRO NO WIPE: " + safeMsg(e));
                logger.error("Erro no wipe do banco", e);
            } finally {
                setScanningState(false);
                ui(() -> {
                    model.phaseProperty.set("Idle");
                    model.progressProperty.set(0);
                    view.getStopButton().setDisable(false);
                });
                logBus.stop();
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
        Path encrypted = Config.getEncryptedDbFilePath();
        Path runtime = Config.getRuntimeDbFilePath();

        if (encrypted == null || runtime == null) {
            logNow(">> ERRO: Caminho do banco não resolvido.");
            return;
        }

        logNow(">> Encerrando conexões e removendo arquivos de banco...");

        try {
            DatabaseBackup.shutdown();
        } catch (Exception ignored) {}

        if (encrypted.getParent() != null) {
            Files.createDirectories(encrypted.getParent());
        }

        boolean anyFailed = false;

        anyFailed |= !deleteIfExistsQuiet(runtime, 12, 75);
        anyFailed |= !deleteIfExistsQuiet(Path.of(runtime.toString() + "-wal"), 12, 75);
        anyFailed |= !deleteIfExistsQuiet(Path.of(runtime.toString() + "-shm"), 12, 75);

        anyFailed |= !deleteIfExistsQuiet(encrypted, 6, 75);
        anyFailed |= !deleteIfExistsQuiet(Path.of(encrypted.toString() + "-wal"), 6, 75);
        anyFailed |= !deleteIfExistsQuiet(Path.of(encrypted.toString() + "-shm"), 6, 75);

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
            logNow(">> Falha ao apagar algum arquivo. Limpando tabelas como fallback.");
            wipeTablesFallback();
        } else {
            logNow(">> Arquivos removidos (ou já não existiam). Um novo banco será criado no próximo backup.");
            DatabaseBackup.init();
        }
    }

    private void wipeBackupStorageBestEffort() {
        Set<Path> baseDirs = new LinkedHashSet<>();

        try {
            String txt = view.getBackupDestinationText();
            if (txt != null && !txt.isBlank()) baseDirs.add(Path.of(txt).toAbsolutePath().normalize());
        } catch (Exception ignored) {}

        try {
            String txt = Config.getLastBackupDestination();
            if (txt != null && !txt.isBlank()) baseDirs.add(Path.of(txt).toAbsolutePath().normalize());
        } catch (Exception ignored) {}

        try {
            Path p = Config.getEncryptedDbFilePath();
            Path parent = (p == null) ? null : p.toAbsolutePath().getParent();
            if (parent != null) baseDirs.add(parent.normalize());
        } catch (Exception ignored) {}

        if (baseDirs.isEmpty()) {
            baseDirs.add(Path.of(".").toAbsolutePath().normalize());
        }

        logNow(">> Apagando backups locais (BlobStore/.keeply/storage)...");
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
                logNow(">> Backups locais removidos.");
            } catch (Exception e) {
                anyFailed = true;
                logNow(">> Aviso: falha ao apagar cofre: " + safeMsg(e));
                logger.warn("Falha ao apagar cofre {}", storageDir, e);
            }
        }

        if (!anyDeleted && !anyFailed) {
            logNow(">> Nenhum cofre encontrado (ok).");
        }
    }

    private static void deleteDirectoryRecursive(Path dir, int attempts, long delayMillis) throws Exception {
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                deletePathWithRetry(file, attempts, delayMillis);
                return java.nio.file.FileVisitResult.CONTINUE;
            }

            @Override
            public java.nio.file.FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
                if (exc != null) throw exc;
                deletePathWithRetry(d, attempts, delayMillis);
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });
    }

    private static void deletePathWithRetry(Path p, int attempts, long delayMillis) throws IOException {
        if (p == null) return;

        IOException last = null;
        int tries = Math.max(1, attempts);

        for (int i = 1; i <= tries; i++) {
            try {
                Files.deleteIfExists(p);
                return;
            } catch (IOException e) {
                last = e;

                // Windows: read-only pode travar delete
                try { Files.setAttribute(p, "dos:readonly", false); } catch (Exception ignored) {}

                if (delayMillis > 0 && i < tries) {
                    try { Thread.sleep(delayMillis); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
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
                if (delayMillis > 0 && i < attempts) {
                    try { Thread.sleep(delayMillis); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
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
            logNow(">> Apagando tabelas...");
            handle.execute("DELETE FROM scan_issues");
            handle.execute("DELETE FROM file_inventory");
            handle.execute("DELETE FROM scans");
            try { handle.execute("DELETE FROM sqlite_sequence"); } catch (Exception ignored) {}
            logNow(">> Compactando arquivo (VACUUM)...");
            handle.execute("VACUUM");
        });
    }

    // --- CONFIGURAÇÃO (Otimizada) ---
    private Backup.ScanConfig buildScanConfig() {
        var defaults = Backup.ScanConfig.defaults();
        var excludes = new ArrayList<>(defaults.excludeGlobs());

        // Exclusões comuns (Win + Linux)
        excludes.addAll(List.of(
                "**/.keeply/**",
                "**/*.keeply*",
                "**/.git/**",
                "**/node_modules/**",
                "**/.venv/**",
                "**/__pycache__/**",
                "**/.DS_Store",
                "**/Thumbs.db"
        ));

        if (isWindows()) {
            // Windows: corte agressivo pra evitar scan infinito/ruim
            excludes.addAll(List.of(
                    "**/Windows/**",
                    "**/Program Files/**",
                    "**/Program Files (x86)/**",
                    "**/ProgramData/**",
                    "**/System Volume Information/**",
                    "**/$Recycle.Bin/**",
                    "**/AppData/**",            // MUITO impacto (se quiser menos, refine depois)
                    "**/ntuser.dat*",
                    "**/hiberfil.sys",
                    "**/pagefile.sys",
                    "**/swapfile.sys"
            ));
        } else if (isLinux()) {
            // Linux: se o usuário apontar pra /, isso salva tua vida
            excludes.addAll(List.of(
                    "**/proc/**",
                    "**/sys/**",
                    "**/dev/**",
                    "**/run/**",
                    "**/tmp/**",
                    "**/var/tmp/**",
                    "**/var/cache/**",
                    "**/.cache/**",
                    "**/.local/share/Trash/**"
            ));
        }

        // AQUI é uma alavanca grande:
        // batch baixo = commit demais = scan lento
        int tunedBatch = Math.max(2000, defaults.dbBatchSize());
        tunedBatch = Math.min(tunedBatch, 10000); // limite sensato

        return new Backup.ScanConfig(
                tunedBatch,
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

        model.filesScannedProperty.set("%,d".formatted(scanned));
        model.rateProperty.set("%.0f files/s".formatted(scanned / seconds));
        model.dbBatchesProperty.set(Long.toString(m.dbBatches.sum()));
        model.errorsProperty.set(Long.toString(m.walkErrors.sum()));

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

        ScannerTask(String rootPath, String backupDest, Backup.ScanConfig config) {
            this.rootPath = rootPath;
            this.backupDest = backupDest;
            this.config = config;
        }

        public boolean isRunning() { return running.get(); }

        public void cancel() {
            cancelRequested.set(true);
            var t = scanThread;
            if (t != null) t.interrupt();
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

            // Log throttled para não esmagar UI
            Consumer<String> scanLog = logBus.throttled(250);

            try {
                ui(() -> {
                    model.phaseProperty.set("Validando metadados...");
                    model.progressProperty.set(-1);
                });

                DatabaseBackup.init();

                // (Opcional, mas fortemente recomendado)
                // Se seu DatabaseBackup.init() não aplica PRAGMAs, aplique aqui:
                applyFastSqlitePragmasBestEffort();

                scanId = Backup.runScan(
                    Path.of(rootPath),
                    Path.of(backupDest),
                    config,
                    metrics,
                    cancelRequested,
                    scanLog
                );

                try {
                    Long firstId = DatabaseBackup.jdbi().withExtension(
                            com.keeply.app.database.KeeplyDao.class,
                            dao -> dao.fetchFirstScanIdForRoot(rootPath)
                    );
                    backupType = (firstId != null && firstId == scanId) ? "FULL" : "INCREMENTAL";
                } catch (Exception ignored) {}

                if (!cancelRequested.get()) {
                    ui(() -> {
                        model.phaseProperty.set("Backup: comprimindo/gravando...");
                        model.progressProperty.set(-1);
                    });

                    backupResult = BlobStore.runBackupIncremental(
                            Path.of(rootPath),
                            config,
                            Path.of(backupDest),
                            scanId,
                            cancelRequested,
                            scanLog,
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

            } catch (Exception e) {
                logNow(">> ERRO FATAL: " + safeMsg(e));
                logger.error("Erro fatal durante o scan", e);
                historyStatus = "ERROR";
                historyMessage = safeMsg(e);
            } finally {
                if (cancelRequested.get()) historyStatus = "CANCELED";

                long files = (backupResult == null) ? 0 : backupResult.filesProcessed();
                long errors = (backupResult == null) ? 0 : backupResult.errors();

                BackupHistoryDb.finish(
                        historyId,
                        historyStatus,
                        files,
                        errors,
                        (scanId <= 0) ? null : scanId,
                        historyMessage,
                        backupType
                );

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
                logBus.stop(); // encerra flusher quando o job termina
            }
        }

        private void cleanup() {
            // Sem pool dedicado
        }

        private void applyFastSqlitePragmasBestEffort() {
            try {
                DatabaseBackup.jdbi().useHandle(h -> {
                    // WAL + synchronous NORMAL = grande ganho sem ser suicídio
                    h.execute("PRAGMA foreign_keys=ON");
                    h.execute("PRAGMA synchronous=NORMAL");
                    h.execute("PRAGMA temp_store=MEMORY");
                    h.execute("PRAGMA cache_size=-20000"); // ~20MB (valor negativo = KB)
                    h.execute("PRAGMA busy_timeout=5000");
                });
            } catch (Exception ignored) {}
        }
    }

    private static String safeMsg(Throwable t) {
        return (t.getMessage() != null) ? t.getMessage() : t.getClass().getSimpleName();
    }

    // -----------------------------
    // LogBus: fila + flush em lote na UI
    // -----------------------------
    private final class LogBus {
        private final ConcurrentLinkedQueue<String> q = new ConcurrentLinkedQueue<>();
        private volatile Timeline flusher;
        private volatile long lastThrottled = 0;

        // Limites pra não explodir memória se alguém logar por arquivo
        private static final int MAX_LINES_PER_FLUSH = 250;
        private static final int MAX_QUEUE_SOFT = 20_000;

        void start() {
            ui(() -> {
                if (flusher != null) return;
                flusher = new Timeline(new KeyFrame(Duration.millis(120), evt -> flushNow()));
                flusher.setCycleCount(Timeline.INDEFINITE);
                flusher.play();
            });
        }

        void stop() {
            ui(() -> {
                if (flusher != null) {
                    flusher.stop();
                    flusher = null;
                }
            });
            // não precisa limpar a fila; mas se quiser:
            q.clear();
        }

        void enqueue(String msg) {
            if (msg == null) return;
            if (q.size() > MAX_QUEUE_SOFT) {
                // Drop silencioso pra preservar responsividade
                return;
            }
            q.add(msg);
            start();
        }

        void enqueueImportant(String msg) {
            // Important = sempre entra
            if (msg == null) return;
            q.add(msg);
            start();
        }

        Consumer<String> throttled(long minIntervalMillis) {
            return (s) -> {
                long now = System.currentTimeMillis();
                if (now - lastThrottled >= minIntervalMillis) {
                    lastThrottled = now;
                    enqueue(s);
                }
            };
        }

        private void flushNow() {
            int n = 0;
            while (n < MAX_LINES_PER_FLUSH) {
                String s = q.poll();
                if (s == null) break;
                view.appendLog(s);
                n++;
            }
        }
    }
}