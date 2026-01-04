package com.keeply.app.inventory;

import com.keeply.app.config.Config;
import com.keeply.app.database.Database;
import com.keeply.app.templates.KeeplyTemplate.ScanModel;

import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.util.Duration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ScanController {

    private final ScanScreen view;
    private final ScanModel model;

    private volatile ScannerTask currentTask;
    private volatile Thread scanThread;
    private volatile Timeline uiUpdater;

    private static final Logger logger = LoggerFactory.getLogger(ScanController.class);

    public ScanController(ScanScreen view, ScanModel model) {
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

    private void setScanningState(boolean scanning) {
        ui(() -> view.setScanningState(scanning));
    }

    // -----------------------------
    // Actions
    // -----------------------------
    private void startScan() {
        if (currentTask != null && currentTask.isRunning()) {
            log(">> Aviso: Scan anterior ainda está finalizando...");
            return;
        }

        var pathText = view.getRootPathText();
        if (pathText == null || pathText.isBlank()) {
            log(">> Erro: Selecione uma pasta primeiro.");
            return;
        }

        try {
            ui(() -> {
                view.clearConsole();
                model.reset();
                view.setScanningState(true);
            });

            // Cria a configuração simplificada (sem hash)
            var config = buildScanConfig();

            // Inicia a tarefa
            currentTask = new ScannerTask(pathText, config);
            
            // Thread Virtual para orquestrar o scan
            scanThread = Thread.ofVirtual()
                    .name("keeply-scan-main")
                    .start(currentTask);

        } catch (Exception e) {
            log(">> ERRO AO INICIAR: " + safeMsg(e));
            logger.error("Erro ao iniciar scan", e);
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
            log("!!! INICIANDO WIPE (LIMPEZA) !!!");
            try {
                requestStopAndWait(5, TimeUnit.SECONDS); 
                performWipeLogic();
                log(">> SUCESSO: Banco de dados limpo e otimizado.");
            } catch (Exception e) {
                log(">> ERRO NO WIPE: " + safeMsg(e));
                logger.error("Erro no wipe do banco", e);
            } finally {
                setScanningState(false);
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
        Path dbPath = Config.getDbFilePath();
        if (dbPath == null) {
            log(">> ERRO: Caminho do banco não resolvido.");
            return;
        }

        log(">> Encerrando conexões e removendo arquivo de banco...");
        try {
            Database.shutdown();

            Files.createDirectories(dbPath.getParent());
            Path wal = Path.of(dbPath.toString() + "-wal");
            Path shm = Path.of(dbPath.toString() + "-shm");
            try { Files.deleteIfExists(wal); } catch (Exception ignored) {}
            try { Files.deleteIfExists(shm); } catch (Exception ignored) {}
            
            if (Files.deleteIfExists(dbPath)) {
                log(">> Banco removido. Um novo arquivo será criado no próximo scan.");
                Database.init();
                return;
            }
            throw new Exception("Não foi possível remover arquivo principal");

        } catch (Exception e) {
            log(">> Falha ao apagar arquivo. Limpando tabelas como fallback.");
            wipeTablesFallback();
        }
    }

    private void wipeTablesFallback() throws Exception {
        Database.init();
        Database.jdbi().useHandle(handle -> {
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
    private Scanner.ScanConfig buildScanConfig() {
        var defaults = Scanner.ScanConfig.defaults();
        
        var excludes = new ArrayList<>(defaults.excludeGlobs());
        // Adiciona exclusões de sistema
        excludes.addAll(List.of(
            "**/Keeply/**", "**/*.keeply*", 
            "**/AppData/Local/Temp/**", "**/AppData/Local/Microsoft/**",
            "**/ntuser.dat*", "**/Cookies/**", "**/$Recycle.Bin/**",
            "**/System Volume Information/**", "**/Windows/**"
        ));

        // Note: removemos os parâmetros de HashWorker, pois o novo Scanner não usa
        return new Scanner.ScanConfig(
                defaults.dbBatchSize(),
                List.copyOf(excludes)
        );
    }

    // --- UI UPDATER ---
    private void startUiUpdater(Scanner.ScanMetrics metrics) {
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

    private void updateModel(Scanner.ScanMetrics m) {
        long elapsed = java.time.Duration.between(m.start, Instant.now()).toSeconds();
        double seconds = Math.max(1.0, elapsed);
        
        long scanned = m.filesSeen.sum();
        
        // UI simplificada sem métricas de Hash
        model.filesScannedProperty.set("%,d".formatted(scanned));
        model.rateProperty.set("%.0f files/s".formatted(scanned / seconds));
        model.dbBatchesProperty.set(Long.toString(m.dbBatches.sum()));
        model.errorsProperty.set(Long.toString(m.walkErrors.sum()));
        
        // Feedback visual de atividade
        model.mbPerSecProperty.set(m.running.get() ? "Validating Metadata..." : "Idle");
    }

    // --- WORKER ---
    private final class ScannerTask implements Runnable {
        private final String rootPath;
        private final Scanner.ScanConfig config;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final AtomicBoolean cancelRequested = new AtomicBoolean(false);
        private Database.SimplePool pool;

        ScannerTask(String rootPath, Scanner.ScanConfig config) {
            this.rootPath = rootPath;
            this.config = config;
        }

        public boolean isRunning() { return running.get(); }
        
        public void cancel() { 
            cancelRequested.set(true);
            if (scanThread != null) scanThread.interrupt();
        }

        @Override
        public void run() {
            var metrics = new Scanner.ScanMetrics();
            running.set(true);
            cancelRequested.set(false);
            metrics.running.set(true);
            ui(() -> startUiUpdater(metrics));

            try {
                log(">> Conectando ao Banco de Dados...");
                // Pool pequeno é suficiente, pois agora só temos 1 Writer e a UI
                Database.init();
                pool = new Database.SimplePool(Config.getDbUrl(), 4);

                log(">> Iniciando motor de metadados...");
                Scanner.runScan(java.nio.file.Path.of(rootPath), config, pool, metrics, cancelRequested, ScanController.this::log);

            } catch (InterruptedException ie) {
                log(">> Operação interrompida.");
            } catch (Exception e) {
                log(">> ERRO FATAL: " + safeMsg(e));
                logger.error("Erro fatal durante o scan", e);
            } finally {
                running.set(false);
                cleanup();
                ui(() -> {
                    stopUiUpdater();
                    view.setScanningState(false);
                    view.getStopButton().setDisable(false);
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
