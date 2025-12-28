package com.keeply.app.controller;

import com.keeply.app.Config;
import com.keeply.app.Database;
import com.keeply.app.Scanner;
import com.keeply.app.view.KeeplyTemplate.ScanModel;
import com.keeply.app.view.ScanScreen;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.util.Duration;

import java.sql.Connection;
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

    public ScanController(ScanScreen view, ScanModel model) {
        this.view = view;
        this.model = model;
        wireEvents();
    }

    private static final Logger logger = LoggerFactory.getLogger(ScanController.class);

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

    // Este método é passado para o Scanner para atualizar a UI
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

            // Cria a configuração com os filtros
            var config = buildScanConfig();

            // Inicia a tarefa
            currentTask = new ScannerTask(pathText, config);
            
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
                requestStopAndWait(5, TimeUnit.SECONDS); // Garante que o scan parou
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
        try (Connection conn = Database.openSingleConnection()) {
            conn.setAutoCommit(false);
            Database.ensureSchema(conn); // garante tabelas antes de operar em bancos antigos
            try (var stmt = conn.createStatement()) {
                stmt.execute("PRAGMA busy_timeout = 5000");
                
                log(">> Apagando tabelas...");
                // Ordem correta para evitar erro de Foreign Key
                stmt.execute("DELETE FROM scan_issues");
                stmt.execute("DELETE FROM file_inventory");
                stmt.execute("DELETE FROM scans");
                
                conn.commit();
                
                log(">> Compactando arquivo (VACUUM)...");
                conn.setAutoCommit(true);
                stmt.execute("VACUUM");
            }
        }
    }

    private Scanner.ScanConfig buildScanConfig() {
        var defaults = Scanner.ScanConfig.defaults();
        
        // Exclusões de segurança e lixo
        var excludes = new ArrayList<>(defaults.excludeGlobs());
        excludes.add("**/Keeply/**");
        excludes.add("**/*.keeply*");
        excludes.add("**/AppData/Local/Temp/**");
        excludes.add("**/AppData/Local/Microsoft/**");
        excludes.add("**/ntuser.dat*");
        excludes.add("**/Cookies/**");
        excludes.add("**/$Recycle.Bin/**");
        excludes.add("**/System Volume Information/**");
        excludes.add("**/Windows/**"); // Opcional: evita scanear o sistema operacional

        return new Scanner.ScanConfig(
                defaults.dbBatchSize(),
                defaults.hashWorkers(),
                true, 
                defaults.hashMaxBytes(),
                defaults.largeFileHashPolicy(),
                defaults.sampledChunkBytes(),
                List.copyOf(excludes)
        );
    }

    private int dbPoolSizeFor(Scanner.ScanConfig cfg) {
        return Math.max(4, cfg.hashWorkers() + 2);
    }

    // --- UI UPDATER ---
    private void startUiUpdater(Scanner.ScanMetrics metrics) {
        stopUiUpdater();
        uiUpdater = new Timeline(new KeyFrame(Duration.millis(500), evt -> updateModel(metrics)));
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
        long hashed = m.hashed.sum();
        
        model.filesScannedProperty.set("%,d".formatted(scanned));
        model.rateProperty.set("%.0f files/s".formatted(scanned / seconds));
        model.dbBatchesProperty.set(Long.toString(m.dbBatches.sum()));
        model.errorsProperty.set(Long.toString(m.walkErrors.sum()));
        model.mbPerSecProperty.set(hashed > 0 ? "Hashing..." : "Scanning..."); 
    }

    // --- WORKER ---
    private final class ScannerTask implements Runnable {
        private final String rootPath;
        private final Scanner.ScanConfig config;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private Database.SimplePool pool;

        ScannerTask(String rootPath, Scanner.ScanConfig config) {
            this.rootPath = rootPath;
            this.config = config;
        }

        public boolean isRunning() { return running.get(); }
        
        public void cancel() { 
            running.set(false);
            if (scanThread != null) scanThread.interrupt();
        }

        @Override
        public void run() {
            var metrics = new Scanner.ScanMetrics();
            metrics.running.set(true);
            ui(() -> startUiUpdater(metrics));

            try {
                log(">> Conectando ao Banco de Dados...");
                pool = new Database.SimplePool(Config.getDbUrl(), dbPoolSizeFor(config));

                // Garante Schema
                try (var conn = pool.borrow()) {
                    Database.ensureSchema(conn);
                    conn.commit();
                }

                log(">> Iniciando motor de varredura...");
                // AQUI: Passamos o método 'log' do controller para o Scanner
                Scanner.runScan(java.nio.file.Path.of(rootPath), config, pool, metrics, running, ScanController.this::log);

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
