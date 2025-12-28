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

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ScanController {

    private final ScanScreen view;
    private final ScanModel model;

    // Use AtomicReference se quiser thread-safety total, mas para UI única isso basta
    private ScannerTask currentTask;
    private Timeline uiUpdater;

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

    private void startScan() {
        // Evita iniciar dois scans simultâneos
        if (currentTask != null && currentTask.isRunning()) return;

        var path = view.getRootPathText();
        if (path == null || path.isBlank()) return;

        view.clearConsole();
        model.reset();
        view.setScanningState(true);

        // Otimização 1: Configuração dinâmica baseada no hardware
        var config = buildScanConfig();

        // Inicia a tarefa em uma Virtual Thread
        currentTask = new ScannerTask(path, config);
        Thread.ofVirtual()
              .name("keeply-scanner-" + System.currentTimeMillis()) // Nome único ajuda no debug
              .start(currentTask);
    }

    private void stopScan() {
        if (currentTask != null && currentTask.isRunning()) {
            view.appendLog("!!! Solicitando interrupção imediata...");
            currentTask.cancel();
        }
    }

    private void wipeDatabase() {
        if (!view.confirmWipe()) return;

        view.setScanningState(true);
        
        // Otimização 2: Isola a lógica de banco em uma tarefa dedicada
        Thread.ofVirtual().name("keeply-db-wipe").start(() -> {
            view.appendLog("!!! INICIANDO LIMPEZA TOTAL DO BANCO !!!");
            try {
                // Chama a lógica isolada (Cleaner Code)
                performWipeLogic();
                view.appendLog(">> Banco de dados limpo com sucesso.");
            } catch (Exception e) {
                view.appendLog("ERRO CRÍTICO NO WIPE: " + e.getMessage());
                e.printStackTrace();
            } finally {
                Platform.runLater(() -> view.setScanningState(false));
            }
        });
    }

    // Lógica de banco extraída para não poluir o método do controller
    private void performWipeLogic() throws Exception {
        // Pool de tamanho 1 é suficiente para uma operação sequencial
        try (var pool = new Database.SimplePool(Config.getDbUrl(), Config.getDbUser(), Config.getDbPass(), 1);
             var conn = pool.borrow();
             var stmt = conn.createStatement()) {
            
            // Cascata garante que limpa tudo sem erros de FK
            stmt.execute("TRUNCATE TABLE file_change, file_state, content, path, scan_issue, scan CASCADE");
            
            if (!conn.getAutoCommit()) conn.commit();
        }
    }

    private Scanner.ScanConfig buildScanConfig() {
        // Otimização 3: Detecta núcleos reais da máquina
        int availableCores = Runtime.getRuntime().availableProcessors();
        // Deixa 1 ou 2 núcleos livres para o SO e UI, usa o resto, mínimo de 2
        int workers = Math.max(2, availableCores - 1); 

        return Scanner.ScanConfig.builder()
                .workers(workers) 
                .dbPoolSize(workers + 2) // Pool um pouco maior que os workers para evitar starvation
                .batchLimit(2000)
                .addExclude("**/node_modules/**")
                .addExclude("**/.git/**")
                .addExclude("**/$Recycle.Bin/**")
                .addExclude("**/System Volume Information/**")
                .build();
    }

    // --- Lógica de Atualização da UI (Polling) ---

    private void startUiUpdater(Scanner.ScanMetrics metrics) {
        stopUiUpdater();
        
        // Otimização 4: 100ms (10fps) dá uma sensação de velocidade maior que 250ms
        uiUpdater = new Timeline(new KeyFrame(Duration.millis(100), evt -> updateModel(metrics)));
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
        // Evita divisão por zero e números negativos no início
        long elapsedMillis = java.time.Duration.between(m.start, Instant.now()).toMillis();
        double seconds = Math.max(0.1, elapsedMillis / 1000.0);

        long scanned = m.filesScanned.sum();
        long hashed  = m.filesHashed.sum();
        long bytes   = m.bytesScanned.sum();

        double hashRate = hashed / seconds;
        double mbPerSec = (bytes / 1048576.0) / seconds; // 1024*1024 constante

        // Batch update visual
        model.filesScannedProperty.set("%,d".formatted(scanned));
        model.mbPerSecProperty.set("%.1f".formatted(mbPerSec));
        model.rateProperty.set("%.0f f/s".formatted(hashRate));
        model.dbBatchesProperty.set(Long.toString(m.dbBatches.sum()));
        model.errorsProperty.set(Long.toString(m.errorsWalk.sum() + m.errorsHash.sum()));
    }

    // --- Worker Interno ---
    
    private final class ScannerTask implements Runnable {
        private final String rootPath;
        private final Scanner.ScanConfig config;
        private final AtomicBoolean running = new AtomicBoolean(true);
        
        // Mantemos referência para fechar forçadamente se necessário
        private Database.SimplePool pool; 

        ScannerTask(String rootPath, Scanner.ScanConfig config) {
            this.rootPath = rootPath;
            this.config = config;
        }

        public boolean isRunning() { return running.get(); }
        public void cancel() { running.set(false); }

        @Override
        public void run() {
            var metrics = new Scanner.ScanMetrics();
            metrics.running.set(true);

            Platform.runLater(() -> startUiUpdater(metrics));

            try {
                view.appendLog("Iniciando pool de conexões (" + config.dbPoolSize() + " conexões)...");
                pool = new Database.SimplePool(Config.getDbUrl(), Config.getDbUser(), Config.getDbPass(), config.dbPoolSize());

                try (var conn = pool.borrow()) {
                    Database.initSchema(conn);
                }

                Scanner.Engine.runScanLogic(rootPath, config, pool, metrics, running, view::appendLog);

            } catch (InterruptedException ie) {
                 view.appendLog(">> Cancelado pelo usuário.");
            } catch (Exception e) {
                view.appendLog(">> ERRO FATAL: " + e.getMessage());
                e.printStackTrace();
            } finally {
                cleanup();
                metrics.running.set(false);
                
                Platform.runLater(() -> {
                    stopUiUpdater();
                    updateModel(metrics);
                    view.setScanningState(false);
                    view.appendLog("=== SCAN FINALIZADO ===");
                });
            }
        }

        private void cleanup() {
            if (pool != null) {
                try { pool.close(); } catch (Exception e) { /* log silencioso ou stderr */ }
            }
        }
    }
}