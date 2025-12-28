package com.keeply.app;

import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.util.Duration;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

public final class MainController {

    private final MainView view;
    private final UI.ScanModel model;

    private ScannerTask currentTask;
    private Timeline uiTimeline;

    public MainController(MainView view, UI.ScanModel model) {
        this.view = view;
        this.model = model;

        wireEvents();
    }

    private void wireEvents() {
        view.scanButton().setOnAction(e -> startScan());
        view.stopButton().setOnAction(e -> stopScan());
        view.wipeButton().setOnAction(e -> wipeDatabase());
    }

    private void startScan() {
        String path = view.getRootPathText();
        if (path == null || path.isBlank()) return;

        view.clearConsole();
        model.reset();

        // config como antes (você pode puxar isso pra UI depois)
        Scanner.ScanConfig config = Scanner.ScanConfig.builder()
                .workers(12)
                .dbPoolSize(15)
                .batchLimit(2000)
                .addExclude("**/node_modules/**")
                .addExclude("**/.git/**")
                .addExclude("**/$Recycle.Bin/**")
                .build();

        view.setScanning(true);

        currentTask = new ScannerTask(path, config);
        Thread.ofVirtual().name("keeply-master").start(currentTask);
    }

    private void stopScan() {
        if (currentTask != null) {
            currentTask.cancelScan();
            view.log("!!! Solicitando parada...");
        }
    }

    private void wipeDatabase() {
        if (!view.confirmWipe()) return;

        Thread.ofVirtual().start(() -> {
            view.log("!!! INICIANDO LIMPEZA TOTAL DO BANCO !!!");
            try (ScannerTask task = new ScannerTask("", Scanner.ScanConfig.builder().build())) {
                task.wipeAllData();
            } catch (Exception e) {
                view.log("Erro ao limpar banco: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    private void onScanFinished() {
        Platform.runLater(() -> {
            view.setScanning(false);
            view.log("=== SCAN FINALIZADO ===");
        });
    }

    private void startUiUpdater(Scanner.ScanMetrics m, AtomicBoolean runningFlag) {
        stopUiUpdater();

        uiTimeline = new Timeline(new KeyFrame(Duration.millis(500), evt -> {
            double sec = Math.max(1.0, (java.time.Duration.between(m.start, Instant.now()).toMillis() / 1000.0));

            long scanned = m.filesScanned.sum();
            long hashed  = m.filesHashed.sum();

            double hashRate = hashed / sec;
            double mbPerSec = (m.bytesScanned.sum() / 1024.0 / 1024.0) / sec;

            model.filesScannedProperty.set(String.format("%,d", scanned));
            model.mbPerSecProperty.set(String.format("%.1f", mbPerSec));
            model.rateProperty.set(String.format("%.0f", hashRate)); // Hash/s de verdade
            model.dbBatchesProperty.set(String.valueOf(m.dbBatches.sum()));
            model.errorsProperty.set(String.valueOf(m.errorsWalk.sum() + m.errorsHash.sum()));

            if (!runningFlag.get()) {
                stopUiUpdater();
            }
        }));

        uiTimeline.setCycleCount(Timeline.INDEFINITE);
        uiTimeline.play();
    }

    private void stopUiUpdater() {
        if (uiTimeline != null) {
            uiTimeline.stop();
            uiTimeline = null;
        }
    }

    // === Task isolada do JavaFX ===
    final class ScannerTask implements Runnable, AutoCloseable {

        private final String rootPath;
        private final Scanner.ScanConfig config;

        private final AtomicBoolean running = new AtomicBoolean(true);
        private Database.SimplePool pool;

        private final String dbUrl = Config.getDbUrl();
        private final String dbUser = Config.getDbUser();
        private final String dbPass = Config.getDbPass();

        ScannerTask(String rootPath, Scanner.ScanConfig config) {
            this.rootPath = rootPath;
            this.config = config;
        }

        void cancelScan() { running.set(false); }

        void wipeAllData() {
            try {
                if (pool == null) pool = new Database.SimplePool(dbUrl, dbUser, dbPass, 2);
                try (Connection c = pool.borrow()) {
                    view.log("Executando TRUNCATE em todas as tabelas...");
                    try (Statement st = c.createStatement()) {
                        st.execute("TRUNCATE TABLE file_change, file_state, content, path, scan_issue, scan CASCADE");
                    }
                    c.commit();
                    view.log("Banco de dados limpo com sucesso!");
                }
            } catch (Exception e) {
                view.log("Erro no Wipe: " + e.getMessage());
            }
        }

        @Override
        public void run() {
            Scanner.ScanMetrics metrics = new Scanner.ScanMetrics();
            metrics.running.set(true);

            Platform.runLater(() -> startUiUpdater(metrics, running));

            try {
                view.log("Conectando ao banco de dados via .env...");
                pool = new Database.SimplePool(dbUrl, dbUser, dbPass, config.dbPoolSize());

                try (Connection c = pool.borrow()) {
                    Database.initSchema(c);
                    view.log("Schema OK. Carregando snapshot...");
                }

                // Seu Main antigo chamava isso direto daqui :contentReference[oaicite:4]{index=4}
                Scanner.Engine.runScanLogic(rootPath, config, pool, metrics, running, view::log);

            } catch (Exception e) {
                view.log("ERRO FATAL: " + e.getMessage());
                e.printStackTrace();
            } finally {
                metrics.running.set(false);
                running.set(false);
                close();
                onScanFinished();
            }
        }

        @Override
        public void close() {
            if (pool != null) pool.close();
        }
    }
}
