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

import java.sql.SQLException;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ScanController {

    private final ScanScreen view;
    private final ScanModel model;

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
        // 1. Defesa: Verifica se já existe algo rodando e impede duplo clique
        if (currentTask != null && currentTask.isRunning()) {
            view.appendLog(">> Aviso: Scan anterior ainda está finalizando...");
            return;
        }

        var path = view.getRootPathText();
        if (path == null || path.isBlank()) {
            view.appendLog(">> Erro: Selecione uma pasta primeiro.");
            return;
        }

        try {
            // 2. Preparação da UI
            view.clearConsole();
            view.appendLog(">> Preparando novo scan em: " + path);
            model.reset();
            view.setScanningState(true);

            // 3. Configuração
            var config = buildScanConfig();

            // 4. Criação da Tarefa
            currentTask = new ScannerTask(path, config);
            
            // 5. Início da Thread Virtual
            Thread.ofVirtual()
                  .name("keeply-worker-" + System.currentTimeMillis())
                  .start(currentTask);

        } catch (Exception e) {
            // Se falhar ANTES da thread iniciar, destrava a UI
            view.appendLog(">> ERRO AO INICIAR: " + e.getMessage());
            e.printStackTrace();
            view.setScanningState(false);
            currentTask = null;
        }
    }

    private void stopScan() {
        if (currentTask != null && currentTask.isRunning()) {
            view.appendLog("!!! Solicitando parada... Aguarde a limpeza dos recursos.");
            currentTask.cancel();
            // Desabilita o botão de stop para evitar spam de cliques
            view.getStopButton().setDisable(true);
        }
    }

    private void wipeDatabase() {
        if (!view.confirmWipe()) return;

        view.setScanningState(true);
        
        Thread.ofVirtual().name("keeply-db-wipe").start(() -> {
            view.appendLog("!!! INICIANDO WIPE (LIMPEZA) !!!");
            
            // Tenta fechar o pool da tarefa anterior à força, se existir
            if (currentTask != null) {
                // Isso é um hack seguro: acessa o pool via reflexão ou apenas garante cleanup
                // Mas o ideal é que o stopScan já tenha cuidado disso.
            }

            try {
                performWipeLogic();
                view.appendLog(">> Banco de dados limpo com sucesso.");
            } catch (SQLException e) {
                // Código 55P03 é "lock_not_available" no Postgres
                if ("55P03".equals(e.getSQLState())) {
                    view.appendLog("ERRO: O banco está ocupado por outra conexão.");
                    view.appendLog("Tente reiniciar a aplicação para liberar os locks.");
                } else {
                    view.appendLog("ERRO SQL NO WIPE: " + e.getMessage());
                }
                e.printStackTrace();
            } catch (Exception e) {
                view.appendLog("ERRO CRÍTICO: " + e.getMessage());
                e.printStackTrace();
            } finally {
                Platform.runLater(() -> view.setScanningState(false));
            }
        });
    }

    // Lógica blindada contra travamentos
    private void performWipeLogic() throws Exception {
        // NÃO USA O POOL AQUI. Usa uma conexão direta e descartável.
        // Isso garante que não pegamos uma conexão "suja" do pool antigo.
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(Config.getDbUrl(), Config.getDbUser(), Config.getDbPass())) {
            conn.setAutoCommit(false);
            
            try (java.sql.Statement stmt = conn.createStatement()) {
                // 1. Configura Schema
                stmt.execute("SET search_path TO keeply");
                
                // 2. IMPORTANTE: Define timeout de 5 segundos para conseguir o Lock.
                // Se o banco estiver travado, ele avisa em vez de congelar para sempre.
                stmt.execute("SET lock_timeout = '5s'");

                view.appendLog(">> Solicitando acesso exclusivo às tabelas...");
                
                // 3. Executa o Truncate
                stmt.execute("TRUNCATE TABLE file_change, file_state, content, path, scan_issue, scan CASCADE");
                
                conn.commit();
            }
        }
    }

    private Scanner.ScanConfig buildScanConfig() {
        int availableCores = Runtime.getRuntime().availableProcessors();
        // Usa metade dos núcleos para não travar o PC
        int workers = Math.max(2, availableCores - 1); 

        return Scanner.ScanConfig.builder()
                .workers(workers)
                .dbPoolSize(workers + 2)
                .batchLimit(3000)
                
                // --- ISSO AQUI SALVA SUA VIDA (E O LOG) ---
                .addExclude("**/AppData/**")           // Ignora configurações de programas e caches
                .addExclude("**/Application Data/**")  // Atalhos antigos de sistema
                .addExclude("**/ntuser.dat*")          // Arquivos de registro do Windows (sempre bloqueados)
                .addExclude("**/Cookies/**")           // Cookies de navegador
                .addExclude("**/Temp/**")              // Lixo temporário (opcional, mas bom ignorar)
                // ------------------------------------------
                
                .addExclude("**/node_modules/**")
                .addExclude("**/.git/**")
                .addExclude("**/$Recycle.Bin/**")
                .addExclude("**/System Volume Information/**")
                .build();
    }

    // --- Atualização da UI ---

    private void startUiUpdater(Scanner.ScanMetrics metrics) {
        stopUiUpdater(); // Garante que não tem timeline antiga rodando
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
        long elapsedMillis = java.time.Duration.between(m.start, Instant.now()).toMillis();
        double seconds = Math.max(0.1, elapsedMillis / 1000.0);

        long scanned = m.filesScanned.sum();
        long hashed  = m.filesHashed.sum();
        long bytes   = m.bytesScanned.sum();

        double hashRate = hashed / seconds;
        double mbPerSec = (bytes / 1048576.0) / seconds;

        model.filesScannedProperty.set("%,d".formatted(scanned));
        model.mbPerSecProperty.set("%.1f".formatted(mbPerSec));
        model.rateProperty.set("%.0f f/s".formatted(hashRate));
        model.dbBatchesProperty.set(Long.toString(m.dbBatches.sum()));
        model.errorsProperty.set(Long.toString(m.errorsWalk.sum() + m.errorsHash.sum()));
    }

    // --- Worker ---
    
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
        public void cancel() { running.set(false); }

        @Override
        public void run() {
            var metrics = new Scanner.ScanMetrics();
            metrics.running.set(true);

            Platform.runLater(() -> startUiUpdater(metrics));

            try {
                view.appendLog("Conectando ao banco...");
                // Se der erro aqui (ex: muitas conexões), cai no catch e libera a UI
                pool = new Database.SimplePool(Config.getDbUrl(), Config.getDbUser(), Config.getDbPass(), config.dbPoolSize());

                try (var conn = pool.borrow()) {
                    Database.initSchema(conn);
                }
                
                view.appendLog("Iniciando varredura...");
                Scanner.Engine.runScanLogic(rootPath, config, pool, metrics, running, view::appendLog);

            } catch (InterruptedException ie) {
                 view.appendLog(">> Cancelado.");
            } catch (Exception e) {
                view.appendLog(">> ERRO FATAL: " + e.getMessage());
                e.printStackTrace();
            } finally {
                // Limpeza crítica
                cleanup();
                metrics.running.set(false);
                running.set(false); // Garante que o controller saiba que acabou
                
                Platform.runLater(() -> {
                    stopUiUpdater();
                    updateModel(metrics); // Último update
                    view.setScanningState(false);
                    view.appendLog("=== FINALIZADO ===");
                    // Garante que o botão de stop seja reabilitado (caso tenha sido desabilitado no cancel)
                    view.getStopButton().setDisable(false);
                });
            }
        }

        private void cleanup() {
            if (pool != null) {
                try {
                    pool.close();
                    view.appendLog("Conexões fechadas.");
                } catch (Exception e) {
                    System.err.println("Erro ao fechar pool: " + e.getMessage());
                }
            }
        }
    }
}