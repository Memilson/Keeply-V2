package com.keeply.app;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.property.*;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.stage.DirectoryChooser;
import javafx.stage.Stage;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.HexFormat;

/**
 * KEEPLY ALL-IN-ONE (Java 21 + JavaFX + PostgreSQL)
 * Versão: 2.3 (Fix Socket Closed - Sincronização de Encerramento)
 */
public class KeeplyApp extends Application {

    // =============================================================================================
    // PARTE 1: GUI (JAVAFX)
    // =============================================================================================

    private final ScanModel model = new ScanModel();
    private TextArea consoleArea;
    private Button btnScan;
    private Button btnStop;
    private Button btnWipe;
    private TextField pathField;
    private ScannerTask currentTask;

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage stage) {
        String darkTheme = """
            -fx-base: #1e1e1e;
            -fx-control-inner-background: #252526;
            -fx-background-color: #1e1e1e;
            -fx-text-fill: white;
            -fx-font-family: 'Segoe UI', sans-serif;
            """;

        VBox root = new VBox(15);
        root.setPadding(new Insets(20));
        root.setStyle(darkTheme);

        Label title = new Label("KEEPLY SCANNER 2.3");
        title.setFont(Font.font("Segoe UI", FontWeight.BOLD, 24));
        title.setTextFill(Color.web("#007acc"));

        GridPane grid = new GridPane();
        grid.setHgap(10); grid.setVgap(10);
        
        pathField = new TextField("C:/Dados");
        pathField.setPromptText("Caminho para Scanear");
        pathField.setMinWidth(400);
        
        Button btnBrowse = new Button("...");
        btnBrowse.setOnAction(e -> {
            DirectoryChooser dc = new DirectoryChooser();
            File f = dc.showDialog(stage);
            if(f != null) pathField.setText(f.getAbsolutePath());
        });

        grid.addRow(0, new Label("Diretório Raiz:"), pathField, btnBrowse);
        grid.addRow(1, new Label("PostgreSQL:"), new Label("jdbc:postgresql://localhost:5432/scan_db"));

        HBox statsBox = new HBox(20);
        statsBox.setAlignment(Pos.CENTER);
        statsBox.getChildren().addAll(
            createStatCard("Arquivos", model.filesScannedProperty),
            createStatCard("MB/s", model.mbPerSecProperty),
            createStatCard("Hash/s", model.rateProperty),
            createStatCard("DB Batches", model.dbBatchesProperty),
            createStatCard("Erros", model.errorsProperty)
        );

        consoleArea = new TextArea();
        consoleArea.setEditable(false);
        consoleArea.setPrefHeight(250);
        consoleArea.setStyle("-fx-font-family: 'Consolas', monospace; -fx-control-inner-background: #101010;");

        HBox actions = new HBox(10);
        actions.setAlignment(Pos.CENTER_RIGHT);
        
        btnScan = new Button("INICIAR SCAN");
        btnScan.setStyle("-fx-background-color: #2da44e; -fx-text-fill: white; -fx-font-weight: bold; -fx-padding: 8 20;");
        
        btnStop = new Button("PARAR");
        btnStop.setStyle("-fx-background-color: #cf222e; -fx-text-fill: white; -fx-font-weight: bold; -fx-padding: 8 20;");
        btnStop.setDisable(true);

        btnWipe = new Button("LIMPAR BANCO");
        btnWipe.setStyle("-fx-background-color: #d27b00; -fx-text-fill: white; -fx-font-weight: bold; -fx-padding: 8 20;");

        actions.getChildren().addAll(btnWipe, new Separator(), btnStop, btnScan);

        btnScan.setOnAction(e -> startScan());
        btnStop.setOnAction(e -> stopScan());
        btnWipe.setOnAction(e -> wipeDatabaseAction());

        root.getChildren().addAll(title, grid, new Separator(), statsBox, new Separator(), consoleArea, actions);

        Scene scene = new Scene(root, 950, 650);
        stage.setScene(scene);
        stage.setTitle("Keeply High-Perf Scanner (Java 21)");
        stage.show();
    }

    private VBox createStatCard(String title, StringProperty valueProp) {
        VBox card = new VBox(5);
        card.setStyle("-fx-background-color: #333; -fx-padding: 10; -fx-background-radius: 5; -fx-min-width: 120;");
        card.setAlignment(Pos.CENTER);
        Label lblTitle = new Label(title);
        lblTitle.setTextFill(Color.LIGHTGRAY);
        Label lblVal = new Label("0");
        lblVal.setFont(Font.font(20));
        lblVal.setStyle("-fx-font-weight: bold");
        lblVal.textProperty().bind(valueProp);
        card.getChildren().addAll(lblTitle, lblVal);
        return card;
    }

    private void log(String msg) {
        Platform.runLater(() -> {
            consoleArea.appendText(msg + "\n");
            consoleArea.setScrollTop(Double.MAX_VALUE); 
        });
    }

    private void wipeDatabaseAction() {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Limpar Banco de Dados");
        alert.setHeaderText("ATENÇÃO: ISSO APAGARÁ TUDO!");
        alert.setContentText("Você tem certeza que deseja executar TRUNCATE em todas as tabelas? O histórico será perdido.");
        
        Optional<ButtonType> result = alert.showAndWait();
        if (result.isPresent() && result.get() == ButtonType.OK) {
            Thread.ofVirtual().start(() -> {
                log("!!! INICIANDO LIMPEZA TOTAL DO BANCO !!!");
                try (ScannerTask task = new ScannerTask("", ScanConfig.builder().build(), this::log, model)) {
                     task.wipeAllData();
                } catch (Exception e) {
                    log("Erro ao limpar banco: " + e.getMessage());
                    e.printStackTrace();
                }
            });
        }
    }

    private void startScan() {
        String path = pathField.getText();
        if (path.isEmpty()) return;

        consoleArea.clear();
        model.reset();
        
        ScanConfig config = ScanConfig.builder()
                .workers(12) 
                .dbPoolSize(15)
                .batchLimit(2000)
                .addExclude("**/node_modules/**")
                .addExclude("**/.git/**")
                .addExclude("**/$Recycle.Bin/**")
                .build();

        currentTask = new ScannerTask(path, config, this::log, model);
        
        btnScan.setDisable(true);
        btnWipe.setDisable(true);
        btnStop.setDisable(false);
        pathField.setDisable(true);

        Thread.ofVirtual().name("keeply-master").start(currentTask);
    }

    private void stopScan() {
        if (currentTask != null) {
            currentTask.cancelScan();
            log("!!! Solicitando parada...");
        }
    }

    private void onScanFinished() {
        Platform.runLater(() -> {
            btnScan.setDisable(false);
            btnWipe.setDisable(false);
            btnStop.setDisable(true);
            pathField.setDisable(false);
            log("=== SCAN FINALIZADO ===");
        });
    }

    // =============================================================================================
    // PARTE 2: BRIDGE
    // =============================================================================================
    
    public static class ScanModel {
        public StringProperty filesScannedProperty = new SimpleStringProperty("0");
        public StringProperty mbPerSecProperty = new SimpleStringProperty("0.0");
        public StringProperty rateProperty = new SimpleStringProperty("0");
        public StringProperty dbBatchesProperty = new SimpleStringProperty("0");
        public StringProperty errorsProperty = new SimpleStringProperty("0");

        public void reset() {
            Platform.runLater(() -> {
                filesScannedProperty.set("0"); mbPerSecProperty.set("0.0"); rateProperty.set("0");
                dbBatchesProperty.set("0"); errorsProperty.set("0");
            });
        }
    }

    class ScannerTask implements Runnable, AutoCloseable {
        private final String rootPath;
        private final ScanConfig config;
        private final java.util.function.Consumer<String> logger;
        private final ScanModel uiModel;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private SimplePool pool; 

        String dbUrl = "jdbc:postgresql://localhost:5432/scan_db?currentSchema=keeply&reWriteBatchedInserts=true";
        String dbUser = "postgres";
        String dbPass = "admin";

        public ScannerTask(String rootPath, ScanConfig config, java.util.function.Consumer<String> logger, ScanModel uiModel) {
            this.rootPath = rootPath; this.config = config; this.logger = logger; this.uiModel = uiModel;
        }

        public void cancelScan() { running.set(false); }

        public void wipeAllData() {
             try {
                 if (pool == null) pool = new SimplePool(dbUrl, dbUser, dbPass, 2);
                 try (Connection c = pool.borrow()) {
                     logger.accept("Executando TRUNCATE em todas as tabelas...");
                     try (Statement st = c.createStatement()) {
                         st.execute("TRUNCATE TABLE file_change, file_state, content, path, scan_issue, scan CASCADE");
                     }
                     c.commit();
                     logger.accept("Banco de dados limpo com sucesso!");
                 }
             } catch (Exception e) {
                 logger.accept("Erro no Wipe: " + e.getMessage());
             }
        }

        @Override
        public void run() {
            try {
                ScanMetrics metrics = new ScanMetrics();
                metrics.running.set(true); 
                startUiUpdater(metrics);

                logger.accept("Conectando ao banco de dados...");
                try {
                    pool = new SimplePool(dbUrl, dbUser, dbPass, config.dbPoolSize);
                    
                    try (Connection c = pool.borrow()) {
                        ScannerEngine.initSchema(c);
                        logger.accept("Schema OK. Carregando snapshot...");
                    }

                    ScannerEngine.runScanLogic(rootPath, config, pool, metrics, running, logger);
                    
                } catch (Exception e) {
                    logger.accept("ERRO FATAL: " + e.getMessage());
                    e.printStackTrace();
                }
                metrics.running.set(false);
            } finally {
                close();
                onScanFinished();
            }
        }
        
        @Override
        public void close() {
            if(pool != null) pool.close();
        }

        private void startUiUpdater(ScanMetrics m) {
            Thread.ofVirtual().start(() -> {
                while (running.get()) {
                    try {
                        Thread.sleep(500);
                        double sec = Math.max(1.0, Duration.between(m.start, Instant.now()).toMillis() / 1000.0);
                        long count = m.filesScanned.sum();
                        double rate = count / sec;
                        double mb = (m.bytesScanned.sum() / 1024.0 / 1024.0) / sec;

                        Platform.runLater(() -> {
                            uiModel.filesScannedProperty.set(String.format("%,d", count));
                            uiModel.mbPerSecProperty.set(String.format("%.1f", mb));
                            uiModel.rateProperty.set(String.format("%.0f", rate));
                            uiModel.dbBatchesProperty.set(String.valueOf(m.dbBatches.sum()));
                            uiModel.errorsProperty.set(String.valueOf(m.errorsWalk.sum() + m.errorsHash.sum()));
                        });
                    } catch (InterruptedException e) { break; }
                }
            });
        }
    }

    // =============================================================================================
    // PARTE 3: BACKEND (Scanner Engine)
    // =============================================================================================

    public static class ScannerEngine {
        
        public static void runScanLogic(String rootPath, ScanConfig cfg, SimplePool pool, ScanMetrics metrics, AtomicBoolean runningControl, java.util.function.Consumer<String> logger) throws Exception {
            
            long scanId;
            Map<String, PrevInfo> index = new HashMap<>();

            try (Connection c = pool.borrow()) {
                scanId = startScanLog(c, rootPath);
                loadIndex(c, rootPath, cfg.preloadIndexMaxRows, index);
                logger.accept("Index carregado: " + index.size() + " arquivos.");
                c.commit();
            }

            // Instancia o Writer com controle de espera (Phaser)
            ParallelDbWriter writer = new ParallelDbWriter(pool, scanId, cfg, metrics);
            if(index.isEmpty()) writer.clearCache();

            List<ExcludeRule> rules = cfg.excludes.stream().map(ExcludeRule::new).toList();
            Path root = Paths.get(rootPath);
            BlockingQueue<FileMeta> queue = new ArrayBlockingQueue<>(5000);

            // Walker
            Thread walker = Thread.ofVirtual().start(() -> {
                try {
                    Files.walkFileTree(root, EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE, new SimpleFileVisitor<>() {
                        @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                            if (!runningControl.get()) return FileVisitResult.TERMINATE;
                            if (!attrs.isRegularFile()) return FileVisitResult.CONTINUE;

                            String rel = root.relativize(file).toString().replace('\\','/');
                            for(var r : rules) if(r.matcher.matches(Path.of(rel))) {
                                metrics.filesIgnored.increment();
                                return FileVisitResult.CONTINUE;
                            }

                            metrics.filesScanned.increment();
                            metrics.bytesScanned.add(attrs.size());

                            FileMeta fm = new FileMeta(rootPath, file.toAbsolutePath().toString(), file.getFileName().toString(),
                                    attrs.size(), safeTime(attrs.creationTime()), safeTime(attrs.lastModifiedTime()),
                                    null, "PATH", file.toAbsolutePath().toString());
                            
                            try { queue.put(fm); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                            return FileVisitResult.CONTINUE;
                        }
                        
                        @Override public FileVisitResult visitFileFailed(Path file, IOException exc) {
                            metrics.errorsWalk.increment();
                            writer.queueIssue(new ScanIssue(scanId, StageEnum.WALK, file.toString(), "PATH", file.toString(), exc.getClass().getSimpleName(), exc.getMessage(), null));
                            return FileVisitResult.CONTINUE;
                        }
                    });
                } catch (IOException e) { 
                    e.printStackTrace(); 
                } finally {
                    try { for(int i=0; i<cfg.workers; i++) queue.put(new FileMeta(null,null,null,0,null,null,null,"POISON",null)); } catch(Exception e){}
                }
            });

            // Workers
            ExecutorService executor = Executors.newFixedThreadPool(cfg.workers);
            for (int i = 0; i < cfg.workers; i++) {
                executor.submit(() -> {
                    while (runningControl.get()) {
                        try {
                            FileMeta fm = queue.take();
                            if ("POISON".equals(fm.identityType)) break;
                            FileResult res = processFile(fm, index, cfg, metrics);
                            writer.queueFile(res);
                        } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
                    }
                });
            }

            walker.join();
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.DAYS);

            logger.accept("Finalizando escritas pendentes...");
            
            // Força o flush do que restou nos buffers
            writer.flushAll();
            
            // AGUARDA TODAS AS THREADS DE BANCO TERMINAREM
            writer.waitForCompletion();

            try (Connection c = pool.borrow()) {
                handleDeletions(c, scanId, rootPath);
                finishScanLog(c, scanId);
                c.commit();
            }
            logger.accept("Sucesso!");
        }

        static FileResult processFile(FileMeta curr, Map<String, PrevInfo> index, ScanConfig cfg, ScanMetrics metrics) {
            String key = curr.identityType + ":" + curr.identityValue;
            PrevInfo prev = index.get(key);
            if (prev == null) {
                var hash = computeHashIfNeeded(curr, cfg, metrics);
                return new FileResult(curr, FileStatus.NEW, hash.algo, hash.hex, "NEW");
            }
            boolean metaDiff = prev.sizeBytes != curr.sizeBytes || !Objects.equals(prev.modifiedAt, curr.modifiedAt);
            if (metaDiff) {
                var hash = computeHashIfNeeded(curr, cfg, metrics);
                return new FileResult(curr, FileStatus.MODIFIED, hash.algo, hash.hex, "MODIFIED");
            }
            return new FileResult(curr, FileStatus.UNCHANGED, prev.contentAlgo, prev.contentHash, null);
        }

        static HashRes computeHashIfNeeded(FileMeta m, ScanConfig cfg, ScanMetrics metrics) {
            if (!cfg.computeHash || (cfg.hashMaxBytes > 0 && m.sizeBytes > cfg.hashMaxBytes)) return new HashRes(null, null);
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                try (InputStream in = new BufferedInputStream(Files.newInputStream(Path.of(m.fullPath)))) {
                    byte[] b = new byte[64*1024]; int r;
                    while ((r = in.read(b)) != -1) md.update(b, 0, r);
                }
                metrics.filesHashed.increment();
                metrics.bytesHashed.add(m.sizeBytes);
                return new HashRes(HexFormat.of().formatHex(md.digest()), "SHA-256");
            } catch (Exception e) {
                metrics.errorsHash.increment();
                return new HashRes(null, null);
            }
        }

        static void initSchema(Connection c) throws SQLException {
            try(Statement s = c.createStatement()) {
                s.execute("CREATE SCHEMA IF NOT EXISTS keeply");
                s.execute("SET search_path TO keeply");
                s.execute("CREATE TABLE IF NOT EXISTS scan (id BIGSERIAL PRIMARY KEY, root_path TEXT, started_at TEXT, finished_at TEXT, status TEXT)");
                s.execute("CREATE TABLE IF NOT EXISTS path (id BIGSERIAL PRIMARY KEY, full_path TEXT UNIQUE)");
                s.execute("CREATE TABLE IF NOT EXISTS content (algo TEXT, hash_hex TEXT, size_bytes BIGINT, PRIMARY KEY(algo, hash_hex))");
                s.execute("CREATE TABLE IF NOT EXISTS file_state (root_path TEXT, identity_type TEXT, identity_value TEXT, path_id BIGINT, name TEXT, size_bytes BIGINT, created_at TEXT, modified_at TEXT, file_key TEXT, content_algo TEXT, content_hash TEXT, last_scan_id BIGINT, PRIMARY KEY (root_path, identity_type, identity_value))");
                s.execute("CREATE TABLE IF NOT EXISTS file_change (id BIGSERIAL PRIMARY KEY, root_path TEXT, identity_type TEXT, identity_value TEXT, scan_id BIGINT, size_bytes BIGINT, modified_at TEXT, content_hash TEXT, reason TEXT)");
                s.execute("CREATE TABLE IF NOT EXISTS scan_issue (id BIGSERIAL PRIMARY KEY, scan_id BIGINT, stage TEXT, path TEXT, identity_type TEXT, identity_value TEXT, error_type TEXT, message TEXT, rule TEXT, created_at TEXT)");
                s.execute("CREATE INDEX IF NOT EXISTS idx_file_state_root_scan ON file_state(root_path, last_scan_id)");
            }
        }

        static long startScanLog(Connection c, String root) throws SQLException {
            try(PreparedStatement ps = c.prepareStatement("INSERT INTO scan(root_path, started_at, status) VALUES(?,?, 'RUNNING')", Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, root); ps.setString(2, Instant.now().toString());
                ps.executeUpdate();
                var rs = ps.getGeneratedKeys(); rs.next(); return rs.getLong(1);
            }
        }

        static void finishScanLog(Connection c, long id) throws SQLException {
            c.createStatement().execute("UPDATE scan SET finished_at='" + Instant.now() + "', status='SUCCESS' WHERE id=" + id);
        }

        static void loadIndex(Connection c, String root, long limit, Map<String, PrevInfo> map) throws SQLException {
            try (PreparedStatement ps = c.prepareStatement("SELECT identity_type, identity_value, path_id, size_bytes, modified_at, content_hash, content_algo FROM file_state WHERE root_path=? LIMIT ?")) {
                ps.setString(1, root); ps.setLong(2, limit);
                var rs = ps.executeQuery();
                while(rs.next()) {
                    map.put(rs.getString(1)+":"+rs.getString(2), new PrevInfo(rs.getLong(3), null, rs.getLong(4), rs.getString(5), rs.getString(6), rs.getString(7)));
                }
            }
        }

        static void handleDeletions(Connection c, long scanId, String root) throws SQLException {
            c.createStatement().execute("INSERT INTO file_change(root_path, identity_type, identity_value, scan_id, reason) SELECT root_path, identity_type, identity_value, "+scanId+", 'DELETED' FROM file_state WHERE root_path='"+root+"' AND last_scan_id < " + scanId);
            c.createStatement().execute("DELETE FROM file_state WHERE root_path='"+root+"' AND last_scan_id < " + scanId);
        }
        
        static String safeTime(FileTime t) { return t == null ? null : t.toInstant().toString(); }
    }

    // =============================================================================================
    // PARTE 4: ESTRUTURAS AUXILIARES
    // =============================================================================================

    static class SimplePool implements AutoCloseable {
        private final String url, user, pass;
        private final BlockingQueue<Connection> pool;
        private final List<Connection> allConnections = new ArrayList<>();

        SimplePool(String url, String user, String pass, int size) throws SQLException {
            try { Class.forName("org.postgresql.Driver"); } catch (ClassNotFoundException e) { System.err.println("PG Driver missing"); }
            this.url = url; this.user = user; this.pass = pass;
            this.pool = new ArrayBlockingQueue<>(size);
            for (int i = 0; i < size; i++) { Connection c = createNew(); pool.offer(c); allConnections.add(c); }
        }

        private Connection createNew() throws SQLException {
            Connection c = DriverManager.getConnection(url, user, pass);
            c.setAutoCommit(false);
            try (Statement st = c.createStatement()) {
                st.execute("SET synchronous_commit = OFF");
                st.execute("SET client_encoding = 'UTF8'");
                st.execute("SET search_path TO keeply");
            }
            return c;
        }

        public Connection borrow() throws InterruptedException { return pool.take(); }
        public void release(Connection c) { if (c != null) pool.offer(c); }
        public void close() { for (Connection c : allConnections) try { c.close(); } catch (SQLException ignored) {} }
    }

    static final class ParallelDbWriter {
        private final SimplePool pool;
        private final long scanId;
        private final ScanConfig cfg;
        private final ScanMetrics metrics;
        private final ConcurrentHashMap<String, Long> pathCache = new ConcurrentHashMap<>(50_000);
        private final ReentrantLock lock = new ReentrantLock();
        private final List<FileResult> fileBuffer;
        private final List<ScanIssue> issueBuffer;
        // CONTROLE DE PENDÊNCIAS (Importante para evitar SocketClosed)
        private final Phaser activeWrites = new Phaser(1);

        ParallelDbWriter(SimplePool pool, long scanId, ScanConfig cfg, ScanMetrics metrics) {
            this.pool = pool; this.scanId = scanId; this.cfg = cfg; this.metrics = metrics;
            this.fileBuffer = new ArrayList<>(cfg.batchLimit * 2);
            this.issueBuffer = new ArrayList<>(cfg.batchLimit * 2);
        }
        
        public void clearCache() { pathCache.clear(); }
        
        public void waitForCompletion() {
            activeWrites.arriveAndAwaitAdvance(); // Espera todas as threads liberarem
        }

        void queueFile(FileResult r) {
            lock.lock();
            try {
                fileBuffer.add(r);
                if (fileBuffer.size() >= cfg.batchLimit) {
                    List<FileResult> batch = new ArrayList<>(fileBuffer); fileBuffer.clear();
                    flushFilesAsync(batch);
                }
            } finally { lock.unlock(); }
        }

        void queueIssue(ScanIssue i) {
            lock.lock();
            try {
                issueBuffer.add(i);
                if (issueBuffer.size() >= cfg.batchLimit) {
                    List<ScanIssue> batch = new ArrayList<>(issueBuffer); issueBuffer.clear();
                    flushIssuesAsync(batch);
                }
            } finally { lock.unlock(); }
        }

        void flushAll() {
            lock.lock();
            try {
                if (!fileBuffer.isEmpty()) { flushFilesAsync(new ArrayList<>(fileBuffer)); fileBuffer.clear(); }
                if (!issueBuffer.isEmpty()) { flushIssuesAsync(new ArrayList<>(issueBuffer)); issueBuffer.clear(); }
            } finally { lock.unlock(); }
        }

        private void flushFilesAsync(List<FileResult> batch) {
            activeWrites.register(); // +1 tarefa pendente
            Thread.ofVirtual().start(() -> {
                Connection conn = null;
                try {
                    conn = pool.borrow();
                    executeFileBatch(conn, batch);
                    conn.commit();
                    metrics.dbBatches.increment();
                } catch (Exception e) {
                    e.printStackTrace();
                    if (conn != null) try { conn.rollback(); } catch (SQLException ignored) {}
                } finally { 
                    pool.release(conn);
                    activeWrites.arriveAndDeregister(); // -1 tarefa pendente
                }
            });
        }

        private void flushIssuesAsync(List<ScanIssue> batch) {
            activeWrites.register();
            Thread.ofVirtual().start(() -> {
                Connection conn = null;
                try {
                    conn = pool.borrow();
                    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO scan_issue(scan_id, stage, path, identity_type, identity_value, error_type, message, rule, created_at) VALUES(?,?,?,?,?,?,?,?,?)")) {
                        String now = Instant.now().toString();
                        for (ScanIssue i : batch) {
                            ps.setLong(1, scanId); ps.setString(2, i.stage().name()); ps.setString(3, i.path());
                            ps.setString(4, i.identityType()); ps.setString(5, i.identityValue());
                            ps.setString(6, i.errorType()); ps.setString(7, i.message()); ps.setString(8, i.rule()); ps.setString(9, now);
                            ps.addBatch();
                        }
                        ps.executeBatch(); conn.commit();
                    }
                } catch (Exception e) { e.printStackTrace(); } finally { 
                    pool.release(conn);
                    activeWrites.arriveAndDeregister();
                }
            });
        }

        private void executeFileBatch(Connection conn, List<FileResult> items) throws SQLException {
             // Ordenação para evitar DEADLOCK
             Collections.sort(items, Comparator.comparing(f -> f.contentHash() == null ? "" : f.contentHash()));

             try (PreparedStatement psPath = conn.prepareStatement("INSERT INTO path(full_path) VALUES(?) ON CONFLICT(full_path) DO NOTHING");
                  PreparedStatement psPathSel = conn.prepareStatement("SELECT id FROM path WHERE full_path=?");
                  PreparedStatement psCont = conn.prepareStatement("INSERT INTO content(algo, hash_hex, size_bytes) VALUES(?,?,?) ON CONFLICT(algo, hash_hex) DO NOTHING");
                  PreparedStatement psState = conn.prepareStatement("INSERT INTO file_state(root_path, identity_type, identity_value, path_id, name, size_bytes, created_at, modified_at, file_key, content_algo, content_hash, last_scan_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(root_path, identity_type, identity_value) DO UPDATE SET path_id=excluded.path_id, name=excluded.name, size_bytes=excluded.size_bytes, modified_at=excluded.modified_at, content_hash=excluded.content_hash, last_scan_id=excluded.last_scan_id");
                  PreparedStatement psTouch = conn.prepareStatement("UPDATE file_state SET last_scan_id=? WHERE root_path=? AND identity_type=? AND identity_value=?");
                  PreparedStatement psLog = conn.prepareStatement("INSERT INTO file_change(root_path, identity_type, identity_value, scan_id, size_bytes, modified_at, content_hash, reason) VALUES(?,?,?,?,?,?,?,?)")) {
                
                for (FileResult res : items) {
                    if (res.status() == FileStatus.UNCHANGED) {
                        psTouch.setLong(1, scanId); psTouch.setString(2, res.meta().rootPath);
                        psTouch.setString(3, res.meta().identityType); psTouch.setString(4, res.meta().identityValue);
                        psTouch.addBatch(); continue;
                    }
                    long pathId = resolvePathId(conn, psPath, psPathSel, res.meta().fullPath);
                    if (res.contentHash != null) {
                        psCont.setString(1, res.contentAlgo); psCont.setString(2, res.contentHash); psCont.setLong(3, res.meta().sizeBytes); psCont.addBatch();
                    }
                    psState.setString(1, res.meta().rootPath); psState.setString(2, res.meta().identityType); psState.setString(3, res.meta().identityValue);
                    psState.setLong(4, pathId); psState.setString(5, res.meta().name); psState.setLong(6, res.meta().sizeBytes);
                    psState.setString(7, res.meta().createdAt); psState.setString(8, res.meta().modifiedAt); psState.setString(9, res.meta().fileKey);
                    psState.setString(10, res.contentAlgo); psState.setString(11, res.contentHash); psState.setLong(12, scanId); psState.addBatch();
                    psLog.setString(1, res.meta().rootPath); psLog.setString(2, res.meta().identityType); psLog.setString(3, res.meta().identityValue);
                    psLog.setLong(4, scanId); psLog.setLong(5, res.meta().sizeBytes); psLog.setString(6, res.meta().modifiedAt);
                    psLog.setString(7, res.contentHash); psLog.setString(8, res.status.name()); psLog.addBatch();
                }
                psCont.executeBatch(); psState.executeBatch(); psTouch.executeBatch(); psLog.executeBatch();
            }
        }
        
        private long resolvePathId(Connection conn, PreparedStatement psIns, PreparedStatement psSel, String path) throws SQLException {
            Long cached = pathCache.get(path); if (cached != null) return cached;
            psIns.setString(1, path); psIns.executeUpdate(); 
            psSel.setString(1, path);
            try (ResultSet rs = psSel.executeQuery()) {
                if (rs.next()) { long id = rs.getLong(1); pathCache.put(path, id); return id; }
            }
            throw new SQLException("Path ID resolution failed for " + path);
        }
    }

    public enum StageEnum { WALK, HASH, DB, IGNORE }
    public enum FileStatus { NEW, MODIFIED, MOVED, UNCHANGED, HASH_FAILED, SKIPPED_SIZE, SKIPPED_DISABLED }
    public record ScanIssue(long scanId, StageEnum stage, String path, String identityType, String identityValue, String errorType, String message, String rule) {}
    public record FileMeta(String rootPath, String fullPath, String name, long sizeBytes, String createdAt, String modifiedAt, String fileKey, String identityType, String identityValue) {}
    public record PrevInfo(long pathId, String knownPath, long sizeBytes, String modifiedAt, String contentHash, String contentAlgo) {}
    public record FileResult(FileMeta meta, FileStatus status, String contentAlgo, String contentHash, String reason) {}
    public record HashRes(String hex, String algo) {}

    static final class ExcludeRule {
        final PathMatcher matcher;
        ExcludeRule(String glob) { this.matcher = FileSystems.getDefault().getPathMatcher("glob:" + glob); }
    }

    public record ScanConfig(int workers, int batchLimit, boolean computeHash, long hashMaxBytes, List<String> excludes, long preloadIndexMaxRows, int dbPoolSize) {
        public static Builder builder() { return new Builder(); }
        public static class Builder {
            private int workers = 8, batchLimit = 2000, dbPoolSize = 10;
            private boolean computeHash = true; private long hashMaxBytes = 200L * 1024 * 1024, preloadIndexMaxRows = 5_000_000;
            private final List<String> excludes = new ArrayList<>();
            public Builder workers(int v) { this.workers = v; return this; }
            public Builder batchLimit(int v) { this.batchLimit = v; return this; }
            public Builder dbPoolSize(int v) { this.dbPoolSize = v; return this; }
            public Builder addExclude(String glob) { this.excludes.add(glob); return this; }
            public ScanConfig build() { return new ScanConfig(workers, batchLimit, computeHash, hashMaxBytes, List.copyOf(excludes), preloadIndexMaxRows, dbPoolSize); }
        }
    }

    static class ScanMetrics {
        final LongAdder filesScanned = new LongAdder();
        final LongAdder filesIgnored = new LongAdder();
        final LongAdder filesHashed = new LongAdder();
        final LongAdder bytesScanned = new LongAdder();
        final LongAdder bytesHashed = new LongAdder();
        final LongAdder errorsWalk = new LongAdder();
        final LongAdder errorsHash = new LongAdder();
        final LongAdder dbBatches = new LongAdder();
        final Instant start = Instant.now();
        final AtomicBoolean running = new AtomicBoolean(false);
    }
}