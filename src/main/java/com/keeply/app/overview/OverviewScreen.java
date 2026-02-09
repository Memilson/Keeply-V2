package com.keeply.app.overview;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.keeply.app.config.Config;
import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.database.DatabaseBackup.InventoryRow;
import com.keeply.app.database.DatabaseBackup.ScanSummary;
import com.keeply.app.database.KeeplyDao;
import com.keeply.app.report.ReportService;

import javafx.application.Platform;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.OverrunStyle;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableRow;
import javafx.scene.control.TableView;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.scene.shape.SVGPath;
import javafx.stage.FileChooser;
import javafx.stage.Window;

public final class OverviewScreen {

    private final SystemMonitorService monitorService = new SystemMonitorService();

    // Cards
    private Label cpuValueLabel;
    private ProgressBar cpuBar;
    private Label ramValueLabel;
    private ProgressBar ramBar;
    private Label uptimeLabel;
    private Label cpuModelLabel;

    // Top 5 selector
    private enum TopMode {
        CPU("CPU"),
        RAM("Memória"),
        DISK("Disco"),
        NET("Internet");

        final String label;
        TopMode(String label) { this.label = label; }
        @Override public String toString() { return label; }
    }

    private ComboBox<TopMode> topModeBox;
    private Label topHintLabel;
    private VBox topContent;

    // Tabelas (alternadas)
    private final ObservableList<SystemMonitorService.ProcessRow> procItems = FXCollections.observableArrayList();
    private final ObservableList<SystemMonitorService.NetIfRow> netItems = FXCollections.observableArrayList();

    private TableView<SystemMonitorService.ProcessRow> procTable;
    private TableView<SystemMonitorService.NetIfRow> netTable;

    private ScheduledExecutorService exec;
    private volatile boolean started = false;

    // layout constants
    private static final double TOP_ROW_HEIGHT = 36;
    private static final int TOP_LIMIT = 5;

    public Node content() {
        VBox root = new VBox(14);
        root.getStyleClass().add("overview-screen");
        root.setPadding(new Insets(8, 0, 0, 0));

        Label title = new Label("Dashboard do Sistema");
        title.getStyleClass().add("page-title");
        Label subtitle = new Label("Monitoramento RealTime de Recursos");
        subtitle.getStyleClass().add("page-subtitle");

        VBox headerLeft = new VBox(4, title, subtitle);
        HBox header = new HBox(12);
        header.setAlignment(Pos.CENTER_LEFT);
        Region headerSpacer = new Region();
        HBox.setHgrow(headerSpacer, Priority.ALWAYS);
        Button reportBtn = new Button("Gerar relatório");
        reportBtn.getStyleClass().addAll("btn", "btn-outline");
        reportBtn.setOnAction(e -> generateReport(root, reportBtn));
        header.getChildren().addAll(headerLeft, headerSpacer, reportBtn);

        GridPane top = new GridPane();
        top.getStyleClass().add("metrics-grid");
        top.setHgap(12);
        top.setVgap(12);

        ColumnConstraints c1 = new ColumnConstraints(); c1.setPercentWidth(33.33); c1.setHgrow(Priority.ALWAYS);
        ColumnConstraints c2 = new ColumnConstraints(); c2.setPercentWidth(33.33); c2.setHgrow(Priority.ALWAYS);
        ColumnConstraints c3 = new ColumnConstraints(); c3.setPercentWidth(33.33); c3.setHgrow(Priority.ALWAYS);
        top.getColumnConstraints().setAll(c1, c2, c3);

        top.add(cardCpu(), 0, 0);
        top.add(cardRam(), 1, 0);
        top.add(cardUptime(), 2, 0);

        Node top5 = top5Panel();
        VBox.setVgrow(top5, Priority.ALWAYS);

        root.getChildren().addAll(header, top, top5);

        // evita thread vazando se trocar de tela
        root.sceneProperty().addListener((obs, oldScene, newScene) -> {
            if (newScene == null) dispose();
        });

        startMonitoring();
        return root;
    }

    private void generateReport(Node ownerNode, Button reportBtn) {
        Window owner = ownerNode.getScene() != null ? ownerNode.getScene().getWindow() : null;

        FileChooser chooser = new FileChooser();
        chooser.setTitle("Salvar relatório PDF");
        chooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("PDF", "*.pdf"));

        String lastPath = Config.getLastPath();
        if (lastPath != null && !lastPath.isBlank()) {
            File lastDir = new File(lastPath);
            if (lastDir.exists() && lastDir.isDirectory()) {
                chooser.setInitialDirectory(lastDir);
            }
        }

        String ts = DateTimeFormatter.ofPattern("yyyyMMdd-HHmm").format(LocalDateTime.now());
        chooser.setInitialFileName("relatorio-keeply-" + ts + ".pdf");

        File out = chooser.showSaveDialog(owner);
        if (out == null) return;

        Config.saveLastPath(out.getParentFile() != null
                ? out.getParentFile().getAbsolutePath()
                : out.getAbsolutePath());

        reportBtn.setDisable(true);
        Thread.ofVirtual().name("keeply-report").start(() -> {
            try {
                DatabaseBackup.init();
                Optional<ScanSummary> lastScan = DatabaseBackup.jdbi()
                        .withExtension(KeeplyDao.class, KeeplyDao::fetchLastScan);
                if (lastScan.isEmpty()) {
                    Platform.runLater(() -> showAlert(Alert.AlertType.WARNING,
                            "Sem dados",
                            "Não há backups para gerar relatório."));
                    return;
                }

                ScanSummary scan = lastScan.get();
                List<InventoryRow> rows = DatabaseBackup.jdbi().withExtension(
                        KeeplyDao.class,
                        dao -> dao.fetchSnapshotFiles(scan.scanId())
                );

                ReportService reportService = new ReportService();
                reportService.exportPdf(rows, out, scan);

                Platform.runLater(() -> showAlert(Alert.AlertType.INFORMATION,
                        "Relatório gerado",
                        "PDF salvo em: " + out.getAbsolutePath()));
            } catch (IOException | RuntimeException ex) {
                Platform.runLater(() -> showAlert(Alert.AlertType.ERROR,
                        "Erro ao gerar relatório",
                        ex.getMessage() != null ? ex.getMessage() : "Falha desconhecida"));
            } finally {
                Platform.runLater(() -> reportBtn.setDisable(false));
            }
        });
    }

    private void showAlert(Alert.AlertType type, String title, String message) {
        Alert alert = new Alert(type, message, ButtonType.OK);
        alert.setTitle(title);
        alert.setHeaderText(null);
        alert.showAndWait();
    }

    public void dispose() {
        if (exec != null) {
            exec.shutdownNow();
            exec = null;
        }
        started = false;
    }

    // -------------------- MONITORAMENTO --------------------

    private void startMonitoring() {
        if (started) return;
        started = true;

        exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "keeply-monitor");
            t.setDaemon(true);
            return t;
        });

        // CPU/RAM/Uptime (2s)
        exec.scheduleAtFixedRate(() -> {
            double cpuLoad = monitorService.getCpuLoad();
            SystemMonitorService.MemoryStats mem = monitorService.getMemoryStats();
            String up = monitorService.getUptime();

            Platform.runLater(() -> {
                cpuValueLabel.setText(String.format("%.1f%%", cpuLoad * 100));
                cpuBar.setProgress(cpuLoad);
                updateBarColor(cpuBar, cpuLoad);

                ramValueLabel.setText(mem.getUsedString() + " / " + mem.getTotalString());
                ramBar.setProgress(mem.getUsagePercentage());
                updateBarColor(ramBar, mem.getUsagePercentage());

                uptimeLabel.setText(up);
            });
        }, 0, 2, TimeUnit.SECONDS);

        // Top 5 (3s)
        exec.scheduleAtFixedRate(this::refreshTopListSafe, 0, 3, TimeUnit.SECONDS);
    }

    private void refreshTopListSafe() {
        TopMode mode = (topModeBox == null || topModeBox.getValue() == null) ? TopMode.CPU : topModeBox.getValue();
        final TopMode finalMode = mode;

        try {
            if (finalMode == TopMode.NET) {
                List<SystemMonitorService.NetIfRow> rows =
                        monitorService.getTopNetworkInterfaces(TOP_LIMIT, Duration.ofSeconds(3));
                Platform.runLater(() -> {
                    netItems.setAll(rows);
                    showNetTable();
                });
            } else {
                List<SystemMonitorService.ProcessRow> rows = switch (finalMode) {
                    case CPU -> monitorService.getTopProcessesCpu(TOP_LIMIT);
                    case RAM -> monitorService.getTopProcessesRam(TOP_LIMIT);
                    case DISK -> monitorService.getTopProcessesDiskIo(TOP_LIMIT, Duration.ofSeconds(3));
                    default -> monitorService.getTopProcessesCpu(TOP_LIMIT);
                };
                Platform.runLater(() -> {
                    procItems.setAll(rows);
                    showProcTable(finalMode);
                });
            }
        } catch (Exception ignored) {
        }
    }

    // -------------------- UI: TOP 5 --------------------

    private Node top5Panel() {
        VBox box = new VBox(10);
        box.getStyleClass().add("card");
        box.setPadding(new Insets(14));

        HBox header = new HBox(10);
        header.setAlignment(Pos.CENTER_LEFT);

        Label title = new Label("Top 5");
        title.getStyleClass().add("card-h2");

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        topModeBox = new ComboBox<>();
        topModeBox.getItems().setAll(TopMode.CPU, TopMode.RAM, TopMode.DISK, TopMode.NET);
        topModeBox.setValue(TopMode.CPU);
        topModeBox.getStyleClass().add("topmode-combo");
        topModeBox.valueProperty().addListener((obs, oldV, newV) -> refreshTopListSafe());

        header.getChildren().addAll(title, spacer, topModeBox);

        topHintLabel = new Label("");
        topHintLabel.getStyleClass().add("muted-small");
        topHintLabel.setWrapText(true);

        topContent = new VBox(8);
        VBox.setVgrow(topContent, Priority.ALWAYS);

        procTable = buildProcTable();
        netTable = buildNetTable();

        topContent.getChildren().add(procTable);

        box.getChildren().addAll(header, topHintLabel, topContent);
        return box;
    }

    private void showProcTable(TopMode mode) {
        topHintLabel.setText(switch (mode) {
            case CPU -> "Processos com maior uso de CPU (instantâneo + média).";
            case RAM -> "Processos que mais consomem memória RAM.";
            case DISK -> "Processos com maior I/O (leitura/escrita) — melhor esforço.";
            default -> "";
        });

        if (topContent.getChildren().isEmpty() || topContent.getChildren().get(0) != procTable) {
            topContent.getChildren().setAll(procTable);
        }
        procTable.refresh();
    }

    private void showNetTable() {
        topHintLabel.setText("Internet: sem rede por processo (leve); exibindo interfaces com maior tráfego.");
        if (topContent.getChildren().isEmpty() || topContent.getChildren().get(0) != netTable) {
            topContent.getChildren().setAll(netTable);
        }
    }

    // -------------------- TABLES --------------------

    private TableView<SystemMonitorService.ProcessRow> buildProcTable() {
        TableView<SystemMonitorService.ProcessRow> table = new TableView<>(procItems);
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
        table.getStyleClass().add("top-table");

        table.setFixedCellSize(TOP_ROW_HEIGHT);
        table.setPrefHeight(TOP_ROW_HEIGHT * (TOP_LIMIT + 1) + 28);
        table.setPlaceholder(new Label("Coletando processos..."));

        TableColumn<SystemMonitorService.ProcessRow, String> cName = new TableColumn<>("Processo");
        cName.setCellValueFactory(c -> new ReadOnlyStringWrapper(c.getValue().name()));
        cName.setCellFactory(col -> labelCell());

        TableColumn<SystemMonitorService.ProcessRow, String> cPid = new TableColumn<>("PID");
        cPid.setMinWidth(90);
        cPid.setMaxWidth(110);
        cPid.setCellValueFactory(c -> new ReadOnlyStringWrapper(String.valueOf(c.getValue().pid())));
        cPid.setCellFactory(col -> labelCell());

        TableColumn<SystemMonitorService.ProcessRow, String> cValue = new TableColumn<>("Valor");
        cValue.setCellValueFactory(c -> new ReadOnlyStringWrapper(formatValueForMode(c.getValue(), topModeBox.getValue())));
        cValue.setCellFactory(col -> labelCell());

        table.setRowFactory(tv -> {
            TableRow<SystemMonitorService.ProcessRow> row = new TableRow<>();
            row.setMinHeight(TOP_ROW_HEIGHT);
            return row;
        });

        table.getColumns().setAll(cName, cPid, cValue);
        return table;
    }

    private TableView<SystemMonitorService.NetIfRow> buildNetTable() {
        TableView<SystemMonitorService.NetIfRow> table = new TableView<>(netItems);
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
        table.getStyleClass().add("top-table");

        table.setFixedCellSize(TOP_ROW_HEIGHT);
        table.setPrefHeight(TOP_ROW_HEIGHT * (TOP_LIMIT + 1) + 28);
        table.setPlaceholder(new Label("Coletando interfaces..."));

        TableColumn<SystemMonitorService.NetIfRow, String> cIf = new TableColumn<>("Interface");
        cIf.setCellValueFactory(c -> new ReadOnlyStringWrapper(c.getValue().name()));
        cIf.setCellFactory(col -> labelCell());

        TableColumn<SystemMonitorService.NetIfRow, String> cValue = new TableColumn<>("Tráfego");
        cValue.setCellValueFactory(c -> new ReadOnlyStringWrapper(
                "↓ " + humanBps(c.getValue().recvBps()) + "/s   ↑ " + humanBps(c.getValue().sentBps()) + "/s"
        ));
        cValue.setCellFactory(col -> labelCell());

        table.setRowFactory(tv -> {
            TableRow<SystemMonitorService.NetIfRow> row = new TableRow<>();
            row.setMinHeight(TOP_ROW_HEIGHT);
            return row;
        });

        table.getColumns().setAll(cIf, cValue);
        return table;
    }

    private static <S> TableCell<S, String> labelCell() {
        return new TableCell<>() {
            private final Label label = new Label();
            {
                label.setPadding(new Insets(0, 10, 0, 10));
                label.setTextOverrun(OverrunStyle.ELLIPSIS);
                label.setMaxWidth(Double.MAX_VALUE);
            }
            @Override protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) setGraphic(null);
                else { label.setText(item); setGraphic(label); }
            }
        };
    }

    // -------------------- VALOR (CURTO) --------------------

    private String formatValueForMode(SystemMonitorService.ProcessRow row, TopMode mode) {
        if (mode == null) mode = TopMode.CPU;

        return switch (mode) {
            case CPU -> String.format("%.1f%% (média %.1f%%) • %s",
                    row.cpuPct(), row.cpuAvgPct(), humanBytes(row.rssBytes()));

            case RAM -> String.format("%s • CPU %.1f%%",
                    humanBytes(row.rssBytes()), row.cpuPct());

            case DISK -> String.format("R %s/s • W %s/s",
                    humanBytes((long) row.readBps()), humanBytes((long) row.writeBps()));

            case NET -> "—";
        };
    }

    // -------------------- Cards --------------------

    private void updateBarColor(ProgressBar bar, double value) {
        bar.getStyleClass().removeAll("bar-ok", "bar-warn", "bar-crit");
        if (value < 0.60) bar.getStyleClass().add("bar-ok");
        else if (value < 0.85) bar.getStyleClass().add("bar-warn");
        else bar.getStyleClass().add("bar-crit");
    }

    private VBox baseCard() {
        VBox card = new VBox(10);
        card.getStyleClass().add("card");
        card.setPadding(new Insets(14));
        return card;
    }

    private Node cardCpu() {
        VBox card = baseCard();
        card.getStyleClass().add("metric-card");

        HBox header = new HBox(8);
        header.setAlignment(Pos.CENTER_LEFT);
        Label t = new Label("Processador");
        t.getStyleClass().add("card-h2");

        SVGPath icon = new SVGPath();
        icon.setContent("M6 4h12a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2zm0 2v12h12V6H6z M9 9h6v6H9V9z");
        icon.getStyleClass().add("mini-icon");

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        header.getChildren().addAll(t, spacer, icon);

        cpuValueLabel = new Label("0%");
        cpuValueLabel.getStyleClass().add("metric-value");

        cpuModelLabel = new Label(monitorService.getCpuModel());
        cpuModelLabel.getStyleClass().add("muted-small");
        cpuModelLabel.setWrapText(true);

        cpuBar = new ProgressBar(0);
        cpuBar.setMaxWidth(Double.MAX_VALUE);
        cpuBar.getStyleClass().add("metric-progress");

        card.getChildren().addAll(header, cpuValueLabel, cpuBar, cpuModelLabel);
        return card;
    }

    private Node cardRam() {
        VBox card = baseCard();
        card.getStyleClass().add("metric-card");

        HBox header = new HBox(8);
        header.setAlignment(Pos.CENTER_LEFT);
        Label t = new Label("Memória RAM");
        t.getStyleClass().add("card-h2");

        SVGPath icon = new SVGPath();
        icon.setContent("M4 6h16v12H4V6zm2 2v8h12V8H6z M8 10h2v4H8v-4z M14 10h2v4h-2v-4z");
        icon.getStyleClass().add("mini-icon");

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        header.getChildren().addAll(t, spacer, icon);

        ramValueLabel = new Label("0 / 0 GB");
        ramValueLabel.getStyleClass().add("metric-value-small");

        ramBar = new ProgressBar(0);
        ramBar.setMaxWidth(Double.MAX_VALUE);
        ramBar.getStyleClass().add("metric-progress");

        card.getChildren().addAll(header, ramValueLabel, ramBar);
        return card;
    }

    private Node cardUptime() {
        VBox card = baseCard();
        card.getStyleClass().add("metric-card");

        HBox header = new HBox(8);
        header.setAlignment(Pos.CENTER_LEFT);
        Label t = new Label("Tempo de Atividade");
        t.getStyleClass().add("card-h2");

        SVGPath icon = new SVGPath();
        icon.setContent("M12 2a10 10 0 1 0 10 10A10 10 0 0 0 12 2zm0 18a8 8 0 1 1 8-8 8 8 0 0 1-8 8zm1-13h-2v6l5.25 3.15.75-1.23-4-2.37z");
        icon.getStyleClass().add("mini-icon");

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        header.getChildren().addAll(t, spacer, icon);

        uptimeLabel = new Label("...");
        uptimeLabel.getStyleClass().add("metric-value");

        Label sub = new Label("Ligado ininterruptamente");
        sub.getStyleClass().add("muted");

        card.getChildren().addAll(header, uptimeLabel, sub);
        return card;
    }

    // -------------------- formatadores --------------------

    private static String humanBytes(long bytes) {
        double b = bytes;
        String[] u = {"B", "KB", "MB", "GB", "TB"};
        int i = 0;
        while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
        return String.format("%.1f %s", b, u[i]);
    }

    private static String humanBps(double bytesPerSec) {
        return humanBytes((long) Math.max(0, bytesPerSec));
    }
}
