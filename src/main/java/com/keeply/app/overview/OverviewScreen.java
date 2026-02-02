package com.keeply.app.overview;

import com.keeply.app.config.Config;
import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.database.DatabaseBackup.InventoryRow;
import com.keeply.app.database.DatabaseBackup.ScanSummary;
import com.keeply.app.database.KeeplyDao;
import com.keeply.app.report.ReportService;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.chart.AreaChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressBar;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.scene.shape.SVGPath;
import javafx.stage.FileChooser;
import javafx.stage.Window;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
public final class OverviewScreen {
    public Node content() {
        VBox root = new VBox(14);
        root.getStyleClass().add("overview-screen");
        root.setPadding(new Insets(8, 0, 0, 0));
        Label title = new Label("Status do Sistema");
        title.getStyleClass().add("page-title");
        Label subtitle = new Label("Resumo operacional do ambiente de backup.");
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
        ColumnConstraints c1 = new ColumnConstraints();
        c1.setPercentWidth(33.33);
        c1.setHgrow(Priority.ALWAYS);
        ColumnConstraints c2 = new ColumnConstraints();
        c2.setPercentWidth(33.33);
        c2.setHgrow(Priority.ALWAYS);
        ColumnConstraints c3 = new ColumnConstraints();
        c3.setPercentWidth(33.33);
        c3.setHgrow(Priority.ALWAYS);
        top.getColumnConstraints().setAll(c1, c2, c3);
        top.add(cardTotalBackups(), 0, 0);
        top.add(cardStorage(), 1, 0);
        top.add(cardRecentActivity(), 2, 0);
        VBox chartCard = cardChart();
        VBox healthCard = cardHealth();
        VBox content = new VBox(16, header, top, chartCard, healthCard);
        content.getStyleClass().add("content-wrap");
        content.setMaxWidth(980);
        root.getChildren().add(content);
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

        Config.saveLastPath(out.getParentFile() != null ? out.getParentFile().getAbsolutePath() : out.getAbsolutePath());

        reportBtn.setDisable(true);
        Thread.ofVirtual().name("keeply-report").start(() -> {
            try {
                DatabaseBackup.init();
                Optional<ScanSummary> lastScan = DatabaseBackup.jdbi().withExtension(KeeplyDao.class, KeeplyDao::fetchLastScan);
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
    private VBox baseCard() {
        VBox card = new VBox(10);
        card.getStyleClass().add("card");
        card.setPadding(new Insets(14));
        return card;
    }
    private Node cardTotalBackups() {
        VBox card = baseCard();
        card.getStyleClass().add("metric-card");
        HBox header = new HBox(8);
        header.setAlignment(Pos.CENTER_LEFT);
        Label t = new Label("Total Backups");
        t.getStyleClass().add("card-h2");
        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        SVGPath icon = new SVGPath();
        icon.setContent("M4 18h16v2H4z M6 10h2v6H6z M11 6h2v10h-2z M16 12h2v4h-2z");
        icon.getStyleClass().add("mini-icon");
        header.getChildren().addAll(t, spacer, icon);
        Label big = new Label("124");
        big.getStyleClass().add("metric-value");
        card.getChildren().addAll(header, big);
        return card;
    }
    private Node cardStorage() {
        VBox card = baseCard();
        card.getStyleClass().add("metric-card");
        HBox header = new HBox(8);
        header.setAlignment(Pos.CENTER_LEFT);
        Label t = new Label("Armazenamento Usado");
        t.getStyleClass().add("card-h2");
        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        SVGPath icon = new SVGPath();
        icon.setContent("M6 19a4 4 0 0 1 0-8 5 5 0 0 1 9.6-1.6A4 4 0 0 1 18 19H6z");
        icon.getStyleClass().add("mini-icon");
        header.getChildren().addAll(t, spacer, icon);
        ProgressBar bar = new ProgressBar(0.24);
        bar.getStyleClass().add("metric-progress");
        bar.setMaxWidth(Double.MAX_VALUE);
        Label txt = new Label("1.2 TB / 5 TB (24%)");
        txt.getStyleClass().add("muted");
        card.getChildren().addAll(header, bar, txt);
        return card;
    }
    private Node cardRecentActivity() {
        VBox card = baseCard();
        card.getStyleClass().add("metric-card");
        Label t = new Label("Atividade Recente");
        t.getStyleClass().add("card-h2");
        VBox list = new VBox(6);
        list.getStyleClass().add("activity-list");
        list.getChildren().add(activityRow("Backup Diário", "Sucesso", "Hoje, 02:00", "status-ok"));
        list.getChildren().add(activityRow("Backup Semanal", "Falha", "Ontem, 03:15", "status-bad"));
        list.getChildren().add(activityRow("Restauração", "Sucesso", "Ontem, 14:30", "status-ok"));

        card.getChildren().addAll(t, list);
        return card;
    }

    private Node activityRow(String name, String status, String when, String statusClass) {
        HBox row = new HBox(8);
        row.setAlignment(Pos.CENTER_LEFT);

        Label left = new Label(name + " - ");
        left.getStyleClass().add("activity-name");

        Label st = new Label(status);
        st.getStyleClass().addAll("status-text", statusClass);

        Label date = new Label(" (" + when + ")");
        date.getStyleClass().add("muted");

        row.getChildren().addAll(left, st, date);
        return row;
    }

    private VBox cardChart() {
        VBox card = baseCard();
        card.getStyleClass().add("wide-card");

        Label t = new Label("Volume de Dados (Últimos 30 Dias)");
        t.getStyleClass().add("card-h2");

        NumberAxis x = new NumberAxis();
        NumberAxis y = new NumberAxis();
        x.setAutoRanging(true);
        y.setAutoRanging(true);
        x.setTickLabelsVisible(false);
        x.setTickMarkVisible(false);
        y.setTickLabelsVisible(false);
        y.setTickMarkVisible(false);

        AreaChart<Number, Number> chart = new AreaChart<>(x, y);
        chart.getStyleClass().add("area-chart");
        chart.setLegendVisible(false);
        chart.setHorizontalGridLinesVisible(false);
        chart.setVerticalGridLinesVisible(false);
        chart.setAlternativeColumnFillVisible(false);
        chart.setAlternativeRowFillVisible(false);
        chart.setAnimated(false);
        chart.setCreateSymbols(false);

        XYChart.Series<Number, Number> s = new XYChart.Series<>();
        for (int i = 1; i <= 10; i++) {
            s.getData().add(new XYChart.Data<>(i, 10 + i * 2));
        }
        chart.getData().add(s);

        VBox.setVgrow(chart, Priority.ALWAYS);
        card.getChildren().addAll(t, chart);
        return card;
    }

    private VBox cardHealth() {
        VBox card = baseCard();
        card.getStyleClass().add("wide-card");

        Label t = new Label("Saúde do Sistema");
        t.getStyleClass().add("card-h2");

        HBox row = new HBox(18);
        row.setAlignment(Pos.CENTER_LEFT);

        row.getChildren().add(healthItem("Agente Local: Online", "dot-ok"));
        row.getChildren().add(healthItem("Conexão Nuvem: Online", "dot-ok"));
        row.getChildren().add(healthItem("Disco de Backup: Saudável", "dot-ok"));

        card.getChildren().addAll(t, row);
        return card;
    }

    private Node healthItem(String text, String dotClass) {
        HBox item = new HBox(8);
        item.setAlignment(Pos.CENTER_LEFT);

        Region dot = new Region();
        dot.getStyleClass().addAll("health-dot", dotClass);
        dot.setMinSize(10, 10);
        dot.setPrefSize(10, 10);
        dot.setMaxSize(10, 10);

        Label label = new Label(text);
        label.getStyleClass().add("health-text");

        item.getChildren().addAll(dot, label);
        return item;
    }
}
