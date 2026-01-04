package com.keeply.app.overview;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.chart.AreaChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressBar;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.scene.shape.SVGPath;

public final class OverviewScreen {

    public Node content() {
        VBox root = new VBox(14);
        root.getStyleClass().add("overview-screen");
        root.setPadding(new Insets(18, 0, 0, 0));

        Label title = new Label("Visão Geral");
        title.getStyleClass().add("page-title");

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

        root.getChildren().addAll(title, top, chartCard, healthCard);
        return root;
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
