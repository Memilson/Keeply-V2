package com.keeply.app;

import javafx.application.Platform;
import javafx.beans.property.StringProperty;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Parent;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.stage.DirectoryChooser;
import javafx.stage.Stage;

import java.io.File;
import java.util.Optional;

public final class MainView {

    private final Stage stage;
    private final UI.ScanModel model;

    private final VBox root;
    private final TextField pathField;
    private final TextArea consoleArea;

    private final Button btnScan;
    private final Button btnStop;
    private final Button btnWipe;
    private final Button btnBrowse;

    public MainView(Stage stage, UI.ScanModel model) {
        this.stage = stage;
        this.model = model;

        String darkTheme = """
            -fx-base: #1e1e1e;
            -fx-control-inner-background: #252526;
            -fx-background-color: #1e1e1e;
            -fx-text-fill: white;
            -fx-font-family: 'Segoe UI', sans-serif;
            """;

        root = new VBox(15);
        root.setPadding(new Insets(20));
        root.setStyle(darkTheme);

        Label title = new Label("KEEPLY SCANNER 2.5 (Env Config)");
        title.setFont(Font.font("Segoe UI", FontWeight.BOLD, 24));
        title.setTextFill(Color.web("#007acc"));

        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);

        pathField = new TextField("C:/Dados");
        pathField.setPromptText("Caminho para Scanear");
        pathField.setMinWidth(400);

        btnBrowse = new Button("...");
        btnBrowse.setOnAction(e -> chooseDirectory());

        grid.addRow(0, new Label("Diretório Raiz:"), pathField, btnBrowse);

        Label lblDb = new Label("PostgreSQL Conectado via .env");
        lblDb.setTextFill(Color.LIGHTGREEN);
        grid.addRow(1, new Label("Database:"), lblDb);

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

        root.getChildren().addAll(title, grid, new Separator(), statsBox, new Separator(), consoleArea, actions);
    }

    public Parent root() { return root; }

    public String getRootPathText() { return pathField.getText(); }

    public Button scanButton() { return btnScan; }
    public Button stopButton() { return btnStop; }
    public Button wipeButton() { return btnWipe; }

    public void setScanning(boolean scanning) {
        btnScan.setDisable(scanning);
        btnWipe.setDisable(scanning);
        btnStop.setDisable(!scanning);
        pathField.setDisable(scanning);
        btnBrowse.setDisable(scanning);
    }

    public void clearConsole() { consoleArea.clear(); }

    public void log(String msg) {
        Platform.runLater(() -> {
            consoleArea.appendText(msg + "\n");
            consoleArea.setScrollTop(Double.MAX_VALUE);
        });
    }

    public boolean confirmWipe() {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Limpar Banco de Dados");
        alert.setHeaderText("ATENÇÃO: ISSO APAGARÁ TUDO!");
        alert.setContentText("Você tem certeza que deseja executar TRUNCATE em todas as tabelas? O histórico será perdido.");
        Optional<ButtonType> result = alert.showAndWait();
        return result.isPresent() && result.get() == ButtonType.OK;
    }

    private void chooseDirectory() {
        DirectoryChooser dc = new DirectoryChooser();
        File f = dc.showDialog(stage);
        if (f != null) pathField.setText(f.getAbsolutePath());
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
}
