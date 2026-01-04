package com.keeply.app.inventory;

import com.keeply.app.config.Config;
import com.keeply.app.database.Database;
import com.keeply.app.templates.KeeplyTemplate.ScanModel;

import javafx.application.Platform;
import javafx.beans.property.StringProperty;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import javafx.scene.shape.SVGPath;
import javafx.stage.DirectoryChooser;
import javafx.stage.Stage;

import java.io.File;
import java.util.Objects;
import java.util.Optional;

public final class BackupScreen {

    // --- Ícones SVG (Paths) ---
    private static final String ICON_FOLDER = "M20 6h-8l-2-2H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm0 12H4V8h16v10z";
    private static final String ICON_PLAY   = "M8 5v14l11-7z";
    private static final String ICON_STOP   = "M6 6h12v12H6z";
    private static final String ICON_TRASH  = "M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z";

    private final Stage stage;
    private final ScanModel model;

    // Campos de Input
    private final TextField pathField = new TextField();
    private final TextField destField = new TextField();
    private final TextArea consoleArea = new TextArea();

    // Botões de Ação
    private final Button btnScan   = new Button("Iniciar backup");
    private final Button btnStop   = new Button("Parar");
    private final Button btnWipe   = new Button("Limpar dados");
    private final Button btnBrowse = new Button();
    private final Button btnBrowseDest = new Button();
    private final Button btnDbOptions = new Button("Opções DB");

    private final HBox backupFooterActions = new HBox(10);

    public BackupScreen(Stage stage, ScanModel model) {
        this.stage = Objects.requireNonNull(stage, "stage");
        this.model = Objects.requireNonNull(model, "model");
        configureControls();
    }

    private void configureControls() {
        // Valores iniciais
        pathField.setText(Config.getLastPath());
        pathField.setPromptText("Selecione a pasta de origem…");

        destField.setText(Config.getLastBackupDestination());
        destField.setPromptText("Selecione a pasta de destino…");

        btnStop.setDisable(true);

        // Eventos (MESMA lógica)
        btnBrowse.setOnAction(e -> chooseDirectory());
        btnBrowse.setTooltip(new Tooltip("Selecionar pasta de origem"));

        btnBrowseDest.setOnAction(e -> chooseDestinationDirectory());
        btnBrowseDest.setTooltip(new Tooltip("Selecionar destino do backup"));

        btnDbOptions.setOnAction(e -> showDbOptions());

        // Console
        consoleArea.setEditable(false);
        consoleArea.setWrapText(true);
    }

    public Node content() {
        var root = new VBox(14);
        root.getStyleClass().add("backup-screen");
        root.setPadding(new Insets(18, 0, 0, 0));

        // Carrega stylesheet (coloque em src/main/resources/styles/keeply.css)
        root.getStylesheets().add(Objects.requireNonNull(
                getClass().getResource("/styles/styles.css"),
                "Missing /styles/keeply.css"
        ).toExternalForm());

        var header = createHeader();
        var pathsCard = createCard(
                sectionTitle("Backup"),
                mutedText("Escolha a pasta de origem e onde o cofre (.keeply/storage) vai ficar."),
                spacer(6),
                sectionLabel("Origem"),
                createPathInputRow(),
                spacer(10),
                sectionLabel("Destino"),
                createDestinationInputRow()
        );

        var stats = createStatsGrid();

        var logCard = createCard(
                sectionLabel("Log em tempo real"),
                consoleArea
        );
        VBox.setVgrow(logCard, Priority.ALWAYS);
        VBox.setVgrow(consoleArea, Priority.ALWAYS);

        root.getChildren().addAll(header, pathsCard, stats, logCard);
        return root;
    }

    public Node footer() {
        var root = new HBox(12);
        root.getStyleClass().add("footer");
        root.setAlignment(Pos.CENTER_LEFT);
        root.setPadding(new Insets(10, 0, 0, 0));

        // Esquerda: opções DB (sempre visível)
        btnDbOptions.getStyleClass().addAll("btn", "btn-secondary");
        btnDbOptions.setMinWidth(140);

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        // Direita: ações
        backupFooterActions.setAlignment(Pos.CENTER_RIGHT);

        styleIconButton(btnWipe, ICON_TRASH);
        styleIconButton(btnStop, ICON_STOP);
        styleIconButton(btnScan, ICON_PLAY);

        btnWipe.getStyleClass().addAll("btn", "btn-secondary", "btn-danger-text");
        btnStop.getStyleClass().addAll("btn", "btn-secondary");
        btnScan.getStyleClass().addAll("btn", "btn-primary");

        btnStop.setMinWidth(92);
        btnScan.setMinWidth(150);

        backupFooterActions.getChildren().setAll(btnWipe, btnStop, btnScan);
        root.getChildren().addAll(btnDbOptions, spacer, backupFooterActions);
        return root;
    }

    // ---------------- UI building ----------------

    private Node createHeader() {
        var box = new HBox(12);
        box.getStyleClass().add("header");
        box.setAlignment(Pos.CENTER_LEFT);
        box.setPadding(new Insets(0, 0, 4, 0));

        var dot = new Circle(7);
        dot.getStyleClass().add("header-dot");

        var titles = new VBox(2);
        var title = new Label("Backup");
        title.getStyleClass().add("h1");
        var subtitle = new Label("Armazenamento deduplicado por conteúdo (hash) com cofre local.");
        subtitle.getStyleClass().add("muted");

        titles.getChildren().addAll(title, subtitle);
        box.getChildren().addAll(dot, titles);
        return box;
    }

    private VBox createCard(Node... children) {
        var card = new VBox(10, children);
        card.getStyleClass().add("card");
        card.setPadding(new Insets(18));
        return card;
    }

    private Label sectionTitle(String text) {
        var l = new Label(text);
        l.getStyleClass().add("card-title");
        return l;
    }

    private Label sectionLabel(String text) {
        var l = new Label(text);
        l.getStyleClass().add("section-label");
        return l;
    }

    private Label mutedText(String text) {
        var l = new Label(text);
        l.getStyleClass().add("muted");
        l.setWrapText(true);
        return l;
    }

    private Region spacer(double h) {
        var r = new Region();
        r.setMinHeight(h);
        return r;
    }

    private Node createPathInputRow() {
        var row = new HBox(10);
        row.setAlignment(Pos.CENTER_LEFT);

        pathField.getStyleClass().add("text-input");
        HBox.setHgrow(pathField, Priority.ALWAYS);

        btnBrowse.getStyleClass().addAll("btn", "btn-icon");
        styleIconOnly(btnBrowse, ICON_FOLDER);

        row.getChildren().addAll(pathField, btnBrowse);
        return row;
    }

    private Node createDestinationInputRow() {
        var row = new HBox(10);
        row.setAlignment(Pos.CENTER_LEFT);

        destField.getStyleClass().add("text-input");
        HBox.setHgrow(destField, Priority.ALWAYS);

        btnBrowseDest.getStyleClass().addAll("btn", "btn-icon");
        styleIconOnly(btnBrowseDest, ICON_FOLDER);

        row.getChildren().addAll(destField, btnBrowseDest);
        return row;
    }

    private GridPane createStatsGrid() {
        var grid = new GridPane();
        grid.getStyleClass().add("stats-grid");
        grid.setHgap(14);
        grid.setVgap(14);

        grid.add(createStatCard("Arquivos escaneados", model.filesScannedProperty, "accent"), 0, 0);
        grid.add(createStatCard("Velocidade (MB/s)", model.mbPerSecProperty, "violet"), 1, 0);
        grid.add(createStatCard("Taxa de scan", model.rateProperty, "amber"), 0, 1);
        grid.add(createStatCard("Erros", model.errorsProperty, "danger"), 1, 1);

        var col = new ColumnConstraints();
        col.setPercentWidth(50);
        grid.getColumnConstraints().setAll(col, col);
        return grid;
    }

    private Node createStatCard(String title, StringProperty valueProp, String accentClass) {
        var card = new HBox(12);
        card.getStyleClass().addAll("stat-card");

        var dot = new Circle(5);
        dot.getStyleClass().addAll("stat-dot", accentClass);

        var box = new VBox(3);
        var lblTitle = new Label(title);
        lblTitle.getStyleClass().add("stat-title");

        var lblValue = new Label();
        lblValue.getStyleClass().add("stat-value");
        lblValue.textProperty().bind(valueProp);

        box.getChildren().addAll(lblTitle, lblValue);
        card.getChildren().addAll(dot, box);
        return card;
    }

    private static void styleIconOnly(Button btn, String svgPath) {
        var icon = new SVGPath();
        icon.setContent(svgPath);
        icon.getStyleClass().add("icon");
        btn.setGraphic(icon);
        btn.setContentDisplay(ContentDisplay.GRAPHIC_ONLY);
        btn.setMinWidth(44);
        btn.setPrefHeight(42);
    }

    private static void styleIconButton(Button btn, String svgPath) {
        var icon = new SVGPath();
        icon.setContent(svgPath);
        icon.getStyleClass().add("icon");
        btn.setGraphic(icon);
        btn.setGraphicTextGap(8);
    }

    // ---------------- Controle (MESMA lógica) ----------------

    private void chooseDirectory() {
        DirectoryChooser dc = new DirectoryChooser();
        File initial = new File(Config.getLastPath());
        if (initial.exists() && initial.isDirectory()) dc.setInitialDirectory(initial);
        dc.setTitle("Selecionar pasta de origem");
        File f = dc.showDialog(stage);
        if (f != null) {
            pathField.setText(f.getAbsolutePath());
            Config.saveLastPath(f.getAbsolutePath());
        }
    }

    private void chooseDestinationDirectory() {
        DirectoryChooser dc = new DirectoryChooser();
        File initial = new File(Config.getLastBackupDestination());
        if (initial.exists() && initial.isDirectory()) dc.setInitialDirectory(initial);
        dc.setTitle("Selecionar destino do backup");
        File f = dc.showDialog(stage);
        if (f != null) {
            destField.setText(f.getAbsolutePath());
            Config.saveLastBackupDestination(f.getAbsolutePath());
        }
    }

    private void showDbOptions() {
        Database.DbEncryptionStatus s = Database.getEncryptionStatus();
        String text = "Status da Criptografia:\n" +
                "Ativado: " + s.encryptionEnabled() + "\n\n" +
                "Arquivos:\n" +
                ".enc exists: " + s.encryptedFileExists() + "\n" +
                "Legacy plain exists: " + s.legacyPlainExists();

        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("Opções do Banco de Dados");
        alert.setHeaderText("Diagnóstico de segurança");
        TextArea area = new TextArea(text);
        area.setEditable(false);
        area.setWrapText(true);
        area.setPrefRowCount(8);
        alert.getDialogPane().setContent(area);
        alert.showAndWait();
    }

    public Button getScanButton() { return btnScan; }
    public Button getStopButton() { return btnStop; }
    public Button getWipeButton() { return btnWipe; }

    public String getRootPathText() { return pathField.getText(); }
    public String getBackupDestinationText() { return destField.getText(); }

    public void setScanningState(boolean isScanning) {
        btnScan.setDisable(isScanning);
        btnWipe.setDisable(isScanning);
        btnBrowse.setDisable(isScanning);
        pathField.setDisable(isScanning);
        btnBrowseDest.setDisable(isScanning);
        destField.setDisable(isScanning);
        btnStop.setDisable(!isScanning);

        double opacity = isScanning ? 0.72 : 1.0;
        pathField.setOpacity(opacity);
        destField.setOpacity(opacity);
    }

    public void clearConsole() { consoleArea.clear(); }

    public void appendLog(String message) {
        Platform.runLater(() -> {
            consoleArea.appendText("• " + message + "\n");
            consoleArea.positionCaret(consoleArea.getLength());
        });
    }

    public boolean confirmWipe() {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Confirmar limpeza");
        alert.setHeaderText("Apagar todos os dados e backups?");
        alert.setContentText("Isso removerá o histórico e os arquivos do cofre (.keeply/storage).");
        Optional<ButtonType> res = alert.showAndWait();
        return res.isPresent() && res.get() == ButtonType.OK;
    }
}
