package com.keeply.app.inventory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import com.keeply.app.config.Config;
import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.templates.KeeplyTemplate.ScanModel;

import javafx.application.Platform;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ContentDisplay;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.TitledPane;
import javafx.scene.control.ToggleButton;
import javafx.scene.control.ToggleGroup;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.shape.SVGPath;
import javafx.stage.DirectoryChooser;
import javafx.stage.Stage;

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

    private final ToggleGroup destinationTypeGroup = new ToggleGroup();
    private final ToggleButton btnLocal = new ToggleButton("Disco local");
    private final ToggleButton btnCloud = new ToggleButton("Nuvem");

    // Botões de Ação
    private final Button btnScan   = new Button("Iniciar backup");
    private final Button btnStop   = new Button("Parar");
    private final Button btnWipe   = new Button("Apagar backups");
    private final Button btnBrowse = new Button("Alterar origem");
    private final Button btnBrowseDest = new Button("Alterar destino");
    private final Button btnDbOptions = new Button("Opções DB");

    private final HBox backupFooterActions = new HBox(10);
    private final BooleanProperty scanning = new SimpleBooleanProperty(false);

    private final ProgressIndicator progressRing = new ProgressIndicator();
    private final ProgressBar progressBar = new ProgressBar(0);
    private final Label progressLabel = new Label("Idle");

    // Opções de Backup
    private final CheckBox encryptionCheckbox = new CheckBox();
    private final PasswordField backupPasswordField = new PasswordField();

    public BackupScreen(Stage stage, ScanModel model) {
        this.stage = Objects.requireNonNull(stage, "stage");
        this.model = Objects.requireNonNull(model, "model");
        configureControls();
    }

    private void configureControls() {
        // Valores iniciais
        pathField.setText(Objects.requireNonNullElse(Config.getLastPath(), System.getProperty("user.home")));
        pathField.setPromptText("Selecione a pasta de origem…");
        pathField.setEditable(false);

        destField.setText(Objects.requireNonNullElse(Config.getLastBackupDestination(), defaultLocalBackupDestination().toString()));
        destField.setPromptText("Selecione a pasta de destino…");
        destField.setEditable(false);

        // Garante que o destino padrão exista (best-effort)
        try {
            Files.createDirectories(Path.of(destField.getText()));
        } catch (Exception ignored) {}

        // Tipo de destino (Local / Nuvem placeholder)
        btnLocal.setToggleGroup(destinationTypeGroup);
        btnCloud.setToggleGroup(destinationTypeGroup);
        btnLocal.setSelected(true);

        btnLocal.getStyleClass().addAll("segmented", "segmented-left");
        btnCloud.getStyleClass().addAll("segmented", "segmented-right");

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

        // Criptografia de backup (senha única)
        if (Config.hasBackupPasswordHash()) {
            backupPasswordField.setPromptText("Senha configurada (digite para desbloquear)");
        } else {
            backupPasswordField.setPromptText("Digite a senha do backup");
        }

        backupPasswordField.textProperty().addListener((obs, oldVal, newVal) -> {
            if (encryptionCheckbox.isSelected()) Config.setBackupEncryptionPassword(newVal);
        });
    }

    public Node content() {
        var root = new VBox(18);
        root.getStyleClass().add("backup-screen");
        root.setPadding(new Insets(8, 0, 0, 0));

        Label pageTitle = new Label("Configurações");
        pageTitle.getStyleClass().add("page-title");

        Label pageSubtitle = new Label("Parâmetros do plano de backup automatizado.");
        pageSubtitle.getStyleClass().add("page-subtitle");

        VBox card = new VBox(14);
        card.getStyleClass().addAll("card", "backup-plan-card");
        card.setPadding(new Insets(16));

        HBox header = new HBox(10);
        header.setAlignment(Pos.CENTER_LEFT);

        Label h2 = new Label("Plano de Backup");
        h2.getStyleClass().add("card-h2");

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        HBox segmented = new HBox(0, btnLocal, btnCloud);
        segmented.getStyleClass().add("segmented-host");

        header.getChildren().addAll(h2, spacer, segmented);

        HBox flow = new HBox(14);
        flow.getStyleClass().add("flow-row");

        Node originPanel = createFlowPanel(
            "Origem",
            "(O que fazer backup)",
            ICON_FOLDER,
            pathField,
            btnBrowse
        );

        Label arrow = new Label("→");
        arrow.getStyleClass().add("flow-arrow");
        StackPane arrowWrap = new StackPane(arrow);
        arrowWrap.getStyleClass().add("flow-arrow-wrap");

        Node destPanel = createDestinationPanel();

        flow.getChildren().addAll(originPanel, arrowWrap, destPanel);
        HBox.setHgrow(originPanel, Priority.ALWAYS);
        HBox.setHgrow(destPanel, Priority.ALWAYS);

        TitledPane options = createOptionsPane();
        options.setExpanded(false);

        Node progress = createProgressPanel();

        card.getChildren().addAll(header, flow, options, progress);
        VBox content = new VBox(16, pageTitle, pageSubtitle, card);
        content.getStyleClass().add("content-wrap");
        content.setMaxWidth(980);

        root.getChildren().add(content);
        return root;
    }

    private Node createProgressPanel() {
        progressRing.setMaxSize(20, 20);
        progressRing.setMinSize(20, 20);
        progressRing.progressProperty().bind(model.progressProperty);

        progressBar.getStyleClass().add("metric-progress");
        progressBar.setMaxWidth(Double.MAX_VALUE);
        progressBar.progressProperty().bind(model.progressProperty);

        progressLabel.getStyleClass().add("muted");
        progressLabel.textProperty().bind(model.phaseProperty);

        HBox top = new HBox(10, progressRing, progressLabel);
        top.setAlignment(Pos.CENTER_LEFT);

        VBox box = new VBox(8, top, progressBar);
        box.setPadding(new Insets(8, 0, 0, 0));
        box.visibleProperty().bind(scanning);
        box.managedProperty().bind(scanning);
        HBox.setHgrow(progressBar, Priority.ALWAYS);
        return box;
    }

    public Node footer() {
        var root = new HBox(12);
        root.getStyleClass().add("footer");
        root.setAlignment(Pos.CENTER_LEFT);
        root.setPadding(new Insets(10, 0, 0, 0));

        // Esquerda: botão principal
        styleIconButton(btnScan, ICON_PLAY);
        btnScan.getStyleClass().addAll("btn", "btn-primary");
        btnScan.setMinWidth(150);

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        // Direita: ações
        backupFooterActions.setAlignment(Pos.CENTER_RIGHT);
        styleIconButton(btnStop, ICON_STOP);
        styleIconButton(btnWipe, ICON_TRASH);

        btnStop.getStyleClass().addAll("btn", "btn-secondary");
        btnWipe.getStyleClass().addAll("btn", "btn-secondary");
        btnDbOptions.getStyleClass().addAll("btn", "btn-secondary");

        btnStop.setMinWidth(92);

        btnWipe.setTooltip(new Tooltip("Apaga o histórico (SQLite) e o cofre de backups (.keeply/storage)."));

        backupFooterActions.getChildren().setAll(btnStop, btnWipe, btnDbOptions);
        root.getChildren().addAll(btnScan, spacer, backupFooterActions);
        return root;
    }

    // ---------------- UI building ----------------

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

    private Node createFlowPanel(String title, String subtitle, String iconPath, TextField boundPath, Button actionButton) {
        VBox panel = new VBox(8);
        panel.getStyleClass().add("flow-panel");
        panel.setPadding(new Insets(12));

        HBox top = new HBox(10);
        top.setAlignment(Pos.CENTER_LEFT);

        SVGPath icon = new SVGPath();
        icon.setContent(iconPath);
        icon.getStyleClass().add("flow-icon");

        VBox titles = new VBox(2);
        Label t = new Label(title);
        t.getStyleClass().add("flow-title");
        Label st = new Label(subtitle);
        st.getStyleClass().add("flow-subtitle");
        titles.getChildren().addAll(t, st);

        top.getChildren().addAll(icon, titles);

        Label path = new Label();
        path.getStyleClass().add("flow-path");
        path.textProperty().bind(boundPath.textProperty());

        actionButton.getStyleClass().addAll("btn", "btn-outline");
        actionButton.setMinWidth(150);

        panel.getChildren().addAll(top, path, actionButton);
        return panel;
    }

    private Node createDestinationPanel() {
        VBox panel = new VBox(8);
        panel.getStyleClass().add("flow-panel");
        panel.setPadding(new Insets(12));

        HBox top = new HBox(10);
        top.setAlignment(Pos.CENTER_LEFT);

        SVGPath icon = new SVGPath();
        icon.setContent("M6 19a4 4 0 0 1 0-8 5 5 0 0 1 9.6-1.6A4 4 0 0 1 18 19H6z");
        icon.getStyleClass().add("flow-icon");

        VBox titles = new VBox(2);
        Label t = new Label("Destino");
        t.getStyleClass().add("flow-title");
        Label st = new Label("(Onde armazenar)");
        st.getStyleClass().add("flow-subtitle");
        titles.getChildren().addAll(t, st);

        top.getChildren().addAll(icon, titles);

        Label destText = new Label();
        destText.getStyleClass().add("flow-path");
        destText.textProperty().bind(javafx.beans.binding.Bindings.createStringBinding(() -> {
            if (isCloudSelected()) return "Nuvem: Azure Blob Storage (container-backup)";
            String v = destField.getText();
            return (v == null || v.isBlank()) ? "-" : v;
        }, destinationTypeGroup.selectedToggleProperty(), destField.textProperty()));

        btnBrowseDest.getStyleClass().addAll("btn", "btn-outline");
        btnBrowseDest.setMinWidth(150);
        btnBrowseDest.disableProperty().bind(scanning.or(destinationTypeGroup.selectedToggleProperty().isEqualTo(btnCloud)));

        panel.getChildren().addAll(top, destText, btnBrowseDest);
        return panel;
    }

    private TitledPane createOptionsPane() {
        Label title = new Label("Opções de Backup");
        title.getStyleClass().add("options-title");

        GridPane grid = new GridPane();
        grid.getStyleClass().add("options-grid");
        grid.setHgap(10);
        grid.setVgap(10);

        addOptionRow(grid, 0, "M19 4h-1V2h-2v2H8V2H6v2H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm0 16H5V9h14v11z", "Agendamento", null);
        addOptionRow(grid, 1, "M4 6h16v2H4zm0 5h10v2H4zm0 5h16v2H4z", "Retenção", null);
        addOptionRow(grid, 2, "M12 2a5 5 0 0 0-5 5v3H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8a2 2 0 0 0-2-2h-1V7a5 5 0 0 0-5-5zm-3 8V7a3 3 0 0 1 6 0v3H9z", "Criptografar backups", encryptionCheckbox);
        addEncryptionDetailsRow(grid, 3);

        TitledPane pane = new TitledPane();
        pane.setText(null);
        pane.setGraphic(title);
        pane.setContent(grid);
        pane.getStyleClass().add("options-pane");
        return pane;
    }

    private void addOptionRow(GridPane grid, int row, String iconPath, String label, CheckBox controlCheckbox) {
        HBox left = new HBox(8);
        left.setAlignment(Pos.CENTER_LEFT);

        SVGPath icon = new SVGPath();
        icon.setContent(iconPath);
        icon.getStyleClass().add("option-icon");

        Label l = new Label(label);
        l.getStyleClass().add("option-label");

        left.getChildren().addAll(icon, l);

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        CheckBox sw = controlCheckbox != null ? controlCheckbox : new CheckBox();
        sw.getStyleClass().add("switch");
        
        // Carregar estado salvo para Criptografia
        if (controlCheckbox == encryptionCheckbox) {
            sw.setSelected(Config.isBackupEncryptionEnabled());
            sw.selectedProperty().addListener((obs, oldVal, newVal) -> {
                Config.saveBackupEncryptionEnabled(newVal);
            });
        }

        HBox right = new HBox(10, spacer, sw);
        right.setAlignment(Pos.CENTER_RIGHT);

        grid.add(left, 0, row);
        grid.add(right, 1, row);
        GridPane.setHgrow(right, Priority.ALWAYS);
    }

    private void addEncryptionDetailsRow(GridPane grid, int row) {
        HBox left = new HBox(8);
        left.setAlignment(Pos.CENTER_LEFT);

        SVGPath icon = new SVGPath();
        icon.setContent("M7 10h10v2H7zm0 4h6v2H7zm2-12h6v2H9zm-5 8h2v6h12v-6h2v8H4z");
        icon.getStyleClass().add("option-icon");

        Label label = new Label("Senha do backup");
        label.getStyleClass().add("option-label");

        left.getChildren().addAll(icon, label);

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        HBox right = new HBox(10, spacer, backupPasswordField);
        right.setAlignment(Pos.CENTER_RIGHT);

        left.visibleProperty().bind(encryptionCheckbox.selectedProperty());
        left.managedProperty().bind(encryptionCheckbox.selectedProperty());
        right.visibleProperty().bind(encryptionCheckbox.selectedProperty());
        right.managedProperty().bind(encryptionCheckbox.selectedProperty());

        backupPasswordField.disableProperty().bind(
                encryptionCheckbox.selectedProperty().not()
        );

        grid.add(left, 0, row);
        grid.add(right, 1, row);
        GridPane.setHgrow(right, Priority.ALWAYS);
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
        File initial = new File(Objects.requireNonNullElse(Config.getLastPath(), System.getProperty("user.home")));
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
        File initial = new File(Objects.requireNonNullElse(Config.getLastBackupDestination(), defaultLocalBackupDestination().toString()));
        if (initial.exists() && initial.isDirectory()) dc.setInitialDirectory(initial);
        dc.setTitle("Selecionar destino do backup");
        File f = dc.showDialog(stage);
        if (f != null) {
            destField.setText(f.getAbsolutePath());
            Config.saveLastBackupDestination(f.getAbsolutePath());

            try {
                Files.createDirectories(f.toPath());
            } catch (Exception ignored) {}
        }
    }

    private static Path defaultLocalBackupDestination() {
        String home = Objects.requireNonNullElse(System.getProperty("user.home"), ".");
        return Path.of(home, "Documents", "Keeply", "Backup");
    }

    public boolean isCloudSelected() {
        return destinationTypeGroup.getSelectedToggle() == btnCloud;
    }

    private void showDbOptions() {
        DatabaseBackup.DbEncryptionStatus s = DatabaseBackup.getEncryptionStatus();
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
    public String getBackupEncryptionPassword() { return backupPasswordField.getText(); }

    public void setScanningState(boolean isScanning) {
        scanning.set(isScanning);
        btnScan.setDisable(isScanning);
        btnWipe.setDisable(isScanning);
        btnBrowse.setDisable(isScanning);
        pathField.setDisable(isScanning);
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

    public boolean isEncryptionEnabled() {
        return encryptionCheckbox.isSelected();
    }

    public boolean confirmWipe() {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Confirmar remoção");
        alert.setHeaderText("Apagar backups do Keeply?");
        alert.setContentText(
                "Isso apagará:\n" +
                "• O histórico/banco de dados (SQLite)\n" +
                "• Os binários armazenados no cofre (.keeply/storage)\n\n" +
                "Isso NÃO apaga os arquivos originais da sua pasta de origem.");
        Optional<ButtonType> res = alert.showAndWait();
        return res.isPresent() && res.get() == ButtonType.OK;
    }
}
