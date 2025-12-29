package com.keeply.app.view;

import com.keeply.app.view.KeeplyTemplate.ScanModel;
import com.keeply.app.view.KeeplyTemplate.Theme;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import javafx.scene.shape.SVGPath;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.stage.DirectoryChooser;
import javafx.stage.Stage;

import java.io.File;
import java.util.Optional;

public final class ScanScreen {

    // --- Ícones SVG (Paths) ---
    private static final String ICON_FOLDER = "M20 6h-8l-2-2H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm0 12H4V8h16v10z";
    private static final String ICON_PLAY   = "M8 5v14l11-7z";
    private static final String ICON_STOP   = "M6 6h12v12H6z";
    private static final String ICON_TRASH  = "M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z";

    private final Stage stage;
    private final ScanModel model;

    private final TextField pathField = new TextField();
    private final TextArea consoleArea = new TextArea();

    // Botões agora com ícones
    private final Button btnScan   = new Button("START");
    private final Button btnStop   = new Button("STOP");
    private final Button btnWipe   = new Button("WIPE");
    private final Button btnBrowse = new Button(); // Sem texto, só ícone

    public ScanScreen(Stage stage, ScanModel model) {
        this.stage = stage;
        this.model = model;
        configureControls();
    }

    private void configureControls() {
        pathField.setText(System.getProperty("user.home"));
        pathField.setPromptText("Select directory to scan...");
        
        btnStop.setDisable(true);
        btnBrowse.setOnAction(e -> chooseDirectory());
        btnBrowse.setTooltip(new Tooltip("Select Folder"));

        // Console styling
        consoleArea.setEditable(false);
        consoleArea.setWrapText(true);
        consoleArea.setStyle("""
            -fx-control-inner-background: %s;
            -fx-text-fill: #A7F3D0; 
            -fx-font-family: %s;
            -fx-font-size: 11px;
            -fx-background-radius: 8;
            -fx-border-radius: 8;
            -fx-border-color: #1E293B; 
            -fx-border-width: 1;
            -fx-padding: 5;
            -fx-highlight-fill: #A7F3D0; 
            -fx-highlight-text-fill: #0F172A;
        """.formatted(Theme.BG_CONSOLE, Theme.FONT_MONO));
    }

    public Node content() {
        VBox layout = new VBox(16);
        layout.setAlignment(Pos.TOP_LEFT);
        layout.setPadding(new Insets(4, 0, 4, 0)); // Pequeno respiro extra

        VBox pathSection = createCard(
                createHeaderLabel("TARGET DIRECTORY"),
                createPathInputRow()
        );

        GridPane statsGrid = createStatsGrid();

        VBox logSection = createCard(
                createHeaderLabel("LIVE EXECUTION LOG"),
                consoleArea
        );
        
        VBox.setVgrow(logSection, Priority.ALWAYS);
        VBox.setVgrow(consoleArea, Priority.ALWAYS);

        layout.getChildren().addAll(pathSection, statsGrid, logSection);
        return layout;
    }

    public Node footer() {
        HBox actions = new HBox(12);
        actions.setAlignment(Pos.CENTER_RIGHT);
        actions.setPadding(new Insets(4, 0, 0, 0));

        // Aplica estilos e ícones
        styleButton(btnWipe, Theme.BG_SECONDARY, Theme.DANGER, true, ICON_TRASH);
        styleButton(btnStop, Theme.BG_PRIMARY, Theme.TEXT_MAIN, true, ICON_STOP);
        styleButton(btnScan, Theme.ACCENT, Theme.TEXT_INVERT, false, ICON_PLAY);

        // Tamanho fixo para os botões de ação principal ficarem uniformes
        btnStop.setMinWidth(90);
        btnScan.setMinWidth(100);

        actions.getChildren().addAll(btnWipe, btnStop, btnScan);
        return actions;
    }

    // --- UI Helpers & Components ---

    private VBox createCard(Node... children) {
        VBox card = new VBox(8, children); // Espaçamento interno reduzido levemente
        card.setPadding(new Insets(16));
        
        // Sombra aprimorada
        String shadow = "-fx-effect: dropshadow(three-pass-box, rgba(0,0,0,0.06), 10, 0, 0, 4);";
        
        card.setStyle("""
            -fx-background-color: %s;
            -fx-background-radius: 12;
            -fx-border-radius: 12;
            -fx-border-color: %s;
            -fx-border-width: 1;
            %s
        """.formatted(Theme.BG_PRIMARY, Theme.BORDER, shadow));
        return card;
    }

    private Node createPathInputRow() {
        HBox row = new HBox(8);
        row.setAlignment(Pos.CENTER_LEFT);

        pathField.setStyle("""
            -fx-background-color: %s;
            -fx-border-color: %s;
            -fx-border-radius: 6;
            -fx-background-radius: 6;
            -fx-padding: 9; 
            -fx-text-fill: %s;
            -fx-font-size: 13px;
        """.formatted(Theme.BG_SECONDARY, Theme.BORDER, Theme.TEXT_MAIN));
        
        HBox.setHgrow(pathField, Priority.ALWAYS);

        // Configura o botão Browse como um ícone quadrado
        styleButton(btnBrowse, Theme.BG_SECONDARY, Theme.TEXT_MAIN, true, ICON_FOLDER);
        btnBrowse.setPadding(new Insets(8));
        btnBrowse.setMinWidth(40);
        btnBrowse.setMaxWidth(40);

        row.getChildren().addAll(pathField, btnBrowse);
        return row;
    }

    private GridPane createStatsGrid() {
        GridPane grid = new GridPane();
        grid.setHgap(12);
        grid.setVgap(12);

        grid.add(createStatCard("FILES SCANNED", model.filesScannedProperty, Color.web(Theme.ACCENT)), 0, 0);
        grid.add(createStatCard("THROUGHPUT (MB/s)", model.mbPerSecProperty, Color.web("#8B5CF6")), 1, 0);
        grid.add(createStatCard("SCAN RATE", model.rateProperty, Color.web("#F59E0B")), 0, 1);
        grid.add(createStatCard("ERRORS", model.errorsProperty, Color.web(Theme.DANGER)), 1, 1);

        ColumnConstraints col = new ColumnConstraints();
        col.setPercentWidth(50);
        grid.getColumnConstraints().addAll(col, col);
        return grid;
    }

    private Node createStatCard(String title, javafx.beans.property.StringProperty valueProp, Color accent) {
        HBox container = new HBox(12);
        container.setPadding(new Insets(12));
        container.setAlignment(Pos.CENTER_LEFT);
        
        container.setStyle("""
            -fx-background-color: %s;
            -fx-background-radius: 8;
            -fx-border-color: %s;
        """.formatted(Theme.BG_SECONDARY, Theme.BORDER));

        Circle dot = new Circle(4, accent);
        // Efeito de brilho no dot (opcional)
        dot.setEffect(new javafx.scene.effect.DropShadow(4, Color.color(accent.getRed(), accent.getGreen(), accent.getBlue(), 0.4)));

        VBox box = new VBox(2);
        Label lblTitle = new Label(title);
        lblTitle.setFont(Font.font("Segoe UI", FontWeight.BOLD, 9));
        lblTitle.setTextFill(Color.web(Theme.TEXT_MUTED));

        Label lblValue = new Label();
        lblValue.setFont(Font.font("Consolas", FontWeight.BOLD, 18));
        lblValue.setTextFill(Color.web(Theme.TEXT_MAIN));
        lblValue.textProperty().bind(valueProp);

        box.getChildren().addAll(lblTitle, lblValue);
        container.getChildren().addAll(dot, box);
        return container;
    }

    private Label createHeaderLabel(String text) {
        Label l = new Label(text);
        l.setFont(Font.font("Segoe UI", FontWeight.BOLD, 10));
        l.setTextFill(Color.web(Theme.TEXT_MUTED));
        l.setPadding(new Insets(0, 0, 4, 0));
        return l;
    }

    // --- Styling Logic Refined ---

    private void styleButton(Button btn, String bgHex, String textHex, boolean outline, String svgIcon) {
        String borderStyle = outline ? "-fx-border-color: " + Theme.BORDER + "; -fx-border-width: 1; -fx-border-radius: 6;" : "";
        
        // Configura o Ícone
        if (svgIcon != null) {
            SVGPath icon = new SVGPath();
            icon.setContent(svgIcon);
            icon.setFill(Color.web(textHex));
            icon.setScaleX(0.85); // Ajuste fino de tamanho
            icon.setScaleY(0.85);
            btn.setGraphic(icon);
            btn.setGraphicTextGap(8);
        }

        String baseStyle = """
            -fx-background-color: %s;
            -fx-text-fill: %s;
            -fx-font-family: 'Segoe UI';
            -fx-font-weight: bold;
            -fx-font-size: 12px;
            -fx-padding: 8 16;
            -fx-background-radius: 6;
            -fx-cursor: hand;
            -fx-transition: -fx-background-color 0.2s, -fx-opacity 0.2s;
            %s
        """.formatted(bgHex, textHex, borderStyle);

        btn.setStyle(baseStyle);

        // Efeito Hover: Escurece levemente o background ou reduz opacidade
        btn.setOnMouseEntered(e -> {
            btn.setOpacity(0.85);
            // Se for outline, muda a cor da borda também
            if(outline) btn.setStyle(baseStyle.replace(Theme.BORDER, "#A1A1AA"));
        });
        
        btn.setOnMouseExited(e -> {
            btn.setOpacity(1.0);
            btn.setStyle(baseStyle);
        });
    }

    // --- Métodos de Controle ---

    private void chooseDirectory() {
        DirectoryChooser dc = new DirectoryChooser();
        dc.setTitle("Select Target Folder");
        File f = dc.showDialog(stage);
        if (f != null) pathField.setText(f.getAbsolutePath());
    }

    public Button getScanButton() { return btnScan; }
    public Button getStopButton() { return btnStop; }
    public Button getWipeButton() { return btnWipe; }

    public String getRootPathText() { return pathField.getText(); }

    public void setScanningState(boolean isScanning) {
        btnScan.setDisable(isScanning);
        btnWipe.setDisable(isScanning);
        btnBrowse.setDisable(isScanning);
        pathField.setDisable(isScanning);
        btnStop.setDisable(!isScanning);
        
        // Efeito visual extra: reduz opacidade da área de input quando bloqueada
        pathField.setOpacity(isScanning ? 0.6 : 1.0);
    }

    public void clearConsole() { consoleArea.clear(); }

    public void appendLog(String message) {
        Platform.runLater(() -> {
            consoleArea.appendText("> " + message + "\n");
            // Método mais robusto para auto-scroll
            consoleArea.selectPositionCaret(consoleArea.getLength()); 
            consoleArea.deselect();
        });
    }

    public boolean confirmWipe() {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Delete Database");
        alert.setHeaderText("Wipe All Data?");
        alert.setContentText("This will permanently delete all scan history and metrics.\nThis action cannot be undone.");
        
        // Estilizando o Alert (Opcional, mas recomendado para consistência)
        DialogPane dialogPane = alert.getDialogPane();
        dialogPane.setStyle("-fx-font-family: 'Segoe UI'; -fx-font-size: 13px;");
        
        Optional<ButtonType> res = alert.showAndWait();
        return res.isPresent() && res.get() == ButtonType.OK;
    }
}


