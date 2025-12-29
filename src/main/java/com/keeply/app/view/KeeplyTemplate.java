package com.keeply.app.view;

import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.shape.SVGPath;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.stage.Stage;

public final class KeeplyTemplate {

    /**
     * TEMA GLOBAL (Zinc / Off-White)
     * Centraliza as cores para garantir consistência entre o Template e a Screen.
     */
    public static final class Theme {
        public static final String BG_PRIMARY   = "#FAFAFA"; // Zinco 50 (Cards)
        public static final String BG_SECONDARY = "#F4F4F5"; // Zinco 100 (Fundo Janela)
        public static final String BG_HEADER    = "#18181B"; // Zinco 900 (Topo)
        public static final String BG_CONSOLE   = "#0F172A"; // Slate 900 (Console)
        
        public static final String BORDER       = "#E4E4E7"; // Zinco 200
        
        public static final String TEXT_MAIN    = "#18181B"; 
        public static final String TEXT_MUTED   = "#71717A"; 
        public static final String TEXT_INVERT  = "#FFFFFF"; 

        public static final String ACCENT       = "#06B6D4"; // Ciano
        public static final String DANGER       = "#EF4444"; // Vermelho
        public static final String SUCCESS      = "#10B981"; // Verde

        public static final String FONT_UI      = "'Segoe UI', 'Roboto', sans-serif";
        public static final String FONT_MONO    = "'Consolas', 'JetBrains Mono', monospace";

        private Theme() {}
    }

    /**
     * MODELO DE DADOS (Estado da UI)
     * Hospedado aqui para ser injetado no Controller e na View.
     */
    public static final class ScanModel {
        public final StringProperty filesScannedProperty = new SimpleStringProperty("0");
        public final StringProperty mbPerSecProperty     = new SimpleStringProperty("0.0");
        public final StringProperty rateProperty         = new SimpleStringProperty("0");
        public final StringProperty dbBatchesProperty    = new SimpleStringProperty("0");
        public final StringProperty errorsProperty       = new SimpleStringProperty("0");

        public void reset() {
            Platform.runLater(() -> {
                filesScannedProperty.set("0");
                mbPerSecProperty.set("0.0");
                rateProperty.set("0");
                dbBatchesProperty.set("0");
                errorsProperty.set("0");
            });
        }
    }

    // --- Ícones da Janela ---
    private static final String ICON_CLOSE = "M 4 4 L 10 10 M 10 4 L 4 10";
    private static final String ICON_MIN   = "M 4 8 L 10 8";

    private final Stage stage;
    private final BorderPane root = new BorderPane();
    private final StackPane contentHost = new StackPane();
    private final StackPane footerHost  = new StackPane();
    private final Label screenTitle = new Label("");

    // Estado do Drag da Janela
    private double xOffset = 0;
    private double yOffset = 0;

    public KeeplyTemplate(Stage stage) {
        this.stage = stage;
        initLayout();
        setupWindowDragging();
    }

    public Parent root() { return root; }

    public void setTitle(String title) {
        screenTitle.setText(title);
    }

    public void setContent(Node node) {
        contentHost.getChildren().setAll(node);
    }

    public void setFooter(Node node) {
        if (node == null) {
            footerHost.getChildren().clear();
            return;
        }
        footerHost.getChildren().setAll(node);
    }

    // --- Construção do Layout ---

    private void initLayout() {
        // Aplica o fundo global e a fonte
        root.setStyle("""
            -fx-background-color: %s;
            -fx-font-family: %s;
            -fx-font-smoothing-type: lcd;
            -fx-border-color: #333;
            -fx-border-width: 1;
        """.formatted(Theme.BG_SECONDARY, Theme.FONT_UI));

        root.setTop(createTitleBar());
        root.setCenter(createBody());
        root.setBottom(createFooter());
    }

    private Node createTitleBar() {
        var bar = new HBox(12);
        bar.setPadding(new Insets(10, 16, 10, 16));
        bar.setAlignment(Pos.CENTER_LEFT);
        bar.setStyle("-fx-background-color: " + Theme.BG_HEADER + ";");

        var brand = new Label("Keeply");
        brand.setFont(Font.font("Segoe UI", FontWeight.BOLD, 15));
        brand.setTextFill(Color.WHITE);

        var separator = new Label("|");
        separator.setTextFill(Color.web("#333"));

        screenTitle.setTextFill(Color.web(Theme.TEXT_MUTED));
        screenTitle.setFont(Font.font("Segoe UI", FontWeight.NORMAL, 13));

        var spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        // Botões de controle da janela (Minimizar / Fechar)
        var btnMin = createWindowButton(ICON_MIN, false);
        btnMin.setOnAction(e -> stage.setIconified(true));

        var btnClose = createWindowButton(ICON_CLOSE, true);
        btnClose.setOnAction(e -> stage.close());

        bar.getChildren().addAll(brand, separator, screenTitle, spacer, btnMin, btnClose);

        // Linha fina colorida (Accent Line)
        var accentLine = new Region();
        accentLine.setPrefHeight(2);
        accentLine.setStyle("-fx-background-color: linear-gradient(to right, %s, transparent);".formatted(Theme.ACCENT));

        return new VBox(bar, accentLine);
    }

    private Node createBody() {
        contentHost.setPadding(new Insets(20));
        return contentHost;
    }

    private Node createFooter() {
        footerHost.setPadding(new Insets(16, 20, 20, 20));
        // Footer com borda superior sutil
        footerHost.setStyle("""
            -fx-background-color: %s;
            -fx-border-color: %s;
            -fx-border-width: 1 0 0 0;
        """.formatted(Theme.BG_SECONDARY, Theme.BORDER));
        return footerHost;
    }

    // --- Helpers de UI ---

    private void setupWindowDragging() {
        var titleBar = (VBox) root.getTop();
        titleBar.setOnMousePressed(event -> {
            xOffset = event.getSceneX();
            yOffset = event.getSceneY();
        });
        titleBar.setOnMouseDragged(event -> {
            stage.setX(event.getScreenX() - xOffset);
            stage.setY(event.getScreenY() - yOffset);
        });
    }

    private Button createWindowButton(String svgPath, boolean isClose) {
        var path = new SVGPath();
        path.setContent(svgPath);
        path.setStroke(Color.web("#EAEAEA"));
        path.setStrokeWidth(1.5);
        path.setFill(Color.TRANSPARENT);

        var btn = new Button();
        btn.setGraphic(path);
        btn.setMinSize(28, 28);
        btn.setMaxSize(28, 28);

        String baseStyle  = "-fx-background-color: transparent; -fx-background-radius: 50%; -fx-cursor: hand;";
        String hoverColor = isClose ? Theme.DANGER : "#3F3F46";

        btn.setStyle(baseStyle);

        btn.setOnMouseEntered(e -> {
            btn.setStyle(baseStyle + "-fx-background-color: " + hoverColor + ";");
            if (isClose) path.setStroke(Color.WHITE);
        });

        btn.setOnMouseExited(e -> {
            btn.setStyle(baseStyle);
            path.setStroke(Color.web("#EAEAEA"));
        });

        return btn;
    }
}


