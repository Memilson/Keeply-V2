package com.keeply.app;

import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.inventory.InventoryController;
import com.keeply.app.inventory.InventoryScreen;
import com.keeply.app.inventory.BackupController;
import com.keeply.app.inventory.BackupScreen;
import com.keeply.app.overview.OverviewScreen;
import com.keeply.app.history.BackupHistoryScreen;
import com.keeply.app.templates.KeeplyTemplate;

import io.github.cdimascio.dotenv.Dotenv;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Rectangle2D;
import javafx.scene.Scene;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.image.Image;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.geometry.Pos;
import javafx.scene.control.ToggleButton;
import javafx.scene.control.ToggleGroup;
import javafx.scene.shape.SVGPath;
import javafx.stage.Screen;
import javafx.stage.Stage;
import javafx.stage.StageStyle;

import java.io.InputStream;
import java.util.Objects;

public class Main extends Application {

    private static final double SCREEN_MARGIN = 12;
    private static final double DEFAULT_WIDTH = 1280;
    private static final double DEFAULT_HEIGHT = 720;

    public static void main(String[] args) {
        try {
            Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
            System.setProperty("DB_URL", Objects.requireNonNullElse(dotenv.get("DB_URL"), "jdbc:sqlite:keeply.db"));
        } catch (Exception e) {
            System.setProperty("DB_URL", "jdbc:sqlite:keeply.db");
        }
        launch(args);
    }

    @Override
    public void start(Stage stage) {
        // 1. Inicializa o Banco
        DatabaseBackup.init();

        // 2. Inicializa o Modelo e Telas (Arquitetura V2)
        KeeplyTemplate.ScanModel scanModel = new KeeplyTemplate.ScanModel();
        
        BackupScreen scanView = new BackupScreen(stage, scanModel);
        BackupHistoryScreen historyView = new BackupHistoryScreen();
        InventoryScreen inventoryView = new InventoryScreen();
        
        // 3. Inicializa Controladores
        new BackupController(scanView, scanModel);
        new InventoryController(inventoryView);

        // 4. Monta o Layout
        KeeplyTemplate layout = new KeeplyTemplate(stage);
        TabPane tabPane = buildTabs(layout, scanView, historyView, inventoryView);
        layout.setContent(tabPane);

        // 5. Configura Janela
        stage.initStyle(StageStyle.UNDECORATED);
        stage.setTitle("Keeply V2");

        Rectangle2D bounds = Screen.getPrimary().getVisualBounds();

        double maxW = Math.max(320, bounds.getWidth() - (SCREEN_MARGIN * 2));
        double maxH = Math.max(240, bounds.getHeight() - (SCREEN_MARGIN * 2));

        double targetW = Math.min(DEFAULT_WIDTH, maxW);
        double targetH = targetW * 9.0 / 16.0;
        if (targetH > maxH) {
            targetH = Math.min(DEFAULT_HEIGHT, maxH);
            targetW = targetH * 16.0 / 9.0;
        }
        
        Scene scene = new Scene(layout.root(), targetW, targetH);
        try {
            scene.getStylesheets().add(Objects.requireNonNull(getClass().getResource("/styles.css")).toExternalForm());
        } catch (Exception e) {
            System.err.println(">> AVISO: styles.css não encontrado.");
        }

        try (InputStream icon = getClass().getResourceAsStream("/icon.png")) {
            if (icon != null) stage.getIcons().add(new Image(icon));
        } catch (Exception ignored) {}

        stage.setScene(scene);
        stage.setResizable(false);

        // Centraliza (16:9)
        stage.setX(bounds.getMinX() + ((bounds.getWidth() - targetW) / 2.0));
        stage.setY(bounds.getMinY() + ((bounds.getHeight() - targetH) / 2.0));

        // Fecha banco ao sair
        stage.setOnCloseRequest(e -> {
            DatabaseBackup.shutdown();
            Platform.exit();
            System.exit(0);
        });

        stage.show();
    }

    private TabPane buildTabs(KeeplyTemplate layout, BackupScreen scan, BackupHistoryScreen history, InventoryScreen inventory) {
        TabPane tabs = new TabPane();
        tabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        tabs.getStyleClass().add("app-tabs");

        OverviewScreen overviewView = new OverviewScreen();
        Tab tOverview = new Tab("Visão Geral", overviewView.content());
        Tab tHistory = new Tab("Atividades", history.content());
        Tab tConfig = new Tab("Configurações", scan.content());
        Tab tInv = new Tab("Armazenamento", inventory.content());

        tabs.getTabs().addAll(tOverview, tHistory, tConfig, tInv);

        // Footer global (mesmos botões em todas as telas)
        layout.setFooter(scan.footer());

        scan.getScanButton().setDisable(true);
        scan.getStopButton().setDisable(true);

        layout.setSidebar(buildSidebarNav(tabs, tOverview, tHistory, tConfig, tInv));

        tabs.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            boolean isBackup = newVal == tConfig;
            scan.getScanButton().setDisable(!isBackup);
            scan.getStopButton().setDisable(!isBackup);

            // Título de tela não exibido no novo layout.
        });

        return tabs;
    }

    private static VBox buildSidebarNav(TabPane tabs, Tab tOverview, Tab tHistory, Tab tConfig, Tab tInv) {
        ToggleGroup group = new ToggleGroup();

        ToggleButton navOverview = navItem("Visão Geral", "M3 13h6v8H3z M10 3l11 10h-3v8h-6v-6H8v6H5v-8H2z");
        ToggleButton navHistory = navItem("Atividades", "M4 18h16v2H4z M6 10h2v6H6z M11 6h2v10h-2z M16 12h2v4h-2z");
        ToggleButton navConfig = navItem("Configurações", "M12 8a4 4 0 1 0 0 8 4 4 0 0 0 0-8zm9 4-2.1-.4a7.6 7.6 0 0 0-.7-1.6l1.2-1.7-1.4-1.4-1.7 1.2c-.5-.3-1-.6-1.6-.7L13 3h-2l-.4 2.1c-.6.1-1.1.4-1.6.7L7.3 4.6 5.9 6l1.2 1.7c-.3.5-.6 1-.7 1.6L4 12l2.1.4c.1.6.4 1.1.7 1.6L5.6 15.7 7 17.1l1.7-1.2c.5.3 1 .6 1.6.7L11 19h2l.4-2.1c.6-.1 1.1-.4 1.6-.7l1.7 1.2 1.4-1.4-1.2-1.7c.3-.5.6-1 .7-1.6L21 12z");
        ToggleButton navInventory = navItem("Armazenamento", "M5 4h14v2H5z M5 10h14v2H5z M5 16h14v2H5z");

        navOverview.setToggleGroup(group);
        navHistory.setToggleGroup(group);
        navConfig.setToggleGroup(group);
        navInventory.setToggleGroup(group);

        navOverview.getStyleClass().add("sidebar-item");
        navHistory.getStyleClass().add("sidebar-item");
        navConfig.getStyleClass().add("sidebar-item");
        navInventory.getStyleClass().add("sidebar-item");

        navOverview.setOnAction(e -> tabs.getSelectionModel().select(tOverview));
        navHistory.setOnAction(e -> tabs.getSelectionModel().select(tHistory));
        navConfig.setOnAction(e -> tabs.getSelectionModel().select(tConfig));
        navInventory.setOnAction(e -> tabs.getSelectionModel().select(tInv));

        group.selectToggle(navOverview);
        tabs.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal == tOverview) group.selectToggle(navOverview);
            else if (newVal == tHistory) group.selectToggle(navHistory);
            else if (newVal == tConfig) group.selectToggle(navConfig);
            else if (newVal == tInv) group.selectToggle(navInventory);
        });

        VBox nav = new VBox(6, navOverview, navHistory, navConfig, navInventory);
        nav.getStyleClass().add("sidebar-nav");
        nav.setAlignment(Pos.TOP_LEFT);
        VBox.setVgrow(nav, Priority.NEVER);

        Region spacer = new Region();
        VBox.setVgrow(spacer, Priority.ALWAYS);

        VBox sidebar = new VBox(18, nav, spacer);
        sidebar.getStyleClass().add("sidebar-container");
        sidebar.setAlignment(Pos.TOP_LEFT);
        return sidebar;
    }

    private static ToggleButton navItem(String text, String svgPath) {
        SVGPath icon = new SVGPath();
        icon.setContent(svgPath);
        icon.getStyleClass().add("sidebar-icon");

        ToggleButton btn = new ToggleButton(text);
        btn.setGraphic(icon);
        btn.setContentDisplay(javafx.scene.control.ContentDisplay.LEFT);
        btn.setGraphicTextGap(10);
        btn.setMaxWidth(Double.MAX_VALUE);
        return btn;
    }
}
