package com.keeply.app;

import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.inventory.InventoryController;
import com.keeply.app.inventory.InventoryScreen;
import com.keeply.app.inventory.BackupController;
import com.keeply.app.inventory.BackupScreen;
import com.keeply.app.overview.OverviewScreen;
import com.keeply.app.templates.KeeplyTemplate;

import io.github.cdimascio.dotenv.Dotenv;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Rectangle2D;
import javafx.scene.Scene;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.image.Image;
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
        InventoryScreen inventoryView = new InventoryScreen();
        
        // 3. Inicializa Controladores
        new BackupController(scanView, scanModel);
        new InventoryController(inventoryView);

        // 4. Monta o Layout
        KeeplyTemplate layout = new KeeplyTemplate(stage);
        TabPane tabPane = buildTabs(layout, scanView, inventoryView);
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

    private TabPane buildTabs(KeeplyTemplate layout, BackupScreen scan, InventoryScreen inventory) {
        TabPane tabs = new TabPane();
        tabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        tabs.getStyleClass().add("app-tabs");

        OverviewScreen overviewView = new OverviewScreen();
        Tab tOverview = new Tab("VISÃO GERAL", overviewView.content());
        Tab tScan = new Tab("BACKUP", scan.content());
        Tab tInv = new Tab("INVENTARIO", inventory.content());

        tabs.getTabs().addAll(tOverview, tScan, tInv);

        // Footer global (mesmos botões em todas as telas)
        layout.setFooter(scan.footer());

        layout.setTitle("Visão Geral");
        scan.getScanButton().setDisable(true);
        scan.getStopButton().setDisable(true);

        tabs.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            boolean isBackup = newVal == tScan;
            scan.getScanButton().setDisable(!isBackup);
            scan.getStopButton().setDisable(!isBackup);

            if (newVal == tOverview) {
                layout.setTitle("Visão Geral");
            } else if (newVal == tScan) {
                layout.setTitle("Backup");
            } else if (newVal == tInv) {
                layout.setTitle("Inventário");
            }
        });

        return tabs;
    }
}
