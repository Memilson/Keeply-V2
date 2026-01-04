package com.keeply.app;

import com.keeply.app.database.Database;
import com.keeply.app.inventory.InventoryController;
import com.keeply.app.inventory.InventoryScreen;
import com.keeply.app.inventory.ScanController;
import com.keeply.app.inventory.ScanScreen;
import com.keeply.app.templates.KeeplyTemplate;
import com.keeply.app.tests.TestsScreen;

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

    private static final double WIDTH = 540;
    private static final double HEIGHT = 780;
    private static final double SCREEN_MARGIN = 12;

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
        Database.init();

        // 2. Inicializa o Modelo e Telas (Arquitetura V2)
        KeeplyTemplate.ScanModel scanModel = new KeeplyTemplate.ScanModel();
        
        ScanScreen scanView = new ScanScreen(stage, scanModel);
        InventoryScreen inventoryView = new InventoryScreen();
        
        // 3. Inicializa Controladores
        new ScanController(scanView, scanModel);
        new InventoryController(inventoryView);

        // 4. Monta o Layout
        KeeplyTemplate layout = new KeeplyTemplate(stage);
        TabPane tabPane = buildTabs(layout, scanView, inventoryView);
        layout.setContent(tabPane);

        // 5. Configura Janela
        stage.initStyle(StageStyle.UNDECORATED);
        stage.setTitle("Keeply V2");
        
        Scene scene = new Scene(layout.root(), WIDTH, HEIGHT);
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

        // Posiciona no canto inferior direito
        Rectangle2D bounds = Screen.getPrimary().getVisualBounds();
        stage.setX(bounds.getMaxX() - WIDTH - SCREEN_MARGIN);
        stage.setY(bounds.getMaxY() - HEIGHT - SCREEN_MARGIN);

        // Fecha banco ao sair
        stage.setOnCloseRequest(e -> {
            Database.shutdown();
            Platform.exit();
            System.exit(0);
        });

        stage.show();
    }

    private TabPane buildTabs(KeeplyTemplate layout, ScanScreen scan, InventoryScreen inventory) {
        TabPane tabs = new TabPane();
        tabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        tabs.setStyle("-fx-background-color: transparent; -fx-padding: 0;");

        Tab tScan = new Tab("SCANNER", scan.content());
        Tab tInv = new Tab("INVENTARIO", inventory.content());
        TestsScreen testsView = new TestsScreen();
        Tab tTests = new Tab("TESTES", testsView.content());

        tabs.getTabs().addAll(tScan, tInv, tTests);

        layout.setTitle("Scanner");
        layout.setFooter(scan.footer());

        tabs.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal == tScan) {
                layout.setTitle("Scanner");
                layout.setFooter(scan.footer());
            } else if (newVal == tInv) {
                layout.setTitle("Inventario");
                layout.setFooter(null);
            } else if (newVal == tTests) {
                layout.setTitle("Testes");
                layout.setFooter(null);
            }
        });

        return tabs;
    }
}
