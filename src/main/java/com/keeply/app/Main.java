package com.keeply.app;

import com.keeply.app.controller.ScanController;
import com.keeply.app.controller.InventoryController;
import com.keeply.app.view.KeeplyTemplate;
import com.keeply.app.view.KeeplyTemplate.ScanModel;
import com.keeply.app.view.InventoryScreen;
import com.keeply.app.view.ScanScreen;
import javafx.application.Application;
import javafx.geometry.Rectangle2D;
import javafx.scene.Scene;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.stage.Screen;
import javafx.stage.Stage;
import javafx.stage.StageStyle;

public class Main extends Application {

    @Override
    public void start(Stage stage) {
        stage.initStyle(StageStyle.UNDECORATED);

        double W = 540, H = 780;

        ScanModel model = new ScanModel();

        KeeplyTemplate shell = new KeeplyTemplate(stage);
        ScanScreen scan = new ScanScreen(stage, model);
        InventoryScreen inventory = new InventoryScreen();

        TabPane tabs = buildTabs(shell, scan, inventory);

        shell.setTitle("Scanner");
        shell.setContent(tabs);
        shell.setFooter(scan.footer());

        new ScanController(scan, model);
        new InventoryController(inventory);

        Scene scene = new Scene(shell.root(), W, H);
        stage.setScene(scene);
        stage.setResizable(false);

        Rectangle2D vb = Screen.getPrimary().getVisualBounds();
        double margin = 12;
        stage.setX(vb.getMaxX() - W - margin);
        stage.setY(vb.getMaxY() - H - margin);

        stage.show();
    }

    private TabPane buildTabs(KeeplyTemplate shell, ScanScreen scan, InventoryScreen inventory) {
        TabPane tabs = new TabPane();
        tabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        tabs.setTabMinWidth(140);
        tabs.setTabMaxWidth(260);
        tabs.setStyle("""
            -fx-background-color: transparent;
            -fx-padding: 0 0 8 0;
            -fx-tab-max-height: 38px;
        """);

        Tab scanTab = new Tab("Scanner", scan.content());
        Tab inventoryTab = new Tab("Inventário", inventory.content());
        tabs.getTabs().addAll(scanTab, inventoryTab);

        tabs.getSelectionModel().selectedItemProperty().addListener((obs, oldTab, newTab) -> {
            if (newTab == inventoryTab) {
                shell.setTitle("Inventário");
                shell.setFooter(inventory.footer());
            } else {
                shell.setTitle("Scanner");
                shell.setFooter(scan.footer());
            }
        });

        return tabs;
    }

    public static void main(String[] args) {
        launch(args);
    }
}
