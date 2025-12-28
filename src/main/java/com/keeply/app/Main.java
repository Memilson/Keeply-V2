package com.keeply.app;

import com.keeply.app.controller.ScanController;
import com.keeply.app.view.KeeplyTemplate;
import com.keeply.app.view.KeeplyTemplate.ScanModel;
import com.keeply.app.view.ScanScreen;
import javafx.application.Application;
import javafx.geometry.Rectangle2D;
import javafx.scene.Scene;
import javafx.stage.Screen;
import javafx.stage.Stage;
import javafx.stage.StageStyle;

public class Main extends Application {

    @Override
    public void start(Stage stage) {
        stage.initStyle(StageStyle.UNDECORATED);

        double W = 420, H = 760;

        ScanModel model = new ScanModel();

        KeeplyTemplate shell = new KeeplyTemplate(stage);
        shell.setTitle("Scanner");

        ScanScreen scan = new ScanScreen(stage, model);
        shell.setContent(scan.content());
        shell.setFooter(scan.footer());

        new ScanController(scan, model);

        Scene scene = new Scene(shell.root(), W, H);
        stage.setScene(scene);
        stage.setResizable(false);

        Rectangle2D vb = Screen.getPrimary().getVisualBounds();
        double margin = 12;
        stage.setX(vb.getMaxX() - W - margin);
        stage.setY(vb.getMaxY() - H - margin);

        stage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}
