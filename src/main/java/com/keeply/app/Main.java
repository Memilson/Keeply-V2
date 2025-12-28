package com.keeply.app;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class Main extends Application {

    @Override
    public void start(Stage stage) {
        UI.ScanModel model = new UI.ScanModel();
        MainView view = new MainView(stage, model);
        new MainController(view, model);

        Scene scene = new Scene(view.root(), 950, 650);
        stage.setScene(scene);
        stage.setTitle("Keeply High-Perf Scanner (Java 21)");
        stage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}
