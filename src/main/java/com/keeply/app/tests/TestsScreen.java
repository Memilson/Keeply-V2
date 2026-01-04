package com.keeply.app.tests;

import com.keeply.app.database.Database;
import com.keeply.app.templates.KeeplyTemplate.Theme;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.TextArea;
import javafx.scene.layout.VBox;

public final class TestsScreen {

    private final Button btnRun = new Button("Rodar testes básicos");

    public TestsScreen() {
    }

    public Node content() {
        VBox root = new VBox(16);
        root.setAlignment(Pos.TOP_LEFT);
        root.setPadding(new Insets(12, 0, 0, 0));

        btnRun.setMinWidth(220);
        btnRun.setOnAction(e -> run());
        btnRun.setStyle("""
            -fx-background-color: %s;
            -fx-text-fill: %s;
            -fx-font-family: 'Segoe UI';
            -fx-font-weight: bold;
            -fx-font-size: 12px;
            -fx-padding: 10 16;
            -fx-background-radius: 6;
            -fx-border-color: %s;
            -fx-border-width: 1;
            -fx-border-radius: 6;
            -fx-cursor: hand;
        """.formatted(Theme.BG_PRIMARY, Theme.TEXT_MAIN, Theme.BORDER));

        root.getChildren().add(btnRun);
        return root;
    }

    private void run() {
        Database.SelfTestResult res = Database.runBasicSelfTests();

        Alert alert = new Alert(res.ok() ? Alert.AlertType.INFORMATION : Alert.AlertType.ERROR);
        alert.setTitle("Testes");
        alert.setHeaderText(res.ok() ? "OK" : "Falhou");

        TextArea area = new TextArea(res.report());
        area.setEditable(false);
        area.setWrapText(true);
        area.setPrefRowCount(16);
        area.setStyle("-fx-font-family: " + Theme.FONT_MONO + "; -fx-font-size: 11px;");
        alert.getDialogPane().setContent(area);

        alert.showAndWait();
    }
}
