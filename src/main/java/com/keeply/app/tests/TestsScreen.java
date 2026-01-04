package com.keeply.app.tests;

import com.keeply.app.database.DatabaseBackup;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.TextArea;
import javafx.scene.layout.VBox;

import java.util.Objects;

public final class TestsScreen {

    private final Button btnRun = new Button("Rodar testes básicos");

    public TestsScreen() {
    }

    public Node content() {
        VBox root = new VBox(16);
        root.setAlignment(Pos.TOP_LEFT);
        root.setPadding(new Insets(12, 0, 0, 0));

        btnRun.setMinWidth(220);
        btnRun.getStyleClass().addAll("btn", "btn-primary");
        btnRun.setOnAction(e -> run());

        root.getChildren().add(btnRun);
        return root;
    }

    private void run() {
        DatabaseBackup.SelfTestResult res = DatabaseBackup.runBasicSelfTests();

        Alert alert = new Alert(res.ok() ? Alert.AlertType.INFORMATION : Alert.AlertType.ERROR);
        alert.setTitle("Testes");
        alert.setHeaderText(res.ok() ? "OK" : "Falhou");

        TextArea area = new TextArea(res.report());
        area.setEditable(false);
        area.setWrapText(true);
        area.setPrefRowCount(16);
        area.getStyleClass().addAll("text-input", "mono-area");
        alert.getDialogPane().setContent(area);

        try {
            alert.getDialogPane().getStylesheets().add(Objects.requireNonNull(
                    getClass().getResource("/styles.css"),
                    "Missing /styles.css"
            ).toExternalForm());
        } catch (Exception ignored) {
            // best-effort: dialog will still show
        }

        alert.showAndWait();
    }
}
