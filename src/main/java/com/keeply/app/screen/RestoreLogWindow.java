package com.keeply.app.screen;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.Window;

public final class RestoreLogWindow {

    private final TextArea logArea;
    private final Button cancelButton;
    private final Button closeButton;
    private final AtomicBoolean cancel = new AtomicBoolean(false);

    private RestoreLogWindow(TextArea logArea, Button cancelButton, Button closeButton) {
        this.logArea = logArea;
        this.cancelButton = cancelButton;
        this.closeButton = closeButton;
    }

    public static RestoreLogWindow open(Window owner, String title) {
        Stage stage = new Stage();
        if (owner != null) stage.initOwner(owner);
        stage.initModality(Modality.NONE);
        stage.setTitle(title);

        Label header = new Label(title);
        header.getStyleClass().add("header-title");

        TextArea area = new TextArea();
        area.setEditable(false);
        area.setWrapText(true);
        VBox.setVgrow(area, Priority.ALWAYS);

        Button cancel = new Button("Cancelar");
        cancel.getStyleClass().add("button-secondary");

        Button close = new Button("Fechar");
        close.getStyleClass().add("button-action");
        close.setDisable(true);

        HBox actions = new HBox(10, cancel, close);
        actions.setAlignment(Pos.CENTER_RIGHT);

        VBox content = new VBox(10, header, area, actions);
        content.setPadding(new Insets(12));
        VBox.setVgrow(area, Priority.ALWAYS);

        BorderPane root = new BorderPane(content);
        Scene scene = new Scene(root, 760, 520);
        stage.setScene(scene);

        RestoreLogWindow window = new RestoreLogWindow(area, cancel, close);

        cancel.setOnAction(e -> {
            window.cancel.set(true);
            window.cancelButton.setDisable(true);
            window.appendLine(">> Cancelamento solicitado... aguardando.");
        });
        close.setOnAction(e -> stage.close());

        stage.show();
        return window;
    }

    public AtomicBoolean cancelFlag() {
        return cancel;
    }

    public Consumer<String> logger() {
        return this::appendLine;
    }

    public void appendLine(String line) {
        if (line == null) return;
        Platform.runLater(() -> {
            if (!logArea.getText().isEmpty()) {
                logArea.appendText(System.lineSeparator());
            }
            logArea.appendText(line);
            logArea.positionCaret(logArea.getLength());
        });
    }

    public void markDoneOk(String summaryLine) {
        if (summaryLine != null && !summaryLine.isBlank()) {
            appendLine(summaryLine);
        }
        Platform.runLater(() -> {
            cancelButton.setDisable(true);
            closeButton.setDisable(false);
        });
    }

    public void markDoneError(String summaryLine) {
        if (summaryLine != null && !summaryLine.isBlank()) {
            appendLine(summaryLine);
        }
        Platform.runLater(() -> {
            cancelButton.setDisable(true);
            closeButton.setDisable(false);
        });
    }
}
