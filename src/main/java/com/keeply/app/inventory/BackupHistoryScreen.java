package com.keeply.app.inventory;

import java.util.List;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.SelectionMode;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
public final class BackupHistoryScreen {

    private final ListView<BackupHistoryDb.HistoryRow> historyList = new ListView<>();
    private final Label detailTitle = new Label("Atividades");
    private final Label detailSubtitle = new Label("Histórico de ações e execuções.");
    private final Label detailsTitle = new Label("Detalhes");
    private final Label detailsContent = new Label();
    private final Label errorLabel = new Label();
    private final Button btnRefresh = new Button("Atualizar");

    private List<BackupHistoryDb.HistoryRow> allHistory = List.of();

    public Node content() {
        VBox root = new VBox(14);
        root.getStyleClass().add("backup-history-screen");
        root.setPadding(new Insets(8, 0, 0, 0));

        VBox content = new VBox(16, buildHistoryPane());
        content.getStyleClass().add("content-wrap");
        content.setMaxWidth(1100);

        root.getChildren().add(content);
        loadHistory();
        return root;
    }

    private VBox buildHistoryPane() {
        detailTitle.getStyleClass().add("section-title");
        detailSubtitle.getStyleClass().add("page-subtitle");

        btnRefresh.getStyleClass().addAll("btn", "btn-outline");
        btnRefresh.setOnAction(e -> loadHistory());

        HBox header = new HBox(10, detailTitle, spacer(), btnRefresh);
        header.setAlignment(Pos.CENTER_LEFT);

        historyList.getStyleClass().add("history-list");
        historyList.setCellFactory(list -> new HistoryCell());
        historyList.getSelectionModel().setSelectionMode(SelectionMode.SINGLE);
        historyList.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> renderDetails(newVal));

        detailsTitle.getStyleClass().add("section-title");
        detailsContent.getStyleClass().add("muted");
        detailsContent.setWrapText(true);

        errorLabel.getStyleClass().add("error-banner");
        errorLabel.setVisible(false);
        errorLabel.setManaged(false);

        VBox detailsBox = new VBox(6, detailsTitle, detailsContent);
        detailsBox.getStyleClass().add("details-box");

        VBox card = new VBox(12, header, detailSubtitle, historyList, errorLabel, detailsBox);
        card.getStyleClass().add("card");
        card.setPadding(new Insets(14));
        VBox.setVgrow(historyList, Priority.ALWAYS);

        VBox right = new VBox(12, card);
        VBox.setVgrow(card, Priority.ALWAYS);
        return right;
    }

    public Node rootNode() { return historyList; }

    public void showError(String msg) {
        errorLabel.setText(msg);
        errorLabel.setVisible(true);
        errorLabel.setManaged(true);
    }

    private void loadHistory() {
        allHistory = BackupHistoryDb.listRecent(200);
        historyList.getItems().setAll(allHistory);
        if (!allHistory.isEmpty()) {
            historyList.getSelectionModel().select(0);
        } else {
            renderDetails(null);
        }
    }

    private void renderDetails(BackupHistoryDb.HistoryRow row) {
        if (row == null) {
            detailsContent.setText("Nenhuma atividade selecionada.");
            return;
        }
        StringBuilder sb = new StringBuilder();
        String type = row.backupType() == null
                ? "Backup"
                : (row.backupType().equalsIgnoreCase("FULL") ? "Backup completo" : "Backup incremental");
        sb.append("Tipo: ").append(type).append('\n');
        sb.append("Status: ").append(row.status()).append('\n');
        sb.append("Início: ").append(row.startedAt()).append('\n');
        sb.append("Fim: ").append(row.finishedAt() == null ? "-" : row.finishedAt()).append('\n');
        sb.append("Origem: ").append(row.rootPath() == null ? "-" : row.rootPath()).append('\n');
        sb.append("Destino: ").append(row.destPath() == null ? "-" : row.destPath()).append('\n');
        sb.append("Arquivos: ").append(row.filesProcessed()).append('\n');
        sb.append("Erros: ").append(row.errors()).append('\n');
        if (row.message() != null && !row.message().isBlank()) {
            sb.append("Detalhes: ").append(row.message()).append('\n');
        }
        detailsContent.setText(sb.toString());
    }

    private static Region spacer() {
        Region r = new Region();
        HBox.setHgrow(r, Priority.ALWAYS);
        return r;
    }

    private static final class HistoryCell extends ListCell<BackupHistoryDb.HistoryRow> {
        @Override
        protected void updateItem(BackupHistoryDb.HistoryRow item, boolean empty) {
            super.updateItem(item, empty);
            if (empty || item == null) { setText(null); setGraphic(null); return; }
            String type = item.backupType() == null
                    ? "Backup"
                    : (item.backupType().equalsIgnoreCase("FULL") ? "Backup completo" : "Backup incremental");
            String ts = item.finishedAt() != null ? item.finishedAt() : item.startedAt();
            Label title = new Label(type + " - " + ts);
            title.getStyleClass().add("history-title");

            Label sub = new Label(item.rootPath() == null ? "-" : item.rootPath());
            sub.getStyleClass().add("muted");

            VBox box = new VBox(2, title, sub);
            setGraphic(box);
        }
    }
}
