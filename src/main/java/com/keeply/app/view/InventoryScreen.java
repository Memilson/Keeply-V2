package com.keeply.app.view;

import com.keeply.app.Database.FileHistoryRow;
import com.keeply.app.Database.InventoryRow;
import com.keeply.app.Database.ScanSummary;
import com.keeply.app.view.KeeplyTemplate.Theme;
import javafx.beans.property.SimpleStringProperty;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.shape.SVGPath;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public final class InventoryScreen {

    private static final String ICON_FOLDER = "M20 6h-8l-2-2H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm0 12H4V8h16v10z";
    private static final String ICON_FILE = "M6 2h9l5 5v13c0 1.1-.9 2-2 2H6c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2zm8 7h5l-5-5v5z";

    private final Button btnRefresh = new Button("REFRESH");
    private final Button btnExpand = new Button("EXPAND");
    private final Button btnCollapse = new Button("COLLAPSE");
    private final Label metaLabel = new Label("Nenhum scan executado ainda.");
    private final Label countLabel = new Label("0 arquivos inventariados.");
    private final Label errorLabel = new Label();
    private final ProgressIndicator loading = new ProgressIndicator();

    private final TreeTableView<FileNode> tree = new TreeTableView<>();

    // Timeline
    private final TableView<HistoryRow> historyTable = new TableView<>();
    private final Label historyHeader = new Label("TIMELINE DE ARQUIVO");
    private final Label historySub = new Label("Selecione um arquivo para ver as versões.");
    private final ProgressIndicator historyLoading = new ProgressIndicator();

    private Consumer<String> onFileSelected;

    public Node content() {
        configureTree();
        configureHistory();

        VBox layout = new VBox(12);
        layout.setAlignment(Pos.TOP_LEFT);
        layout.setPadding(new Insets(4, 0, 4, 0));

        Label title = new Label("INVENTÁRIO DE ARQUIVOS");
        title.setFont(Font.font("Segoe UI", FontWeight.BOLD, 10));
        title.setTextFill(Color.web(Theme.TEXT_MUTED));

        Label subtitle = new Label("Visualize o inventário atual em formato de pastas.");
        subtitle.setFont(Font.font("Segoe UI", FontWeight.NORMAL, 12));
        subtitle.setTextFill(Color.web(Theme.TEXT_MAIN));

        VBox header = new VBox(2, title, subtitle);

        metaLabel.setTextFill(Color.web(Theme.TEXT_MUTED));
        metaLabel.setFont(Font.font("Segoe UI", FontWeight.NORMAL, 11));

        countLabel.setTextFill(Color.web(Theme.TEXT_MAIN));
        countLabel.setFont(Font.font("Segoe UI", FontWeight.BOLD, 12));

        errorLabel.setTextFill(Color.web(Theme.DANGER));
        errorLabel.setFont(Font.font("Segoe UI", FontWeight.NORMAL, 11));
        errorLabel.setVisible(false);

        HBox statsRow = new HBox(12, metaLabel, countLabel);
        statsRow.setAlignment(Pos.CENTER_LEFT);
        statsRow.setPadding(new Insets(4, 0, 4, 0));

        StackPane treeWrapper = new StackPane(tree, loading);
        StackPane.setMargin(tree, new Insets(4, 0, 0, 0));
        loading.setVisible(false);
        loading.setMaxSize(48, 48);

        VBox treeCard = createCard(
                header,
                statsRow,
                errorLabel,
                treeWrapper
        );

        VBox.setVgrow(treeCard, Priority.ALWAYS);
        VBox.setVgrow(treeWrapper, Priority.ALWAYS);
        VBox.setVgrow(tree, Priority.ALWAYS);

        VBox historyCard = createCard(
                historyHeader,
                historySub,
                createHistoryWrapper()
        );
        VBox.setVgrow(historyCard, Priority.SOMETIMES);

        layout.getChildren().addAll(treeCard, historyCard);
        return layout;
    }

    public Node footer() {
        HBox actions = new HBox(10);
        actions.setAlignment(Pos.CENTER_RIGHT);
        actions.setPadding(new Insets(4, 0, 0, 0));

        styleButton(btnCollapse, Theme.BG_SECONDARY, Theme.TEXT_MAIN, true);
        styleButton(btnExpand, Theme.BG_SECONDARY, Theme.TEXT_MAIN, true);
        styleButton(btnRefresh, Theme.ACCENT, Theme.TEXT_INVERT, false);

        btnRefresh.setMinWidth(110);

        actions.getChildren().addAll(btnCollapse, btnExpand, btnRefresh);
        return actions;
    }

    public Button refreshButton() { return btnRefresh; }
    public Button expandButton() { return btnExpand; }
    public Button collapseButton() { return btnCollapse; }

    public void onFileSelected(Consumer<String> consumer) {
        this.onFileSelected = consumer;
    }

    public void showLoading(boolean value) {
        loading.setVisible(value);
        tree.setDisable(value);
        btnRefresh.setDisable(value);
        btnExpand.setDisable(value);
        btnCollapse.setDisable(value);
    }

    public void showError(String message) {
        errorLabel.setText(message);
        errorLabel.setVisible(true);
    }

    public void clearError() {
        errorLabel.setVisible(false);
        errorLabel.setText("");
    }

    public void renderTree(List<InventoryRow> rows, ScanSummary scan) {
        clearError();
        if (rows == null || rows.isEmpty()) {
            tree.setRoot(null);
            tree.setPlaceholder(new Label("Nenhum arquivo inventariado. Rode um scan primeiro."));
            metaLabel.setText("Inventário vazio.");
            countLabel.setText("0 arquivos.");
            return;
        }

        String rootLabel = (scan != null && scan.rootPath() != null && !scan.rootPath().isBlank())
                ? scan.rootPath()
                : "Inventário";

        TreeItem<FileNode> root = new TreeItem<>(new FileNode(rootLabel, "", true, 0, "", "", 0));
        root.setExpanded(true);

        Map<String, TreeItem<FileNode>> folderCache = new HashMap<>();
        folderCache.put("", root);

        for (InventoryRow row : rows) {
            String[] segments = row.pathRel().split("/");
            StringBuilder pathBuilder = new StringBuilder();
            TreeItem<FileNode> parent = root;

            for (int i = 0; i < segments.length - 1; i++) {
                if (pathBuilder.length() > 0) pathBuilder.append('/');
                pathBuilder.append(segments[i]);
                String folderKey = pathBuilder.toString();
                TreeItem<FileNode> folder = folderCache.get(folderKey);
                if (folder == null) {
                    folder = new TreeItem<>(new FileNode(segments[i], folderKey, true, 0, "", "", 0));
                    folderCache.put(folderKey, folder);
                    addOrdered(parent.getChildren(), folder);
                }
                parent = folder;
            }

            String fileName = segments[segments.length - 1];
            TreeItem<FileNode> fileItem = new TreeItem<>(new FileNode(
                    fileName,
                    row.pathRel(),
                    false,
                    row.sizeBytes(),
                    row.status(),
                    row.hashHex(),
                    row.modifiedMillis()
            ));
            addOrdered(parent.getChildren(), fileItem);
        }

        tree.setRoot(root);

        countLabel.setText(rows.size() + " arquivos inventariados.");
        metaLabel.setText(buildMeta(scan, rows.size()));
    }

    public void expandAll() { walk(tree.getRoot(), true); }
    public void collapseAll() { walk(tree.getRoot(), false); }

    public void showHistoryLoading(boolean value) {
        historyLoading.setVisible(value);
        historyTable.setDisable(value);
    }

    public void renderHistory(List<FileHistoryRow> rows, String pathRel) {
        historyTable.getItems().clear();
        if (rows == null || rows.isEmpty()) {
            historySub.setText(pathRel == null ? "Selecione um arquivo para ver as versões." : "Sem versões registradas para " + pathRel);
            return;
        }
        historySub.setText("Histórico de: " + pathRel);
        List<HistoryRow> items = new ArrayList<>();
        for (FileHistoryRow r : rows) {
            items.add(new HistoryRow(
                    r.scanId(),
                    r.rootPath(),
                    r.startedAt(),
                    r.finishedAt(),
                    r.hashHex(),
                    r.sizeBytes(),
                    r.statusEvent(),
                    r.createdAt()
            ));
        }
        historyTable.getItems().setAll(items);
    }

    private void walk(TreeItem<FileNode> item, boolean expand) {
        if (item == null) return;
        item.setExpanded(expand);
        for (TreeItem<FileNode> child : item.getChildren()) walk(child, expand);
    }

    private void configureTree() {
        tree.setShowRoot(true);
        tree.setEditable(false);
        tree.setColumnResizePolicy(TreeTableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
        tree.setPlaceholder(new Label("Carregando inventário..."));
        tree.setStyle("""
            -fx-background-color: %s;
            -fx-border-color: %s;
            -fx-border-radius: 10;
            -fx-background-radius: 10;
        """.formatted(Theme.BG_PRIMARY, Theme.BORDER));

        TreeTableColumn<FileNode, String> nameCol = new TreeTableColumn<>("Nome / Pasta");
        nameCol.setPrefWidth(260);
        nameCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getValue().name()));
        nameCol.setCellFactory(col -> new TreeTableCell<>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(null); setGraphic(null); return; }

                var treeItem = getTreeTableView().getTreeItem(getIndex());
                FileNode node = (treeItem != null) ? treeItem.getValue() : null;
                if (node == null) { setText(null); setGraphic(null); return; }

                Label text = new Label(item);
                text.setTextFill(Color.web(Theme.TEXT_MAIN));
                text.setFont(Font.font("Segoe UI", node.directory() ? FontWeight.BOLD : FontWeight.NORMAL, 12));

                SVGPath icon = new SVGPath();
                icon.setContent(node.directory() ? ICON_FOLDER : ICON_FILE);
                icon.setFill(Color.web(node.directory() ? Theme.ACCENT : "#9CA3AF"));
                icon.setScaleX(0.55);
                icon.setScaleY(0.55);

                HBox wrapper = new HBox(8, icon, text);
                wrapper.setAlignment(Pos.CENTER_LEFT);
                setGraphic(wrapper);
                setText(null);
            }
        });

        TreeTableColumn<FileNode, String> statusCol = new TreeTableColumn<>("Status");
        statusCol.setPrefWidth(90);
        statusCol.setCellValueFactory(data -> new SimpleStringProperty(Optional.ofNullable(data.getValue().getValue().status()).orElse("-")));
        statusCol.setCellFactory(col -> new TreeTableCell<>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setGraphic(null); setText(null); return; }
                Label pill = new Label(item);
                pill.setFont(Font.font("Segoe UI", FontWeight.BOLD, 11));
                pill.setPadding(new Insets(4, 8, 4, 8));
                pill.setTextFill(Color.web("#0F172A"));
                pill.setStyle("-fx-background-radius: 999;");
                pill.setBackground(new Background(new BackgroundFill(Color.web(statusColor(item)), new CornerRadii(999), Insets.EMPTY)));
                setGraphic(pill);
                setText(null);
            }
        });

        TreeTableColumn<FileNode, String> sizeCol = new TreeTableColumn<>("Tamanho");
        sizeCol.setPrefWidth(100);
        sizeCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getValue().directory() ? "-" : humanSize(data.getValue().getValue().sizeBytes())));

        TreeTableColumn<FileNode, String> modCol = new TreeTableColumn<>("Modificado");
        modCol.setPrefWidth(150);
        modCol.setCellValueFactory(data -> new SimpleStringProperty(formatInstant(data.getValue().getValue().modifiedMillis())));

        TreeTableColumn<FileNode, String> hashCol = new TreeTableColumn<>("Hash");
        hashCol.setPrefWidth(160);
        hashCol.setCellValueFactory(data -> {
            FileNode n = data.getValue().getValue();
            if (n.directory()) return new SimpleStringProperty("-");
            String h = n.hash() == null ? "-" : abbreviate(n.hash(), 16);
            return new SimpleStringProperty(h);
        });

        tree.getColumns().setAll(nameCol, statusCol, sizeCol, modCol, hashCol);

        tree.getSelectionModel().selectedItemProperty().addListener((obs, oldSel, newSel) -> {
            if (onFileSelected == null) return;
            if (newSel == null || newSel.getValue() == null || newSel.getValue().directory()) {
                onFileSelected.accept(null);
            } else {
                onFileSelected.accept(newSel.getValue().path());
            }
        });
    }

    private void configureHistory() {
        historyTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
        historyTable.setPlaceholder(new Label("Selecione um arquivo para ver o histórico."));
        historyTable.setStyle("""
            -fx-background-color: %s;
            -fx-border-color: %s;
            -fx-border-radius: 8;
            -fx-background-radius: 8;
        """.formatted(Theme.BG_PRIMARY, Theme.BORDER));

        TableColumn<HistoryRow, String> scanCol = new TableColumn<>("#Scan");
        scanCol.setPrefWidth(60);
        scanCol.setCellValueFactory(data -> new SimpleStringProperty(Long.toString(data.getValue().scanId())));

        TableColumn<HistoryRow, String> dateCol = new TableColumn<>("Data (started)");
        dateCol.setPrefWidth(150);
        dateCol.setCellValueFactory(data -> new SimpleStringProperty(orDash(data.getValue().startedAt())));

        TableColumn<HistoryRow, String> statusCol = new TableColumn<>("Evento");
        statusCol.setPrefWidth(100);
        statusCol.setCellValueFactory(data -> new SimpleStringProperty(orDash(data.getValue().status())));

        TableColumn<HistoryRow, String> sizeCol = new TableColumn<>("Tamanho");
        sizeCol.setPrefWidth(100);
        sizeCol.setCellValueFactory(data -> new SimpleStringProperty(humanSize(data.getValue().sizeBytes())));

        TableColumn<HistoryRow, String> hashCol = new TableColumn<>("Hash");
        hashCol.setPrefWidth(180);
        hashCol.setCellValueFactory(data -> new SimpleStringProperty(abbreviate(data.getValue().hash(), 24)));

        historyTable.getColumns().setAll(scanCol, dateCol, statusCol, sizeCol, hashCol);
    }

    private Node createHistoryWrapper() {
        historyLoading.setVisible(false);
        historyLoading.setMaxSize(36, 36);
        StackPane wrapper = new StackPane(historyTable, historyLoading);
        VBox.setVgrow(wrapper, Priority.ALWAYS);
        return wrapper;
    }

    private void addOrdered(List<TreeItem<FileNode>> siblings, TreeItem<FileNode> child) {
        siblings.add(child);
        siblings.sort(Comparator
                .comparing((TreeItem<FileNode> n) -> n.getValue().directory() ? 0 : 1)
                .thenComparing(n -> n.getValue().name().toLowerCase()));
    }

    private VBox createCard(Node... children) {
        VBox card = new VBox(10, children);
        card.setPadding(new Insets(16));
        card.setStyle("""
            -fx-background-color: %s;
            -fx-background-radius: 12;
            -fx-border-radius: 12;
            -fx-border-color: %s;
            -fx-border-width: 1;
            -fx-effect: dropshadow(three-pass-box, rgba(0,0,0,0.06), 10, 0, 0, 4);
        """.formatted(Theme.BG_PRIMARY, Theme.BORDER));
        return card;
    }

    private void styleButton(Button btn, String bgHex, String textHex, boolean outline) {
        String borderStyle = outline ? "-fx-border-color: " + Theme.BORDER + "; -fx-border-width: 1; -fx-border-radius: 6;" : "";
        String baseStyle = """
            -fx-background-color: %s;
            -fx-text-fill: %s;
            -fx-font-family: 'Segoe UI';
            -fx-font-weight: bold;
            -fx-font-size: 12px;
            -fx-padding: 8 16;
            -fx-background-radius: 6;
            -fx-cursor: hand;
            %s
        """.formatted(bgHex, textHex, borderStyle);

        btn.setStyle(baseStyle);
        btn.setOnMouseEntered(e -> btn.setOpacity(0.86));
        btn.setOnMouseExited(e -> btn.setOpacity(1.0));
    }

    private String buildMeta(ScanSummary scan, int total) {
        if (scan == null) return "Inventário pronto. Total: " + total + " arquivos.";
        String started = scan.startedAt() != null ? scan.startedAt() : "-";
        String finished = scan.finishedAt() != null ? scan.finishedAt() : "-";
        String root = (scan.rootPath() != null && !scan.rootPath().isBlank()) ? scan.rootPath() : "-";
        return "Último scan #" + scan.scanId() + " | " + started + " → " + finished + " | Raiz: " + root;
    }

    private String humanSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        double v = bytes;
        String[] units = {"KB","MB","GB","TB"};
        int idx = 0;
        while (v >= 1024 && idx < units.length - 1) { v /= 1024; idx++; }
        return "%.1f %s".formatted(v, units[idx]);
    }

    private String formatInstant(long millis) {
        if (millis <= 0) return "-";
        var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").withZone(ZoneId.systemDefault());
        return formatter.format(Instant.ofEpochMilli(millis));
    }

    private String abbreviate(String value, int max) {
        if (value == null || value.isBlank()) return "-";
        if (value.length() <= max) return value;
        return value.substring(0, max - 3) + "...";
    }

    private String statusColor(String status) {
        if (status == null) return "#E4E4E7";
        return switch (status.toUpperCase()) {
            case "NEW" -> Theme.ACCENT;
            case "MODIFIED" -> "#F59E0B";
            case "SYNCED", "SYNCHED", "HASHED", "STABLE" -> Theme.SUCCESS;
            default -> "#E4E4E7";
        };
    }

    private String orDash(String v) {
        return (v == null || v.isBlank()) ? "-" : v;
    }

    public record FileNode(String name, String path, boolean directory, long sizeBytes, String status, String hash, long modifiedMillis) {}
    public record HistoryRow(long scanId, String rootPath, String startedAt, String finishedAt, String hash, long sizeBytes, String status, String createdAt) {}
}
