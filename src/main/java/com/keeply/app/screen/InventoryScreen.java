package com.keeply.app.screen;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.keeply.app.database.DatabaseBackup.FileHistoryRow;
import com.keeply.app.database.DatabaseBackup.InventoryRow;
import com.keeply.app.database.DatabaseBackup.ScanSummary;
import com.keeply.app.inventory.BackupHistoryDb;

import javafx.beans.binding.Bindings;
import javafx.beans.property.SimpleStringProperty;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ContentDisplay;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.MenuItem;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.SelectionMode;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeTableCell;
import javafx.scene.control.TreeTableColumn;
import javafx.scene.control.TreeTableRow;
import javafx.scene.control.TreeTableView;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.shape.SVGPath;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.Window;

public final class InventoryScreen {

    private static final String SVG_FOLDER = "M20 6h-8l-2-2H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm0 12H4V8h16v10z";
    private static final String SVG_FILE = "M14 2H6c-1.1 0-1.99.9-1.99 2L4 20c0 1.1.89 2 1.99 2H18c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z";

    private final Button btnRefresh = new Button("Recarregar");
    private final Button btnRestoreSnapshot = new Button("Restaurar backup");
    private final ListView<BackupHistoryDb.HistoryRow> scanList = new ListView<>();

    private final Label metaLabel = new Label("Aguardando dados...");
    private final Label errorLabel = new Label();
    private final ProgressIndicator loading = new ProgressIndicator();

    private final TreeTableView<FileNode> tree = new TreeTableView<>();

    private Consumer<String> onFileSelected;
    private Consumer<String> onShowHistory;
    private Runnable onRestoreSelected;

    public Node content() {
        configureTree();

        Label title = new Label("Armazenamento");
        title.getStyleClass().add("page-title");

        Label subtitle = new Label("Auditoria e estrutura do conteúdo protegido.");
        subtitle.getStyleClass().add("page-subtitle");

        metaLabel.getStyleClass().add("header-subtitle");
        metaLabel.setWrapText(true);

        loading.setMaxSize(22, 22);
        loading.setVisible(false);
        loading.setManaged(false);

        HBox metaRow = new HBox(8, metaLabel, loading);
        metaRow.setAlignment(Pos.CENTER_LEFT);

        VBox headerBox = new VBox(4, title, subtitle, metaRow);
        headerBox.setFillWidth(true);

        scanList.getStyleClass().add("history-list");
        scanList.setCellFactory(p -> new ScanListCell());
        scanList.getSelectionModel().setSelectionMode(SelectionMode.SINGLE);
        scanList.setPlaceholder(new Label("Nenhum backup encontrado."));
        scanList.setFocusTraversable(false);
        VBox.setVgrow(scanList, Priority.ALWAYS);

        btnRefresh.getStyleClass().addAll("btn", "btn-outline");
        btnRestoreSnapshot.getStyleClass().addAll("btn", "btn-primary");
        lockButtonWidth(btnRefresh);
        lockButtonWidth(btnRestoreSnapshot);

        FlowPane actionsRow = new FlowPane(Orientation.HORIZONTAL, 10, 10);
        actionsRow.setAlignment(Pos.CENTER_LEFT);
        actionsRow.getChildren().addAll(btnRestoreSnapshot, btnRefresh);
        actionsRow.setMaxWidth(Double.MAX_VALUE);

        Label listTitle = new Label("Histórico de backups");
        listTitle.getStyleClass().add("section-title");

        VBox listBox = new VBox(8, listTitle, scanList);
        listBox.setFillWidth(true);
        VBox.setVgrow(listBox, Priority.ALWAYS);

        VBox toolbar = new VBox(10, listBox, actionsRow);
        toolbar.getStyleClass().add("toolbar-container");
        toolbar.setFillWidth(true);
        VBox.setVgrow(toolbar, Priority.ALWAYS);

        errorLabel.getStyleClass().add("error-banner");
        errorLabel.setWrapText(true);
        errorLabel.setVisible(false);
        errorLabel.setManaged(false);

        VBox card = new VBox(14, headerBox, toolbar, errorLabel);
        card.getStyleClass().add("card");
        card.setFillWidth(true);
        VBox.setVgrow(card, Priority.ALWAYS);

        StackPane loadingOverlay = new StackPane(new ProgressIndicator());
        loadingOverlay.setVisible(false);
        loadingOverlay.setManaged(false);
        loadingOverlay.setPickOnBounds(true);
        loadingOverlay.setStyle("-fx-background-color: rgba(0,0,0,0.04); -fx-background-radius: 14;");
        ((ProgressIndicator) loadingOverlay.getChildren().get(0)).setMaxSize(42, 42);

        StackPane cardWrap = new StackPane(card, loadingOverlay);
        cardWrap.setMaxWidth(Double.MAX_VALUE);

        VBox content = new VBox(16, cardWrap);
        content.getStyleClass().add("content-wrap");
        content.setFillWidth(true);
        content.setMaxWidth(980);

        HBox center = new HBox(content);
        center.setAlignment(Pos.TOP_CENTER);
        center.setFillHeight(true);
        HBox.setHgrow(content, Priority.ALWAYS);
        center.setPadding(new Insets(8, 0, 0, 0));

        VBox page = new VBox(center);
        page.setFillWidth(true);
        VBox.setVgrow(center, Priority.ALWAYS);

        loadingOverlay.visibleProperty().bind(loading.visibleProperty());
        loadingOverlay.managedProperty().bind(loading.managedProperty());

        errorLabel.maxWidthProperty().bind(content.widthProperty().subtract(28));
        scanList.prefWidthProperty().bind(content.widthProperty().subtract(28));

        return page;
    }

    private void configureTree() {
        tree.setShowRoot(false);
        tree.setColumnResizePolicy(TreeTableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
        tree.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
        tree.getStyleClass().add("tree-table");
        tree.setPlaceholder(new Label("Selecione um backup para visualizar os arquivos."));

        TreeTableColumn<FileNode, String> colName = new TreeTableColumn<>("Nome");
        colName.setPrefWidth(380);
        colName.setCellValueFactory(p -> new SimpleStringProperty(p.getValue().getValue().name()));
        colName.setCellFactory(c -> new TreeTableCell<>() {
            @Override protected void updateItem(String item, boolean e) {
                super.updateItem(item, e);
                if (e || item == null) { setGraphic(null); setText(null); return; }

                TreeTableView<FileNode> table = getTreeTableView();
                TreeItem<FileNode> treeItem = (table != null) ? table.getTreeItem(getIndex()) : null;
                FileNode n = (treeItem != null) ? treeItem.getValue() : null;
                if (n == null) { setGraphic(null); setText(null); return; }

                SVGPath icon = new SVGPath();
                icon.setContent(n.directory ? SVG_FOLDER : SVG_FILE);
                icon.getStyleClass().add(n.directory ? "icon-folder" : "icon-file");
                icon.setScaleX(0.9);
                icon.setScaleY(0.9);

                Label text = new Label(item);
                text.setMaxWidth(Double.MAX_VALUE);
                text.setWrapText(false);
                HBox.setHgrow(text, Priority.ALWAYS);

                HBox box = new HBox(8, icon, text);
                box.setAlignment(Pos.CENTER_LEFT);
                setGraphic(box);
                setText(null);
            }
        });

        TreeTableColumn<FileNode, String> colStatus = new TreeTableColumn<>("Status");
        colStatus.setPrefWidth(120);
        colStatus.setMaxWidth(140);
        colStatus.setCellValueFactory(p -> new SimpleStringProperty(p.getValue().getValue().status()));
        colStatus.setCellFactory(c -> new TreeTableCell<>() {
            @Override protected void updateItem(String item, boolean e) {
                super.updateItem(item, e);
                if (e || item == null) { setGraphic(null); setText(null); return; }

                String v = item.toUpperCase();
                Label badge = new Label(v);
                badge.getStyleClass().add("status-pill");
                badge.setMinWidth(Region.USE_PREF_SIZE);

                switch (v) {
                    case "NEW", "CREATED" -> badge.getStyleClass().add("status-new");
                    case "MODIFIED" -> badge.getStyleClass().add("status-modified");
                    case "DELETED" -> badge.getStyleClass().add("status-deleted");
                    case "SYNCED", "STABLE" -> badge.getStyleClass().add("status-synced");
                    default -> badge.getStyleClass().add("status-default");
                }
                setGraphic(badge);
                setText(null);
                setAlignment(Pos.CENTER);
            }
        });

        tree.getColumns().setAll(List.of(colName, colStatus));

        tree.setRowFactory(tv -> {
            TreeTableRow<FileNode> row = new TreeTableRow<>();
            row.setOnContextMenuRequested(e -> {
                if (row.isEmpty()) return;
                if (!tree.getSelectionModel().getSelectedIndices().contains(row.getIndex())) {
                    tree.getSelectionModel().clearSelection();
                    tree.getSelectionModel().select(row.getIndex());
                }
                ContextMenu menu = new ContextMenu();
                MenuItem restore = new MenuItem("Restaurar selecionados");
                restore.disableProperty().bind(Bindings.isEmpty(tree.getSelectionModel().getSelectedItems()));
                restore.setOnAction(ev -> { if (onRestoreSelected != null) onRestoreSelected.run(); });
                menu.getItems().add(restore);
                if (!row.getItem().directory) {
                    MenuItem history = new MenuItem("Ver Histórico de Versões");
                    history.setOnAction(ev -> { if (onShowHistory != null) onShowHistory.accept(row.getItem().path); });
                    menu.getItems().add(history);
                }
                menu.show(row, e.getScreenX(), e.getScreenY());
            });
            return row;
        });

        tree.getSelectionModel().selectedItemProperty().addListener((o, old, val) -> {
            if (onFileSelected != null) onFileSelected.accept(val != null && !val.getValue().directory ? val.getValue().path : null);
        });
    }

    public void renderTree(List<InventoryRow> rows, ScanSummary scan) {
        if (rows == null || rows.isEmpty()) {
            tree.setRoot(null);
            tree.setPlaceholder(new Label("Selecione um backup para visualizar os arquivos."));
            return;
        }
        TreeItem<FileNode> root = new TreeItem<>(new FileNode("root", "", true, 0, "SYNCED", 0));
        root.setExpanded(true);

        Map<String, TreeItem<FileNode>> index = new HashMap<>();
        index.put("", root);

        for (InventoryRow row : rows) {
            if (row.pathRel() == null || row.pathRel().isBlank()) continue;

            String rel = row.pathRel().replace('\\', '/');
            String[] parts = rel.split("/");
            if (parts.length == 0) continue;

            StringBuilder pathSoFar = new StringBuilder();
            TreeItem<FileNode> parent = root;

            for (int i = 0; i < parts.length; i++) {
                String part = parts[i];
                if (part.isBlank()) continue;

                if (pathSoFar.length() > 0) pathSoFar.append('/');
                pathSoFar.append(part);

                String key = pathSoFar.toString();
                boolean isFile = (i == parts.length - 1);

                TreeItem<FileNode> current = index.get(key);
                if (current == null) {
                    if (isFile) {
                        current = new TreeItem<>(new FileNode(part, key, false, row.sizeBytes(), row.status(), row.modifiedMillis()));
                    } else {
                        current = new TreeItem<>(new FileNode(part, key, true, 0, "SYNCED", 0));
                        current.setExpanded(false);
                    }
                    parent.getChildren().add(current);
                    index.put(key, current);
                }
                parent = current;
            }
        }

        sortTree(root);
        tree.setRoot(root);
    }

    private void sortTree(TreeItem<FileNode> node) {
        if (node == null || node.getChildren().isEmpty()) return;
        node.getChildren().sort((a, b) -> {
            FileNode na = a.getValue();
            FileNode nb = b.getValue();
            if (na == null || nb == null) return 0;
            if (na.directory != nb.directory) return na.directory ? -1 : 1;
            return na.name.compareToIgnoreCase(nb.name);
        });
        for (TreeItem<FileNode> child : node.getChildren()) sortTree(child);
    }

    public void showHistoryDialog(List<FileHistoryRow> rows, String pathRel) {
        List<FileHistoryRow> safeRows = (rows == null) ? new ArrayList<>() : new ArrayList<>(rows);
        safeRows.sort((a, b) -> Long.compare(b.scanId(), a.scanId()));

        TableView<FileHistoryRow> table = new TableView<>();
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);

        TableColumn<FileHistoryRow, String> cScan = new TableColumn<>("#Scan");
        cScan.setCellValueFactory(p -> new SimpleStringProperty(String.valueOf(p.getValue().scanId())));
        cScan.setMaxWidth(90);

        TableColumn<FileHistoryRow, String> cDate = new TableColumn<>("Data");
        cDate.setCellValueFactory(p -> new SimpleStringProperty(formatDate(p.getValue().createdAt())));

        TableColumn<FileHistoryRow, String> cEvent = new TableColumn<>("Evento");
        cEvent.setCellValueFactory(p -> new SimpleStringProperty(p.getValue().statusEvent()));

        TableColumn<FileHistoryRow, String> cSize = new TableColumn<>("Tamanho");
        cSize.setCellValueFactory(p -> new SimpleStringProperty(humanSize(p.getValue().sizeBytes())));
        cSize.setCellFactory(c -> new TableCell<>() {
            { getStyleClass().add("cell-right"); }
            @Override protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                setText(empty ? null : item);
            }
        });

        table.getColumns().setAll(List.of(cScan, cDate, cEvent, cSize));
        table.getItems().setAll(safeRows);

        Dialog<Void> dialog = new Dialog<>();
        dialog.setTitle("Histórico de Versões");
        if (pathRel != null && !pathRel.isBlank()) {
            String label = pathRel.length() > 70 ? "..." + pathRel.substring(pathRel.length() - 70) : pathRel;
            dialog.setHeaderText(label);
        } else {
            dialog.setHeaderText("Arquivo");
        }
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        dialog.getDialogPane().setContent(table);
        dialog.getDialogPane().setPrefSize(720, 440);
        dialog.show();
    }

    public Button refreshButton() { return btnRefresh; }
    public Button restoreSnapshotButton() { return btnRestoreSnapshot; }
    public ListView<BackupHistoryDb.HistoryRow> scanList() { return scanList; }
    public Node rootNode() { return scanList; }
    public void setMeta(String text) { metaLabel.setText(text == null ? "" : text); }
    public void onFileSelected(Consumer<String> c) { this.onFileSelected = c; }
    public void onShowHistory(Consumer<String> c) { this.onShowHistory = c; }
    public void onRestoreSelected(Runnable r) { this.onRestoreSelected = r; }

    public void showLoading(boolean value) {
        loading.setVisible(value);
        loading.setManaged(value);
        btnRefresh.setDisable(value);
        btnRestoreSnapshot.setDisable(value);
        scanList.setDisable(value);
    }

    public void showFilesWindow(List<InventoryRow> rows, ScanSummary scan, Consumer<List<SelectedNode>> onRestore) {
        if (rows == null) rows = List.of();
        renderTree(rows, scan);

        Label title = new Label("Arquivos do backup");
        title.getStyleClass().add("page-title");

        Label subtitle = new Label(scan == null ? "" : ("Backup #" + scan.scanId() + " • " + formatDate(scan.finishedAt())));
        subtitle.getStyleClass().add("page-subtitle");
        subtitle.setWrapText(true);

        Label selectedCount = new Label();
        selectedCount.getStyleClass().add("muted");
        selectedCount.textProperty().bind(Bindings.size(tree.getSelectionModel().getSelectedItems()).asString("Selecionados: %d"));

        VBox headerLeft = new VBox(4, title, subtitle, selectedCount);
        headerLeft.setFillWidth(true);

        Button restoreBtn = new Button("Restaurar selecionados");
        restoreBtn.getStyleClass().addAll("btn", "btn-primary");
        restoreBtn.disableProperty().bind(Bindings.isEmpty(tree.getSelectionModel().getSelectedItems()));
        restoreBtn.setOnAction(e -> {
            List<SelectedNode> selected = getSelectedNodes();
            if (selected.isEmpty()) {
                showError("Selecione arquivos/pastas (até 10) para restaurar.");
                return;
            }
            if (onRestore != null) onRestore.accept(selected);
        });

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        HBox topBar = new HBox(12, headerLeft, spacer, restoreBtn);
        topBar.setAlignment(Pos.TOP_LEFT);
        topBar.setPadding(new Insets(14, 14, 10, 14));

        StackPane treeCard = new StackPane(tree);
        treeCard.getStyleClass().add("card");
        BorderPane.setMargin(treeCard, new Insets(0, 14, 14, 14));

        BorderPane root = new BorderPane();
        root.setTop(topBar);
        root.setCenter(treeCard);

        Stage stage = new Stage();
        stage.setTitle("Arquivos do backup");
        Window owner = rootNode().getScene() != null ? rootNode().getScene().getWindow() : null;
        if (owner != null) stage.initOwner(owner);
        stage.initModality(Modality.NONE);

        Scene scene = new Scene(root, 900, 560);
        try { scene.getStylesheets().add(getClass().getResource("/styles.css").toExternalForm()); } catch (Exception ignored) {}
        stage.setScene(scene);
        stage.show();
    }

    public List<SelectedNode> getSelectedNodes() {
        var items = tree.getSelectionModel().getSelectedItems();
        if (items == null || items.isEmpty()) return List.of();
        List<SelectedNode> out = new ArrayList<>();
        for (TreeItem<FileNode> ti : items) {
            if (ti == null) continue;
            FileNode n = ti.getValue();
            if (n == null) continue;
            if (n.path == null || n.path.isBlank()) continue;
            out.add(new SelectedNode(n.path, n.directory));
        }
        return out;
    }

    public void showError(String msg) {
        errorLabel.setText(msg);
        errorLabel.setVisible(true);
        errorLabel.setManaged(true);
    }

    private String humanSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "";
        return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }

    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm");

    private static String formatIso(String dt) {
        if (dt == null || dt.isBlank()) return "-";
        try {
            Instant instant = Instant.parse(dt);
            return TS_FMT.format(instant.atZone(ZoneId.systemDefault()));
        } catch (Exception e) {
            return dt.length() > 16 ? dt.substring(0, 16) : dt;
        }
    }

    private String formatDate(String dt) { return formatIso(dt); }

    private void lockButtonWidth(Region r) {
        r.setMinWidth(Region.USE_PREF_SIZE);
        r.setMaxWidth(Region.USE_PREF_SIZE);
    }

    public record FileNode(String name, String path, boolean directory, long sizeBytes, String status, long modifiedMillis) {}
    public record SelectedNode(String pathRel, boolean directory) {}

    private class ScanListCell extends ListCell<BackupHistoryDb.HistoryRow> {
        @Override protected void updateItem(BackupHistoryDb.HistoryRow item, boolean empty) {
            super.updateItem(item, empty);
            if (empty || item == null) {
                setText(null);
                setGraphic(null);
                return;
            }

            setText(null);
            setContentDisplay(ContentDisplay.GRAPHIC_ONLY);
            setPrefWidth(0);
            setMaxWidth(Double.MAX_VALUE);

            String type = item.backupType() == null ? "Backup" : (item.backupType().equalsIgnoreCase("FULL") ? "Completo" : "Incremental");
            String tsRaw = item.finishedAt() != null ? item.finishedAt() : item.startedAt();
            String ts = formatIso(tsRaw);

            Label title = new Label(ts);
            title.getStyleClass().add("history-title");
            title.setWrapText(false);
            title.setMaxWidth(Double.MAX_VALUE);

            Label pill = new Label(type);
            pill.getStyleClass().addAll("history-pill", item.backupType() != null && item.backupType().equalsIgnoreCase("FULL") ? "pill-full" : "pill-incremental");
            pill.setMinWidth(Region.USE_PREF_SIZE);

            Label line1 = new Label("Plano: " + safeText(item.rootPath()));
            line1.getStyleClass().add("muted");
            line1.setWrapText(true);
            line1.setMaxWidth(Double.MAX_VALUE);

            Label line2 = new Label("Arquivos: " + item.filesProcessed() + " • Erros: " + item.errors());
            line2.getStyleClass().add("muted");
            line2.setWrapText(true);
            line2.setMaxWidth(Double.MAX_VALUE);

            Button restoreBtn = new Button("Recuperar arquivos/pastas");
            restoreBtn.getStyleClass().addAll("btn", "btn-primary", "btn-mini");
            restoreBtn.setOnAction(ev -> {
                getListView().getSelectionModel().select(item);
                btnRestoreSnapshot.fire();
            });

            Button actionsBtn = new Button("Ações");
            actionsBtn.getStyleClass().addAll("btn", "btn-outline", "btn-mini");
            actionsBtn.setOnAction(ev -> getListView().getSelectionModel().select(item));

            FlowPane actions = new FlowPane(8, 8, restoreBtn, actionsBtn);
            actions.setAlignment(Pos.CENTER_LEFT);

            VBox card = new VBox(6, title, line1, line2, actions);
            card.getStyleClass().add("history-card");
            card.setMaxWidth(Double.MAX_VALUE);
            VBox.setVgrow(card, Priority.NEVER);

            Region line = new Region();
            line.getStyleClass().add("history-line");
            Region dot = new Region();
            dot.getStyleClass().add("history-dot");
            VBox rail = new VBox(dot, line);
            rail.getStyleClass().add("history-rail");
            VBox.setVgrow(line, Priority.ALWAYS);

            HBox row = new HBox(10, rail, card, pill);
            row.setAlignment(Pos.TOP_LEFT);
            row.getStyleClass().add("history-row");
            row.setFillHeight(true);

            HBox.setHgrow(card, Priority.ALWAYS);
            pill.setMaxHeight(Double.MAX_VALUE);

            setGraphic(row);
        }
    }

    private static String safeText(String v) {
        if (v == null || v.isBlank()) return "-";
        return v.length() > 48 ? "…" + v.substring(v.length() - 47) : v;
    }
}
