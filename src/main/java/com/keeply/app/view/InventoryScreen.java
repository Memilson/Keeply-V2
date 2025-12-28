package com.keeply.app.view;

import com.keeply.app.Database.FileHistoryRow;
import com.keeply.app.Database.InventoryRow;
import com.keeply.app.Database.ScanSummary;
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

import java.util.*;
import java.util.function.Consumer;

public final class InventoryScreen {

    private static final String ICON_FOLDER = "M20 6h-8l-2-2H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm0 12H4V8h16v10z";
    private static final String ICON_FILE = "M14 2H6c-1.1 0-1.99.9-1.99 2L4 20c0 1.1.89 2 1.99 2H18c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z";

    // Tema interno
    private static final class Theme {
        static final String BG_PRIMARY = "#FAFAFA";
        static final String BG_SECONDARY = "#F4F4F5";
        static final String TEXT_MAIN = "#18181B";
        static final String TEXT_MUTED = "#71717A";
        static final String TEXT_INVERT = "#FFFFFF";
        static final String ACCENT = "#06B6D4";
        static final String BORDER = "#E4E4E7";
        static final String DANGER = "#EF4444";
        static final String SUCCESS = "#10B981";
        static final String WARN = "#F59E0B";
    }

    private final Button btnRefresh = new Button("Recarregar");
    private final Button btnExpand = new Button("Expandir");
    private final Button btnCollapse = new Button("Colapsar");
    private final TextField txtSearch = new TextField();
    private final ComboBox<ScanSummary> cbScans = new ComboBox<>();
    
    private final Label metaLabel = new Label("Aguardando dados...");
    private final Label errorLabel = new Label();
    private final ProgressIndicator loading = new ProgressIndicator();
    private final TreeTableView<FileNode> tree = new TreeTableView<>();

    private Consumer<String> onFileSelected;
    private Consumer<String> onShowHistory;

    public Node content() {
        configureTree();

        VBox layout = new VBox(10);
        layout.setPadding(new Insets(10));

        // Header
        Label title = new Label("INVENTÁRIO");
        title.setFont(Font.font("Segoe UI", FontWeight.BOLD, 14));
        title.setTextFill(Color.web(Theme.TEXT_MAIN));
        metaLabel.setTextFill(Color.web(Theme.TEXT_MUTED));
        metaLabel.setFont(Font.font("Segoe UI", 11));
        VBox headerText = new VBox(2, title, metaLabel);

        // Toolbar
        HBox toolbar = new HBox(8);
        toolbar.setAlignment(Pos.CENTER_LEFT);
        
        cbScans.setPromptText("Selecione um Snapshot...");
        cbScans.setPrefWidth(220);
        cbScans.setButtonCell(new ScanListCell());
        cbScans.setCellFactory(p -> new ScanListCell());
        styleField(cbScans);

        txtSearch.setPromptText("Buscar arquivo...");
        txtSearch.setPrefWidth(200);
        styleField(txtSearch);

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        styleButton(btnCollapse, Theme.BG_SECONDARY, Theme.TEXT_MAIN);
        styleButton(btnExpand, Theme.BG_SECONDARY, Theme.TEXT_MAIN);
        styleButton(btnRefresh, Theme.ACCENT, Theme.TEXT_INVERT);
        btnCollapse.setMinWidth(80);
        btnExpand.setMinWidth(80);
        btnRefresh.setMinWidth(100);

        toolbar.getChildren().addAll(cbScans, txtSearch, spacer, btnCollapse, btnExpand, btnRefresh);

        // Tree Area
        StackPane treeWrapper = new StackPane(tree, loading);
        loading.setVisible(false);
        loading.setMaxSize(40,40);
        VBox.setVgrow(treeWrapper, Priority.ALWAYS);

        // Error Banner
        errorLabel.setPadding(new Insets(8));
        errorLabel.setStyle("-fx-background-color: #FEE2E2; -fx-text-fill: #991B1B; -fx-background-radius: 4;");
        errorLabel.setVisible(false);
        errorLabel.setManaged(false);

        layout.getChildren().addAll(headerText, toolbar, errorLabel, treeWrapper);
        return layout;
    }

    public void showHistoryDialog(List<FileHistoryRow> rows, String pathRel) {
        if (rows == null) rows = new ArrayList<>();
        rows.sort((a, b) -> Long.compare(b.scanId(), a.scanId())); // Mais recente primeiro

        TabPane tabs = new TabPane();
        tabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);

        // Aba 1: Timeline
        TableView<HistoryRow> timelineTable = createBaseHistoryTable();
        List<HistoryRow> uiRows = new ArrayList<>();
        for(FileHistoryRow r : rows) {
            uiRows.add(new HistoryRow(r.scanId(), r.rootPath(), r.startedAt(), r.finishedAt(), r.hashHex(), r.sizeBytes(), r.statusEvent(), r.createdAt(), 0));
        }
        timelineTable.getItems().setAll(uiRows);
        Tab tabTimeline = new Tab("Timeline", timelineTable);

        // Aba 2: Crescimento (Cálculo de Delta)
        TableView<HistoryRow> growthTable = createGrowthTable();
        List<FileHistoryRow> chronoOrder = new ArrayList<>(rows);
        Collections.reverse(chronoOrder); // Antigo -> Novo para calcular
        
        List<HistoryRow> growthRows = new ArrayList<>();
        long prevSize = 0;
        boolean first = true;
        
        for (FileHistoryRow r : chronoOrder) {
            long delta = first ? 0 : r.sizeBytes() - prevSize;
            growthRows.add(new HistoryRow(r.scanId(), r.rootPath(), r.startedAt(), r.finishedAt(), r.hashHex(), r.sizeBytes(), r.statusEvent(), r.createdAt(), delta));
            prevSize = r.sizeBytes();
            first = false;
        }
        Collections.reverse(growthRows); // Novo -> Antigo para exibir
        growthTable.getItems().setAll(growthRows);
        Tab tabGrowth = new Tab("Crescimento", growthTable);

        // Aba 3: Detalhes
        GridPane detailsPane = new GridPane();
        detailsPane.setHgap(10); detailsPane.setVgap(8); detailsPane.setPadding(new Insets(15));
        var latest = rows.isEmpty() ? null : rows.get(0);
        addDetailRow(detailsPane, 0, "Arquivo:", pathRel);
        addDetailRow(detailsPane, 1, "Último Hash:", latest != null ? latest.hashHex() : "-");
        addDetailRow(detailsPane, 2, "Tamanho Atual:", latest != null ? humanSize(latest.sizeBytes()) : "-");
        addDetailRow(detailsPane, 3, "Último Evento:", latest != null ? latest.statusEvent() : "-");
        Tab tabDetails = new Tab("Detalhes", detailsPane);

        tabs.getTabs().addAll(tabTimeline, tabGrowth, tabDetails);

        Dialog<Void> dialog = new Dialog<>();
        dialog.setTitle("Histórico de Versões");
        dialog.setHeaderText("Arquivo: " + (pathRel.length() > 50 ? "..." + pathRel.substring(pathRel.length()-50) : pathRel));
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        dialog.getDialogPane().setContent(tabs);
        dialog.getDialogPane().setPrefSize(600, 400);
        dialog.show();
    }

    private TableView<HistoryRow> createBaseHistoryTable() {
        TableView<HistoryRow> table = new TableView<>();
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
        
        TableColumn<HistoryRow, String> cScan = new TableColumn<>("#Scan");
        cScan.setCellValueFactory(p -> new SimpleStringProperty(String.valueOf(p.getValue().scanId())));
        cScan.setPrefWidth(50);

        TableColumn<HistoryRow, String> cDate = new TableColumn<>("Data");
        cDate.setCellValueFactory(p -> new SimpleStringProperty(formatDate(p.getValue().createdAt())));

        TableColumn<HistoryRow, String> cEvt = new TableColumn<>("Evento");
        cEvt.setCellValueFactory(p -> new SimpleStringProperty(p.getValue().status()));
        cEvt.setCellFactory(c -> new TableCell<>() {
            @Override protected void updateItem(String item, boolean e) {
                super.updateItem(item, e);
                if (e || item == null) { setText(null); setStyle(""); return; }
                setText(item);
                if (item.equalsIgnoreCase("MODIFIED")) setStyle("-fx-text-fill: " + Theme.WARN + "; -fx-font-weight: bold;");
                else if (item.equalsIgnoreCase("NEW")) setStyle("-fx-text-fill: " + Theme.SUCCESS + "; -fx-font-weight: bold;");
                else setStyle("-fx-text-fill: " + Theme.TEXT_MAIN + ";");
            }
        });

        TableColumn<HistoryRow, String> cSize = new TableColumn<>("Tamanho");
        cSize.setCellValueFactory(p -> new SimpleStringProperty(humanSize(p.getValue().sizeBytes())));
        cSize.setStyle("-fx-alignment: CENTER-RIGHT;");

        table.getColumns().addAll(List.of(cScan, cDate, cEvt, cSize));
        return table;
    }

    private TableView<HistoryRow> createGrowthTable() {
        TableView<HistoryRow> table = new TableView<>();
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);

        TableColumn<HistoryRow, String> cScan = new TableColumn<>("#Scan");
        cScan.setCellValueFactory(p -> new SimpleStringProperty(String.valueOf(p.getValue().scanId())));

        TableColumn<HistoryRow, String> cDate = new TableColumn<>("Data");
        cDate.setCellValueFactory(p -> new SimpleStringProperty(formatDate(p.getValue().createdAt())));

        TableColumn<HistoryRow, String> cSize = new TableColumn<>("Tamanho");
        cSize.setCellValueFactory(p -> new SimpleStringProperty(humanSize(p.getValue().sizeBytes())));

        TableColumn<HistoryRow, String> cDelta = new TableColumn<>("Variação");
        cDelta.setCellValueFactory(p -> new SimpleStringProperty(p.getValue().growthLabel()));
        cDelta.setCellFactory(c -> new TableCell<>() {
            @Override protected void updateItem(String item, boolean e) {
                super.updateItem(item, e);
                if (e || item == null) { setText(null); setStyle(""); return; }
                setText(item);
                if (item.startsWith("+")) setStyle("-fx-text-fill: " + Theme.DANGER + ";");
                else if (item.startsWith("-")) setStyle("-fx-text-fill: " + Theme.SUCCESS + ";");
                else setStyle("-fx-text-fill: " + Theme.TEXT_MUTED + ";");
            }
        });

        table.getColumns().addAll(List.of(cScan, cDate, cSize, cDelta));
        return table;
    }

    private void addDetailRow(GridPane pane, int row, String label, String value) {
        Label l = new Label(label);
        l.setFont(Font.font("Segoe UI", FontWeight.BOLD, 12));
        l.setTextFill(Color.web(Theme.TEXT_MUTED));
        Label v = new Label(value);
        v.setFont(Font.font("Segoe UI", 12));
        v.setTextFill(Color.web(Theme.TEXT_MAIN));
        v.setWrapText(true);
        pane.add(l, 0, row);
        pane.add(v, 1, row);
    }

    private void configureTree() {
        tree.setShowRoot(true);
        tree.setColumnResizePolicy(TreeTableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
        
        TreeTableColumn<FileNode, String> colName = new TreeTableColumn<>("Nome");
        colName.setPrefWidth(250);
        colName.setCellValueFactory(p -> new SimpleStringProperty(p.getValue().getValue().name()));
        colName.setCellFactory(c -> new TreeTableCell<>(){
            @Override protected void updateItem(String item, boolean e) {
                super.updateItem(item, e);
                if (e || item == null) { setGraphic(null); setText(null); return; }
                FileNode n = getTreeTableRow().getItem();
                if (n==null) return;
                SVGPath icon = new SVGPath();
                icon.setContent(n.directory ? ICON_FOLDER : ICON_FILE);
                icon.setFill(Color.web(n.directory ? Theme.WARN : Theme.TEXT_MUTED));
                icon.setScaleX(0.8); icon.setScaleY(0.8);
                HBox box = new HBox(6, icon, new Label(item));
                box.setAlignment(Pos.CENTER_LEFT);
                setGraphic(box); setText(null);
            }
        });
        
        TreeTableColumn<FileNode, String> colStatus = new TreeTableColumn<>("Estado");
        colStatus.setCellValueFactory(p -> new SimpleStringProperty(p.getValue().getValue().status()));
        colStatus.setCellFactory(c -> new TreeTableCell<>(){
             @Override protected void updateItem(String item, boolean e) {
                 super.updateItem(item, e);
                 if (e||item==null) { setGraphic(null); setText(null); return; }
                 Label l = new Label(item);
                 String color = switch(item.toUpperCase()) {
                     case "NEW","CREATED" -> Theme.SUCCESS; case "MODIFIED" -> Theme.WARN; default -> Theme.ACCENT;
                 };
                 l.setStyle("-fx-background-color:"+color+";-fx-text-fill:white;-fx-background-radius:10;-fx-padding:2 8;-fx-font-weight:bold;-fx-font-size:10px;");
                 setGraphic(l); setText(null); setAlignment(Pos.CENTER);
             }
        });

        tree.getColumns().setAll(List.of(colName, colStatus));
        
        tree.setRowFactory(tv -> {
            TreeTableRow<FileNode> row = new TreeTableRow<>();
            row.setOnMouseClicked(e -> {
                if (e.getButton() == javafx.scene.input.MouseButton.SECONDARY && !row.isEmpty() && !row.getItem().directory) {
                    ContextMenu menu = new ContextMenu();
                    MenuItem item = new MenuItem("Ver Histórico Completo");
                    item.setOnAction(ev -> { if(onShowHistory!=null) onShowHistory.accept(row.getItem().path); });
                    menu.getItems().add(item);
                    menu.show(row, e.getScreenX(), e.getScreenY());
                }
            });
            return row;
        });
        
        tree.getSelectionModel().selectedItemProperty().addListener((o,old,val) -> {
            if(onFileSelected!=null) onFileSelected.accept(val!=null && !val.getValue().directory ? val.getValue().path : null);
        });
    }

    public void renderTree(List<InventoryRow> rows, ScanSummary scan) {
        if (rows == null || rows.isEmpty()) { tree.setRoot(null); tree.setPlaceholder(new Label("Sem dados.")); return; }
        String rootLabel = (scan != null) ? scan.rootPath() : "Root";
        TreeItem<FileNode> root = new TreeItem<>(new FileNode(rootLabel, "", true, 0, "", "", 0));
        root.setExpanded(true);
        Map<String, TreeItem<FileNode>> cache = new HashMap<>();
        cache.put("", root);

        for (InventoryRow row : rows) {
            String[] parts = row.pathRel().split("/");
            TreeItem<FileNode> parent = root;
            String current = "";
            for (int i=0; i<parts.length-1; i++) {
                current = current.isEmpty() ? parts[i] : current + "/" + parts[i];
                TreeItem<FileNode> node = cache.get(current);
                if (node == null) {
                    node = new TreeItem<>(new FileNode(parts[i], current, true, 0, "", "", 0));
                    cache.put(current, node);
                    addOrdered(parent.getChildren(), node);
                }
                parent = node;
            }
            String name = parts[parts.length-1];
            TreeItem<FileNode> item = new TreeItem<>(new FileNode(name, row.pathRel(), false, row.sizeBytes(), row.status(), row.hashHex(), row.modifiedMillis()));
            addOrdered(parent.getChildren(), item);
        }
        tree.setRoot(root);
        metaLabel.setText(scan != null ? "Scan #" + scan.scanId() + " (" + scan.finishedAt() + ") | " + rows.size() + " itens" : "");
    }

    public void expandAll() {
        if (tree.getRoot() == null) return;
        setExpandedRecursive(tree.getRoot(), true);
    }

    public void collapseAll() {
        if (tree.getRoot() == null) return;
        setExpandedRecursive(tree.getRoot(), false);
    }

    private void setExpandedRecursive(TreeItem<?> item, boolean expanded) {
        if (item == null || item.isLeaf()) return;
        item.setExpanded(expanded);
        for (TreeItem<?> child : item.getChildren()) {
            setExpandedRecursive(child, expanded);
        }
    }

    private void addOrdered(List<TreeItem<FileNode>> l, TreeItem<FileNode> c) {
        l.add(c);
        l.sort((a,b) -> {
            if (a.getValue().directory && !b.getValue().directory) return -1;
            if (!a.getValue().directory && b.getValue().directory) return 1;
            return a.getValue().name.compareToIgnoreCase(b.getValue().name);
        });
    }

    // Getters & Setters
    public Button refreshButton() { return btnRefresh; }
    public Button expandButton() { return btnExpand; }
    public Button collapseButton() { return btnCollapse; }
    public TextField searchField() { return txtSearch; }
    public ComboBox<ScanSummary> scanSelector() { return cbScans; }
    public void onFileSelected(Consumer<String> c) { this.onFileSelected = c; }
    public void onShowHistory(Consumer<String> c) { this.onShowHistory = c; }
    
    public void showLoading(boolean value) {
        loading.setVisible(value);
        tree.setDisable(value);
        btnRefresh.setDisable(value);
        cbScans.setDisable(value);
    }
    
    public void showError(String msg) {
        errorLabel.setText(msg);
        errorLabel.setVisible(true);
        errorLabel.setManaged(true);
    }

    // Utils
    private void styleField(Control c) { c.setStyle("-fx-background-radius:4;-fx-border-color:#D1D5DB;-fx-border-radius:4;-fx-padding:6;"); }
    private void styleButton(Button b, String bg, String fg) { b.setStyle("-fx-background-color:"+bg+";-fx-text-fill:"+fg+";-fx-font-weight:bold;-fx-cursor:hand;-fx-background-radius:4;-fx-padding:6 12;"); }
    private String humanSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp-1) + "";
        return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }
    private String formatDate(String dt) { return (dt != null && dt.length() > 16) ? dt.substring(0, 16) : dt; }

    // Records
    public record FileNode(String name, String path, boolean directory, long sizeBytes, String status, String hash, long modifiedMillis) {}
    
    public record HistoryRow(long scanId, String rootPath, String startedAt, String finishedAt, String hash, long sizeBytes, String status, String createdAt, long growthBytes) {
        public String growthLabel() {
            if (growthBytes == 0) return "-";
            double mb = growthBytes / 1024.0 / 1024.0;
            if (Math.abs(mb) < 0.01) return (growthBytes > 0 ? "+" : "") + growthBytes + " B";
            return (growthBytes > 0 ? "+" : "") + String.format("%.2f MB", mb);
        }
    }

    private static class ScanListCell extends ListCell<ScanSummary> {
        @Override protected void updateItem(ScanSummary item, boolean e) {
            super.updateItem(item, e);
            if (e||item==null) setText(null); else setText("Scan #"+item.scanId()+" - "+item.startedAt());
        }
    }
}