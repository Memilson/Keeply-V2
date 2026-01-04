package com.keeply.app.inventory;

import javafx.beans.property.SimpleStringProperty;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.shape.SVGPath;

import java.util.*;
import java.util.function.Consumer;

import com.keeply.app.database.Database.FileHistoryRow;
import com.keeply.app.database.Database.InventoryRow;
import com.keeply.app.database.Database.ScanSummary;

public final class InventoryScreen {

    // Ícones SVG simplificados
    private static final String SVG_FOLDER = "M20 6h-8l-2-2H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm0 12H4V8h16v10z";
    private static final String SVG_FILE = "M14 2H6c-1.1 0-1.99.9-1.99 2L4 20c0 1.1.89 2 1.99 2H18c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z";
    private static final int TOP_LIMIT = 50;

    // Controles
    private final Button btnRefresh = new Button("Recarregar");
    private final Button btnExpand = new Button("Expandir");
    private final Button btnCollapse = new Button("Colapsar");
    private final Button btnRestoreSnapshot = new Button("Restaurar Snapshot");
    private final Button btnRestoreSelected = new Button("Restaurar Selecionados");
    private final MenuButton btnExport = new MenuButton("Report");
    private final MenuItem miExportCsv = new MenuItem("CSV");
    private final MenuItem miExportPdf = new MenuItem("PDF");
    private final TextField txtSearch = new TextField();
    private final ComboBox<ScanSummary> cbScans = new ComboBox<>();
    
    private final Label metaLabel = new Label("Aguardando dados...");
    private final Label errorLabel = new Label();
    private final ProgressIndicator loading = new ProgressIndicator();
    
    // Tabelas
    private final TreeTableView<FileNode> tree = new TreeTableView<>();
    private final TabPane dataTabs = new TabPane();
    private final TableView<FileSizeRow> largestFilesTable = new TableView<>();
    private final TableView<FolderSizeRow> largestFoldersTable = new TableView<>();

    private Consumer<String> onFileSelected;
    private Consumer<String> onShowHistory;

    public Node content() {
        configureTree();
        configureLargestTables();

        VBox layout = new VBox();
        layout.setFillWidth(true);
        layout.setPadding(new Insets(20));
        layout.setSpacing(16);

        // --- 1. Header (Título) ---
        Label title = new Label("Inventário de Arquivos");
        title.getStyleClass().add("header-title");
        
        metaLabel.getStyleClass().add("header-subtitle");
        VBox headerBox = new VBox(4, title, metaLabel);

        // --- 2. Toolbar (Filtros e Ações) ---
        VBox toolbar = new VBox(8);
        toolbar.getStyleClass().add("toolbar-container");
        
        // Inputs
        cbScans.setPromptText("Selecione um Snapshot...");
        cbScans.setPrefWidth(240);
        cbScans.setButtonCell(new ScanListCell());
        cbScans.setCellFactory(p -> new ScanListCell());
        
        txtSearch.setPromptText("Filtrar por nome...");
        txtSearch.setPrefWidth(220);


        // Estilização dos Botões via CSS
        btnRefresh.getStyleClass().add("button-action");
        btnExpand.getStyleClass().add("button-secondary");
        btnCollapse.getStyleClass().add("button-secondary");
        btnRestoreSnapshot.getStyleClass().add("button-action");
        btnRestoreSelected.getStyleClass().add("button-secondary");

        btnExport.getItems().setAll(miExportCsv, miExportPdf);
        btnExport.getStyleClass().add("button-secondary");

        lockButtonWidth(btnExpand);
        lockButtonWidth(btnCollapse);
        lockButtonWidth(btnRefresh);
        lockButtonWidth(btnRestoreSnapshot);
        lockButtonWidth(btnRestoreSelected);
        lockButtonWidth(btnExport);


        txtSearch.setMinWidth(140);
        txtSearch.setMaxWidth(Double.MAX_VALUE);
        HBox.setHgrow(txtSearch, Priority.ALWAYS);

        HBox filterRow = new HBox(10, cbScans, txtSearch);
        filterRow.setAlignment(Pos.CENTER_LEFT);

        HBox actionsRow = new HBox(10, btnExpand, btnCollapse, btnRefresh, btnRestoreSelected, btnRestoreSnapshot, btnExport);
        actionsRow.setAlignment(Pos.CENTER_RIGHT);
        actionsRow.setMaxWidth(Double.MAX_VALUE);

        toolbar.getChildren().addAll(filterRow, actionsRow);

        // --- 3. Tabs e Dados ---
        dataTabs.getStyleClass().add("modern-tabs");
        
        Tab treeTab = new Tab("Estrutura de Arquivos", tree);
        Tab filesTab = new Tab("Top Arquivos", largestFilesTable);
        Tab foldersTab = new Tab("Top Pastas", largestFoldersTable);
        
        dataTabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        dataTabs.getTabs().setAll(treeTab, filesTab, foldersTab);

        // Container com Loading Overlay
        StackPane dataWrapper = new StackPane(dataTabs, loading);
        loading.setVisible(false);
        VBox.setVgrow(dataWrapper, Priority.ALWAYS);

        // Mensagem de Erro
        errorLabel.getStyleClass().add("error-banner"); // Definir no CSS se quiser um fundo vermelho
        errorLabel.setVisible(false);
        errorLabel.setManaged(false);

        // Montagem Final dentro de um Card
        VBox card = new VBox(10, headerBox, toolbar, errorLabel, dataWrapper);
        card.getStyleClass().add("card"); // Usa o estilo de card do styles.css
        VBox.setVgrow(card, Priority.ALWAYS);

        layout.getChildren().add(card);
        return layout;
    }

    // --- Configuração da Árvore (Visual Renovado) ---
    private void configureTree() {
        tree.setShowRoot(true);
        tree.setColumnResizePolicy(TreeTableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
        tree.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
        
        // Coluna Nome + Ícone
        TreeTableColumn<FileNode, String> colName = new TreeTableColumn<>("Nome");
        colName.setPrefWidth(350); // Mais espaço para o nome
        colName.setCellValueFactory(p -> new SimpleStringProperty(p.getValue().getValue().name()));
        colName.setCellFactory(c -> new TreeTableCell<>(){
            @Override protected void updateItem(String item, boolean e) {
                super.updateItem(item, e);
                if (e || item == null) { setGraphic(null); setText(null); return; }
                
                TreeTableView<FileNode> table = getTreeTableView();
                TreeItem<FileNode> treeItem = (table != null) ? table.getTreeItem(getIndex()) : null;
                FileNode n = (treeItem != null) ? treeItem.getValue() : null;
                if (n == null) return;

                // Ícone via SVGPath + CSS Class
                SVGPath icon = new SVGPath();
                icon.setContent(n.directory ? SVG_FOLDER : SVG_FILE);
                icon.getStyleClass().add(n.directory ? "icon-folder" : "icon-file");
                icon.setScaleX(0.9); icon.setScaleY(0.9);

                HBox box = new HBox(8, icon, new Label(item));
                box.setAlignment(Pos.CENTER_LEFT);
                setGraphic(box); setText(null);
            }
        });
        
        // Coluna Status (Pílula Colorida)
        TreeTableColumn<FileNode, String> colStatus = new TreeTableColumn<>("Status");
        colStatus.setPrefWidth(100);
        colStatus.setMaxWidth(120);
        colStatus.setCellValueFactory(p -> new SimpleStringProperty(p.getValue().getValue().status()));
        colStatus.setCellFactory(c -> new TreeTableCell<>(){
             @Override protected void updateItem(String item, boolean e) {
                 super.updateItem(item, e);
                 if (e||item==null) { setGraphic(null); setText(null); return; }
                 
                 Label badge = new Label(item);
                 badge.getStyleClass().add("status-pill");
                 
                 // Mapeia o texto do banco para a classe CSS correta
                 switch(item.toUpperCase()) {
                     case "NEW", "CREATED" -> badge.getStyleClass().add("status-new");
                     case "MODIFIED" -> badge.getStyleClass().add("status-modified");
                     case "DELETED" -> badge.getStyleClass().add("status-deleted");
                     case "SYNCED", "STABLE" -> badge.getStyleClass().add("status-synced");
                     default -> badge.getStyleClass().add("status-default");
                 }
                 setGraphic(badge); setText(null); setAlignment(Pos.CENTER);
             }
        });

        tree.getColumns().setAll(List.of(colName, colStatus));
        
        // Context Menu e Eventos
        tree.setRowFactory(tv -> {
            TreeTableRow<FileNode> row = new TreeTableRow<>();
            row.setOnMouseClicked(e -> {
                if (e.getButton() == javafx.scene.input.MouseButton.SECONDARY && !row.isEmpty() && !row.getItem().directory) {
                    ContextMenu menu = new ContextMenu();
                    MenuItem item = new MenuItem("Ver Histórico de Versões");
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

    // --- Outros Componentes (Tabelas e Diálogos) ---

    private void configureLargestTables() {
        // Configuração simples, o CSS trata do visual
        largestFilesTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
        largestFilesTable.setPlaceholder(new Label("Nenhum arquivo encontrado."));

        TableColumn<FileSizeRow, String> fName = new TableColumn<>("Arquivo");
        fName.setCellValueFactory(p -> new SimpleStringProperty(p.getValue().name()));

        TableColumn<FileSizeRow, String> fPath = new TableColumn<>("Caminho Relativo");
        fPath.setCellValueFactory(p -> new SimpleStringProperty(p.getValue().path()));

        TableColumn<FileSizeRow, String> fSize = new TableColumn<>("Tamanho");
        fSize.setCellValueFactory(p -> new SimpleStringProperty(humanSize(p.getValue().sizeBytes())));
        fSize.setStyle("-fx-alignment: CENTER-RIGHT;");

        largestFilesTable.getColumns().setAll(List.of(fName, fPath, fSize));

        largestFoldersTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
        largestFoldersTable.setPlaceholder(new Label("Nenhuma pasta encontrada."));

        TableColumn<FolderSizeRow, String> dPath = new TableColumn<>("Pasta");
        dPath.setCellValueFactory(p -> new SimpleStringProperty(p.getValue().path()));

        TableColumn<FolderSizeRow, String> dSize = new TableColumn<>("Tamanho Total");
        dSize.setCellValueFactory(p -> new SimpleStringProperty(humanSize(p.getValue().sizeBytes())));
        dSize.setStyle("-fx-alignment: CENTER-RIGHT;");

        largestFoldersTable.getColumns().setAll(List.of(dPath, dSize));
    }

    // [O RESTO DO CÓDIGO (renderLargest, renderTree, showHistoryDialog, etc) MANTÉM-SE IGUAL AO ORIGINAL]
    // Apenas removi o código duplicado aqui para poupar espaço, mas você deve manter a lógica de
    // preenchimento de dados (renderTree, renderLargest, etc.) exatamente como estava, 
    // pois a mudança foi apenas visual (CSS e Layout).
    
    // --- Mantendo os métodos lógicos para o código compilar ---
    
    public void renderLargest(List<InventoryRow> rows) {
        largestFilesTable.getItems().clear();
        largestFoldersTable.getItems().clear();
        if (rows == null || rows.isEmpty()) return;

        var fileRows = rows.stream()
                .sorted(Comparator.comparingLong(InventoryRow::sizeBytes).reversed()
                        .thenComparing(InventoryRow::pathRel, String.CASE_INSENSITIVE_ORDER))
                .limit(TOP_LIMIT)
                .map(r -> new FileSizeRow(r.name(), r.pathRel(), r.sizeBytes()))
                .toList();
        largestFilesTable.getItems().setAll(fileRows);

        Map<String, Long> folderSizes = new HashMap<>();
        for (InventoryRow row : rows) {
            String path = row.pathRel();
            if (path == null || path.isBlank()) continue;
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash <= 0) continue;
            String dir = path.substring(0, lastSlash);
            while (!dir.isEmpty()) {
                folderSizes.merge(dir, row.sizeBytes(), Long::sum);
                int slash = dir.lastIndexOf('/');
                if (slash < 0) break;
                dir = dir.substring(0, slash);
            }
        }

        var folderRows = folderSizes.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed()
                        .thenComparing(Map.Entry.comparingByKey(String.CASE_INSENSITIVE_ORDER)))
                .limit(TOP_LIMIT)
                .map(e -> new FolderSizeRow(e.getKey(), e.getValue()))
                .toList();
        largestFoldersTable.getItems().setAll(folderRows);
    }

    public void renderTree(List<InventoryRow> rows, ScanSummary scan) {
        if (rows == null || rows.isEmpty()) { tree.setRoot(null); tree.setPlaceholder(new Label("Nenhum dado disponível.")); return; }
        String rootLabel = (scan != null) ? scan.rootPath() : "Root";
        TreeItem<FileNode> root = new TreeItem<>(new FileNode(rootLabel, "", true, 0, "SYNCED", 0));
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
                    node = new TreeItem<>(new FileNode(parts[i], current, true, 0, "SYNCED", 0));
                    cache.put(current, node);
                    parent.getChildren().add(node);
                }
                parent = node;
            }
            String name = parts[parts.length-1];
            TreeItem<FileNode> item = new TreeItem<>(new FileNode(name, row.pathRel(), false, row.sizeBytes(), row.status(), row.modifiedMillis()));
            parent.getChildren().add(item);
        }
        sortTree(root);
        tree.setRoot(root);
        metaLabel.setText(scan != null ? "Snapshot #" + scan.scanId() + " (" + scan.finishedAt() + ") • " + rows.size() + " itens" : "");
    }

    public void showHistoryDialog(List<FileHistoryRow> rows, String pathRel) {
        List<FileHistoryRow> safeRows = (rows == null) ? new ArrayList<>() : new ArrayList<>(rows);
        safeRows.sort((a, b) -> Long.compare(b.scanId(), a.scanId()));

        TableView<FileHistoryRow> table = new TableView<>();
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);

        TableColumn<FileHistoryRow, String> cScan = new TableColumn<>("#Scan");
        cScan.setCellValueFactory(p -> new SimpleStringProperty(String.valueOf(p.getValue().scanId())));
        cScan.setMaxWidth(80);

        TableColumn<FileHistoryRow, String> cDate = new TableColumn<>("Data");
        cDate.setCellValueFactory(p -> new SimpleStringProperty(formatDate(p.getValue().createdAt())));

        TableColumn<FileHistoryRow, String> cEvent = new TableColumn<>("Evento");
        cEvent.setCellValueFactory(p -> new SimpleStringProperty(p.getValue().statusEvent()));

        TableColumn<FileHistoryRow, String> cSize = new TableColumn<>("Tamanho");
        cSize.setCellValueFactory(p -> new SimpleStringProperty(humanSize(p.getValue().sizeBytes())));
        cSize.setStyle("-fx-alignment: CENTER-RIGHT;");

        table.getColumns().setAll(List.of(cScan, cDate, cEvent, cSize));
        table.getItems().setAll(safeRows);

        Dialog<Void> dialog = new Dialog<>();
        dialog.setTitle("Historico de Versoes");
        if (pathRel != null && !pathRel.isBlank()) {
            String label = pathRel.length() > 70 ? "..." + pathRel.substring(pathRel.length() - 70) : pathRel;
            dialog.setHeaderText(label);
        } else {
            dialog.setHeaderText("Arquivo");
        }
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        dialog.getDialogPane().setContent(table);
        dialog.getDialogPane().setPrefSize(640, 420);
        dialog.show();
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

    private void sortTree(TreeItem<FileNode> item) {
        if (item == null || item.isLeaf()) return;
        var children = item.getChildren();
        children.sort((a,b) -> {
            if (a.getValue().directory && !b.getValue().directory) return -1;
            if (!a.getValue().directory && b.getValue().directory) return 1;
            return a.getValue().name.compareToIgnoreCase(b.getValue().name);
        });
        for (TreeItem<FileNode> child : children) {
            sortTree(child);
        }
    }

    // Getters
    public Button refreshButton() { return btnRefresh; }
    public Button expandButton() { return btnExpand; }
    public Button collapseButton() { return btnCollapse; }
    public Button restoreSnapshotButton() { return btnRestoreSnapshot; }
    public Button restoreSelectedButton() { return btnRestoreSelected; }
    public MenuItem exportCsvItem() { return miExportCsv; }
    public MenuItem exportPdfItem() { return miExportPdf; }
    public MenuButton exportMenuButton() { return btnExport; }
    public TextField searchField() { return txtSearch; }
    public ComboBox<ScanSummary> scanSelector() { return cbScans; }
    public void onFileSelected(Consumer<String> c) { this.onFileSelected = c; }
    public void onShowHistory(Consumer<String> c) { this.onShowHistory = c; }
    
    public void showLoading(boolean value) {
        loading.setVisible(value);
        dataTabs.setDisable(value);
        btnRefresh.setDisable(value);
        btnRestoreSnapshot.setDisable(value);
        btnRestoreSelected.setDisable(value);
        cbScans.setDisable(value);
    }

    public List<SelectedNode> getSelectedNodes() {
        var items = tree.getSelectionModel().getSelectedItems();
        if (items == null || items.isEmpty()) return List.of();

        List<SelectedNode> out = new ArrayList<>();
        for (TreeItem<FileNode> ti : items) {
            if (ti == null) continue;
            FileNode n = ti.getValue();
            if (n == null) continue;
            if (n.path == null || n.path.isBlank()) continue; // ignore root
            out.add(new SelectedNode(n.path, n.directory));
        }
        return out;
    }
    
    public void showError(String msg) {
        errorLabel.setText(msg);
        errorLabel.setVisible(true);
        errorLabel.setManaged(true);
    }

    // Records & Helpers
    private String humanSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp-1) + "";
        return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }

    private String formatDate(String dt) {
        if (dt == null || dt.isBlank()) return "-";
        return dt.length() > 16 ? dt.substring(0, 16) : dt;
    }

    private void lockButtonWidth(Region r) {
        r.setMinWidth(Region.USE_PREF_SIZE);
        r.setMaxWidth(Region.USE_PREF_SIZE);
    }

    public record FileNode(String name, String path, boolean directory, long sizeBytes, String status, long modifiedMillis) {}
    public record SelectedNode(String pathRel, boolean directory) {}
    public record FileSizeRow(String name, String path, long sizeBytes) {}
    public record FolderSizeRow(String path, long sizeBytes) {}
    
    private static class ScanListCell extends ListCell<ScanSummary> {
        @Override protected void updateItem(ScanSummary item, boolean e) {
            super.updateItem(item, e);
            if (e||item==null) setText(null); else setText("Snapshot #"+item.scanId()+" ("+item.startedAt()+")");
        }
    }
}
