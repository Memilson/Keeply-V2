package com.keeply.app.inventory;

import javafx.beans.property.SimpleStringProperty;
import javafx.beans.binding.Bindings;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.*;
import javafx.scene.control.OverrunStyle;
import javafx.scene.layout.*;
import javafx.scene.shape.SVGPath;

import java.util.*;
import java.util.function.Consumer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.commons.lang3.StringUtils;

import com.keeply.app.database.DatabaseBackup.FileHistoryRow;
import com.keeply.app.database.DatabaseBackup.InventoryRow;
import com.keeply.app.database.DatabaseBackup.ScanSummary;
import com.keeply.app.history.BackupHistoryDb;

public final class InventoryScreen {

    // Ícones SVG simplificados
    private static final String SVG_FOLDER = "M20 6h-8l-2-2H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm0 12H4V8h16v10z";
    private static final String SVG_FILE = "M14 2H6c-1.1 0-1.99.9-1.99 2L4 20c0 1.1.89 2 1.99 2H18c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z";
    private static final int TOP_LIMIT = 50;

    // Controles
    private final Button btnRefresh = new Button("Recarregar");
    private final Button btnExpand = new Button("Expandir");
    private final Button btnCollapse = new Button("Colapsar");
    private final Button btnRestoreSnapshot = new Button("Restaurar backup");
    private final MenuButton btnExport = new MenuButton("Report");
    private final MenuItem miExportCsv = new MenuItem("CSV");
    private final MenuItem miExportPdf = new MenuItem("PDF");
    private final TextField txtSearch = new TextField();
    private final ListView<BackupHistoryDb.HistoryRow> scanList = new ListView<>();
    
    private final Label metaLabel = new Label("Aguardando dados...");
    private final Label errorLabel = new Label();
    private final ProgressIndicator loading = new ProgressIndicator();
    
    // Tabelas
    private final TreeTableView<FileNode> tree = new TreeTableView<>();
    private final TabPane dataTabs = new TabPane();

    // Top views (blocos)
    private final TilePane topFilesTiles = new TilePane();
    private final ScrollPane topFilesScroll = new ScrollPane(topFilesTiles);
    private final TilePane topFoldersTiles = new TilePane();
    private final ScrollPane topFoldersScroll = new ScrollPane(topFoldersTiles);

    private Consumer<String> onFileSelected;
    private Consumer<String> onShowHistory;
    private Runnable onRestoreSelected;

    public Node content() {
        configureTree();
        configureLargestViews();

        VBox layout = new VBox();
        layout.setFillWidth(true);
        layout.setPadding(new Insets(8, 0, 0, 0));
        layout.setSpacing(16);

        // --- 1. Header (Título) ---
        Label title = new Label("Armazenamento");
        title.getStyleClass().add("page-title");

        Label subtitle = new Label("Auditoria e estrutura do conteúdo protegido.");
        subtitle.getStyleClass().add("page-subtitle");

        metaLabel.getStyleClass().add("header-subtitle");
        VBox headerBox = new VBox(4, title, subtitle, metaLabel);

        // --- 2. Toolbar (Filtros e Ações) ---
        VBox toolbar = new VBox(8);
        toolbar.getStyleClass().add("toolbar-container");
        
        // Lista de backups (histórico)
        scanList.setMinHeight(160);
        scanList.setPrefHeight(190);
        scanList.setMaxHeight(240);
        scanList.getStyleClass().add("history-list");
        scanList.setCellFactory(p -> new ScanListCell());
        scanList.getSelectionModel().setSelectionMode(SelectionMode.SINGLE);
        
        txtSearch.setPromptText("Filtrar por nome...");
        txtSearch.setPrefWidth(220);


        // Estilização dos Botões via CSS
        btnRefresh.getStyleClass().add("button-action");
        btnExpand.getStyleClass().add("button-secondary");
        btnCollapse.getStyleClass().add("button-secondary");
        btnRestoreSnapshot.getStyleClass().add("button-action");

        btnExport.getItems().setAll(miExportCsv, miExportPdf);
        btnExport.getStyleClass().add("button-secondary");

        lockButtonWidth(btnExpand);
        lockButtonWidth(btnCollapse);
        lockButtonWidth(btnRefresh);
        lockButtonWidth(btnRestoreSnapshot);
        lockButtonWidth(btnExport);


        txtSearch.setMinWidth(140);
        txtSearch.setMaxWidth(Double.MAX_VALUE);
        HBox.setHgrow(txtSearch, Priority.ALWAYS);

        HBox filterRow = new HBox(10, txtSearch);
        filterRow.setAlignment(Pos.CENTER_LEFT);

        HBox actionsRow = new HBox(10, btnRestoreSnapshot, btnExpand, btnCollapse, btnRefresh, btnExport);
        actionsRow.setAlignment(Pos.CENTER_LEFT);
        actionsRow.setMaxWidth(Double.MAX_VALUE);

        Label listTitle = new Label("Histórico de backups");
        listTitle.getStyleClass().add("section-title");
        VBox listBox = new VBox(6, listTitle, scanList);
        listBox.setMinHeight(180);
        toolbar.getChildren().addAll(listBox, filterRow, actionsRow);

        // --- 3. Tabs e Dados ---
        dataTabs.getStyleClass().add("modern-tabs");
        
        Tab treeTab = new Tab("Estrutura de Arquivos", tree);
        Tab filesTab = new Tab("Top Arquivos", topFilesScroll);
        Tab foldersTab = new Tab("Top Pastas", topFoldersScroll);
        
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

        VBox content = new VBox(16, card);
        content.getStyleClass().add("content-wrap");
        content.setMaxWidth(980);

        layout.getChildren().add(content);
        return layout;
    }

    // --- Configuração da Árvore (Visual Renovado) ---
    private void configureTree() {
        tree.setShowRoot(false);
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

            row.setOnContextMenuRequested(e -> {
                if (row.isEmpty()) return;

                // Se clicou com o botão direito em algo fora da seleção, seleciona esse item
                if (!tree.getSelectionModel().getSelectedIndices().contains(row.getIndex())) {
                    tree.getSelectionModel().clearSelection();
                    tree.getSelectionModel().select(row.getIndex());
                }

                ContextMenu menu = new ContextMenu();

                MenuItem restore = new MenuItem("Restaurar selecionados");
                restore.disableProperty().bind(Bindings.isEmpty(tree.getSelectionModel().getSelectedItems()));
                restore.setOnAction(ev -> {
                    if (onRestoreSelected != null) onRestoreSelected.run();
                });
                menu.getItems().add(restore);

                // Para arquivos: manter opção de histórico
                if (!row.getItem().directory) {
                    MenuItem history = new MenuItem("Ver Histórico de Versões");
                    history.setOnAction(ev -> { if (onShowHistory != null) onShowHistory.accept(row.getItem().path); });
                    menu.getItems().add(history);
                }

                menu.show(row, e.getScreenX(), e.getScreenY());
            });

            return row;
        });
        
        tree.getSelectionModel().selectedItemProperty().addListener((o,old,val) -> {
            if(onFileSelected!=null) onFileSelected.accept(val!=null && !val.getValue().directory ? val.getValue().path : null);
        });
    }

    // --- Outros Componentes (Tabelas e Diálogos) ---

    private void configureLargestViews() {
        configureTilePane(topFilesTiles);
        configureTilePane(topFoldersTiles);

        topFilesScroll.setFitToWidth(true);
        topFilesScroll.setHbarPolicy(ScrollPane.ScrollBarPolicy.NEVER);
        topFilesScroll.setVbarPolicy(ScrollPane.ScrollBarPolicy.AS_NEEDED);
        topFilesScroll.setPannable(true);

        topFoldersScroll.setFitToWidth(true);
        topFoldersScroll.setHbarPolicy(ScrollPane.ScrollBarPolicy.NEVER);
        topFoldersScroll.setVbarPolicy(ScrollPane.ScrollBarPolicy.AS_NEEDED);
        topFoldersScroll.setPannable(true);

        topFilesScroll.setContent(topFilesTiles);
        topFoldersScroll.setContent(topFoldersTiles);
    }

    private static void configureTilePane(TilePane pane) {
        pane.setPadding(new Insets(14));
        pane.setHgap(12);
        pane.setVgap(12);
        pane.setPrefTileWidth(420);
        pane.setPrefTileHeight(120);
        pane.setTileAlignment(Pos.TOP_LEFT);
    }

    private Node buildTopFileCard(FileSizeRow row) {
        Label name = new Label(StringUtils.firstNonBlank(row.name(), "(sem nome)"));
        name.getStyleClass().add("flow-title");
        name.setTextOverrun(OverrunStyle.ELLIPSIS);
        name.setMaxWidth(Double.MAX_VALUE);

        Label size = new Label(humanSize(row.sizeBytes()));
        size.getStyleClass().add("card-h2");

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        HBox header = new HBox(10, name, spacer, size);
        header.setAlignment(Pos.CENTER_LEFT);

        String dir = extractParentDir(row.path());
        Label path = new Label(dir.isBlank() ? "(raiz)" : dir);
        path.getStyleClass().add("flow-subtitle");
        path.setTextOverrun(OverrunStyle.ELLIPSIS);
        path.setMaxWidth(Double.MAX_VALUE);

        VBox card = new VBox(6, header, path);
        card.getStyleClass().add("card");
        card.setPadding(new Insets(12));
        card.setMinHeight(Region.USE_PREF_SIZE);
        return card;
    }

    private Node buildTopFolderCard(FolderSizeRow row) {
        String p = row.path();
        String nameText = extractLastSegment(p);
        if (nameText.isBlank()) nameText = "(raiz)";

        Label name = new Label(nameText);
        name.getStyleClass().add("flow-title");
        name.setTextOverrun(OverrunStyle.ELLIPSIS);
        name.setMaxWidth(Double.MAX_VALUE);

        Label size = new Label(humanSize(row.sizeBytes()));
        size.getStyleClass().add("card-h2");

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        HBox header = new HBox(10, name, spacer, size);
        header.setAlignment(Pos.CENTER_LEFT);

        Label path = new Label(p == null ? "" : p);
        path.getStyleClass().add("flow-subtitle");
        path.setTextOverrun(OverrunStyle.ELLIPSIS);
        path.setMaxWidth(Double.MAX_VALUE);

        double pct = Math.max(0, Math.min(1, row.percentOfTotal()));
        Label pctLabel = new Label(String.format(Locale.US, "%.0f%%", pct * 100.0));
        pctLabel.getStyleClass().add("flow-subtitle");

        ProgressBar bar = new ProgressBar(pct);
        bar.getStyleClass().addAll("metric-progress", "metric-progress-soft");
        bar.setMaxWidth(Double.MAX_VALUE);

        HBox pctRow = new HBox(10, pctLabel, bar);
        pctRow.setAlignment(Pos.CENTER_LEFT);
        HBox.setHgrow(bar, Priority.ALWAYS);

        VBox card = new VBox(6, header, path, pctRow);
        card.getStyleClass().add("card");
        card.setPadding(new Insets(12));
        card.setMinHeight(Region.USE_PREF_SIZE);
        return card;
    }

    private static String extractParentDir(String pathRel) {
        if (pathRel == null) return "";
        String p = pathRel.replace('\\', '/');
        int i = p.lastIndexOf('/');
        return (i <= 0) ? "" : p.substring(0, i);
    }

    private static String extractLastSegment(String path) {
        if (path == null) return "";
        String p = path.replace('\\', '/');
        int i = p.lastIndexOf('/');
        return (i >= 0 && i + 1 < p.length()) ? p.substring(i + 1) : p;
    }

    // [O RESTO DO CÓDIGO (renderLargest, renderTree, showHistoryDialog, etc) MANTÉM-SE IGUAL AO ORIGINAL]
    // Apenas removi o código duplicado aqui para poupar espaço, mas você deve manter a lógica de
    // preenchimento de dados (renderTree, renderLargest, etc.) exatamente como estava, 
    // pois a mudança foi apenas visual (CSS e Layout).
    
    // --- Mantendo os métodos lógicos para o código compilar ---
    
    public void renderLargest(List<InventoryRow> rows) {
        topFilesTiles.getChildren().clear();
        topFoldersTiles.getChildren().clear();
        if (rows == null || rows.isEmpty()) return;

        long totalBytes = rows.stream().mapToLong(InventoryRow::sizeBytes).sum();

        var fileRows = rows.stream()
                .sorted(Comparator.comparingLong(InventoryRow::sizeBytes).reversed()
                        .thenComparing(InventoryRow::pathRel, String.CASE_INSENSITIVE_ORDER))
                .limit(TOP_LIMIT)
                .map(r -> new FileSizeRow(r.name(), r.pathRel(), r.sizeBytes()))
                .toList();
        for (FileSizeRow r : fileRows) topFilesTiles.getChildren().add(buildTopFileCard(r));

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
            .map(e -> new FolderSizeRow(e.getKey(), e.getValue(), totalBytes <= 0 ? 0.0 : (double) e.getValue() / (double) totalBytes))
                .toList();
        for (FolderSizeRow r : folderRows) topFoldersTiles.getChildren().add(buildTopFolderCard(r));
    }

    public void renderTree(List<InventoryRow> rows, ScanSummary scan) {
        if (rows == null || rows.isEmpty()) {
            tree.setRoot(null);
            tree.setPlaceholder(new Label("Selecione um backup para visualizar os arquivos."));
            metaLabel.setText("");
            return;
        }
        TreeItem<FileNode> root = new TreeItem<>(new FileNode("root", "", true, 0, "SYNCED", 0));
        root.setExpanded(true);

        for (InventoryRow row : rows) {
            if (row.pathRel() == null || row.pathRel().isBlank()) continue;
            // Lista plana de arquivos (sem pastas)
            String name = row.pathRel();
            TreeItem<FileNode> item = new TreeItem<>(new FileNode(name, row.pathRel(), false, row.sizeBytes(), row.status(), row.modifiedMillis()));
            root.getChildren().add(item);
        }

        tree.setRoot(root);
        metaLabel.setText(scan != null ? "Backup #" + scan.scanId() + " (" + formatDate(scan.finishedAt()) + ") • " + rows.size() + " itens" : "");
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
        cSize.setCellFactory(c -> new TableCell<>() {
            {
                getStyleClass().add("cell-right");
            }

            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                setText(empty ? null : item);
            }
        });

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
    public MenuItem exportCsvItem() { return miExportCsv; }
    public MenuItem exportPdfItem() { return miExportPdf; }
    public MenuButton exportMenuButton() { return btnExport; }
    public TextField searchField() { return txtSearch; }
    public ListView<BackupHistoryDb.HistoryRow> scanList() { return scanList; }
    public void onFileSelected(Consumer<String> c) { this.onFileSelected = c; }
    public void onShowHistory(Consumer<String> c) { this.onShowHistory = c; }
    public void onRestoreSelected(Runnable r) { this.onRestoreSelected = r; }
    
    public void showLoading(boolean value) {
        loading.setVisible(value);
        dataTabs.setDisable(value);
        btnRefresh.setDisable(value);
        btnRestoreSnapshot.setDisable(value);
        scanList.setDisable(value);
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
        try {
            Instant instant = Instant.parse(dt);
            return TS_FMT.format(instant.atZone(ZoneId.systemDefault()));
        } catch (Exception e) {
            return dt.length() > 16 ? dt.substring(0, 16) : dt;
        }
    }

    private void lockButtonWidth(Region r) {
        r.setMinWidth(Region.USE_PREF_SIZE);
        r.setMaxWidth(Region.USE_PREF_SIZE);
    }

    public record FileNode(String name, String path, boolean directory, long sizeBytes, String status, long modifiedMillis) {}
    public record SelectedNode(String pathRel, boolean directory) {}
    public record FileSizeRow(String name, String path, long sizeBytes) {}
    public record FolderSizeRow(String path, long sizeBytes, double percentOfTotal) {}
    
    private class ScanListCell extends ListCell<BackupHistoryDb.HistoryRow> {
        @Override protected void updateItem(BackupHistoryDb.HistoryRow item, boolean e) {
            super.updateItem(item, e);
            if (e || item == null) {
                setText(null);
                setGraphic(null);
                return;
            }
            String type = item.backupType() == null
                    ? "Backup"
                    : (item.backupType().equalsIgnoreCase("FULL") ? "Completo" : "Incremental");
            String tsRaw = item.finishedAt() != null ? item.finishedAt() : item.startedAt();
            String ts = formatIso(tsRaw);
            Label title = new Label(ts);
            title.getStyleClass().add("history-title");

            Label pill = new Label(type);
            pill.getStyleClass().addAll("history-pill", item.backupType() != null && item.backupType().equalsIgnoreCase("FULL") ? "pill-full" : "pill-incremental");

            Label line1 = new Label("Plano: " + safeText(item.rootPath()));
            line1.getStyleClass().add("muted");
            Label line2 = new Label("Arquivos: " + item.filesProcessed() + " • Erros: " + item.errors());
            line2.getStyleClass().add("muted");

            Button restoreBtn = new Button("Recuperar arquivos/pastas");
            restoreBtn.getStyleClass().addAll("btn", "btn-primary", "btn-mini");
            restoreBtn.setOnAction(ev -> {
                // Seleciona o backup para exibir a árvore abaixo
                getListView().getSelectionModel().select(item);
            });

            Button actionsBtn = new Button("Ações");
            actionsBtn.getStyleClass().addAll("btn", "btn-outline", "btn-mini");
            actionsBtn.setOnAction(ev -> getListView().getSelectionModel().select(item));

            HBox actions = new HBox(8, restoreBtn, actionsBtn);
            actions.setAlignment(Pos.CENTER_LEFT);

            VBox card = new VBox(6, title, line1, line2, actions);
            card.getStyleClass().add("history-card");

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
            setGraphic(row);
        }
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

    private static String safeText(String v) {
        if (v == null || v.isBlank()) return "-";
        return v.length() > 48 ? "…" + v.substring(v.length() - 47) : v;
    }
}
