package com.keeply.app.inventory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.keeply.app.database.DatabaseBackup.FileHistoryRow;
import com.keeply.app.database.DatabaseBackup.InventoryRow;
import com.keeply.app.database.DatabaseBackup.ScanSummary;
import com.keeply.app.history.BackupHistoryDb;

import javafx.beans.binding.Bindings;
import javafx.beans.property.SimpleStringProperty;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
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
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.scene.shape.SVGPath;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.Window;

public final class InventoryScreen {

    // Ícones SVG simplificados
    private static final String SVG_FOLDER = "M20 6h-8l-2-2H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm0 12H4V8h16v10z";
    private static final String SVG_FILE = "M14 2H6c-1.1 0-1.99.9-1.99 2L4 20c0 1.1.89 2 1.99 2H18c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z";
    // Controles
    private final Button btnRefresh = new Button("Recarregar");
    private final Button btnRestoreSnapshot = new Button("Restaurar backup");
    private final ListView<BackupHistoryDb.HistoryRow> scanList = new ListView<>();
    
    private final Label metaLabel = new Label("Aguardando dados...");
    private final Label errorLabel = new Label();
    private final ProgressIndicator loading = new ProgressIndicator();
    
    // Tabela de arquivos (usada apenas na janela de arquivos)
    private final TreeTableView<FileNode> tree = new TreeTableView<>();

    private Consumer<String> onFileSelected;
    private Consumer<String> onShowHistory;
    private Runnable onRestoreSelected;

    public Node content() {
        configureTree();

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
        
        // Estilização dos Botões via CSS
        btnRefresh.getStyleClass().add("button-action");
        btnRestoreSnapshot.getStyleClass().add("button-action");
        lockButtonWidth(btnRefresh);
        lockButtonWidth(btnRestoreSnapshot);

        HBox actionsRow = new HBox(10, btnRestoreSnapshot, btnRefresh);
        actionsRow.setAlignment(Pos.CENTER_LEFT);
        actionsRow.setMaxWidth(Double.MAX_VALUE);

        Label listTitle = new Label("Histórico de backups");
        listTitle.getStyleClass().add("section-title");
        VBox listBox = new VBox(6, listTitle, scanList);
        listBox.setMinHeight(180);
        toolbar.getChildren().addAll(listBox, actionsRow);

        // Mensagem de Erro
        errorLabel.getStyleClass().add("error-banner"); // Definir no CSS se quiser um fundo vermelho
        errorLabel.setVisible(false);
        errorLabel.setManaged(false);

        // Montagem Final dentro de um Card
        VBox card = new VBox(10, headerBox, toolbar, errorLabel);
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

    public void renderTree(List<InventoryRow> rows, ScanSummary scan) {
        if (rows == null || rows.isEmpty()) {
            tree.setRoot(null);
            tree.setPlaceholder(new Label("Selecione um backup para visualizar os arquivos."));
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

    // Getters
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
        btnRefresh.setDisable(value);
        btnRestoreSnapshot.setDisable(value);
        scanList.setDisable(value);
    }

    public void showFilesWindow(List<InventoryRow> rows, ScanSummary scan, Consumer<List<SelectedNode>> onRestore) {
        if (rows == null) rows = List.of();
        renderTree(rows, scan);

        Label title = new Label("Arquivos do backup");
        title.getStyleClass().add("section-title");

        Label subtitle = new Label(scan == null ? "" : ("Backup #" + scan.scanId() + " • " + formatDate(scan.finishedAt())));
        subtitle.getStyleClass().add("page-subtitle");

        Button restoreBtn = new Button("Restaurar selecionados");
        restoreBtn.getStyleClass().addAll("btn", "btn-primary");
        restoreBtn.setOnAction(e -> {
            List<SelectedNode> selected = getSelectedNodes();
            if (selected.isEmpty()) {
                showError("Selecione arquivos/pastas (até 10) para restaurar.");
                return;
            }
            if (onRestore != null) onRestore.accept(selected);
        });

        VBox box = new VBox(10, title, subtitle, tree, restoreBtn);
        box.setPadding(new Insets(14));
        VBox.setVgrow(tree, Priority.ALWAYS);

        Stage stage = new Stage();
        stage.setTitle("Arquivos do backup");
        Window owner = rootNode().getScene() != null ? rootNode().getScene().getWindow() : null;
        if (owner != null) stage.initOwner(owner);
        stage.initModality(Modality.NONE);

        Scene scene = new Scene(box, 820, 520);
        try {
            scene.getStylesheets().add(getClass().getResource("/styles.css").toExternalForm());
        } catch (Exception ignored) {}
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

    @SuppressWarnings("unused")
    public record FileNode(@SuppressWarnings("unused") String name,
                           @SuppressWarnings("unused") String path,
                           @SuppressWarnings("unused") boolean directory,
                           @SuppressWarnings("unused") long sizeBytes,
                           @SuppressWarnings("unused") String status,
                           @SuppressWarnings("unused") long modifiedMillis) {}
    public record SelectedNode(String pathRel, boolean directory) {}
    
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
                getListView().getSelectionModel().select(item);
                btnRestoreSnapshot.fire();
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
