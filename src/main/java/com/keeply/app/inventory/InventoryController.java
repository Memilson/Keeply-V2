package com.keeply.app.inventory;

import com.keeply.app.config.Config;
import com.keeply.app.blob.BlobStore;
import com.keeply.app.blob.RestoreLogWindow;
import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.database.KeeplyDao;
import com.keeply.app.database.DatabaseBackup.CapacityReport;
import com.keeply.app.database.DatabaseBackup.InventoryRow;
import com.keeply.app.database.DatabaseBackup.ScanSummary;
import com.keeply.app.report.ReportService;

import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.PasswordField;
import javafx.scene.control.RadioButton;
import javafx.scene.control.ToggleGroup;
import javafx.scene.layout.VBox;
import javafx.geometry.Insets;
import javafx.stage.DirectoryChooser;
import javafx.stage.FileChooser;
import javafx.stage.Window;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Controla inventário, exportação e restauração.
 */
public final class InventoryController {

    private final InventoryScreen view;
    private List<InventoryRow> allRows = new ArrayList<>();
    private ScanSummary currentScanData = null;
    private boolean suppressSelection = false;

    public InventoryController(InventoryScreen view) {
        this.view = view;
        wire();
        refresh();
    }

    private static final Logger logger = LoggerFactory.getLogger(InventoryController.class);
    private static final DateTimeFormatter EXPORT_TS = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private void wire() {
        view.refreshButton().setOnAction(e -> refresh());
        view.expandButton().setOnAction(e -> view.expandAll());
        view.collapseButton().setOnAction(e -> view.collapseAll());
        view.restoreSnapshotButton().setOnAction(e -> restoreSnapshot());
        view.onRestoreSelected(this::restoreSelected);
        view.exportCsvItem().setOnAction(e -> exportCsv());
        view.exportPdfItem().setOnAction(e -> exportPdf());
        view.searchField().textProperty().addListener((obs, oldVal, newVal) -> applyFilter(newVal));

        view.scanList().getSelectionModel().selectedItemProperty().addListener((obs, oldScan, newScan) -> {
            if (suppressSelection) return;
            if (newScan != null && newScan.scanId() != null) {
                loadSnapshot(new ScanSummary(newScan.scanId(), newScan.rootPath(), newScan.startedAt(), newScan.finishedAt()));
            }
        });

        view.onFileSelected(path -> {});
        view.onShowHistory(this::loadDialogHistory);
    }

    private void restoreSnapshot() {
        var row = view.scanList().getSelectionModel().getSelectedItem();
        ScanSummary scan = (row == null || row.scanId() == null)
                ? null
                : new ScanSummary(row.scanId(), row.rootPath(), row.startedAt(), row.finishedAt());
        if (scan == null) {
            view.showError("Selecione um backup antes de restaurar.");
            return;
        }

        String baseDirRaw = Config.getLastBackupDestination();
        if (baseDirRaw == null || baseDirRaw.isBlank()) {
            view.showError("Destino de Backup não configurado. Rode um Backup primeiro.");
            return;
        }

        Path baseDir;
        try {
            baseDir = Path.of(baseDirRaw);
        } catch (Exception e) {
            view.showError("Destino de Backup inválido: " + baseDirRaw);
            return;
        }

        RestoreOptions options = promptRestoreOptions("Restaurar Backup #" + scan.scanId());
        if (options == null) return;
        Config.setBackupEncryptionPassword(options.password());

        Path destinationDir = null;
        Path originalRoot = null;
        if (options.mode() == BlobStore.RestoreMode.ORIGINAL_PATH) {
            if (scan.rootPath() == null || scan.rootPath().isBlank()) {
                view.showError("Caminho original não disponível neste snapshot.");
                return;
            }
            originalRoot = Path.of(scan.rootPath());
        } else {
            DirectoryChooser chooser = new DirectoryChooser();
            chooser.setTitle("Restaurar Backup #" + scan.scanId() + " (somente NEW/MODIFIED)");

            File initialDir = new File(Config.getLastPath());
            if (initialDir.exists() && initialDir.isDirectory()) {
                chooser.setInitialDirectory(initialDir);
            }

            Window window = view.exportMenuButton().getScene() != null ? view.exportMenuButton().getScene().getWindow() : null;
            File destDir = chooser.showDialog(window);
            if (destDir == null) return;

            Config.saveLastPath(destDir.getAbsolutePath());
            destinationDir = destDir.toPath();
        }

        final Path finalDestinationDir = destinationDir;
        final Path finalOriginalRoot = originalRoot;
        final BlobStore.RestoreMode finalMode = options.mode();
        Window owner = view.exportMenuButton().getScene() != null ? view.exportMenuButton().getScene().getWindow() : null;
        RestoreLogWindow log = RestoreLogWindow.open(owner, "Restaurando Backup #" + scan.scanId());
        log.appendLine(">> Backup #" + scan.scanId() + " (somente NEW/MODIFIED)");

        Thread.ofVirtual().name("keeply-restore-snapshot").start(() -> {
            try {
                BlobStore.RestoreResult result = BlobStore.restoreChangedFilesFromScan(
                        scan.scanId(),
                        baseDir,
                        finalDestinationDir,
                        finalOriginalRoot,
                        finalMode,
                        log.cancelFlag(),
                        log.logger()
                );

                log.markDoneOk(">> Restore concluído: arquivos=" + result.filesRestored() + ", erros=" + result.errors());
            } catch (Exception e) {
                logger.error("Erro ao restaurar snapshot", e);
                log.markDoneError(">> Erro ao restaurar snapshot: " + e.getMessage());
                Platform.runLater(() -> view.showError("Erro ao restaurar snapshot: " + e.getMessage()));
            }
        });
    }

    private void restoreSelected() {
        var row = view.scanList().getSelectionModel().getSelectedItem();
        ScanSummary scan = (row == null || row.scanId() == null)
                ? null
                : new ScanSummary(row.scanId(), row.rootPath(), row.startedAt(), row.finishedAt());
        if (scan == null) {
            view.showError("Selecione um backup antes de restaurar.");
            return;
        }

        List<InventoryScreen.SelectedNode> selected = view.getSelectedNodes();
        if (selected == null || selected.isEmpty()) {
            view.showError("Selecione arquivos/pastas (até 10) para restaurar.");
            return;
        }

        if (selected.size() > 10) {
            view.showError("Selecione no máximo 10 itens para restaurar. Selecionados=" + selected.size());
            return;
        }

        String baseDirRaw = Config.getLastBackupDestination();
        if (baseDirRaw == null || baseDirRaw.isBlank()) {
            view.showError("Destino de Backup não configurado. Rode um Backup primeiro.");
            return;
        }

        Path baseDir;
        try {
            baseDir = Path.of(baseDirRaw);
        } catch (Exception e) {
            view.showError("Destino de Backup inválido: " + baseDirRaw);
            return;
        }

        RestoreOptions options = promptRestoreOptions("Restaurar Selecionados (Backup #" + scan.scanId() + ")");
        if (options == null) return;
        Config.setBackupEncryptionPassword(options.password());

        Path destinationDir = null;
        Path originalRoot = null;
        if (options.mode() == BlobStore.RestoreMode.ORIGINAL_PATH) {
            if (scan.rootPath() == null || scan.rootPath().isBlank()) {
                view.showError("Caminho original não disponível neste snapshot.");
                return;
            }
            originalRoot = Path.of(scan.rootPath());
        } else {
            DirectoryChooser chooser = new DirectoryChooser();
            chooser.setTitle("Restaurar Selecionados (Backup #" + scan.scanId() + ")");

            File initialDir = new File(Config.getLastPath());
            if (initialDir.exists() && initialDir.isDirectory()) {
                chooser.setInitialDirectory(initialDir);
            }

            Window window = view.exportMenuButton().getScene() != null ? view.exportMenuButton().getScene().getWindow() : null;
            File destDir = chooser.showDialog(window);
            if (destDir == null) return;
            Config.saveLastPath(destDir.getAbsolutePath());
            destinationDir = destDir.toPath();
        }

        List<String> filePaths = new ArrayList<>();
        List<String> dirPrefixes = new ArrayList<>();
        for (InventoryScreen.SelectedNode n : selected) {
            if (n == null) continue;
            String p = n.pathRel();
            if (p == null || p.isBlank()) continue;
            if (n.directory()) dirPrefixes.add(p);
            else filePaths.add(p);
        }

        final Path finalDestinationDir = destinationDir;
        final Path finalOriginalRoot = originalRoot;
        final BlobStore.RestoreMode finalMode = options.mode();
        Window owner = view.exportMenuButton().getScene() != null ? view.exportMenuButton().getScene().getWindow() : null;
        RestoreLogWindow log = RestoreLogWindow.open(owner, "Restaurando Seleção (Backup #" + scan.scanId() + ")");
        log.appendLine(">> Backup #" + scan.scanId());
        log.appendLine(">> Seleção: arquivos=" + filePaths.size() + ", pastas=" + dirPrefixes.size());

        Thread.ofVirtual().name("keeply-restore-selected").start(() -> {
            try {
                BlobStore.RestoreResult result = BlobStore.restoreSelectionFromSnapshot(
                        scan.scanId(),
                        filePaths,
                        dirPrefixes,
                        baseDir,
                        finalDestinationDir,
                        finalOriginalRoot,
                        finalMode,
                        log.cancelFlag(),
                        log.logger()
                );
                log.markDoneOk(">> Restore concluído: arquivos=" + result.filesRestored() + ", erros=" + result.errors());
            } catch (Exception e) {
                logger.error("Erro ao restaurar selecionados", e);
                log.markDoneError(">> Erro ao restaurar selecionados: " + e.getMessage());
                Platform.runLater(() -> view.showError("Erro ao restaurar selecionados: " + e.getMessage()));
            }
        });
    }

    private void refresh() {
        view.showLoading(true);
        Thread.ofVirtual().name("keeply-refresh").start(() -> {
            try {
                DatabaseBackup.init();
                var jdbi = DatabaseBackup.jdbi();
                var rows = jdbi.withExtension(KeeplyDao.class, KeeplyDao::fetchInventory);
                var lastScan = jdbi.withExtension(KeeplyDao.class, dao -> dao.fetchLastScan().orElse(null));
                var allScans = jdbi.withExtension(KeeplyDao.class, KeeplyDao::fetchAllScans);
                var history = com.keeply.app.history.BackupHistoryDb.listRecent(200);
                var orderedHistory = new java.util.ArrayList<>(history);
                orderedHistory.sort((a, b) -> {
                    int ra = "FULL".equalsIgnoreCase(a.backupType()) ? 1 : 0;
                    int rb = "FULL".equalsIgnoreCase(b.backupType()) ? 1 : 0;
                    if (ra != rb) return Integer.compare(ra, rb);
                    long ta = parseTs(a.finishedAt() != null ? a.finishedAt() : a.startedAt());
                    long tb = parseTs(b.finishedAt() != null ? b.finishedAt() : b.startedAt());
                    return Long.compare(tb, ta);
                });
                var reports = jdbi.withExtension(KeeplyDao.class, KeeplyDao::predictGrowth);

                printCapacityAnalysis(reports);

                Platform.runLater(() -> {
                    view.showLoading(false);
                    
                    suppressSelection = true;
                    view.scanList().getItems().setAll(orderedHistory);
                    
                    if (!orderedHistory.isEmpty()) {
                        suppressSelection = false;
                        this.allRows = List.of();
                        this.currentScanData = null;
                        view.renderTree(List.of(), null);
                        view.renderLargest(List.of());
                    } else {
                        suppressSelection = false;
                        this.allRows = rows;
                        this.currentScanData = lastScan;
                        view.renderTree(rows, lastScan);
                        view.renderLargest(rows);
                    }
                });
            } catch (Exception e) {
                logger.error("Erro ao atualizar inventário", e);
                Platform.runLater(() -> { view.showLoading(false); view.showError("Erro: " + e.getMessage()); });
            }
        });
    }

    private RestoreOptions promptRestoreOptions(String title) {
        Dialog<RestoreOptions> dialog = new Dialog<>();
        dialog.setTitle(title);
        dialog.setHeaderText("Escolha o modo de restauração e informe a senha.");
        ButtonType ok = new ButtonType("OK", ButtonBar.ButtonData.OK_DONE);
        dialog.getDialogPane().getButtonTypes().addAll(ok, ButtonType.CANCEL);

        ToggleGroup group = new ToggleGroup();
        RadioButton original = new RadioButton("Restaurar no caminho original");
        original.setToggleGroup(group);
        original.setUserData(BlobStore.RestoreMode.ORIGINAL_PATH);

        RadioButton withStructure = new RadioButton("Restaurar mantendo a estrutura de pastas");
        withStructure.setToggleGroup(group);
        withStructure.setUserData(BlobStore.RestoreMode.DEST_WITH_STRUCTURE);

        RadioButton flat = new RadioButton("Restaurar somente no destino (sem estrutura)");
        flat.setToggleGroup(group);
        flat.setUserData(BlobStore.RestoreMode.DEST_FLAT);

        withStructure.setSelected(true);

        PasswordField field = new PasswordField();
        field.setPromptText("Senha do backup");

        VBox content = new VBox(10, original, withStructure, flat, field);
        content.setPadding(new Insets(8));
        dialog.getDialogPane().setContent(content);

        var okButton = dialog.getDialogPane().lookupButton(ok);
        okButton.disableProperty().bind(Bindings.createBooleanBinding(() -> {
            String v = field.getText();
            return v == null || v.isBlank();
        }, field.textProperty()));

        dialog.setResultConverter(btn -> {
            if (btn != ok) return null;
            var toggle = group.getSelectedToggle();
            if (toggle == null) return null;
            return new RestoreOptions((BlobStore.RestoreMode) toggle.getUserData(), field.getText());
        });

        return dialog.showAndWait().orElse(null);
    }

    private record RestoreOptions(BlobStore.RestoreMode mode, String password) {}

    private void loadSnapshot(ScanSummary scan) {
        view.showLoading(true);
        Thread.ofVirtual().name("keeply-snapshot").start(() -> {
            try {
                DatabaseBackup.init();
                var snapshotRows = DatabaseBackup.jdbi().withExtension(
                        KeeplyDao.class,
                        dao -> dao.fetchSnapshotFiles(scan.scanId())
                );
                this.allRows = snapshotRows;
                this.currentScanData = scan;
                Platform.runLater(() -> {
                    view.showLoading(false);
                    view.renderTree(snapshotRows, scan);
                    view.renderLargest(snapshotRows);
                    applyFilter(view.searchField().getText());
                });
            } catch (Exception e) {
                logger.error("Erro ao carregar snapshot", e);
                Platform.runLater(() -> { view.showLoading(false); view.showError("Erro no backup: " + e.getMessage()); });
            }
        });
    }

    private void applyFilter(String query) {
        if (allRows == null || allRows.isEmpty()) return;
        if (query == null || query.isBlank()) { view.renderTree(allRows, currentScanData); return; }
        
        String term = query.toLowerCase(Locale.ROOT);
        var filtered = allRows.stream()
                .filter(r -> r.pathRel().toLowerCase().contains(term) || r.name().toLowerCase().contains(term))
                .toList();
                
        view.renderTree(filtered, currentScanData);
        if (!filtered.isEmpty()) view.expandAll();
    }


    private void exportCsv() {
        File file = chooseExportFile("CSV", ".csv", "inventario");
        if (file == null) return;
        exportToCsv(allRows, file);
    }

    private void exportPdf() {
        File file = chooseExportFile("PDF", ".pdf", "relatorio");
        if (file == null) return;
        try {
            new ReportService().exportPdf(allRows, file, currentScanData);
        } catch (Exception e) {
            logger.error("Erro ao exportar PDF", e);
            Platform.runLater(() -> view.showError("Erro ao exportar PDF: " + e.getMessage()));
        }
    }

    private File chooseExportFile(String label, String extension, String baseName) {
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Exportar " + label);
        chooser.getExtensionFilters().add(new FileChooser.ExtensionFilter(label + " (*" + extension + ")", "*" + extension));
        String stamp = LocalDateTime.now().format(EXPORT_TS);
        chooser.setInitialFileName(baseName + "-" + stamp + extension);
        File initialDir = new File(Config.getLastPath());
        if (initialDir.exists() && initialDir.isDirectory()) {
            chooser.setInitialDirectory(initialDir);
        }
        Window window = view.exportMenuButton().getScene() != null ? view.exportMenuButton().getScene().getWindow() : null;
        File file = chooser.showSaveDialog(window);
        if (file == null) return null;
        file = ensureExtension(file, extension);
        if (file.getParentFile() != null) {
            Config.saveLastPath(file.getParentFile().getAbsolutePath());
        }
        return file;
    }

    private static long parseTs(String s) {
        if (s == null || s.isBlank()) return 0L;
        try {
            return java.time.Instant.parse(s).toEpochMilli();
        } catch (Exception e) {
            return 0L;
        }
    }

    private static File ensureExtension(File file, String extension) {
        String name = file.getName();
        if (!name.toLowerCase().endsWith(extension)) {
            return new File(file.getParentFile(), name + extension);
        }
        return file;
    }

    public void exportToCsv(List<InventoryRow> rows, File file) {
        if (rows == null || file == null) {
            logger.warn("Export cancelado: dados ou arquivo ausente.");
            return;
        }
        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setHeader("Caminho", "Nome", "Tamanho (Bytes)", "Estado", "Criado Em", "Modificado Em")
                .build();
        try (var writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8);
             var printer = new CSVPrinter(writer, format)) {
            for (var row : rows) {
                printer.printRecord(
                        row.pathRel(),
                        row.name(),
                        row.sizeBytes(),
                        row.status(),
                        formatInstant(row.createdMillis()),
                        formatInstant(row.modifiedMillis())
                );
            }
        } catch (IOException e) {
            logger.error("Erro ao exportar", e);
        }
    }

    private static String formatInstant(long millis) {
        if (millis <= 0) return "";
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .format(LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault()));
    }

    private void loadDialogHistory(String pathRel) {
        if (pathRel == null) return;
        Thread.ofVirtual().start(() -> {
            try {
                DatabaseBackup.init();
                var rows = DatabaseBackup.jdbi().withExtension(
                        KeeplyDao.class,
                        dao -> dao.fetchFileHistory(pathRel)
                );
                Platform.runLater(() -> view.showHistoryDialog(rows, pathRel));
            } catch (Exception e) {
                logger.error("Erro ao carregar histórico", e);
            }
        });
    }

    private void printCapacityAnalysis(List<CapacityReport> reports) {
        if (reports == null || reports.isEmpty()) return;
        logger.info("=== RELATÓRIO DE CAPACIDADE ===");
        var latest = reports.get(0);
        double gb = latest.totalBytes() / (1024.0 * 1024.0 * 1024.0);
        logger.info(String.format("Total: %.2f GB", gb));
        
        if (latest.growthBytes() > 0) logger.info(String.format("Crescimento: +%.2f MB", latest.growthBytes() / (1024.0 * 1024.0)));
        else if (latest.growthBytes() < 0) logger.info(String.format("Redução: %.2f MB", latest.growthBytes() / (1024.0 * 1024.0)));
        else logger.info("Estável.");
        logger.info("==================================");
    }
    
}
