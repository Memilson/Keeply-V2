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

        view.scanSelector().valueProperty().addListener((obs, oldScan, newScan) -> {
            if (suppressSelection) return;
            if (newScan != null) loadSnapshot(newScan);
        });

        view.onFileSelected(path -> {});
        view.onShowHistory(this::loadDialogHistory);
    }

    private void restoreSnapshot() {
        ScanSummary scan = view.scanSelector().getValue();
        if (scan == null) {
            view.showError("Selecione um Snapshot antes de restaurar.");
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

        DirectoryChooser chooser = new DirectoryChooser();
        chooser.setTitle("Restaurar Snapshot #" + scan.scanId() + " (somente NEW/MODIFIED)");

        File initialDir = new File(Config.getLastPath());
        if (initialDir.exists() && initialDir.isDirectory()) {
            chooser.setInitialDirectory(initialDir);
        }

        Window window = view.exportMenuButton().getScene() != null ? view.exportMenuButton().getScene().getWindow() : null;
        File destDir = chooser.showDialog(window);
        if (destDir == null) return;

        Config.saveLastPath(destDir.getAbsolutePath());

        Window owner = view.exportMenuButton().getScene() != null ? view.exportMenuButton().getScene().getWindow() : null;
        RestoreLogWindow log = RestoreLogWindow.open(owner, "Restaurando Snapshot #" + scan.scanId());
        log.appendLine(">> Snapshot #" + scan.scanId() + " (somente NEW/MODIFIED)");

        Path destinationDir = destDir.toPath();

        Thread.ofVirtual().name("keeply-restore-snapshot").start(() -> {
            try {
                BlobStore.RestoreResult result = BlobStore.restoreChangedFilesFromScan(
                        scan.scanId(),
                        baseDir,
                        destinationDir,
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
        ScanSummary scan = view.scanSelector().getValue();
        if (scan == null) {
            view.showError("Selecione um Snapshot antes de restaurar.");
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

        DirectoryChooser chooser = new DirectoryChooser();
        chooser.setTitle("Restaurar Selecionados (Snapshot #" + scan.scanId() + ")");

        File initialDir = new File(Config.getLastPath());
        if (initialDir.exists() && initialDir.isDirectory()) {
            chooser.setInitialDirectory(initialDir);
        }

        Window window = view.exportMenuButton().getScene() != null ? view.exportMenuButton().getScene().getWindow() : null;
        File destDir = chooser.showDialog(window);
        if (destDir == null) return;
        Config.saveLastPath(destDir.getAbsolutePath());

        List<String> filePaths = new ArrayList<>();
        List<String> dirPrefixes = new ArrayList<>();
        for (InventoryScreen.SelectedNode n : selected) {
            if (n == null) continue;
            String p = n.pathRel();
            if (p == null || p.isBlank()) continue;
            if (n.directory()) dirPrefixes.add(p);
            else filePaths.add(p);
        }

        Window owner = view.exportMenuButton().getScene() != null ? view.exportMenuButton().getScene().getWindow() : null;
        RestoreLogWindow log = RestoreLogWindow.open(owner, "Restaurando Seleção (Snapshot #" + scan.scanId() + ")");
        log.appendLine(">> Snapshot #" + scan.scanId());
        log.appendLine(">> Seleção: arquivos=" + filePaths.size() + ", pastas=" + dirPrefixes.size());

        Path destinationDir = destDir.toPath();

        Thread.ofVirtual().name("keeply-restore-selected").start(() -> {
            try {
                BlobStore.RestoreResult result = BlobStore.restoreSelectionFromSnapshot(
                        scan.scanId(),
                        filePaths,
                        dirPrefixes,
                        baseDir,
                        destinationDir,
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
                var reports = jdbi.withExtension(KeeplyDao.class, KeeplyDao::predictGrowth);

                printCapacityAnalysis(reports);

                Platform.runLater(() -> {
                    view.showLoading(false);
                    
                    suppressSelection = true;
                    view.scanSelector().getItems().setAll(allScans);
                    
                    if (!allScans.isEmpty()) {
                        view.scanSelector().getSelectionModel().select(0); 
                        suppressSelection = false;
                        loadSnapshot(allScans.get(0));
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
                Platform.runLater(() -> { view.showLoading(false); view.showError("Erro Snapshot: " + e.getMessage()); });
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
