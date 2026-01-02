package com.keeply.app.controller;

import com.keeply.app.Database;
import com.keeply.app.Config;
import com.keeply.app.report.ReportService;
// IMPORTANTE: Importando as classes estáticas dentro de Database
import com.keeply.app.Database.InventoryRow;
import com.keeply.app.Database.ScanSummary;
import com.keeply.app.Database.CapacityReport;
import com.keeply.app.view.InventoryScreen;
import javafx.application.Platform;
import javafx.stage.FileChooser;
import javafx.stage.Window;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

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
        view.exportCsvItem().setOnAction(e -> exportCsv());
        view.exportPdfItem().setOnAction(e -> exportPdf());
        view.searchField().textProperty().addListener((obs, oldVal, newVal) -> applyFilter(newVal));

        view.scanSelector().valueProperty().addListener((obs, oldScan, newScan) -> {
            if (suppressSelection) return;
            if (newScan != null) loadSnapshot(newScan);
        });

        // Evento vazio para seleção simples
        view.onFileSelected(path -> {}); 
        
        // Evento de menu de contexto -> Abre Dialog
        view.onShowHistory(this::loadDialogHistory);
    }

    private void refresh() {
        view.showLoading(true);
        Thread.ofVirtual().name("keeply-refresh").start(() -> {
            Connection conn = null;
            try {
                conn = Database.openSingleConnection();
                Database.ensureSchema(conn);
                var rows = Database.fetchInventory(conn);
                var lastScan = Database.fetchLastScan(conn);
                var allScans = Database.fetchAllScans(conn);
                var reports = Database.predictGrowth(conn);

                printCapacityAnalysis(reports);

                Platform.runLater(() -> {
                    view.showLoading(false);
                    
                    // Atualiza Combo sem disparar evento
                    suppressSelection = true;
                    view.scanSelector().getItems().setAll(allScans);
                    
                    if (!allScans.isEmpty()) {
                        // Seleciona o último e carrega via Snapshot (consistência)
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
            } finally {
                Database.safeClose(conn);
            }
        });
    }

    private void loadSnapshot(ScanSummary scan) {
        view.showLoading(true);
        Thread.ofVirtual().name("keeply-snapshot").start(() -> {
            Connection conn = null;
            try {
                conn = Database.openSingleConnection();
                var snapshotRows = Database.fetchSnapshotFiles(conn, scan.scanId());
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
            } finally {
                Database.safeClose(conn);
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
        try (var writer = new PrintWriter(file, StandardCharsets.UTF_8)) {
            writer.println("Caminho,Nome,Tamanho (Bytes),Estado,Criado Em,Modificado Em");
            for (var row : rows) {
                String line = String.format("%s,%s,%d,%s,%s,%s",
                    csvEscape(row.pathRel()),
                    csvEscape(row.name()),
                    row.sizeBytes(),
                    csvEscape(row.status()),
                    csvEscape(formatInstant(row.createdMillis())),
                    csvEscape(formatInstant(row.modifiedMillis())));
                writer.println(line);
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

    private static String csvEscape(String value) {
        if (value == null) return "\"\"";
        String cleaned = value.replace("\"", "\"\"");
        cleaned = cleaned.replace("\r", " ").replace("\n", " ");
        return "\"" + cleaned + "\"";
    }

    private void loadDialogHistory(String pathRel) {
        if (pathRel == null) return;
        Thread.ofVirtual().start(() -> {
            Connection conn = null;
            try {
                conn = Database.openSingleConnection();
                var rows = Database.fetchFileHistory(conn, pathRel);
                Platform.runLater(() -> view.showHistoryDialog(rows, pathRel));
            } catch (Exception e) {
                logger.error("Erro ao carregar histórico", e);
            } finally {
                Database.safeClose(conn);
            }
        });
    }

    private void printCapacityAnalysis(List<CapacityReport> reports) {
        if (reports == null || reports.isEmpty()) return;
        logger.info("=== RELATÓRIO DE CAPACIDADE ===");
        var latest = reports.get(0);
        double gb = latest.totalBytes() / (1024.0 * 1024.0 * 1024.0);
        logger.info(String.format("Total: %.2f GB", gb));
        
        // Uso dos records diretos, sem getters
        if (latest.growthBytes() > 0) logger.info(String.format("Crescimento: +%.2f MB", latest.growthBytes() / (1024.0 * 1024.0)));
        else if (latest.growthBytes() < 0) logger.info(String.format("Redução: %.2f MB", latest.growthBytes() / (1024.0 * 1024.0)));
        else logger.info("Estável.");
        logger.info("==================================");
    }
    
}


