package com.keeply.app.controller;

import com.keeply.app.Database;
// IMPORTANTE: Importando as classes estáticas dentro de Database
import com.keeply.app.Database.InventoryRow;
import com.keeply.app.Database.ScanSummary;
import com.keeply.app.Database.CapacityReport;
import com.keeply.app.view.InventoryScreen;
import javafx.application.Platform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private void wire() {
        view.refreshButton().setOnAction(e -> refresh());
        view.expandButton().setOnAction(e -> view.expandAll());
        view.collapseButton().setOnAction(e -> view.collapseAll());
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
            try (var conn = Database.openSingleConnection()) {
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
            try (var conn = Database.openSingleConnection()) {
                var snapshotRows = Database.fetchSnapshotFiles(conn, scan.scanId());
                this.allRows = snapshotRows;
                this.currentScanData = scan;
                Platform.runLater(() -> {
                    view.showLoading(false);
                    view.renderTree(snapshotRows, scan);
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

    private void loadDialogHistory(String pathRel) {
        if (pathRel == null) return;
        Thread.ofVirtual().start(() -> {
            try (var conn = Database.openSingleConnection()) {
                var rows = Database.fetchFileHistory(conn, pathRel);
                Platform.runLater(() -> view.showHistoryDialog(rows, pathRel));
            } catch (Exception e) { logger.error("Erro ao carregar histórico", e); }
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
    
    private static String safeMsg(Throwable t) {
        return (t.getMessage() != null) ? t.getMessage() : t.getClass().getSimpleName();
    }
}