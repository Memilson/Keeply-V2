package com.keeply.app.controller;

import com.keeply.app.Database;
import com.keeply.app.view.InventoryScreen;
import javafx.application.Platform;

import java.util.ArrayList;

public final class InventoryController {

    private final InventoryScreen view;

    public InventoryController(InventoryScreen view) {
        this.view = view;
        wire();
        refresh();
    }

    private void wire() {
        view.refreshButton().setOnAction(e -> refresh());
        view.expandButton().setOnAction(e -> view.expandAll());
        view.collapseButton().setOnAction(e -> view.collapseAll());
        view.onFileSelected(this::loadHistory);
    }

    private void refresh() {
        view.showLoading(true);
        Thread.ofVirtual().name("keeply-inventory-refresh").start(() -> {
            try (var conn = Database.openSingleConnection()) {
                Database.ensureSchema(conn);
                var rows = Database.fetchInventory(conn);
                var scan = Database.fetchLastScan(conn);
                Platform.runLater(() -> {
                    view.showLoading(false);
                    view.renderTree(rows, scan);
                    view.renderHistory(new ArrayList<>(), null);
                });
            } catch (Exception e) {
                Platform.runLater(() -> {
                    view.showLoading(false);
                    view.showError("Erro ao carregar inventário: " + safeMsg(e));
                });
            }
        });
    }

    private void loadHistory(String pathRel) {
        if (pathRel == null || pathRel.isBlank()) {
            view.renderHistory(new ArrayList<>(), null);
            return;
        }
        view.showHistoryLoading(true);
        Thread.ofVirtual().name("keeply-history-load").start(() -> {
            try (var conn = Database.openSingleConnection()) {
                var rows = Database.fetchFileHistory(conn, pathRel);
                Platform.runLater(() -> {
                    view.showHistoryLoading(false);
                    view.renderHistory(rows, pathRel);
                });
            } catch (Exception e) {
                Platform.runLater(() -> {
                    view.showHistoryLoading(false);
                    view.showError("Erro ao carregar histórico: " + safeMsg(e));
                });
            }
        });
    }

    private static String safeMsg(Throwable t) {
        return (t.getMessage() != null) ? t.getMessage() : t.getClass().getSimpleName();
    }
}
