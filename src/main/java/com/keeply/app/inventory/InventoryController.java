package com.keeply.app.inventory;

import java.io.File;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.keeply.app.blob.BlobStore;
import com.keeply.app.blob.RestoreLogWindow;
import com.keeply.app.config.Config;
import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.database.DatabaseBackup.ScanSummary;
import com.keeply.app.database.KeeplyDao;

import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.geometry.Insets;
import javafx.scene.control.Alert;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.PasswordField;
import javafx.scene.control.RadioButton;
import javafx.scene.control.ToggleGroup;
import javafx.scene.layout.VBox;
import javafx.stage.DirectoryChooser;
import javafx.stage.Window;

/**
 * Controla histórico de backups e restauração.
 */
public final class InventoryController {

    private final InventoryScreen view;
    public InventoryController(InventoryScreen view) {
        this.view = view;
        wire();
        refresh();
    }

    private static final Logger logger = LoggerFactory.getLogger(InventoryController.class);
    private void wire() {
        view.refreshButton().setOnAction(e -> refresh());
        view.restoreSnapshotButton().setOnAction(e -> restoreSnapshot());
        view.onRestoreSelected(this::restoreSelected);

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

        RestoreChoice choice = promptRestoreChoice();
        if (choice == RestoreChoice.CANCEL) return;
        if (choice == RestoreChoice.SELECTED) {
            openFilesWindow(scan);
            return;
        }
        restoreFull(scan);
    }

    private void restoreSelected() {
        view.showError("Use 'Recuperar arquivos/pastas' no histórico para escolher itens.");
    }

    private void openFilesWindow(ScanSummary scan) {
        Thread.ofVirtual().name("keeply-files-window").start(() -> {
            try {
                DatabaseBackup.init();
                var rows = DatabaseBackup.jdbi().withExtension(
                        KeeplyDao.class,
                        dao -> dao.fetchSnapshotFiles(scan.scanId())
                );
                Platform.runLater(() -> view.showFilesWindow(rows, scan, selected -> restoreSelectedForScan(scan, selected)));
            } catch (RuntimeException e) {
                Platform.runLater(() -> view.showError("Erro ao abrir arquivos: " + e.getMessage()));
            }
        });
    }

    private void restoreFull(ScanSummary scan) {
        String baseDirRaw = Config.getLastBackupDestination();
        if (baseDirRaw == null || baseDirRaw.isBlank()) {
            view.showError("Destino de Backup não configurado. Rode um Backup primeiro.");
            return;
        }

        Path baseDir;
        try {
            baseDir = Path.of(baseDirRaw);
        } catch (java.nio.file.InvalidPathException e) {
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

            Window window = view.rootNode().getScene() != null ? view.rootNode().getScene().getWindow() : null;
            File destDir = chooser.showDialog(window);
            if (destDir == null) return;

            Config.saveLastPath(destDir.getAbsolutePath());
            destinationDir = destDir.toPath();
        }

        final Path finalDestinationDir = destinationDir;
        final Path finalOriginalRoot = originalRoot;
        final BlobStore.RestoreMode finalMode = options.mode();
        Window owner = view.rootNode().getScene() != null ? view.rootNode().getScene().getWindow() : null;
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
            } catch (java.io.IOException | RuntimeException e) {
                logger.error("Erro ao restaurar snapshot", e);
                log.markDoneError(">> Erro ao restaurar snapshot: " + e.getMessage());
                Platform.runLater(() -> view.showError("Erro ao restaurar snapshot: " + e.getMessage()));
            }
        });
    }

    private void restoreSelectedForScan(ScanSummary scan, List<InventoryScreen.SelectedNode> selected) {
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
        } catch (java.nio.file.InvalidPathException e) {
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

            Window window = view.rootNode().getScene() != null ? view.rootNode().getScene().getWindow() : null;
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
        Window owner = view.rootNode().getScene() != null ? view.rootNode().getScene().getWindow() : null;
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
            } catch (java.io.IOException | RuntimeException e) {
                logger.error("Erro ao restaurar selecionados", e);
                log.markDoneError(">> Erro ao restaurar selecionados: " + e.getMessage());
                Platform.runLater(() -> view.showError("Erro ao restaurar selecionados: " + e.getMessage()));
            }
        });
    }

    private RestoreChoice promptRestoreChoice() {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Restaurar backup");
        alert.setHeaderText("Escolha o tipo de restauração");
        ButtonType full = new ButtonType("Restaurar backup completo", ButtonBar.ButtonData.OK_DONE);
        ButtonType select = new ButtonType("Selecionar arquivos/pastas", ButtonBar.ButtonData.OTHER);
        alert.getButtonTypes().setAll(full, select, ButtonType.CANCEL);
        ButtonType res = alert.showAndWait().orElse(ButtonType.CANCEL);
        if (res == full) return RestoreChoice.FULL;
        if (res == select) return RestoreChoice.SELECTED;
        return RestoreChoice.CANCEL;
    }

    private enum RestoreChoice { FULL, SELECTED, CANCEL }

    private void refresh() {
        view.showLoading(true);
        Thread.ofVirtual().name("keeply-refresh").start(() -> {
            try {
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

                Platform.runLater(() -> {
                    view.showLoading(false);
                    
                    view.scanList().getItems().setAll(orderedHistory);
                    
                    if (!orderedHistory.isEmpty()) {
                        view.renderTree(List.of(), null);
                        var latest = orderedHistory.get(0);
                        view.setMeta(buildMeta(latest));
                    } else {
                        view.setMeta("Nenhum backup encontrado.");
                        view.renderTree(List.of(), null);
                    }
                });
            } catch (RuntimeException e) {
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

    @SuppressWarnings("unused")
    private record RestoreOptions(@SuppressWarnings("unused") BlobStore.RestoreMode mode,
                                  @SuppressWarnings("unused") String password) {}

    private static long parseTs(String s) {
        if (s == null || s.isBlank()) return 0L;
        try {
            return java.time.Instant.parse(s).toEpochMilli();
        } catch (java.time.format.DateTimeParseException e) {
            return 0L;
        }
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
            } catch (RuntimeException e) {
                logger.error("Erro ao carregar histórico", e);
            }
        });
    }

    private static String buildMeta(com.keeply.app.history.BackupHistoryDb.HistoryRow row) {
        if (row == null) return "";
        String tsRaw = row.finishedAt() != null ? row.finishedAt() : row.startedAt();
        String ts = formatDisplayDate(tsRaw);
        long files = row.filesProcessed();
        return "Último backup: " + ts + " • " + files + " itens";
    }

    private static String formatDisplayDate(String dt) {
        if (dt == null || dt.isBlank()) return "-";
        try {
            return DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm")
                    .format(Instant.parse(dt).atZone(ZoneId.systemDefault()));
        } catch (java.time.format.DateTimeParseException e) {
            return dt.length() > 16 ? dt.substring(0, 16) : dt;
        }
    }
}
