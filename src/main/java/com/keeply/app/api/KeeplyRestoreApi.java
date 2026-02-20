package com.keeply.app.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.keeply.app.blob.BlobStore;
import com.keeply.app.inventory.BackupHistoryDb;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

final class KeeplyRestoreApi {

    void handle(HttpExchange ex, KeeplyWsServer wsServer) throws IOException {
        if (!KeeplyHttp.isMethod(ex, "POST")) {
            KeeplyHttp.methodNotAllowed(ex, "POST");
            return;
        }

        JsonNode body = KeeplyHttp.readJsonBody(ex);

        RestoreRequest req = parseRequest(body);
        if (req == null) {
            KeeplyHttp.sendError(ex, 400, "bad_request", "Informe backupId/scanId numerico valido");
            return;
        }

        BackupHistoryDb.HistoryRow row = BackupHistoryDb.findByScanId(req.scanId());
        if (row == null) {
            KeeplyHttp.sendError(ex, 404, "not_found", "Nao encontrei backup_history para scanId informado");
            return;
        }
        if (KeeplyHttp.isBlank(row.destPath()) || KeeplyHttp.isBlank(row.rootPath())) {
            KeeplyHttp.sendError(ex, 400, "bad_request", "Backup sem root/dest no historico");
            return;
        }

        BlobStore.RestoreMode mode = parseMode(req.targetMode());
        if (mode == null) {
            KeeplyHttp.sendError(ex, 400, "bad_request", "targetMode invalido (use: original | dest)");
            return;
        }

        RestorePaths paths;
        try {
            paths = buildPaths(row, mode, req.targetPath());
        } catch (InvalidPathException ipe) {
            KeeplyHttp.sendError(ex, 400, "bad_request", "Caminho invalido: " + KeeplyHttp.safeMsg(ipe));
            return;
        } catch (IllegalArgumentException iae) {
            KeeplyHttp.sendError(ex, 400, "bad_request", KeeplyHttp.safeMsg(iae));
            return;
        } catch (Exception e) {
            // Aqui é mais “falha interna/corrupção de dados” do que erro do usuário
            KeeplyHttp.sendError(ex, 500, "internal_error", "Falha ao preparar paths: " + KeeplyHttp.safeMsg(e));
            return;
        }

        Instant now = Instant.now();
        KeeplyHttp.logInfo(
                "restore running scanId=" + req.scanId() +
                " mode=" + mode +
                " from " + KeeplyHttp.clientOf(ex) +
                " baseDir=" + paths.baseDir() +
                " target=" + (paths.destinationDir() == null ? paths.originalRoot() : paths.destinationDir()) +
                " deviceId=" + KeeplyHttp.nvl(req.deviceId())
        );

        broadcast(wsServer, "restore.running", Map.of(
                "scanId", req.scanId(),
                "mode", mode.name(),
                "ts", now.toString()
        ));

        try {
            long scanId = req.scanId();
            BlobStore.RestoreResult result = BlobStore.restoreChangedFilesFromScan(
                    scanId,
                    paths.baseDir(),
                    paths.destinationDir(),
                    paths.originalRoot(),
                    mode,
                    new AtomicBoolean(false),
                    msg -> KeeplyHttp.logInfo("restore[" + scanId + "] " + msg)
            );

            ObjectNode out = KeeplyHttp.mapper().createObjectNode();
            out.put("ok", true);
            out.put("scanId", scanId);
            out.put("mode", mode.name());
            out.put("filesRestored", result.filesRestored());
            out.put("errors", result.errors());
            KeeplyHttp.sendJson(ex, 200, out);

            broadcast(wsServer, "restore.success", Map.of(
                    "scanId", scanId,
                    "filesRestored", result.filesRestored(),
                    "errors", result.errors(),
                    "ts", Instant.now().toString()
            ));

            KeeplyHttp.logInfo("restore success scanId=" + scanId +
                    " files=" + result.filesRestored() +
                    " errors=" + result.errors());

        } catch (Exception e) {
            KeeplyHttp.logWarn("restore failed scanId=" + req.scanId() + " msg=" + KeeplyHttp.safeMsg(e));

            broadcast(wsServer, "restore.failed", Map.of(
                    "scanId", req.scanId(),
                    "message", KeeplyHttp.safeMsg(e),
                    "ts", Instant.now().toString()
            ));

            KeeplyHttp.sendError(ex, 500, "restore_failed", KeeplyHttp.safeMsg(e));
        }
    }

    // ----------------- Parsing -----------------

    private RestoreRequest parseRequest(JsonNode body) {
        String backupId = KeeplyHttp.text(body, "backupId");
        Long scanId = KeeplyHttp.longOrNull(body, "scanId");

        if (scanId == null) {
            scanId = tryParseLong(KeeplyHttp.nvl(backupId).trim());
        }
        if (scanId == null || scanId <= 0) return null;

        return new RestoreRequest(
                scanId,
                KeeplyHttp.text(body, "targetMode"),
                KeeplyHttp.text(body, "targetPath"),
                KeeplyHttp.text(body, "deviceId")
        );
    }

    private static Long tryParseLong(String v) {
        try {
            if (v == null || v.isBlank()) return null;
            return Long.parseLong(v);
        } catch (Exception ignored) {
            return null;
        }
    }

    private BlobStore.RestoreMode parseMode(String targetMode) {
        if (KeeplyHttp.isBlank(targetMode) || "original".equalsIgnoreCase(targetMode)) {
            return BlobStore.RestoreMode.ORIGINAL_PATH;
        }
        if ("dest".equalsIgnoreCase(targetMode) || "dest_with_structure".equalsIgnoreCase(targetMode)) {
            return BlobStore.RestoreMode.DEST_WITH_STRUCTURE;
        }
        return null;
    }

    // ----------------- Paths -----------------

    private RestorePaths buildPaths(BackupHistoryDb.HistoryRow row,
                                   BlobStore.RestoreMode mode,
                                   String targetPath) {

        Path baseDir = Path.of(row.destPath()).toAbsolutePath().normalize();
        Path originalRoot = Path.of(row.rootPath()).toAbsolutePath().normalize();

        Path destinationDir = null;
        if (mode != BlobStore.RestoreMode.ORIGINAL_PATH) {
            if (KeeplyHttp.isBlank(targetPath)) {
                throw new IllegalArgumentException("targetPath obrigatorio para restore em outro local");
            }
            destinationDir = Path.of(targetPath).toAbsolutePath().normalize();
        }

        return new RestorePaths(baseDir, originalRoot, destinationDir);
    }

    // ----------------- WS helper -----------------

    private static void broadcast(KeeplyWsServer wsServer, String event, Map<String, Object> payload) {
        if (wsServer == null) return;
        try {
            wsServer.broadcastEvent(event, payload);
        } catch (Exception ignored) {
            // WS é "best effort"; não deve quebrar o restore
        }
    }

    // ----------------- DTOs -----------------

    private record RestoreRequest(long scanId, String targetMode, String targetPath, String deviceId) {}
    private record RestorePaths(Path baseDir, Path originalRoot, Path destinationDir) {}
}
