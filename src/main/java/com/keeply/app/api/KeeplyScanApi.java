package com.keeply.app.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.keeply.app.cli.cli;
import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.inventory.BackupHistoryDb;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class KeeplyScanApi {
    private static final Duration JOB_TTL = Duration.ofHours(6);
    private static final Duration MAX_SCAN_RUNTIME = Duration.ofMinutes(45);
    private static final Duration SCAN_HEARTBEAT_EVERY = Duration.ofSeconds(2);
    private static final Duration MAX_SCAN_HEARTBEAT_GAP = Duration.ofMinutes(2);
    private static final Duration STALE_HISTORY_RUNNING_AGE = Duration.ofMinutes(10);
    private static final String SCAN_PATH = "/api/keeply/scan";
    private static final String LEGACY_WINDOWS_DEST = "C:\\Temp\\keeply";
    private static final String DEFAULT_WINDOWS_DEST = "C:\\Users\\angel\\AppData\\Roaming\\Keeply\\backup-store";
    private static final ConcurrentMap<String, ScanJob> JOBS = new ConcurrentHashMap<>();

    void handle(HttpExchange ex, ExecutorService scanPool, KeeplyWsServer wsServer) throws IOException {
        // suporta:
        // POST   /scan
        // GET    /scan/{jobId}
        // DELETE /scan/{jobId}
        String path = ex.getRequestURI().getPath();
        String rest = path.length() > SCAN_PATH.length() ? path.substring(SCAN_PATH.length()) : "";

        if (rest.isBlank() || "/".equals(rest)) {
            if (!KeeplyHttp.isMethod(ex, "POST")) {
                KeeplyHttp.methodNotAllowed(ex, "POST");
                return;
            }
            String schemaError = ensureScanSchemaReady();
            if (schemaError != null) {
                KeeplyHttp.sendError(ex, 503, "scan_storage_unavailable", schemaError);
                return;
            }

            JsonNode body = KeeplyHttp.readJsonBody(ex);
            String root = KeeplyHttp.text(body, "root");
            String dest = normalizeBackupDestination(KeeplyHttp.text(body, "dest"));
            String password = KeeplyHttp.text(body, "password");

            if (KeeplyHttp.isBlank(root) || KeeplyHttp.isBlank(dest)) {
                KeeplyHttp.sendError(ex, 400, "bad_request", "Informe root e dest");
                return;
            }
            if (root.indexOf('\0') >= 0 || dest.indexOf('\0') >= 0) {
                KeeplyHttp.sendError(ex, 400, "bad_request", "Caminho invalido");
                return;
            }

            String jobId = UUID.randomUUID().toString();
            ScanJob job = ScanJob.created(jobId, root, dest);
            JOBS.put(jobId, job);

            KeeplyHttp.logInfo("scan enqueue jobId=" + jobId + " root=" + root + " dest=" + dest + " from " + KeeplyHttp.clientOf(ex));
            if (wsServer != null) wsServer.broadcastEvent("scan.created", Map.of("jobId", jobId));

            Future<?> f = scanPool.submit(() -> runScanJob(job, root, dest, password, wsServer));
            job.attachFuture(f);

            ObjectNode out = KeeplyHttp.mapper().createObjectNode();
            out.put("ok", true);
            out.put("jobId", jobId);
            out.put("statusUrl", SCAN_PATH + "/" + jobId);
            out.put("message", "scan enfileirado");
            KeeplyHttp.sendJson(ex, 202, out);
            return;
        }

        if (!rest.startsWith("/")) {
            KeeplyHttp.sendError(ex, 404, "not_found", "Rota invalida");
            return;
        }

        String[] parts = rest.substring(1).split("/", 2);
        String jobId = parts[0].trim();
        if (KeeplyHttp.isBlank(jobId)) {
            KeeplyHttp.sendError(ex, 400, "bad_request", "jobId invalido");
            return;
        }

        ScanJob job = JOBS.get(jobId);
        if (job == null) {
            KeeplyHttp.sendError(ex, 404, "not_found", "jobId nao existe");
            return;
        }

        if (KeeplyHttp.isMethod(ex, "GET")) {
            KeeplyHttp.logInfo("scan status jobId=" + jobId + " state=" + job.state + " from " + KeeplyHttp.clientOf(ex));
            KeeplyHttp.sendJson(ex, 200, job.toJson());
            return;
        }

        if (KeeplyHttp.isMethod(ex, "DELETE")) {
            boolean cancelled = job.cancel();
            KeeplyHttp.logWarn("scan cancel request jobId=" + jobId + " cancelled=" + cancelled + " from " + KeeplyHttp.clientOf(ex));

            ObjectNode out = KeeplyHttp.mapper().createObjectNode();
            out.put("ok", true);
            out.put("jobId", jobId);
            out.put("cancelled", cancelled);
            out.put("state", job.state);
            KeeplyHttp.sendJson(ex, 202, out);

            if (cancelled && wsServer != null) {
                wsServer.broadcastEvent("scan.cancel_requested", Map.of("jobId", jobId));
            }
            return;
        }

        KeeplyHttp.methodNotAllowed(ex, "GET, DELETE");
    }

    void cleanupOldJobs(KeeplyWsServer wsServer) {
        Instant now = Instant.now();
        Instant cutoff = now.minus(JOB_TTL);

        int removed = 0;
        for (Map.Entry<String, ScanJob> e : JOBS.entrySet()) {
            ScanJob j = e.getValue();
            if (j == null) continue;

            if ("running".equals(j.state)) {
                Instant started = j.startedAt == null ? j.createdAt : j.startedAt;
                Instant heartbeat = j.lastHeartbeatAt == null ? started : j.lastHeartbeatAt;
                boolean runtimeExceeded = started.plus(MAX_SCAN_RUNTIME).isBefore(now);
                boolean heartbeatLost = heartbeat.plus(MAX_SCAN_HEARTBEAT_GAP).isBefore(now);

                if (runtimeExceeded || heartbeatLost) {
                    String msg = runtimeExceeded
                            ? "scan timeout (" + MAX_SCAN_RUNTIME.toMinutes() + " min)"
                            : "scan heartbeat timeout";
                    boolean changed = j.markFailed(124, msg);
                    Future<?> f = j.future;
                    if (f != null) f.cancel(true);

                    if (changed && wsServer != null) {
                        wsServer.broadcastEvent("scan.failed", Map.of(
                                "jobId", j.id,
                                "exitCode", 124,
                                "message", msg
                        ));
                    }
                }
            }

            boolean done = !"running".equals(j.state);
            Instant ref = (j.finishedAt != null) ? j.finishedAt : j.createdAt;

            if (done && ref.isBefore(cutoff)) {
                if (JOBS.remove(e.getKey(), j)) removed++;
            }
        }

        if (removed > 0 && wsServer != null) {
            wsServer.broadcastEvent("scan.cleanup", Map.of("removed", removed));
        }
    }

    void bootstrapStorage() {
        try {
            BackupHistoryDb.init();
            int fixed = BackupHistoryDb.recoverStaleRunning(
                    STALE_HISTORY_RUNNING_AGE,
                    "stale job recovered on boot"
            );
            if (fixed > 0) {
                KeeplyHttp.logWarn("scan startup recovery: " + fixed + " RUNNING antigos marcados como ERROR");
            }
            String schemaError = ensureScanSchemaReady();
            if (schemaError != null) {
                KeeplyHttp.logWarn("scan schema check failed on startup: " + schemaError);
            }
        } catch (Exception e) {
            KeeplyHttp.logWarn("scan startup bootstrap failed: " + KeeplyHttp.safeMsg(e));
        }
    }

    private static void runScanJob(ScanJob job, String root, String dest, String password, KeeplyWsServer wsServer) {
        job.markRunning();
        job.touch();
        KeeplyHttp.logInfo("scan running jobId=" + job.id);
        if (wsServer != null) wsServer.broadcastEvent("scan.running", Map.of("jobId", job.id));

        List<String> cliArgs = new ArrayList<>();
        cliArgs.add("scan");
        cliArgs.add("--root");
        cliArgs.add(root);
        cliArgs.add("--dest");
        cliArgs.add(dest);
        if (!KeeplyHttp.isBlank(password)) {
            cliArgs.add("--password");
            cliArgs.add(password);
        }

        int exit;
        try {
            exit = executeScanCliWithWatchdog(job, cliArgs);
        } catch (java.util.concurrent.CancellationException ce) {
            job.markCancelled("cancelled");
            KeeplyHttp.logWarn("scan cancelled jobId=" + job.id);
            if (wsServer != null) wsServer.broadcastEvent("scan.cancelled", Map.of("jobId", job.id));
            return;
        } catch (java.util.concurrent.TimeoutException te) {
            exit = 124;
            job.markFailed(exit, KeeplyHttp.safeMsg(te));
            KeeplyHttp.logWarn("scan timeout jobId=" + job.id + " msg=" + job.message);
            if (wsServer != null) {
                wsServer.broadcastEvent("scan.failed", Map.of(
                        "jobId", job.id,
                        "exitCode", exit,
                        "message", job.message
                ));
            }
            return;
        } catch (Throwable t) {
            exit = 999;
            job.markFailed(exit, KeeplyHttp.safeMsg(t));
            KeeplyHttp.logWarn("scan failed jobId=" + job.id + " exit=" + exit + " msg=" + job.message);
            if (wsServer != null) {
                wsServer.broadcastEvent("scan.failed", Map.of(
                        "jobId", job.id,
                        "exitCode", exit,
                        "message", job.message
                ));
            }
            return;
        }

        if (exit == 0) {
            job.markSuccess();
            KeeplyHttp.logInfo("scan success jobId=" + job.id);
            if (wsServer != null) wsServer.broadcastEvent("scan.success", Map.of("jobId", job.id));
        } else {
            job.markFailed(exit, "scan retornou exitCode != 0");
            KeeplyHttp.logWarn("scan failed jobId=" + job.id + " exit=" + exit + " msg=" + job.message);
            if (wsServer != null) {
                wsServer.broadcastEvent("scan.failed", Map.of(
                        "jobId", job.id,
                        "exitCode", exit,
                        "message", job.message
                ));
            }
        }
    }

    private static int executeScanCliWithWatchdog(ScanJob job, List<String> cliArgs) throws Exception {
        ExecutorService runner = java.util.concurrent.Executors.newSingleThreadExecutor(
                Thread.ofVirtual().name("keeply-scan-cli-", 0).factory()
        );
        Future<Integer> task = null;
        Instant deadline = Instant.now().plus(MAX_SCAN_RUNTIME);

        try {
            task = runner.submit(() -> cli.executeEmbedded(cliArgs.toArray(String[]::new)));
            while (true) {
                try {
                    int exit = task.get(SCAN_HEARTBEAT_EVERY.toMillis(), TimeUnit.MILLISECONDS);
                    job.touch();
                    return exit;
                } catch (java.util.concurrent.TimeoutException ignored) {
                    job.touch();
                    if (Instant.now().isAfter(deadline)) {
                        task.cancel(true);
                        throw new java.util.concurrent.TimeoutException(
                                "scan excedeu limite de " + MAX_SCAN_RUNTIME.toMinutes() + " minutos"
                        );
                    }
                }
            }
        } finally {
            if (task != null && !task.isDone()) task.cancel(true);
            runner.shutdownNow();
        }
    }

    private static String ensureScanSchemaReady() {
        try {
            BackupHistoryDb.init();
            DatabaseBackup.init();

            try (Connection c = DatabaseBackup.openSingleConnection()) {
                if (!tableExists(c, "scans")) return "Tabela obrigatoria ausente: scans";
                if (!tableExists(c, "file_inventory")) return "Tabela obrigatoria ausente: file_inventory";
                if (!tableExists(c, "file_history")) return "Tabela obrigatoria ausente: file_history";
            }
            return null;
        } catch (Exception e) {
            return "Falha ao validar schema do scan: " + KeeplyHttp.safeMsg(e);
        }
    }

    private static boolean tableExists(Connection c, String tableName) {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND lower(name)=lower(?) LIMIT 1"
        )) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (Exception ignored) {
            return false;
        }
    }

    private static String normalizeBackupDestination(String dest) {
        if (KeeplyHttp.isBlank(dest)) return dest;
        String trimmed = dest.trim();
        if (LEGACY_WINDOWS_DEST.equalsIgnoreCase(trimmed)) return DEFAULT_WINDOWS_DEST;
        return trimmed;
    }

    private static final class ScanJob {
        private final String id;
        private final String root;
        private final String dest;
        private final Instant createdAt;
        private volatile Instant startedAt;
        private volatile Instant finishedAt;
        private volatile Instant lastHeartbeatAt;
        private volatile String state;
        private volatile Integer exitCode;
        private volatile String message;
        private final AtomicBoolean started = new AtomicBoolean(false);
        private final AtomicBoolean terminal = new AtomicBoolean(false);
        private volatile Future<?> future;

        private ScanJob(String id, String root, String dest) {
            this.id = id;
            this.root = root;
            this.dest = dest;
            this.createdAt = Instant.now();
            this.lastHeartbeatAt = this.createdAt;
            this.state = "created";
        }

        static ScanJob created(String id, String root, String dest) {
            return new ScanJob(id, root, dest);
        }

        void attachFuture(Future<?> f) {
            this.future = f;
        }

        boolean cancel() {
            Future<?> f = this.future;
            if (f == null) return false;

            boolean ok = f.cancel(true);
            if (ok) markCancelled("cancel requested");
            return ok;
        }

        void markRunning() {
            if (!terminal.get() && started.compareAndSet(false, true)) {
                this.startedAt = Instant.now();
                this.lastHeartbeatAt = this.startedAt;
                this.state = "running";
            }
        }

        void touch() {
            this.lastHeartbeatAt = Instant.now();
        }

        boolean markSuccess() {
            return finish("success", 0, "ok");
        }

        boolean markCancelled(String msg) {
            return finish("cancelled", null, msg);
        }

        boolean markFailed(Integer exitCode, String msg) {
            return finish("failed", exitCode, msg);
        }

        private boolean finish(String newState, Integer newExitCode, String newMessage) {
            if (!terminal.compareAndSet(false, true)) return false;
            this.state = newState;
            this.exitCode = newExitCode;
            this.finishedAt = Instant.now();
            this.lastHeartbeatAt = this.finishedAt;
            this.message = newMessage;
            return true;
        }

        ObjectNode toJson() {
            ObjectNode out = KeeplyHttp.mapper().createObjectNode();
            out.put("ok", true);

            ObjectNode job = out.putObject("job");
            job.put("id", id);
            job.put("state", state);
            job.put("root", root);
            job.put("dest", dest);
            job.put("createdAt", createdAt.toString());

            if (startedAt == null) job.putNull("startedAt"); else job.put("startedAt", startedAt.toString());
            if (finishedAt == null) job.putNull("finishedAt"); else job.put("finishedAt", finishedAt.toString());
            if (lastHeartbeatAt == null) job.putNull("lastHeartbeatAt"); else job.put("lastHeartbeatAt", lastHeartbeatAt.toString());
            if (exitCode == null) job.putNull("exitCode"); else job.put("exitCode", exitCode);
            if (message == null) job.putNull("message"); else job.put("message", message);

            return out;
        }
    }
}
