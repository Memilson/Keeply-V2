package com.keeply.app.api;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.keeply.app.blob.BlobStore;
import com.keeply.app.cli.cli;
import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.inventory.BackupHistoryDb;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.cdimascio.dotenv.Dotenv;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
public final class KeeplyApi {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_BODY_BYTES = 64 * 1024;
    private static final Duration JOB_TTL = Duration.ofHours(6);
    private static final Duration CLEANUP_EVERY = Duration.ofSeconds(30);
    private static final Duration MAX_SCAN_RUNTIME = Duration.ofMinutes(45);
    private static final Duration SCAN_HEARTBEAT_EVERY = Duration.ofSeconds(2);
    private static final Duration MAX_SCAN_HEARTBEAT_GAP = Duration.ofMinutes(2);
    private static final Duration STALE_HISTORY_RUNNING_AGE = Duration.ofMinutes(10);
    private static final String BASE = "/api/keeply";
    private static final String HEALTH = BASE + "/health";
    private static final String HISTORY = BASE + "/history";
    private static final String SCAN = BASE + "/scan";
    private static final String RESTORE = BASE + "/restore";
    private static final String FOLDERS = BASE + "/folders";
    private static final String PAIRING = BASE + "/pairing";
    private static final String AGENT = BASE + "/agent";
    private static final String AGENT_CHALLENGE = AGENT + "/challenge";
    private static final String AGENT_LOGIN = AGENT + "/login";
    private static final ConcurrentMap<String, ScanJob> JOBS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, ChallengeState> CHALLENGES = new ConcurrentHashMap<>();
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final Duration PAIRING_TTL = Duration.ofMinutes(2);
    private static final Duration CHALLENGE_TTL = Duration.ofMinutes(1);
    private static final Duration AGENT_JWT_TTL = Duration.ofMinutes(15);
    private static final String STATE_KEY_DEVICE_IDENTITY = "agent.device_identity";
    private static final String STATE_KEY_PAIRING = "agent.pairing_state";
    private static final String STATE_KEY_LINK = "agent.link_state";
    private static final String LEGACY_WINDOWS_DEST = "C:\\Temp\\keeply";
    private static final String DEFAULT_WINDOWS_DEST = "C:\\Users\\angel\\AppData\\Roaming\\Keeply\\backup-store";
    private KeeplyApi() {}
    public static void run(String[] args) {
        bootstrapEnv();
        bootstrapScanStorage();
        Config cfg = parseConfig(args);
        ExecutorService httpPool = null;
        ExecutorService scanPool = null;
        ScheduledExecutorService maint = null;
        HttpServer server = null;
        KeeplyWsServer wsServer = null;
        var stopLatch = new CountDownLatch(1);

        try {
            InetAddress bindAddr = InetAddress.getByName(cfg.bindHost());
            server = HttpServer.create(new InetSocketAddress(bindAddr, cfg.port()), 0);

            if (cfg.wsPort() > 0) {
                wsServer = new KeeplyWsServer(new InetSocketAddress(bindAddr, cfg.wsPort()), cfg.token());
                wsServer.start();
            }
            httpPool = java.util.concurrent.Executors.newThreadPerTaskExecutor(
                    Thread.ofVirtual().name("keeply-http-", 0).factory()
            );
            scanPool = java.util.concurrent.Executors.newThreadPerTaskExecutor(
                    Thread.ofVirtual().name("keeply-scan-", 0).factory()
            );
            maint = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                    Thread.ofPlatform().name("keeply-maint-", 0).factory()
            );
            server.setExecutor(httpPool);
            KeeplyWsServer finalWsServer = wsServer;
            ExecutorService finalScanPool = scanPool;
            server.createContext(HEALTH, ex -> safeHandle(ex, cfg, () -> handleHealth(ex)));
            server.createContext(HISTORY, ex -> safeHandle(ex, cfg, () -> handleHistory(ex)));
            server.createContext(SCAN, ex -> safeHandle(ex, cfg, () -> handleScan(ex, finalScanPool, finalWsServer)));
            server.createContext(RESTORE, ex -> safeHandle(ex, cfg, () -> handleRestore(ex, finalWsServer)));
            server.createContext(FOLDERS, ex -> safeHandle(ex, cfg, () -> handleFolders(ex)));
            server.createContext(PAIRING, ex -> safeHandle(ex, cfg, () -> handlePairing(ex)));
            server.createContext(AGENT_CHALLENGE, ex -> safeHandle(ex, cfg, () -> handleAgentChallenge(ex)));
            server.createContext(AGENT_LOGIN, ex -> safeHandle(ex, cfg, () -> handleAgentLogin(ex)));
            maint.scheduleAtFixedRate(
                    () -> {
                        cleanupOldJobs(finalWsServer);
                        cleanupChallenges();
                    },
                    CLEANUP_EVERY.toMillis(),
                    CLEANUP_EVERY.toMillis(),
                    TimeUnit.MILLISECONDS
            );
            server.start();
            printStartupInfo(cfg);
            AgentLinkState startupLink = loadAgentLinkState();
            if (!startupLink.paired()) {
                PairingIssued startupPairing = issuePairingCode();
                System.out.println("Pairing (agente local):");
                System.out.println("  Codigo: " + startupPairing.code());
                System.out.println("  Expira em: " + startupPairing.expiresAt());
                System.out.println("  Use este codigo no painel web para vincular.");
            }

            HttpServer shutdownServer = server;
            ExecutorService shutdownScan = scanPool;
            ExecutorService shutdownHttp = httpPool;
            ScheduledExecutorService shutdownMaint = maint;
            KeeplyWsServer shutdownWs = wsServer;
            Runtime.getRuntime().addShutdownHook(
                    Thread.ofPlatform().name("keeply-api-shutdown").unstarted(() -> {
                        try { if (shutdownServer != null) shutdownServer.stop(0); } catch (Exception ignored) {}
                        try { if (shutdownMaint != null) shutdownMaint.shutdownNow(); } catch (Exception ignored) {}
                        try { if (shutdownScan != null) shutdownScan.shutdownNow(); } catch (Exception ignored) {}
                        try { if (shutdownHttp != null) shutdownHttp.shutdownNow(); } catch (Exception ignored) {}
                        if (shutdownWs != null) {
                            try { shutdownWs.stop(1000); } catch (Exception ignored) {}
                        }
                        stopLatch.countDown();
                    })
            );

            // mantém a JVM viva de forma correta (sem depender de daemon threads)
            stopLatch.await();

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println("Falha ao iniciar API: " + safeMsg(e));
            System.exit(1);
        }
    }

    private static void printStartupInfo(Config cfg) {
        System.out.println("Keeply API online em http://" + cfg.bindHost() + ":" + cfg.port());
        System.out.println("Endpoints:");
        System.out.println("  GET    " + HEALTH);
        System.out.println("  GET    " + HISTORY + "?limit=20");
        System.out.println("  GET    " + PAIRING);
        System.out.println("  POST   " + PAIRING + "/rotate");
        System.out.println("  POST   " + PAIRING + "/request");
        System.out.println("  POST   " + PAIRING + "/approve");
        System.out.println("  POST   " + PAIRING + "/mark-linked");
        System.out.println("  POST   " + PAIRING + "/mark-unlinked");
        System.out.println("  POST   " + SCAN);
        System.out.println("  GET    " + SCAN + "/{jobId}");
        System.out.println("  DELETE " + SCAN + "/{jobId}  (cancel)");
        System.out.println("  POST   " + RESTORE);
        System.out.println("  GET    " + FOLDERS + "?path=...");
        System.out.println("  GET    " + AGENT_CHALLENGE + "?deviceId=...");
        System.out.println("  POST   " + AGENT_LOGIN);
        if (cfg.wsPort() > 0) {
            System.out.println("WebSocket:");
            System.out.println("  ws://" + cfg.bindHost() + ":" + cfg.wsPort() + "/ws/keeply");
        }
    }

    // ----------------- Handlers -----------------

    private static void handleHealth(HttpExchange ex) throws IOException {
        if (!isMethod(ex, "GET")) {
            methodNotAllowed(ex, "GET");
            return;
        }

        ObjectNode out = MAPPER.createObjectNode();
        out.put("ok", true);
        out.put("service", "keeply-api");
        out.put("ts", Instant.now().toString());
        sendJson(ex, 200, out);
    }

    private static void handleHistory(HttpExchange ex) throws IOException {
        if (!isMethod(ex, "GET")) {
            methodNotAllowed(ex, "GET");
            return;
        }

        int limit = clampInt(parseQuery(ex.getRequestURI()).get("limit"), 20, 1, 200);
        logInfo("history request from " + clientOf(ex) + " limit=" + limit);

        List<BackupHistoryDb.HistoryRow> rows = new ArrayList<>(BackupHistoryDb.listRecent(limit));

        ObjectNode out = MAPPER.createObjectNode();
        out.put("ok", true);
        out.put("limit", limit);

        ArrayNode items = out.putArray("items");
        for (BackupHistoryDb.HistoryRow r : rows) {
            ObjectNode it = items.addObject();
            it.put("id", r.id());
            putNullable(it, "startedAt", r.startedAt());
            putNullable(it, "finishedAt", r.finishedAt());
            putNullable(it, "status", r.status());
            putNullable(it, "backupType", r.backupType());
            putNullable(it, "rootPath", r.rootPath());
            putNullable(it, "destPath", r.destPath());
            it.put("filesProcessed", r.filesProcessed());
            it.put("errors", r.errors());
            if (r.scanId() == null) it.putNull("scanId"); else it.put("scanId", r.scanId());
            putNullable(it, "message", r.message());
        }

        logInfo("history response items=" + rows.size());
        sendJson(ex, 200, out);
    }

    private static void handleScan(HttpExchange ex, ExecutorService scanPool, KeeplyWsServer wsServer) throws IOException {
        // suporta:
        // POST   /scan
        // GET    /scan/{jobId}
        // DELETE /scan/{jobId}
        String path = ex.getRequestURI().getPath();
        String rest = path.length() > SCAN.length() ? path.substring(SCAN.length()) : "";

            if (rest.isBlank() || "/".equals(rest)) {
            if (!isMethod(ex, "POST")) {
                methodNotAllowed(ex, "POST");
                return;
            }
            String schemaError = ensureScanSchemaReady();
            if (schemaError != null) {
                sendError(ex, 503, "scan_storage_unavailable", schemaError);
                return;
            }

            JsonNode body = readJsonBody(ex);
            String root = text(body, "root");
            String dest = normalizeBackupDestination(text(body, "dest"));
            String password = text(body, "password");

            if (isBlank(root) || isBlank(dest)) {
                sendError(ex, 400, "bad_request", "Informe root e dest");
                return;
            }
            if (root.indexOf('\0') >= 0 || dest.indexOf('\0') >= 0) {
                sendError(ex, 400, "bad_request", "Caminho inválido");
                return;
            }

            String jobId = UUID.randomUUID().toString();
            ScanJob job = ScanJob.created(jobId, root, dest);
            JOBS.put(jobId, job);

            logInfo("scan enqueue jobId=" + jobId + " root=" + root + " dest=" + dest + " from " + clientOf(ex));
            if (wsServer != null) wsServer.broadcastEvent("scan.created", Map.of("jobId", jobId));

            Future<?> f = scanPool.submit(() -> runScanJob(job, root, dest, password, wsServer));
            job.attachFuture(f);

            ObjectNode out = MAPPER.createObjectNode();
            out.put("ok", true);
            out.put("jobId", jobId);
            out.put("statusUrl", SCAN + "/" + jobId);
            out.put("message", "scan enfileirado");
            sendJson(ex, 202, out);
            return;
        }

        if (!rest.startsWith("/")) {
            sendError(ex, 404, "not_found", "Rota inválida");
            return;
        }

        String[] parts = rest.substring(1).split("/", 2);
        String jobId = parts[0].trim();
        if (isBlank(jobId)) {
            sendError(ex, 400, "bad_request", "jobId inválido");
            return;
        }

        ScanJob job = JOBS.get(jobId);
        if (job == null) {
            sendError(ex, 404, "not_found", "jobId não existe");
            return;
        }

        if (isMethod(ex, "GET")) {
            logInfo("scan status jobId=" + jobId + " state=" + job.state + " from " + clientOf(ex));
            sendJson(ex, 200, job.toJson());
            return;
        }

        if (isMethod(ex, "DELETE")) {
            boolean cancelled = job.cancel();
            logWarn("scan cancel request jobId=" + jobId + " cancelled=" + cancelled + " from " + clientOf(ex));

            ObjectNode out = MAPPER.createObjectNode();
            out.put("ok", true);
            out.put("jobId", jobId);
            out.put("cancelled", cancelled);
            out.put("state", job.state);
            sendJson(ex, 202, out);

            if (cancelled && wsServer != null) {
                wsServer.broadcastEvent("scan.cancel_requested", Map.of("jobId", jobId));
            }
            return;
        }

        methodNotAllowed(ex, "GET, DELETE");
    }

    private static void runScanJob(ScanJob job, String root, String dest, String password, KeeplyWsServer wsServer) {
        job.markRunning();
        job.touch();
        logInfo("scan running jobId=" + job.id);
        if (wsServer != null) wsServer.broadcastEvent("scan.running", Map.of("jobId", job.id));

        List<String> cliArgs = new ArrayList<>();
        cliArgs.add("scan");
        cliArgs.add("--root");
        cliArgs.add(root);
        cliArgs.add("--dest");
        cliArgs.add(dest);
        if (!isBlank(password)) {
            cliArgs.add("--password");
            cliArgs.add(password);
        }

        int exit;
        try {
            exit = executeScanCliWithWatchdog(job, cliArgs);
        } catch (java.util.concurrent.CancellationException ce) {
            job.markCancelled("cancelled");
            logWarn("scan cancelled jobId=" + job.id);
            if (wsServer != null) wsServer.broadcastEvent("scan.cancelled", Map.of("jobId", job.id));
            return;
        } catch (java.util.concurrent.TimeoutException te) {
            exit = 124;
            job.markFailed(exit, safeMsg(te));
            logWarn("scan timeout jobId=" + job.id + " msg=" + job.message);
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
            job.markFailed(exit, safeMsg(t));
            logWarn("scan failed jobId=" + job.id + " exit=" + exit + " msg=" + job.message);
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
            logInfo("scan success jobId=" + job.id);
            if (wsServer != null) wsServer.broadcastEvent("scan.success", Map.of("jobId", job.id));
        } else {
            job.markFailed(exit, "scan retornou exitCode != 0");
            logWarn("scan failed jobId=" + job.id + " exit=" + exit + " msg=" + job.message);
            if (wsServer != null) {
                wsServer.broadcastEvent("scan.failed", Map.of(
                        "jobId", job.id,
                        "exitCode", exit,
                        "message", job.message
                ));
            }
        }
    }

    private static void handleRestore(HttpExchange ex, KeeplyWsServer wsServer) throws IOException {
        if (!isMethod(ex, "POST")) {
            methodNotAllowed(ex, "POST");
            return;
        }

        JsonNode body = readJsonBody(ex);
        String backupId = text(body, "backupId");
        String targetMode = text(body, "targetMode");
        String targetPath = text(body, "targetPath");
        String deviceId = text(body, "deviceId");
        Long scanId = longOrNull(body, "scanId");
        if (scanId == null) {
            try { scanId = Long.parseLong(nvl(backupId).trim()); }
            catch (Exception ignored) { scanId = null; }
        }
        if (scanId == null || scanId <= 0) {
            sendError(ex, 400, "bad_request", "Informe backupId/scanId numerico valido");
            return;
        }

        BackupHistoryDb.HistoryRow row = BackupHistoryDb.findByScanId(scanId);
        if (row == null) {
            sendError(ex, 404, "not_found", "Nao encontrei backup_history para scanId informado");
            return;
        }
        if (isBlank(row.destPath()) || isBlank(row.rootPath())) {
            sendError(ex, 400, "bad_request", "Backup sem root/dest no historico");
            return;
        }

        BlobStore.RestoreMode mode;
        if ("original".equalsIgnoreCase(targetMode) || isBlank(targetMode)) mode = BlobStore.RestoreMode.ORIGINAL_PATH;
        else mode = BlobStore.RestoreMode.DEST_WITH_STRUCTURE;

        Path baseDir = Path.of(row.destPath()).toAbsolutePath().normalize();
        Path originalRoot = Path.of(row.rootPath()).toAbsolutePath().normalize();
        Path destinationDir = null;
        if (mode != BlobStore.RestoreMode.ORIGINAL_PATH) {
            if (isBlank(targetPath)) {
                sendError(ex, 400, "bad_request", "targetPath obrigatorio para restore em outro local");
                return;
            }
            destinationDir = Path.of(targetPath).toAbsolutePath().normalize();
        }

        logInfo("restore running scanId=" + scanId + " mode=" + mode + " from " + clientOf(ex)
                + " baseDir=" + baseDir + " target=" + (destinationDir == null ? originalRoot : destinationDir)
                + " deviceId=" + nvl(deviceId));
        if (wsServer != null) {
            wsServer.broadcastEvent("restore.running", Map.of(
                    "scanId", scanId,
                    "mode", mode.name(),
                    "ts", Instant.now().toString()
            ));
        }

        try {
            long finalScanId = scanId;
            BlobStore.RestoreResult result = BlobStore.restoreChangedFilesFromScan(
                    scanId,
                    baseDir,
                    destinationDir,
                    originalRoot,
                    mode,
                    new AtomicBoolean(false),
                    msg -> logInfo("restore[" + finalScanId + "] " + msg)
            );

            ObjectNode out = MAPPER.createObjectNode();
            out.put("ok", true);
            out.put("scanId", scanId);
            out.put("mode", mode.name());
            out.put("filesRestored", result.filesRestored());
            out.put("errors", result.errors());
            sendJson(ex, 200, out);

            if (wsServer != null) {
                wsServer.broadcastEvent("restore.success", Map.of(
                        "scanId", scanId,
                        "filesRestored", result.filesRestored(),
                        "errors", result.errors(),
                        "ts", Instant.now().toString()
                ));
            }
            logInfo("restore success scanId=" + scanId + " files=" + result.filesRestored() + " errors=" + result.errors());
        } catch (Exception e) {
            logWarn("restore failed scanId=" + scanId + " msg=" + safeMsg(e));
            if (wsServer != null) {
                wsServer.broadcastEvent("restore.failed", Map.of(
                        "scanId", scanId,
                        "message", safeMsg(e),
                        "ts", Instant.now().toString()
                ));
            }
            sendError(ex, 500, "restore_failed", safeMsg(e));
        }
    }

    private static void handleFolders(HttpExchange ex) throws IOException {
        if (!isMethod(ex, "GET")) {
            methodNotAllowed(ex, "GET");
            return;
        }

        String rawPath = parseQuery(ex.getRequestURI()).get("path");
        Path current;
        try {
            current = isBlank(rawPath) ? Path.of(System.getProperty("user.home")) : Path.of(rawPath);
            current = current.toAbsolutePath().normalize();
        } catch (Exception e) {
            sendError(ex, 400, "bad_request", "path invalido");
            return;
        }

        if (!Files.exists(current) || !Files.isDirectory(current)) {
            sendError(ex, 404, "not_found", "pasta nao encontrada");
            return;
        }

        ObjectNode out = MAPPER.createObjectNode();
        out.put("ok", true);
        out.put("current", current.toString());
        out.put("parent", current.getParent() == null ? "" : current.getParent().toString());
        ArrayNode items = out.putArray("items");

        try (var stream = Files.list(current)) {
            stream
                    .filter(Files::isDirectory)
                    .sorted(Comparator.comparing(
                            p -> p.getFileName() == null ? p.toString() : p.getFileName().toString(),
                            String.CASE_INSENSITIVE_ORDER
                    ))
                    .limit(300)
                    .forEach(p -> {
                        ObjectNode it = items.addObject();
                        it.put("name", p.getFileName() == null ? p.toString() : p.getFileName().toString());
                        it.put("path", p.toAbsolutePath().normalize().toString());
                    });
        } catch (Exception e) {
            sendError(ex, 500, "list_failed", safeMsg(e));
            return;
        }

        logInfo("folders list from " + clientOf(ex) + " current=" + current + " items=" + items.size());
        sendJson(ex, 200, out);
    }

    private static void handlePairing(HttpExchange ex) throws IOException {
        // suporta:
        // GET  /pairing
        // POST /pairing/rotate
        // POST /pairing/request
        // POST /pairing/approve
        // POST /pairing/mark-linked
        // POST /pairing/mark-unlinked
        String path = ex.getRequestURI().getPath();
        String rest = path.length() > PAIRING.length() ? path.substring(PAIRING.length()) : "";

        DeviceIdentity identity = loadOrCreateDeviceIdentity();
        AgentLinkState linkState = loadAgentLinkState();
        PairingState current = loadPairingState();

        if (rest.isBlank() || "/".equals(rest)) {
            if (!isMethod(ex, "GET")) {
                methodNotAllowed(ex, "GET");
                return;
            }

            ObjectNode out = MAPPER.createObjectNode();
            out.put("ok", true);
            out.put("paired", linkState.paired());
            out.put("requiresPairing", !linkState.paired());
            out.put("deviceId", identity.deviceId());
            out.put("publicKey", identity.publicKeyBase64());

            if (linkState.paired()) {
                out.putNull("code");
                out.putNull("createdAt");
                out.putNull("expiresAt");
                putNullable(out, "linkedAt", linkState.linkedAt());
                if (linkState.machineId() == null) out.putNull("machineId"); else out.put("machineId", linkState.machineId());
                if (linkState.userId() == null) out.putNull("userId"); else out.put("userId", linkState.userId());
                putNullable(out, "userEmail", linkState.userEmail());
                putNullable(out, "lastSeenAt", linkState.lastSeenAt());
            } else {
                boolean hasActiveCode = current != null && !current.isExpired() && current.usedAt() == null;
                out.putNull("code");
                if (current == null) {
                    out.putNull("createdAt");
                    out.putNull("expiresAt");
                } else {
                    putNullable(out, "createdAt", current.createdAt());
                    putNullable(out, "expiresAt", current.expiresAt());
                }
                out.put("hasActiveCode", hasActiveCode);
            }

            out.set("machine", machineInfoNode());
            sendJson(ex, 200, out);
            return;
        }

        if ("/rotate".equals(rest)) {
            if (!isMethod(ex, "POST")) {
                methodNotAllowed(ex, "POST");
                return;
            }
            if (linkState.paired()) {
                sendError(ex, 409, "already_linked", "Agente ja vinculado; nao deve gerar novo codigo");
                return;
            }

            PairingIssued rotated = issuePairingCode();
            ObjectNode out = MAPPER.createObjectNode();
            out.put("ok", true);
            out.put("code", rotated.code());
            out.put("createdAt", rotated.createdAt());
            out.put("expiresAt", rotated.expiresAt());
            out.put("deviceId", identity.deviceId());
            out.put("publicKey", identity.publicKeyBase64());
            sendJson(ex, 200, out);
            return;
        }

        if ("/request".equals(rest)) {
            if (!isMethod(ex, "POST")) {
                methodNotAllowed(ex, "POST");
                return;
            }
            if (linkState.paired()) {
                sendError(ex, 409, "already_linked", "Agente ja vinculado");
                return;
            }

            PairingIssued issued = issuePairingCode();
            ObjectNode out = MAPPER.createObjectNode();
            out.put("ok", true);
            out.put("deviceId", identity.deviceId());
            out.put("publicKey", identity.publicKeyBase64());
            out.put("code", issued.code());
            out.put("createdAt", issued.createdAt());
            out.put("expiresAt", issued.expiresAt());
            out.set("machine", machineInfoNode());
            sendJson(ex, 200, out);
            return;
        }

        if ("/approve".equals(rest)) {
            if (!isMethod(ex, "POST")) {
                methodNotAllowed(ex, "POST");
                return;
            }
            if (linkState.paired()) {
                sendError(ex, 409, "already_linked", "Agente ja vinculado");
                return;
            }

            JsonNode body = readJsonBody(ex);
            String informedCode = normalizePairingCode(text(body, "code"));
            if (isBlank(informedCode)) {
                sendError(ex, 400, "bad_request", "Informe o codigo de pairing");
                return;
            }

            PairingState state = loadPairingState();
            if (state == null || state.isExpired()) {
                sendError(ex, 410, "pairing_expired", "Codigo expirado; gere um novo");
                return;
            }
            if (state.usedAt() != null) {
                sendError(ex, 409, "pairing_already_used", "Codigo de pairing ja consumido");
                return;
            }

            String expected = sha256Hex(state.salt() + ":" + informedCode);
            if (!timingSafeEquals(expected, state.codeHash())) {
                sendError(ex, 400, "pairing_code_invalid", "Codigo invalido");
                return;
            }

            PairingState used = state.markUsed(Instant.now().toString());
            savePairingState(used);

            ObjectNode out = MAPPER.createObjectNode();
            out.put("ok", true);
            out.put("deviceId", identity.deviceId());
            out.put("publicKey", identity.publicKeyBase64());
            out.put("approvedAt", Instant.now().toString());
            out.set("machine", machineInfoNode());
            sendJson(ex, 200, out);
            return;
        }

        if ("/mark-linked".equals(rest)) {
            if (!isMethod(ex, "POST")) {
                methodNotAllowed(ex, "POST");
                return;
            }

            JsonNode body = readJsonBody(ex);
            Long machineId = longOrNull(body, "machineId");
            Long userId = longOrNull(body, "userId");
            String userEmail = text(body, "userEmail");

            AgentLinkState updated = new AgentLinkState(
                    true,
                    machineId,
                    userId,
                    userEmail,
                    Instant.now().toString(),
                    Instant.now().toString(),
                    identity.deviceId()
            );
            saveAgentLinkState(updated);

            ObjectNode out = MAPPER.createObjectNode();
            out.put("ok", true);
            out.put("paired", true);
            putNullable(out, "linkedAt", updated.linkedAt());
            if (updated.machineId() == null) out.putNull("machineId"); else out.put("machineId", updated.machineId());
            if (updated.userId() == null) out.putNull("userId"); else out.put("userId", updated.userId());
            putNullable(out, "userEmail", updated.userEmail());
            putNullable(out, "lastSeenAt", updated.lastSeenAt());
            out.put("deviceId", identity.deviceId());
            sendJson(ex, 200, out);
            return;
        }

        if ("/mark-unlinked".equals(rest)) {
            if (!isMethod(ex, "POST")) {
                methodNotAllowed(ex, "POST");
                return;
            }

            saveAgentLinkState(AgentLinkState.unpaired());

            ObjectNode out = MAPPER.createObjectNode();
            out.put("ok", true);
            out.put("paired", false);
            out.put("requiresPairing", true);
            out.putNull("code");
            out.putNull("createdAt");
            out.putNull("expiresAt");
            out.put("deviceId", identity.deviceId());
            out.put("publicKey", identity.publicKeyBase64());
            sendJson(ex, 200, out);
            return;
        }

        sendError(ex, 404, "not_found", "Rota de pairing invalida");
    }

    private static void handleAgentChallenge(HttpExchange ex) throws IOException {
        if (!isMethod(ex, "GET")) {
            methodNotAllowed(ex, "GET");
            return;
        }

        String deviceId = parseQuery(ex.getRequestURI()).get("deviceId");
        DeviceIdentity identity = loadOrCreateDeviceIdentity();
        if (isBlank(deviceId) || !timingSafeEquals(deviceId.trim(), identity.deviceId())) {
            sendError(ex, 404, "device_not_found", "deviceId invalido");
            return;
        }

        String nonce = randomBase64Url(24);
        Instant expiresAt = Instant.now().plus(CHALLENGE_TTL);
        CHALLENGES.put(identity.deviceId(), new ChallengeState(nonce, expiresAt));

        ObjectNode out = MAPPER.createObjectNode();
        out.put("ok", true);
        out.put("deviceId", identity.deviceId());
        out.put("nonce", nonce);
        out.put("expiresAt", expiresAt.toString());
        sendJson(ex, 200, out);
    }

    private static void handleAgentLogin(HttpExchange ex) throws IOException {
        if (!isMethod(ex, "POST")) {
            methodNotAllowed(ex, "POST");
            return;
        }

        JsonNode body = readJsonBody(ex);
        String deviceId = text(body, "deviceId");
        String nonce = text(body, "nonce");
        String signatureB64 = text(body, "signature");

        if (isBlank(deviceId) || isBlank(nonce) || isBlank(signatureB64)) {
            sendError(ex, 400, "bad_request", "Informe deviceId, nonce e signature");
            return;
        }

        DeviceIdentity identity = loadOrCreateDeviceIdentity();
        if (!timingSafeEquals(deviceId, identity.deviceId())) {
            sendError(ex, 404, "device_not_found", "deviceId invalido");
            return;
        }

        ChallengeState challenge = CHALLENGES.get(identity.deviceId());
        if (challenge == null || challenge.isExpired() || !timingSafeEquals(challenge.nonce(), nonce)) {
            sendError(ex, 401, "invalid_challenge", "Challenge invalido ou expirado");
            return;
        }

        boolean signatureOk = verifyEd25519(identity.publicKeyBase64(), nonce + ":" + identity.deviceId(), signatureB64);
        if (!signatureOk) {
            sendError(ex, 401, "invalid_signature", "Assinatura invalida");
            return;
        }

        CHALLENGES.remove(identity.deviceId(), challenge);

        String jwt = issueAgentJwt(identity.deviceId());
        Instant expiresAt = Instant.now().plus(AGENT_JWT_TTL);
        touchAgentLastSeen(identity.deviceId(), Instant.now().toString());

        ObjectNode out = MAPPER.createObjectNode();
        out.put("ok", true);
        out.put("deviceId", identity.deviceId());
        out.put("agentJwt", jwt);
        out.put("expiresAt", expiresAt.toString());
        sendJson(ex, 200, out);
    }

    // ----------------- Infra / middleware -----------------

    private static void safeHandle(HttpExchange ex, Config cfg, Handler handler) throws IOException {
        try {
            addCommonHeaders(ex.getResponseHeaders());

            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
                Headers h = ex.getResponseHeaders();
                h.set("Access-Control-Allow-Origin", "*");
                h.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
                h.set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
                ex.sendResponseHeaders(204, -1);
                return;
            }

            if (!authorize(ex, cfg)) {
                sendError(ex, 401, "unauthorized", "Token inválido ou ausente");
                return;
            }

            handler.handle();

        } catch (JsonProcessingException jpe) {
            sendError(ex, 400, "bad_json", "JSON inválido: " + safeMsg(jpe));
        } catch (IllegalArgumentException iae) {
            sendError(ex, 400, "bad_request", safeMsg(iae));
        } catch (Throwable t) {
            sendError(ex, 500, "internal_error", safeMsg(t));
        } finally {
            try { ex.close(); } catch (Exception ignored) {}
        }
    }

    private interface Handler { void handle() throws Exception; }

    private static boolean authorize(HttpExchange ex, Config cfg) {
        if (!cfg.authEnabled()) return true;

        String auth = firstHeader(ex.getRequestHeaders(), "Authorization");
        if (isBlank(auth)) return false;

        String token = auth.trim();
        if (token.regionMatches(true, 0, "bearer ", 0, 7)) token = token.substring(7).trim();

        String expected = cfg.token() == null ? "" : cfg.token();
        return timingSafeEquals(token, expected);
    }

    private static String firstHeader(Headers h, String name) {
        List<String> v = h.get(name);
        return (v == null || v.isEmpty()) ? null : v.get(0);
    }

    private static void addCommonHeaders(Headers h) {
        h.set("Access-Control-Allow-Origin", "*");
        h.set("Cache-Control", "no-store");
        h.set("X-Content-Type-Options", "nosniff");
    }

    private static void methodNotAllowed(HttpExchange ex, String allow) throws IOException {
        ex.getResponseHeaders().set("Allow", allow);
        sendError(ex, 405, "method_not_allowed", "Método não permitido");
    }

    private static boolean isMethod(HttpExchange ex, String method) {
        return method.equalsIgnoreCase(ex.getRequestMethod());
    }

    private static void sendError(HttpExchange ex, int status, String code, String message) throws IOException {
        ObjectNode out = MAPPER.createObjectNode();
        out.put("ok", false);
        ObjectNode err = out.putObject("error");
        err.put("code", code);
        err.put("message", message);
        sendJson(ex, status, out);
    }

    private static void sendJson(HttpExchange ex, int status, JsonNode body) throws IOException {
        byte[] bytes = MAPPER.writeValueAsBytes(body);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        addCommonHeaders(ex.getResponseHeaders());
        ex.sendResponseHeaders(status, bytes.length);
        ex.getResponseBody().write(bytes);
    }

    private static JsonNode readJsonBody(HttpExchange ex) throws IOException {
        String ct = firstHeader(ex.getRequestHeaders(), "Content-Type");
        if (ct != null && !ct.toLowerCase(Locale.ROOT).contains("application/json")) {
            throw new IllegalArgumentException("Content-Type deve ser application/json");
        }

        byte[] data = readAllBytesLimited(ex.getRequestBody(), MAX_BODY_BYTES);
        if (data.length == 0) return MAPPER.createObjectNode();
        return MAPPER.readTree(data);
    }

    private static byte[] readAllBytesLimited(InputStream in, int limit) throws IOException {
        byte[] buf = new byte[8192];
        int read;
        int total = 0;

        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.min(limit, 16 * 1024));
        while ((read = in.read(buf)) != -1) {
            total += read;
            if (total > limit) throw new IllegalArgumentException("Body excede limite de " + limit + " bytes");
            out.write(buf, 0, read);
        }
        return out.toByteArray();
    }

    // ----------------- Jobs / challenges maintenance -----------------

    private static void cleanupOldJobs(KeeplyWsServer wsServer) {
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

    private static void cleanupChallenges() {
        for (var e : CHALLENGES.entrySet()) {
            ChallengeState st = e.getValue();
            if (st == null || st.isExpired()) CHALLENGES.remove(e.getKey(), st);
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

    private static void bootstrapScanStorage() {
        try {
            BackupHistoryDb.init();
            ensureAgentStateStore();
            int fixed = BackupHistoryDb.recoverStaleRunning(
                    STALE_HISTORY_RUNNING_AGE,
                    "stale job recovered on boot"
            );
            if (fixed > 0) {
                logWarn("scan startup recovery: " + fixed + " RUNNING antigos marcados como ERROR");
            }
            String schemaError = ensureScanSchemaReady();
            if (schemaError != null) {
                logWarn("scan schema check failed on startup: " + schemaError);
            }
        } catch (Exception e) {
            logWarn("scan startup bootstrap failed: " + safeMsg(e));
        }
    }

    private static void ensureAgentStateStore() {
        try (Connection c = DatabaseBackup.openSingleConnection();
             PreparedStatement ps = c.prepareStatement("""
                     CREATE TABLE IF NOT EXISTS agent_state (
                         state_key TEXT PRIMARY KEY,
                         json_value TEXT NOT NULL,
                         updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                     )
                     """)) {
            ps.execute();
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao preparar tabela agent_state: " + safeMsg(e), e);
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
            return "Falha ao validar schema do scan: " + safeMsg(e);
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

    // ----------------- Utils -----------------

    private static Map<String, String> parseQuery(URI uri) {
        Map<String, String> out = new HashMap<>();
        String query = uri == null ? null : uri.getRawQuery();
        if (isBlank(query)) return out;

        for (String item : query.split("&")) {
            if (isBlank(item)) continue;
            int idx = item.indexOf('=');
            if (idx < 0) out.put(urlDecode(item), "");
            else out.put(urlDecode(item.substring(0, idx)), urlDecode(item.substring(idx + 1)));
        }
        return out;
    }

    private static String urlDecode(String value) {
        try {
            return java.net.URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return value;
        }
    }

    private static int clampInt(String value, int fallback, int min, int max) {
        if (isBlank(value)) return fallback;
        try {
            int v = Integer.parseInt(value.trim());
            return Math.max(min, Math.min(max, v));
        } catch (Exception e) {
            return fallback;
        }
    }

    private static String text(JsonNode body, String field) {
        JsonNode n = body == null ? null : body.get(field);
        return (n == null || n.isNull()) ? null : n.asText(null);
    }

    private static Long longOrNull(JsonNode body, String field) {
        if (body == null) return null;
        JsonNode n = body.get(field);
        if (n == null || n.isNull()) return null;
        if (n.isNumber()) return n.asLong();
        try {
            String raw = n.asText();
            if (raw == null || raw.isBlank()) return null;
            return Long.parseLong(raw.trim());
        } catch (Exception ignored) {
            return null;
        }
    }

    private static void putNullable(ObjectNode obj, String key, String value) {
        if (value == null) obj.putNull(key);
        else obj.put(key, value);
    }

    private static boolean isBlank(String v) {
        return v == null || v.isBlank();
    }

    private static void logInfo(String message) {
        System.out.println("[" + Instant.now() + "] [api] " + message);
    }

    private static void logWarn(String message) {
        System.err.println("[" + Instant.now() + "] [api] " + message);
    }

    private static String clientOf(HttpExchange ex) {
        try {
            if (ex == null || ex.getRemoteAddress() == null) return "-";
            return String.valueOf(ex.getRemoteAddress());
        } catch (Exception ignored) {
            return "-";
        }
    }

    private static String safeMsg(Throwable t) {
        return (t.getMessage() == null || t.getMessage().isBlank())
                ? t.getClass().getSimpleName()
                : t.getMessage();
    }

    private static String safe(String value) {
        return isBlank(value) ? null : value;
    }

    private static String normalizeBackupDestination(String dest) {
        if (isBlank(dest)) return dest;
        String trimmed = dest.trim();
        if (LEGACY_WINDOWS_DEST.equalsIgnoreCase(trimmed)) return DEFAULT_WINDOWS_DEST;
        return trimmed;
    }

    private static String nvl(String value) {
        return value == null ? "" : value;
    }

    static boolean timingSafeEquals(String a, String b) {
        if (a == null || b == null) return false;
        byte[] x = a.getBytes(StandardCharsets.UTF_8);
        byte[] y = b.getBytes(StandardCharsets.UTF_8);
        if (x.length != y.length) return false;
        int r = 0;
        for (int i = 0; i < x.length; i++) r |= x[i] ^ y[i];
        return r == 0;
    }

    // ----------------- Persistência (atômica) -----------------

    private static void writeJsonAtomic(Path target, JsonNode node) throws IOException {
        Path parent = target.getParent();
        if (parent != null) Files.createDirectories(parent);

        Path dir = parent != null ? parent : Path.of(".").toAbsolutePath();
        String baseName = target.getFileName().toString();
        Path tmp = Files.createTempFile(dir, baseName, ".tmp");

        boolean moved = false;
        try {
            String json = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node);
            Files.writeString(tmp, json, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

            try {
                Files.move(tmp, target, REPLACE_EXISTING, ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, target, REPLACE_EXISTING);
            }
            moved = true;
        } finally {
            if (!moved) {
                try { Files.deleteIfExists(tmp); } catch (Exception ignored) {}
            }
        }
    }

    // ----------------- Device identity + pairing persistence -----------------

    private static DeviceIdentity loadOrCreateDeviceIdentity() {
        try {
            JsonNode dbNode = loadStateJson(STATE_KEY_DEVICE_IDENTITY);
            String deviceId = text(dbNode, "deviceId");
            String publicKey = text(dbNode, "publicKey");
            String privateKey = text(dbNode, "privateKey");
            if (!isBlank(deviceId) && !isBlank(publicKey) && !isBlank(privateKey)) {
                return new DeviceIdentity(deviceId, publicKey, privateKey);
            }
        } catch (Exception e) {
            logWarn("Falha ao ler identidade do dispositivo no sqlite: " + safeMsg(e));
        }

        Path path = deviceIdentityFilePath(); // fallback/migracao legada
        try {
            if (Files.exists(path)) {
                JsonNode node = MAPPER.readTree(Files.readString(path, StandardCharsets.UTF_8));
                String deviceId = text(node, "deviceId");
                String publicKey = text(node, "publicKey");
                String privateKey = text(node, "privateKey");
                if (!isBlank(deviceId) && !isBlank(publicKey) && !isBlank(privateKey)) {
                    DeviceIdentity legacy = new DeviceIdentity(deviceId, publicKey, privateKey);
                    saveDeviceIdentity(legacy);
                    tryDeleteLegacyFile(path);
                    return legacy;
                }
            }
        } catch (Exception e) {
            logWarn("Falha ao ler identidade do dispositivo legado: " + safeMsg(e));
        }

        try {
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("Ed25519");
            KeyPair kp = kpg.generateKeyPair();

            DeviceIdentity created = new DeviceIdentity(
                    UUID.randomUUID().toString(),
                    Base64.getEncoder().encodeToString(kp.getPublic().getEncoded()),
                    Base64.getEncoder().encodeToString(kp.getPrivate().getEncoded())
            );
            saveDeviceIdentity(created);
            return created;
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao gerar identidade do dispositivo: " + safeMsg(e), e);
        }
    }

    private static void saveDeviceIdentity(DeviceIdentity identity) {
        try {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("deviceId", identity.deviceId());
            node.put("publicKey", identity.publicKeyBase64());
            node.put("privateKey", identity.privateKeyBase64());
            saveStateJson(STATE_KEY_DEVICE_IDENTITY, node);
        } catch (Exception e) {
            logWarn("Falha ao salvar identidade do dispositivo no sqlite: " + safeMsg(e));
        }
    }

    private static PairingState loadPairingState() {
        try {
            JsonNode node = loadStateJson(STATE_KEY_PAIRING);
            if (node == null || !node.isObject()) return null;
            return new PairingState(
                    text(node, "codeHash"),
                    text(node, "salt"),
                    text(node, "createdAt"),
                    text(node, "expiresAt"),
                    text(node, "usedAt")
            );
        } catch (Exception e) {
            logWarn("Falha ao ler pairing state no sqlite: " + safeMsg(e));
        }

        Path path = pairingFilePath(); // fallback/migracao legada
        try {
            if (!Files.exists(path)) return null;
            JsonNode node = MAPPER.readTree(Files.readString(path, StandardCharsets.UTF_8));
            if (node == null || !node.isObject()) return null;
            PairingState legacy = new PairingState(
                    text(node, "codeHash"),
                    text(node, "salt"),
                    text(node, "createdAt"),
                    text(node, "expiresAt"),
                    text(node, "usedAt")
            );
            savePairingState(legacy);
            tryDeleteLegacyFile(path);
            return legacy;
        } catch (Exception e) {
            logWarn("Falha ao ler pairing state legado: " + safeMsg(e));
            return null;
        }
    }

    private static void savePairingState(PairingState state) {
        try {
            ObjectNode node = MAPPER.createObjectNode();
            putNullable(node, "codeHash", state.codeHash());
            putNullable(node, "salt", state.salt());
            putNullable(node, "createdAt", state.createdAt());
            putNullable(node, "expiresAt", state.expiresAt());
            putNullable(node, "usedAt", state.usedAt());
            saveStateJson(STATE_KEY_PAIRING, node);
        } catch (Exception e) {
            logWarn("Falha ao salvar pairing state no sqlite: " + safeMsg(e));
        }
    }

    private static AgentLinkState loadAgentLinkState() {
        try {
            JsonNode node = loadStateJson(STATE_KEY_LINK);
            if (node != null && node.isObject()) {
                boolean paired = node.path("paired").asBoolean(false);
                Long machineId = longOrNull(node, "machineId");
                Long userId = longOrNull(node, "userId");
                String userEmail = text(node, "userEmail");
                String linkedAt = text(node, "linkedAt");
                String lastSeenAt = text(node, "lastSeenAt");
                String deviceId = text(node, "deviceId");
                return new AgentLinkState(paired, machineId, userId, userEmail, linkedAt, lastSeenAt, deviceId);
            }
        } catch (Exception e) {
            logWarn("Falha ao ler agent link state no sqlite: " + safeMsg(e));
        }

        Path path = agentStateFilePath(); // fallback/migracao legada
        try {
            if (!Files.exists(path)) return AgentLinkState.unpaired();
            JsonNode node = MAPPER.readTree(Files.readString(path, StandardCharsets.UTF_8));

            boolean paired = node != null && node.path("paired").asBoolean(false);
            Long machineId = longOrNull(node, "machineId");
            Long userId = longOrNull(node, "userId");
            String userEmail = text(node, "userEmail");
            String linkedAt = text(node, "linkedAt");
            String lastSeenAt = text(node, "lastSeenAt");
            String deviceId = text(node, "deviceId");
            AgentLinkState legacy = new AgentLinkState(paired, machineId, userId, userEmail, linkedAt, lastSeenAt, deviceId);
            saveAgentLinkState(legacy);
            tryDeleteLegacyFile(path);
            return legacy;
        } catch (Exception e) {
            logWarn("Falha ao ler agent link state legado: " + safeMsg(e));
            return AgentLinkState.unpaired();
        }
    }

    private static void saveAgentLinkState(AgentLinkState state) {
        try {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("paired", state.paired());
            if (state.machineId() == null) node.putNull("machineId"); else node.put("machineId", state.machineId());
            if (state.userId() == null) node.putNull("userId"); else node.put("userId", state.userId());
            putNullable(node, "userEmail", state.userEmail());
            putNullable(node, "linkedAt", state.linkedAt());
            putNullable(node, "lastSeenAt", state.lastSeenAt());
            putNullable(node, "deviceId", state.deviceId());
            saveStateJson(STATE_KEY_LINK, node);
        } catch (Exception e) {
            logWarn("Falha ao salvar agent link state no sqlite: " + safeMsg(e));
        }
    }

    private static JsonNode loadStateJson(String stateKey) {
        if (isBlank(stateKey)) return null;
        try {
            ensureAgentStateStore();
            try (Connection c = DatabaseBackup.openSingleConnection();
                 PreparedStatement ps = c.prepareStatement("SELECT json_value FROM agent_state WHERE state_key = ? LIMIT 1")) {
                ps.setString(1, stateKey);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return null;
                    String raw = rs.getString(1);
                    if (isBlank(raw)) return null;
                    return MAPPER.readTree(raw);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao ler state_key=" + stateKey + " no sqlite: " + safeMsg(e), e);
        }
    }

    private static void saveStateJson(String stateKey, JsonNode node) {
        if (isBlank(stateKey) || node == null) return;
        try {
            ensureAgentStateStore();
            String raw = MAPPER.writeValueAsString(node);
            try (Connection c = DatabaseBackup.openSingleConnection();
                 PreparedStatement ps = c.prepareStatement("""
                         INSERT INTO agent_state(state_key, json_value, updated_at)
                         VALUES(?, ?, datetime('now'))
                         ON CONFLICT(state_key) DO UPDATE SET
                           json_value=excluded.json_value,
                           updated_at=datetime('now')
                         """)) {
                ps.setString(1, stateKey);
                ps.setString(2, raw);
                ps.executeUpdate();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao salvar state_key=" + stateKey + " no sqlite: " + safeMsg(e), e);
        }
    }

    private static void tryDeleteLegacyFile(Path path) {
        try {
            if (path != null) Files.deleteIfExists(path);
        } catch (Exception ignored) {
            // cleanup best effort
        }
    }

    private static PairingIssued issuePairingCode() {
        String code = newPairingCode();
        String createdAt = Instant.now().toString();
        String expiresAt = Instant.now().plus(PAIRING_TTL).toString();
        String salt = randomBase64Url(16);
        String codeHash = sha256Hex(salt + ":" + normalizePairingCode(code));
        savePairingState(new PairingState(codeHash, salt, createdAt, expiresAt, null));
        return new PairingIssued(code, createdAt, expiresAt);
    }

    private static Path pairingFilePath() {
        String custom = System.getProperty("KEEPLY_PAIRING_FILE");
        if (!isBlank(custom)) return Path.of(custom).toAbsolutePath().normalize();
        return Path.of(System.getProperty("user.dir"), ".keeply-pairing.json").toAbsolutePath().normalize();
    }

    private static Path agentStateFilePath() {
        String custom = System.getProperty("KEEPLY_AGENT_STATE_FILE");
        if (!isBlank(custom)) return Path.of(custom).toAbsolutePath().normalize();
        return Path.of(System.getProperty("user.dir"), ".keeply-agent-state.json").toAbsolutePath().normalize();
    }

    private static Path deviceIdentityFilePath() {
        String custom = System.getProperty("KEEPLY_DEVICE_IDENTITY_FILE");
        if (!isBlank(custom)) return Path.of(custom).toAbsolutePath().normalize();
        return Path.of(System.getProperty("user.dir"), ".keeply-device-identity.json").toAbsolutePath().normalize();
    }

    private static Path agentJwtSecretFilePath() {
        String custom = System.getProperty("KEEPLY_AGENT_JWT_SECRET_FILE");
        if (!isBlank(custom)) return Path.of(custom).toAbsolutePath().normalize();
        return Path.of(System.getProperty("user.dir"), ".keeply-agent-jwt.json").toAbsolutePath().normalize();
    }

    private static String newPairingCode() {
        final char[] alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789".toCharArray();
        StringBuilder out = new StringBuilder(11);
        for (int i = 0; i < 10; i++) {
            if (i == 5) out.append('-');
            out.append(alphabet[RANDOM.nextInt(alphabet.length)]);
        }
        return out.toString();
    }

    // sem regex (mais rápido e previsível)
    private static String normalizePairingCode(String code) {
        if (code == null) return null;
        String raw = code.trim().toUpperCase(Locale.ROOT);
        if (raw.isEmpty()) return null;

        StringBuilder sb = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if ((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) sb.append(c);
        }
        return sb.isEmpty() ? null : sb.toString();
    }

    private static String randomBase64Url(int numBytes) {
        byte[] bytes = new byte[numBytes];
        RANDOM.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    // ----------------- Agent JWT + helpers (usado pelo WS) -----------------

    static Optional<AgentJwtClaims> verifyAgentJwt(String token) {
        try {
            if (isBlank(token)) return Optional.empty();
            String[] parts = token.split("\\.");
            if (parts.length != 3) return Optional.empty();

            String data = parts[0] + "." + parts[1];
            String expectedSig = hmacSha256Base64Url(agentJwtSecret(), data);
            if (!timingSafeEquals(parts[2], expectedSig)) return Optional.empty();

            JsonNode payload = MAPPER.readTree(Base64.getUrlDecoder().decode(parts[1]));
            String typ = text(payload, "typ");
            String sub = text(payload, "sub");
            long exp = payload == null ? 0 : payload.path("exp").asLong(0);

            if (!"agent".equals(typ) || isBlank(sub) || exp <= Instant.now().getEpochSecond()) return Optional.empty();
            return Optional.of(new AgentJwtClaims(sub, exp));
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    static boolean isAgentPaired() {
        AgentLinkState st = loadAgentLinkState();
        return st != null && st.paired();
    }

    static Optional<String> getPairedDeviceId() {
        AgentLinkState st = loadAgentLinkState();
        if (st == null || !st.paired()) return Optional.empty();
        if (isBlank(st.deviceId())) return Optional.empty();
        return Optional.of(st.deviceId());
    }

    static void touchAgentLastSeen(String deviceId, String lastSeenAt) {
        if (isBlank(deviceId) || isBlank(lastSeenAt)) return;

        AgentLinkState state = loadAgentLinkState();
        if (state == null || !state.paired()) return;
        if (!isBlank(state.deviceId()) && !timingSafeEquals(state.deviceId(), deviceId)) return;

        saveAgentLinkState(new AgentLinkState(
                state.paired(),
                state.machineId(),
                state.userId(),
                state.userEmail(),
                state.linkedAt(),
                lastSeenAt,
                deviceId
        ));
    }

    private static String issueAgentJwt(String deviceId) {
        ObjectNode header = MAPPER.createObjectNode();
        header.put("alg", "HS256");
        header.put("typ", "JWT");

        long iat = Instant.now().getEpochSecond();
        long exp = iat + AGENT_JWT_TTL.toSeconds();

        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("typ", "agent");
        payload.put("sub", deviceId);
        payload.put("iat", iat);
        payload.put("exp", exp);
        payload.put("jti", randomBase64Url(12));

        String h = base64UrlJson(header);
        String p = base64UrlJson(payload);
        String data = h + "." + p;
        return data + "." + hmacSha256Base64Url(agentJwtSecret(), data);
    }

    private static String base64UrlJson(JsonNode node) {
        try {
            return Base64.getUrlEncoder().withoutPadding().encodeToString(MAPPER.writeValueAsBytes(node));
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao serializar JWT: " + safeMsg(e), e);
        }
    }

    private static String hmacSha256Base64Url(String secret, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] sig = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(sig);
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao assinar JWT: " + safeMsg(e), e);
        }
    }

    private static String agentJwtSecret() {
        Path path = agentJwtSecretFilePath();
        try {
            if (Files.exists(path)) {
                JsonNode node = MAPPER.readTree(Files.readString(path, StandardCharsets.UTF_8));
                String secret = text(node, "secret");
                if (!isBlank(secret)) return secret;
            }
        } catch (Exception e) {
            logWarn("Falha ao ler agent jwt secret: " + safeMsg(e));
        }

        String generated = randomBase64Url(48);
        try {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("secret", generated);
            writeJsonAtomic(path, node);
        } catch (Exception e) {
            logWarn("Falha ao salvar agent jwt secret: " + safeMsg(e));
        }
        return generated;
    }

    private static boolean verifyEd25519(String publicKeyB64, String message, String signatureB64) {
        try {
            byte[] pub = Base64.getDecoder().decode(publicKeyB64);
            byte[] sig = Base64.getDecoder().decode(signatureB64);

            KeyFactory kf = KeyFactory.getInstance("Ed25519");
            PublicKey publicKey = kf.generatePublic(new X509EncodedKeySpec(pub));

            Signature verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(publicKey);
            verifier.update(message.getBytes(StandardCharsets.UTF_8));
            return verifier.verify(sig);
        } catch (Exception ignored) {
            return false;
        }
    }

    // ----------------- Machine info -----------------

    private static ObjectNode machineInfoNode() {
        String hostname = safe(System.getenv("COMPUTERNAME"));
        if (isBlank(hostname)) hostname = safe(System.getenv("HOSTNAME"));
        if (isBlank(hostname)) hostname = "unknown-host";

        String osName = safe(System.getProperty("os.name"));
        String osVersion = safe(System.getProperty("os.version"));
        String arch = safe(System.getProperty("os.arch"));
        String userName = safe(System.getProperty("user.name"));
        String machineName = (isBlank(userName) ? "usuario" : userName) + "@" + hostname;
        String machineAlias = hostname;

        String lastIp = detectFirstIpv4();
        String hardwareHashHex = sha256Hex(String.join("|",
                nvl(hostname), nvl(osName), nvl(osVersion), nvl(arch), nvl(lastIp)));

        ObjectNode out = MAPPER.createObjectNode();
        out.put("machineName", machineName);
        out.put("machineAlias", machineAlias);
        putNullable(out, "hostname", hostname);
        putNullable(out, "osName", osName);
        putNullable(out, "osVersion", osVersion);
        putNullable(out, "arch", arch);
        putNullable(out, "lastIp", lastIp);
        out.put("hardwareHashHex", hardwareHashHex);
        return out;
    }

    private static String detectFirstIpv4() {
        try {
            Enumeration<java.net.NetworkInterface> nifs = java.net.NetworkInterface.getNetworkInterfaces();
            if (nifs == null) return null;

            while (nifs.hasMoreElements()) {
                java.net.NetworkInterface nif = nifs.nextElement();
                if (!nif.isUp() || nif.isLoopback()) continue;

                Enumeration<InetAddress> addrs = nif.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    InetAddress addr = addrs.nextElement();
                    if (addr instanceof java.net.Inet4Address && !addr.isLoopbackAddress()) {
                        return addr.getHostAddress();
                    }
                }
            }
        } catch (Exception ignored) {
            return null;
        }
        return null;
    }

    private static String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder out = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) out.append(String.format("%02x", b));
            return out.toString();
        } catch (Exception ignored) {
            return "";
        }
    }

    // ----------------- Config / env -----------------

    private static void bootstrapEnv() {
        try {
            Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

            String dbUrl = Objects.requireNonNullElse(dotenv.get("DB_URL"), "jdbc:sqlite:keeply.db");
            if (isBlank(System.getProperty("DB_URL"))) System.setProperty("DB_URL", dbUrl);

            String token = dotenv.get("KEEPLY_API_TOKEN");
            if (!isBlank(token) && isBlank(System.getProperty("KEEPLY_API_TOKEN"))) {
                System.setProperty("KEEPLY_API_TOKEN", token);
            }
        } catch (Exception ignored) {
            if (isBlank(System.getProperty("DB_URL"))) {
                System.setProperty("DB_URL", "jdbc:sqlite:keeply.db");
            }
        }
    }

    private static Config parseConfig(String[] args) {
        int port = 25420;
        String bind = "127.0.0.1";
        String token = System.getProperty("KEEPLY_API_TOKEN");
        int wsPort = 8091;

        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                if ("--port".equalsIgnoreCase(a) && i + 1 < args.length) port = tryInt(args[++i], port);
                else if ("--bind".equalsIgnoreCase(a) && i + 1 < args.length) bind = args[++i];
                else if ("--token".equalsIgnoreCase(a) && i + 1 < args.length) token = args[++i];
                else if ("--ws-port".equalsIgnoreCase(a) && i + 1 < args.length) wsPort = tryInt(args[++i], wsPort);
            }
        }

        port = Math.max(1, Math.min(65535, port));
        wsPort = Math.max(0, Math.min(65535, wsPort));

        boolean authEnabled = !isBlank(token);
        // httpWorkers/scanWorkers ficaram desnecessários com virtual threads, mas mantemos no record por compatibilidade
        return new Config(bind, port, authEnabled, token, 0, 0, wsPort);
    }

    private static int tryInt(String s, int fallback) {
        try { return Integer.parseInt(s.trim()); }
        catch (Exception e) { return fallback; }
    }

    // ----------------- Records (DTO/config/state) -----------------

    private record Config(
            String bindHost,
            int port,
            boolean authEnabled,
            String token,
            int httpWorkers,
            int scanWorkers,
            int wsPort
    ) {}

    private record PairingState(String codeHash, String salt, String createdAt, String expiresAt, String usedAt) {
        boolean isExpired() {
            if (isBlank(expiresAt)) return true;
            try { return Instant.now().isAfter(Instant.parse(expiresAt)); }
            catch (Exception e) { return true; }
        }
        PairingState markUsed(String ts) {
            return new PairingState(codeHash, salt, createdAt, expiresAt, ts);
        }
    }

    private record PairingIssued(String code, String createdAt, String expiresAt) {}

    private record AgentLinkState(
            boolean paired,
            Long machineId,
            Long userId,
            String userEmail,
            String linkedAt,
            String lastSeenAt,
            String deviceId
    ) {
        static AgentLinkState unpaired() {
            return new AgentLinkState(false, null, null, null, null, null, null);
        }
    }

    private record DeviceIdentity(String deviceId, String publicKeyBase64, String privateKeyBase64) {}

    private record ChallengeState(String nonce, Instant expiresAt) {
        boolean isExpired() {
            return expiresAt == null || Instant.now().isAfter(expiresAt);
        }
    }

    static record AgentJwtClaims(String deviceId, long expEpochSeconds) {}
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
            ObjectNode out = MAPPER.createObjectNode();
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
