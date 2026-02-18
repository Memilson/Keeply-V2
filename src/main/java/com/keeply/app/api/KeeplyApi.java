// KeeplyApi.java
package com.keeply.app.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.keeply.app.cli.cli;
import com.keeply.app.inventory.BackupHistoryDb;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.cdimascio.dotenv.Dotenv;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public final class KeeplyApi {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final int MAX_BODY_BYTES = 64 * 1024;

    private static final Duration JOB_TTL = Duration.ofHours(6);
    private static final Duration CLEANUP_EVERY = Duration.ofSeconds(30);

    private static final String BASE = "/api/keeply";
    private static final String HEALTH = BASE + "/health";
    private static final String HISTORY = BASE + "/history";
    private static final String SCAN = BASE + "/scan";
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

    private KeeplyApi() {}

    public static void run(String[] args) {
        bootstrapEnv();
        Config cfg = parseConfig(args);

        ExecutorService httpPool = null;
        ExecutorService scanPool = null;
        ScheduledExecutorService maint = null;
        HttpServer server = null;
        KeeplyWsServer wsServer = null;

        try {
            InetAddress bindAddr = InetAddress.getByName(cfg.bindHost);
            server = HttpServer.create(new InetSocketAddress(bindAddr, cfg.port), 0);

            if (cfg.wsPort > 0) {
                wsServer = new KeeplyWsServer(new InetSocketAddress(bindAddr, cfg.wsPort), cfg.token);
                wsServer.start();
            }

            httpPool = Executors.newFixedThreadPool(cfg.httpWorkers, namedFactory("keeply-http-"));
            scanPool = Executors.newFixedThreadPool(cfg.scanWorkers, namedFactory("keeply-scan-"));
            maint = Executors.newSingleThreadScheduledExecutor(namedFactory("keeply-maint-"));

            server.setExecutor(httpPool);

            KeeplyWsServer finalWsServer = wsServer;
            ExecutorService finalScanPool = scanPool;

            server.createContext(HEALTH, ex -> safeHandle(ex, cfg, () -> handleHealth(ex)));
            server.createContext(HISTORY, ex -> safeHandle(ex, cfg, () -> handleHistory(ex)));
            server.createContext(SCAN, ex -> safeHandle(ex, cfg, () -> handleScan(ex, finalScanPool, finalWsServer)));
            server.createContext(PAIRING, ex -> safeHandle(ex, cfg, () -> handlePairing(ex)));
            server.createContext(AGENT_CHALLENGE, ex -> safeHandle(ex, cfg, () -> handleAgentChallenge(ex)));
            server.createContext(AGENT_LOGIN, ex -> safeHandle(ex, cfg, () -> handleAgentLogin(ex)));

            // manutenção: jobs + challenges
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

            System.out.println("Keeply API online em http://" + cfg.bindHost + ":" + cfg.port);
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
            System.out.println("  GET    " + AGENT_CHALLENGE + "?deviceId=...");
            System.out.println("  POST   " + AGENT_LOGIN);
            if (cfg.wsPort > 0) {
                System.out.println("WebSocket:");
                System.out.println("  ws://" + cfg.bindHost + ":" + cfg.wsPort + "/ws/keeply");
            }

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

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try { if (shutdownServer != null) shutdownServer.stop(0); } catch (Exception ignored) {}
                try { if (shutdownMaint != null) shutdownMaint.shutdownNow(); } catch (Exception ignored) {}
                try { if (shutdownScan != null) shutdownScan.shutdownNow(); } catch (Exception ignored) {}
                try { if (shutdownHttp != null) shutdownHttp.shutdownNow(); } catch (Exception ignored) {}
                if (shutdownWs != null) {
                    try { shutdownWs.stop(1000); } catch (Exception ignored) {}
                }
            }, "keeply-api-shutdown"));

        } catch (Exception e) {
            System.err.println("Falha ao iniciar API: " + safeMsg(e));
            System.exit(1);
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

            JsonNode body = readJsonBody(ex);
            String root = text(body, "root");
            String dest = text(body, "dest");
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

            if (wsServer != null) wsServer.broadcastEvent("scan.created", Map.of("jobId", jobId));

            Future<?> f = scanPool.submit(() -> {
                job.markRunning();
                if (wsServer != null) wsServer.broadcastEvent("scan.running", Map.of("jobId", jobId));

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
                    exit = cli.execute(cliArgs.toArray(String[]::new));
                } catch (CancellationException ce) {
                    job.markCancelled("cancelled");
                    if (wsServer != null) wsServer.broadcastEvent("scan.cancelled", Map.of("jobId", jobId));
                    return;
                } catch (Throwable t) {
                    exit = 999;
                    job.markFailed(exit, safeMsg(t));
                    if (wsServer != null) {
                        wsServer.broadcastEvent("scan.failed", Map.of(
                                "jobId", jobId,
                                "exitCode", exit,
                                "message", job.message
                        ));
                    }
                    return;
                }

                if (exit == 0) {
                    job.markSuccess();
                    if (wsServer != null) wsServer.broadcastEvent("scan.success", Map.of("jobId", jobId));
                } else {
                    job.markFailed(exit, "scan retornou exitCode != 0");
                    if (wsServer != null) {
                        wsServer.broadcastEvent("scan.failed", Map.of(
                                "jobId", jobId,
                                "exitCode", exit,
                                "message", job.message
                        ));
                    }
                }
            });

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
            sendJson(ex, 200, job.toJson());
            return;
        }

        if (isMethod(ex, "DELETE")) {
            boolean cancelled = job.cancel();

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
            if (!timingSafeEquals(sha256Hex(state.salt() + ":" + informedCode), state.codeHash())) {
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
        if (!cfg.authEnabled) return true;

        String auth = firstHeader(ex.getRequestHeaders(), "Authorization");
        if (isBlank(auth)) return false;

        String token = auth.trim();
        if (token.regionMatches(true, 0, "bearer ", 0, 7)) token = token.substring(7).trim();

        String expected = cfg.token == null ? "" : cfg.token;
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
        return MAPPER.readTree(new String(data, StandardCharsets.UTF_8));
    }

    private static byte[] readAllBytesLimited(InputStream in, int limit) throws IOException {
        byte[] buf = new byte[8192];
        int read;
        int total = 0;
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();

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

    private static String safeMsg(Throwable t) {
        return (t.getMessage() == null || t.getMessage().isBlank())
                ? t.getClass().getSimpleName()
                : t.getMessage();
    }

    private static String safe(String value) {
        return isBlank(value) ? null : value;
    }

    private static String nvl(String value) {
        return value == null ? "" : value;
    }

    private static ThreadFactory namedFactory(String prefix) {
        return new ThreadFactory() {
            private final ThreadFactory base = Executors.defaultThreadFactory();
            private final AtomicInteger seq = new AtomicInteger(1);
            @Override public Thread newThread(Runnable r) {
                Thread t = base.newThread(r);
                t.setName(prefix + seq.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        };
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

    // ----------------- Device identity + pairing persistence -----------------

    private static DeviceIdentity loadOrCreateDeviceIdentity() {
        Path path = deviceIdentityFilePath();
        try {
            if (Files.exists(path)) {
                JsonNode node = MAPPER.readTree(Files.readString(path));
                String deviceId = text(node, "deviceId");
                String publicKey = text(node, "publicKey");
                String privateKey = text(node, "privateKey");
                if (!isBlank(deviceId) && !isBlank(publicKey) && !isBlank(privateKey)) {
                    return new DeviceIdentity(deviceId, publicKey, privateKey);
                }
            }
        } catch (Exception ignored) {}

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
        Path path = deviceIdentityFilePath();
        try {
            Path parent = path.getParent();
            if (parent != null) Files.createDirectories(parent);
            ObjectNode node = MAPPER.createObjectNode();
            node.put("deviceId", identity.deviceId());
            node.put("publicKey", identity.publicKeyBase64());
            node.put("privateKey", identity.privateKeyBase64());
            Files.writeString(
                    path,
                    MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
        } catch (Exception ignored) {}
    }

    private static PairingState loadPairingState() {
        Path path = pairingFilePath();
        try {
            if (!Files.exists(path)) return null;
            JsonNode node = MAPPER.readTree(Files.readString(path));
            if (node == null || !node.isObject()) return null;
            return new PairingState(
                    text(node, "codeHash"),
                    text(node, "salt"),
                    text(node, "createdAt"),
                    text(node, "expiresAt"),
                    text(node, "usedAt")
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static void savePairingState(PairingState state) {
        Path path = pairingFilePath();
        try {
            Path parent = path.getParent();
            if (parent != null) Files.createDirectories(parent);
            ObjectNode node = MAPPER.createObjectNode();
            putNullable(node, "codeHash", state.codeHash());
            putNullable(node, "salt", state.salt());
            putNullable(node, "createdAt", state.createdAt());
            putNullable(node, "expiresAt", state.expiresAt());
            putNullable(node, "usedAt", state.usedAt());
            Files.writeString(
                    path,
                    MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
        } catch (Exception ignored) {}
    }

    private static AgentLinkState loadAgentLinkState() {
        Path path = agentStateFilePath();
        try {
            if (!Files.exists(path)) return AgentLinkState.unpaired();
            JsonNode node = MAPPER.readTree(Files.readString(path));
            boolean paired = node != null && node.path("paired").asBoolean(false);
            Long machineId = longOrNull(node, "machineId");
            Long userId = longOrNull(node, "userId");
            String userEmail = text(node, "userEmail");
            String linkedAt = text(node, "linkedAt");
            String lastSeenAt = text(node, "lastSeenAt");
            String deviceId = text(node, "deviceId");
            return new AgentLinkState(paired, machineId, userId, userEmail, linkedAt, lastSeenAt, deviceId);
        } catch (Exception ignored) {
            return AgentLinkState.unpaired();
        }
    }

    private static void saveAgentLinkState(AgentLinkState state) {
        Path path = agentStateFilePath();
        try {
            Path parent = path.getParent();
            if (parent != null) Files.createDirectories(parent);
            ObjectNode node = MAPPER.createObjectNode();
            node.put("paired", state.paired());
            if (state.machineId() == null) node.putNull("machineId"); else node.put("machineId", state.machineId());
            if (state.userId() == null) node.putNull("userId"); else node.put("userId", state.userId());
            putNullable(node, "userEmail", state.userEmail());
            putNullable(node, "linkedAt", state.linkedAt());
            putNullable(node, "lastSeenAt", state.lastSeenAt());
            putNullable(node, "deviceId", state.deviceId());
            Files.writeString(
                    path,
                    MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
        } catch (Exception ignored) {}
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

    private static String normalizePairingCode(String code) {
        if (code == null) return null;
        String raw = code.trim().toUpperCase(Locale.ROOT);
        if (raw.isEmpty()) return null;
        return raw.replaceAll("[^A-Z0-9]", "");
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

            JsonNode payload = MAPPER.readTree(new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8));
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
                JsonNode node = MAPPER.readTree(Files.readString(path));
                String secret = text(node, "secret");
                if (!isBlank(secret)) return secret;
            }
        } catch (Exception ignored) {}

        String generated = randomBase64Url(48);
        try {
            Path parent = path.getParent();
            if (parent != null) Files.createDirectories(parent);
            ObjectNode node = MAPPER.createObjectNode();
            node.put("secret", generated);
            Files.writeString(
                    path,
                    MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
        } catch (Exception ignored) {}
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
        int httpWorkers = 8;
        int scanWorkers = 2;
        int wsPort = 8091;

        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                if ("--port".equalsIgnoreCase(a) && i + 1 < args.length) port = tryInt(args[++i], port);
                else if ("--bind".equalsIgnoreCase(a) && i + 1 < args.length) bind = args[++i];
                else if ("--token".equalsIgnoreCase(a) && i + 1 < args.length) token = args[++i];
                else if ("--http-workers".equalsIgnoreCase(a) && i + 1 < args.length) httpWorkers = tryInt(args[++i], httpWorkers);
                else if ("--scan-workers".equalsIgnoreCase(a) && i + 1 < args.length) scanWorkers = tryInt(args[++i], scanWorkers);
                else if ("--ws-port".equalsIgnoreCase(a) && i + 1 < args.length) wsPort = tryInt(args[++i], wsPort);
            }
        }

        port = Math.max(1, Math.min(65535, port));
        httpWorkers = Math.max(2, Math.min(64, httpWorkers));
        scanWorkers = Math.max(1, Math.min(16, scanWorkers));
        wsPort = Math.max(0, Math.min(65535, wsPort));

        boolean authEnabled = !isBlank(token);
        return new Config(bind, port, authEnabled, token, httpWorkers, scanWorkers, wsPort);
    }

    private static int tryInt(String s, int fallback) {
        try { return Integer.parseInt(s.trim()); }
        catch (Exception e) { return fallback; }
    }

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

    // ----------------- Job model -----------------

    private static final class ScanJob {
        private final String id;
        private final String root;
        private final String dest;
        private final Instant createdAt;

        private volatile Instant startedAt;
        private volatile Instant finishedAt;

        private volatile String state; // created | running | success | failed | cancelled
        private volatile Integer exitCode;
        private volatile String message;

        private final AtomicBoolean started = new AtomicBoolean(false);
        private volatile Future<?> future;

        private ScanJob(String id, String root, String dest) {
            this.id = id;
            this.root = root;
            this.dest = dest;
            this.createdAt = Instant.now();
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
            if (started.compareAndSet(false, true)) {
                this.startedAt = Instant.now();
                this.state = "running";
            }
        }

        void markSuccess() {
            this.state = "success";
            this.exitCode = 0;
            this.finishedAt = Instant.now();
            this.message = "ok";
        }

        void markCancelled(String msg) {
            this.state = "cancelled";
            this.exitCode = null;
            this.finishedAt = Instant.now();
            this.message = msg;
        }

        void markFailed(int exitCode, String msg) {
            this.state = "failed";
            this.exitCode = exitCode;
            this.finishedAt = Instant.now();
            this.message = msg;
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
            if (exitCode == null) job.putNull("exitCode"); else job.put("exitCode", exitCode);
            if (message == null) job.putNull("message"); else job.put("message", message);

            return out;
        }
    }
}
