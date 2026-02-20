package com.keeply.app.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.keeply.app.inventory.BackupHistoryDb;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.cdimascio.dotenv.Dotenv;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class KeeplyApi {
    private static final Duration CLEANUP_EVERY = Duration.ofSeconds(30);

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

    private static final KeeplyScanApi SCAN_API = new KeeplyScanApi();
    private static final KeeplyRestoreApi RESTORE_API = new KeeplyRestoreApi();
    private static final AgentStateStore STATE_STORE = new AgentStateStore();
    private static final KeeplyPairingApi PAIRING_API = new KeeplyPairingApi(STATE_STORE);
    private static final KeeplyAgentAuthApi AGENT_AUTH_API = new KeeplyAgentAuthApi(STATE_STORE);

    private KeeplyApi() {}

    public static void run(String[] args) {
        bootstrapEnv();
        bootstrapStorage();

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
            server.createContext(SCAN, ex -> safeHandle(ex, cfg, () -> SCAN_API.handle(ex, finalScanPool, finalWsServer)));
            server.createContext(RESTORE, ex -> safeHandle(ex, cfg, () -> RESTORE_API.handle(ex, finalWsServer)));
            server.createContext(FOLDERS, ex -> safeHandle(ex, cfg, () -> handleFolders(ex)));
            server.createContext(PAIRING, ex -> safeHandle(ex, cfg, () -> PAIRING_API.handle(ex)));
            server.createContext(AGENT_CHALLENGE, ex -> safeHandle(ex, cfg, () -> AGENT_AUTH_API.handleChallenge(ex)));
            server.createContext(AGENT_LOGIN, ex -> safeHandle(ex, cfg, () -> AGENT_AUTH_API.handleLogin(ex)));

            maint.scheduleAtFixedRate(
                    () -> {
                        SCAN_API.cleanupOldJobs(finalWsServer);
                        AGENT_AUTH_API.cleanupChallenges();
                    },
                    CLEANUP_EVERY.toMillis(),
                    CLEANUP_EVERY.toMillis(),
                    TimeUnit.MILLISECONDS
            );

            server.start();
            printStartupInfo(cfg);
            PAIRING_API.printStartupPairingIfNeeded();

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

            stopLatch.await();

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println("Falha ao iniciar API: " + KeeplyHttp.safeMsg(e));
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

    private static void handleHealth(HttpExchange ex) throws IOException {
        if (!KeeplyHttp.isMethod(ex, "GET")) {
            KeeplyHttp.methodNotAllowed(ex, "GET");
            return;
        }

        ObjectNode out = KeeplyHttp.mapper().createObjectNode();
        out.put("ok", true);
        out.put("service", "keeply-api");
        out.put("ts", Instant.now().toString());
        KeeplyHttp.sendJson(ex, 200, out);
    }

    private static void handleHistory(HttpExchange ex) throws IOException {
        if (!KeeplyHttp.isMethod(ex, "GET")) {
            KeeplyHttp.methodNotAllowed(ex, "GET");
            return;
        }

        int limit = KeeplyHttp.clampInt(KeeplyHttp.parseQuery(ex.getRequestURI()).get("limit"), 20, 1, 200);
        KeeplyHttp.logInfo("history request from " + KeeplyHttp.clientOf(ex) + " limit=" + limit);

        List<BackupHistoryDb.HistoryRow> rows = new ArrayList<>(BackupHistoryDb.listRecent(limit));

        ObjectNode out = KeeplyHttp.mapper().createObjectNode();
        out.put("ok", true);
        out.put("limit", limit);

        ArrayNode items = out.putArray("items");
        for (BackupHistoryDb.HistoryRow r : rows) {
            ObjectNode it = items.addObject();
            it.put("id", r.id());
            KeeplyHttp.putNullable(it, "startedAt", r.startedAt());
            KeeplyHttp.putNullable(it, "finishedAt", r.finishedAt());
            KeeplyHttp.putNullable(it, "status", r.status());
            KeeplyHttp.putNullable(it, "backupType", r.backupType());
            KeeplyHttp.putNullable(it, "rootPath", r.rootPath());
            KeeplyHttp.putNullable(it, "destPath", r.destPath());
            it.put("filesProcessed", r.filesProcessed());
            it.put("errors", r.errors());
            if (r.scanId() == null) it.putNull("scanId"); else it.put("scanId", r.scanId());
            KeeplyHttp.putNullable(it, "message", r.message());
        }

        KeeplyHttp.logInfo("history response items=" + rows.size());
        KeeplyHttp.sendJson(ex, 200, out);
    }

    private static void handleFolders(HttpExchange ex) throws IOException {
        if (!KeeplyHttp.isMethod(ex, "GET")) {
            KeeplyHttp.methodNotAllowed(ex, "GET");
            return;
        }

        String rawPath = KeeplyHttp.parseQuery(ex.getRequestURI()).get("path");
        Path current;
        try {
            current = KeeplyHttp.isBlank(rawPath) ? Path.of(System.getProperty("user.home")) : Path.of(rawPath);
            current = current.toAbsolutePath().normalize();
        } catch (Exception e) {
            KeeplyHttp.sendError(ex, 400, "bad_request", "path invalido");
            return;
        }

        if (!Files.exists(current) || !Files.isDirectory(current)) {
            KeeplyHttp.sendError(ex, 404, "not_found", "pasta nao encontrada");
            return;
        }

        ObjectNode out = KeeplyHttp.mapper().createObjectNode();
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
            KeeplyHttp.sendError(ex, 500, "list_failed", KeeplyHttp.safeMsg(e));
            return;
        }

        KeeplyHttp.logInfo("folders list from " + KeeplyHttp.clientOf(ex) + " current=" + current + " items=" + items.size());
        KeeplyHttp.sendJson(ex, 200, out);
    }

    private static void safeHandle(HttpExchange ex, Config cfg, Handler handler) throws IOException {
        try {
            KeeplyHttp.addCommonHeaders(ex.getResponseHeaders());

            if (KeeplyHttp.handlePreflightIfNeeded(ex)) {
                return;
            }

            if (!authorize(ex, cfg)) {
                KeeplyHttp.sendError(ex, 401, "unauthorized", "Token inválido ou ausente");
                return;
            }

            handler.handle();

        } catch (JsonProcessingException jpe) {
            KeeplyHttp.sendError(ex, 400, "bad_json", "JSON inválido: " + KeeplyHttp.safeMsg(jpe));
        } catch (IllegalArgumentException iae) {
            KeeplyHttp.sendError(ex, 400, "bad_request", KeeplyHttp.safeMsg(iae));
        } catch (Throwable t) {
            KeeplyHttp.sendError(ex, 500, "internal_error", KeeplyHttp.safeMsg(t));
        } finally {
            try { ex.close(); } catch (Exception ignored) {}
        }
    }

    private interface Handler {
        void handle() throws Exception;
    }

    private static boolean authorize(HttpExchange ex, Config cfg) {
        if (!cfg.authEnabled()) return true;

        String auth = KeeplyHttp.firstHeader(ex, "Authorization");
        if (KeeplyHttp.isBlank(auth)) return false;

        String token = auth.trim();
        if (token.regionMatches(true, 0, "bearer ", 0, 7)) token = token.substring(7).trim();

        String expected = cfg.token() == null ? "" : cfg.token();
        return timingSafeEquals(token, expected);
    }

    private static void bootstrapStorage() {
        try {
            STATE_STORE.bootstrap();
        } catch (Exception e) {
            KeeplyHttp.logWarn("agent state bootstrap failed: " + KeeplyHttp.safeMsg(e));
        }
        SCAN_API.bootstrapStorage();
    }

    private static void bootstrapEnv() {
        try {
            Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

            String dbUrl = Objects.requireNonNullElse(dotenv.get("DB_URL"), "jdbc:sqlite:keeply.db");
            if (KeeplyHttp.isBlank(System.getProperty("DB_URL"))) System.setProperty("DB_URL", dbUrl);

            String token = dotenv.get("KEEPLY_API_TOKEN");
            if (!KeeplyHttp.isBlank(token) && KeeplyHttp.isBlank(System.getProperty("KEEPLY_API_TOKEN"))) {
                System.setProperty("KEEPLY_API_TOKEN", token);
            }
        } catch (Exception ignored) {
            if (KeeplyHttp.isBlank(System.getProperty("DB_URL"))) {
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

        boolean authEnabled = !KeeplyHttp.isBlank(token);
        return new Config(bind, port, authEnabled, token, 0, 0, wsPort);
    }

    private static int tryInt(String s, int fallback) {
        try { return Integer.parseInt(s.trim()); }
        catch (Exception e) { return fallback; }
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

    static Optional<AgentJwtClaims> verifyAgentJwt(String token) {
        return AGENT_AUTH_API.verifyAgentJwt(token);
    }

    static boolean isAgentPaired() {
        return STATE_STORE.isAgentPaired();
    }

    static Optional<String> getPairedDeviceId() {
        return STATE_STORE.getPairedDeviceId();
    }

    static void touchAgentLastSeen(String deviceId, String lastSeenAt) {
        STATE_STORE.touchAgentLastSeen(deviceId, lastSeenAt);
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

    static record AgentJwtClaims(String deviceId, long expEpochSeconds) {}
}
