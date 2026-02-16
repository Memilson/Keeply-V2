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
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class KeeplyApi {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Limite de payload (evita request gigante travando memória)
    private static final int MAX_BODY_BYTES = 64 * 1024;

    // Evita JOBS crescer pra sempre
    private static final Duration JOB_TTL = Duration.ofHours(6);
    private static final Duration CLEANUP_EVERY = Duration.ofSeconds(30);

    private static final String BASE = "/api/keeply";
    private static final String HEALTH = BASE + "/health";
    private static final String HISTORY = BASE + "/history";
    private static final String SCAN = BASE + "/scan";

    private static final ConcurrentMap<String, ScanJob> JOBS = new ConcurrentHashMap<>();

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
            // Context em /scan cobre também /scan/{id} e /scan/{id}/cancel
            server.createContext(SCAN, ex -> safeHandle(ex, cfg, () -> handleScan(ex, finalScanPool, finalWsServer)));

            // limpeza de jobs antigos
            maint.scheduleAtFixedRate(
                    () -> cleanupOldJobs(finalWsServer),
                    CLEANUP_EVERY.toMillis(),
                    CLEANUP_EVERY.toMillis(),
                    TimeUnit.MILLISECONDS
            );

            server.start();

            System.out.println("Keeply API online em http://" + cfg.bindHost + ":" + cfg.port);
            System.out.println("Endpoints:");
            System.out.println("  GET    " + HEALTH);
            System.out.println("  GET    " + HISTORY + "?limit=20");
            System.out.println("  POST   " + SCAN);
            System.out.println("  GET    " + SCAN + "/{jobId}");
            System.out.println("  DELETE " + SCAN + "/{jobId}  (cancel)");
            if (cfg.wsPort > 0) {
                System.out.println("WebSocket:");
                System.out.println("  ws://" + cfg.bindHost + ":" + cfg.wsPort + "/ws/keeply");
            }

            HttpServer shutdownServer = server;
            ExecutorService shutdownScan = scanPool;
            ExecutorService shutdownHttp = httpPool;
            ScheduledExecutorService shutdownMaint = maint;
            KeeplyWsServer shutdownWs = wsServer;

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    if (shutdownServer != null) shutdownServer.stop(0);
                } catch (Exception ignored) {}
                try {
                    if (shutdownMaint != null) shutdownMaint.shutdownNow();
                } catch (Exception ignored) {}
                try {
                    if (shutdownScan != null) shutdownScan.shutdownNow();
                } catch (Exception ignored) {}
                try {
                    if (shutdownHttp != null) shutdownHttp.shutdownNow();
                } catch (Exception ignored) {}
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
        // DELETE /scan/{jobId}  (cancel)
        String path = ex.getRequestURI().getPath();

        String rest = path.length() > SCAN.length() ? path.substring(SCAN.length()) : "";
        if (rest.isBlank() || "/".equals(rest)) {
            // POST create job
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

            // Segurança básica: evita NUL e coisas bizarras
            if (root.indexOf('\0') >= 0 || dest.indexOf('\0') >= 0) {
                sendError(ex, 400, "bad_request", "Caminho inválido");
                return;
            }

            String jobId = UUID.randomUUID().toString();
            ScanJob job = ScanJob.created(jobId, root, dest);
            JOBS.put(jobId, job);

            if (wsServer != null) {
                wsServer.broadcastEvent("scan.created", Map.of("jobId", jobId));
            }

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
                    if (wsServer != null) wsServer.broadcastEvent("scan.failed", Map.of(
                            "jobId", jobId,
                            "exitCode", exit,
                            "message", job.message
                    ));
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

        // GET/DELETE status/cancel
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

    // ----------------- Infra / middleware -----------------

    private static void safeHandle(HttpExchange ex, Config cfg, Handler handler) throws IOException {
        try {
            addCommonHeaders(ex.getResponseHeaders());

            // Preflight CORS deve passar sem auth
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

    private interface Handler {
        void handle() throws Exception;
    }

    private static boolean authorize(HttpExchange ex, Config cfg) {
        if (!cfg.authEnabled) return true;

        String auth = firstHeader(ex.getRequestHeaders(), "Authorization");
        if (isBlank(auth)) return false;

        // aceita: "Bearer <token>" ou "<token>"
        String token = auth.trim();
        if (token.regionMatches(true, 0, "bearer ", 0, 7)) {
            token = token.substring(7).trim();
        }

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

    // ----------------- Jobs maintenance -----------------

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

    // ----------------- Utils -----------------

    private static Map<String, String> parseQuery(URI uri) {
        Map<String, String> out = new HashMap<>();
        String query = uri == null ? null : uri.getRawQuery();
        if (isBlank(query)) return out;

        for (String item : query.split("&")) {
            if (isBlank(item)) continue;
            int idx = item.indexOf('=');
            if (idx < 0) {
                out.put(urlDecode(item), "");
            } else {
                out.put(urlDecode(item.substring(0, idx)), urlDecode(item.substring(idx + 1)));
            }
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

    private static boolean timingSafeEquals(String a, String b) {
        if (a == null || b == null) return false;
        byte[] x = a.getBytes(StandardCharsets.UTF_8);
        byte[] y = b.getBytes(StandardCharsets.UTF_8);
        if (x.length != y.length) return false;
        int r = 0;
        for (int i = 0; i < x.length; i++) r |= x[i] ^ y[i];
        return r == 0;
    }

    // ----------------- Config / env -----------------

    private static void bootstrapEnv() {
        try {
            Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
            String dbUrl = Objects.requireNonNullElse(dotenv.get("DB_URL"), "jdbc:sqlite:keeply.db");
            if (isBlank(System.getProperty("DB_URL"))) {
                System.setProperty("DB_URL", dbUrl);
            }
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
            // tenta cancelar tarefa (melhor esforço)
            Future<?> f = this.future;
            if (f == null) return false;

            boolean ok = f.cancel(true);
            if (ok) {
                markCancelled("cancel requested");
            }
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
