package com.keeply.app.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.java_websocket.WebSocket;
import org.java_websocket.framing.CloseFrame;
import org.java_websocket.framing.Framedata;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
final class KeeplyWsServer extends WebSocketServer {
    private static final String PRIMARY_PATH = "/ws/keeply";
    private static final String LEGACY_PATH  = "/ws";
    private static final int MAX_MESSAGE_BYTES = 64 * 1024; // 64KB
    private static final int RATE_LIMIT_PER_10S = 30;       // msgs / 10s por conexão
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Map<WebSocket, SessionState> sessions = new ConcurrentHashMap<>();
    private final String expectedToken;
    KeeplyWsServer(InetSocketAddress address, String expectedToken) {
        super(address);
        this.expectedToken = (expectedToken == null || expectedToken.isBlank()) ? null : expectedToken.trim();}
    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        String rd = (handshake == null) ? "" : String.valueOf(handshake.getResourceDescriptor());
        UriParts parts = parsePathAndQuery(rd);
        if (!PRIMARY_PATH.equals(parts.path()) && !LEGACY_PATH.equals(parts.path())) {
            conn.close(CloseFrame.POLICY_VALIDATION, "invalid_path");
            return;   }
        if (expectedToken != null) {
            String token = extractToken(handshake, parts.query());
            if (token == null || !timingSafeEquals(token, expectedToken)) {
                conn.close(CloseFrame.POLICY_VALIDATION, "unauthorized");
                return;}}
        SessionState st = new SessionState(parts.path(), Instant.now().toString());
        sessions.put(conn, st);
        safeSend(conn, jsonOk("connected", Map.of(
                "service", "keeply-ws",
                "path", parts.path(),
                "ts", Instant.now().toString())));}
    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        sessions.remove(conn);}
    @Override
    public void onMessage(WebSocket conn, String message) {
        if (conn == null) return;
        SessionState st = sessions.get(conn);
        if (st == null) {
            conn.close(CloseFrame.PROTOCOL_ERROR, "no_session");
            return;}
        if (!st.rate.allow()) {
            conn.close(CloseFrame.POLICY_VALIDATION, "rate_limited");
            return;}
        if (message != null && message.getBytes(StandardCharsets.UTF_8).length > MAX_MESSAGE_BYTES) {
            conn.close(1009, "message_too_big");
            return;}
        try {
            JsonNode root = (message == null || message.isBlank())
                    ? MAPPER.createObjectNode()
                    : MAPPER.readTree(message);
            String type = text(root, "type");
            String requestId = text(root, "requestId");
            if (type == null) {
                safeSend(conn, jsonErr("bad_request", "missing type", requestId));
                return;
            }
            switch (type) {
                case "ping" -> safeSend(conn, jsonOk("pong", Map.of("ts", Instant.now().toString()), requestId));
                case "echo" -> {
                    JsonNode payload = root.path("payload");
                    safeSend(conn, jsonOk("echo", Map.of("payload", payload), requestId));}
                default -> safeSend(conn, jsonErr("unknown_type", "unsupported type: " + type, requestId));}
        } catch (Exception e) {
            safeSend(conn, jsonErr("bad_json", safeMsg(e), null));}}
    @Override
    public void onMessage(WebSocket conn, ByteBuffer bytes) {
        if (conn != null) conn.close(1003, "binary_not_supported");}
    @Override
    public void onError(WebSocket conn, Exception ex) {
        if (conn != null) sessions.remove(conn);
        System.err.println("keeply-ws error: " + safeMsg(ex));}
    @Override
    public void onStart() {
        setConnectionLostTimeout(30);
        System.out.println("keeply-ws started on " + getAddress());}
    @Override
    public void onWebsocketPong(WebSocket conn, Framedata f) {
        SessionState st = sessions.get(conn);
        if (st != null) st.lastPongEpochMs = System.currentTimeMillis();}
    int broadcastEvent(String type, Map<String, Object> payload) {
        String msg = jsonOk(type, payload);
        return broadcastRaw(msg);}
    int broadcastRaw(String payload) {
        if (payload == null) return 0;
        int delivered = 0;
        for (WebSocket ws : new ArrayList<>(sessions.keySet())) {
            if (ws == null || !ws.isOpen()) {
                sessions.remove(ws);
                continue;}
            if (safeSend(ws, payload)) delivered++;}
        return delivered;}
    private boolean safeSend(WebSocket ws, String text) {
        try {
            if (ws != null && ws.isOpen()) {
                ws.send(text);
                return true;}
        } catch (Exception ignored) {}
        return false;}
    private static String jsonOk(String type, Map<String, Object> payload) {
        return jsonOk(type, payload, null);}
    private static String jsonOk(String type, Map<String, Object> payload, String requestId) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("ok", true);
        o.put("type", type);
        if (requestId != null) o.put("requestId", requestId);
        ObjectNode p = o.putObject("payload");
        if (payload != null) payload.forEach((k, v) -> p.set(k, MAPPER.valueToTree(v)));
        return toJson(o);}
    private static String jsonErr(String code, String message, String requestId) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("ok", false);
        o.put("type", "error");
        if (requestId != null) o.put("requestId", requestId);
        ObjectNode err = o.putObject("error");
        err.put("code", code);
        err.put("message", message);
        return toJson(o);}
    private static String toJson(JsonNode node) {
        try {
            return MAPPER.writeValueAsString(node);
        } catch (Exception e) {
            return "{\"ok\":false,\"type\":\"error\",\"error\":{\"code\":\"json_encode\",\"message\":\"failed\"}}";}}
    private static String text(JsonNode node, String key) {
        JsonNode n = node == null ? null : node.get(key);
        if (n == null || n.isNull()) return null;
        String v = n.asText(null);
        return (v == null || v.isBlank()) ? null : v;}
    private static String safeMsg(Throwable t) {
        return (t.getMessage() == null || t.getMessage().isBlank())
                ? t.getClass().getSimpleName()
                : t.getMessage();}
    private String extractToken(ClientHandshake hs, Map<String, String> query) {
        String qt = query.get("token");
        if (qt != null && !qt.isBlank()) return qt.trim();
        try {
            String auth = hs == null ? null : hs.getFieldValue("Authorization");
            if (auth != null) {
                auth = auth.trim();
                if (auth.regionMatches(true, 0, "bearer ", 0, 7)) {
                    return auth.substring(7).trim();}
                return auth;}
        } catch (Exception ignored) {}
        return null;}
    private static boolean timingSafeEquals(String a, String b) {
        if (a == null || b == null) return false;
        byte[] x = a.getBytes(StandardCharsets.UTF_8);
        byte[] y = b.getBytes(StandardCharsets.UTF_8);
        if (x.length != y.length) return false;
        int r = 0;
        for (int i = 0; i < x.length; i++) r |= x[i] ^ y[i];
        return r == 0;}
    private static UriParts parsePathAndQuery(String resourceDescriptor) {
        try {
            URI u = URI.create("ws://localhost" + (resourceDescriptor == null ? "" : resourceDescriptor));
            String path = u.getPath() == null ? "" : u.getPath();
            Map<String, String> q = parseQuery(u.getRawQuery());
            return new UriParts(path, q);
        } catch (Exception e) {
            return new UriParts(resourceDescriptor == null ? "" : resourceDescriptor, Map.of());}}
    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> out = new HashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) return out;
        for (String part : rawQuery.split("&")) {
            if (part.isBlank()) continue;
            int i = part.indexOf('=');
            if (i < 0) out.put(urlDecode(part), "");
            else out.put(urlDecode(part.substring(0, i)), urlDecode(part.substring(i + 1)));}
        return out;}
    private static String urlDecode(String s) {
        try {
            return java.net.URLDecoder.decode(s, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return s;}}
    private record UriParts(String path, Map<String, String> query) {}
    private static final class SessionState {
        final String path;
        final String connectedAt;
        final RateLimiter rate = new RateLimiter(RATE_LIMIT_PER_10S, 10_000);
        volatile long lastPongEpochMs = System.currentTimeMillis();
        SessionState(String path, String connectedAt) {
            this.path = path;
            this.connectedAt = connectedAt;}}
    private static final class RateLimiter {
        private final int max;
        private final long windowMs;
        private final Deque<Long> hits = new ArrayDeque<>();
        RateLimiter(int max, long windowMs) {
            this.max = max;
            this.windowMs = windowMs;
        }
        synchronized boolean allow() {
            long now = System.currentTimeMillis();
            while (!hits.isEmpty() && now - hits.peekFirst() > windowMs) hits.pollFirst();
            if (hits.size() >= max) return false;
            hits.addLast(now);
            return true;}}}
