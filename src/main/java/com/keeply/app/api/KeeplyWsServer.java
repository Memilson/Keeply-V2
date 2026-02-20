package com.keeply.app.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.java_websocket.WebSocket;
import org.java_websocket.framing.CloseFrame;
import org.java_websocket.framing.Framedata;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

final class KeeplyWsServer extends WebSocketServer {
    private static final String PRIMARY_PATH = "/ws/keeply";
    private static final String LEGACY_PATH  = "/ws";

    private static final int MAX_MESSAGE_BYTES = 64 * 1024; // 64KB
    private static final int RATE_LIMIT_MAX_PER_WINDOW = 30; // msgs / janela
    private static final long RATE_LIMIT_WINDOW_MS = 10_000; // 10s

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ConcurrentMap<WebSocket, SessionState> sessions = new ConcurrentHashMap<>();
    private final String expectedToken; // token estático do UI/admin (pode ser null)

    KeeplyWsServer(InetSocketAddress address, String expectedToken) {
        super(address);
        this.expectedToken = (expectedToken == null || expectedToken.isBlank()) ? null : expectedToken.trim();
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        if (conn == null) return;

        var rd = handshake == null ? "" : String.valueOf(handshake.getResourceDescriptor());
        var parts = parsePathAndQuery(rd);

        if (!PRIMARY_PATH.equals(parts.path()) && !LEGACY_PATH.equals(parts.path())) {
            safeClose(conn, CloseFrame.POLICY_VALIDATION, "invalid_path");
            return;
        }

        var token = extractToken(handshake, parts.query());
        boolean localPeer = isLocalPeer(conn);

        boolean staticTokenOk = expectedToken != null
                && token != null
                && KeeplyApi.timingSafeEquals(token, expectedToken);

        KeeplyApi.AgentJwtClaims jwtClaims = KeeplyApi.verifyAgentJwt(token).orElse(null);
        boolean agentOk = jwtClaims != null;

        // UI/admin pode conectar com token estático.
        // Dev local: permite sem token para conexões da própria máquina
        // (loopback e interfaces locais, incluindo bridges Docker do host).
        boolean uiOk = staticTokenOk || ((token == null || token.isBlank()) && localPeer);

        if (!uiOk && !agentOk) {
            safeClose(conn, CloseFrame.POLICY_VALIDATION, "unauthorized");
            return;
        }

        SessionRole role;
        String deviceId = null;

        if (agentOk) {
            deviceId = jwtClaims.deviceId();

            Optional<String> pairedDevice = KeeplyApi.getPairedDeviceId();
            if (pairedDevice.isPresent() && !KeeplyApi.timingSafeEquals(pairedDevice.get(), deviceId)) {
                safeClose(conn, CloseFrame.POLICY_VALIDATION, "device_mismatch");
                return;
            }

            // Se está marcado como paired, mas não há deviceId persistido => estado inconsistente
            if (KeeplyApi.isAgentPaired() && pairedDevice.isEmpty()) {
                safeClose(conn, CloseFrame.POLICY_VALIDATION, "paired_state_invalid");
                return;
            }

            role = SessionRole.AGENT;
        } else {
            role = SessionRole.UI;
        }

        var st = new SessionState(
                parts.path(),
                Instant.now().toString(),
                role,
                deviceId,
                new FixedWindowRateLimiter(RATE_LIMIT_MAX_PER_WINDOW, RATE_LIMIT_WINDOW_MS),
                new AtomicLong(System.currentTimeMillis())
        );

        sessions.put(conn, st);

        if (st.deviceId() != null) {
            KeeplyApi.touchAgentLastSeen(st.deviceId(), Instant.now().toString());
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("service", "keeply-ws");
        payload.put("path", parts.path());
        payload.put("role", st.role().name().toLowerCase(Locale.ROOT));
        payload.put("ts", Instant.now().toString());
        if (st.deviceId() != null) payload.put("deviceId", st.deviceId());

        safeSend(conn, jsonOk("connected", payload, null));
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        if (conn != null) sessions.remove(conn);
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        if (conn == null) return;

        SessionState st = sessions.get(conn);
        if (st == null) {
            safeClose(conn, CloseFrame.PROTOCOL_ERROR, "no_session");
            return;
        }

        if (!st.rate().allow()) {
            safeClose(conn, CloseFrame.POLICY_VALIDATION, "rate_limited");
            return;
        }

        if (message != null && utf8LenExceeds(message, MAX_MESSAGE_BYTES)) {
            safeClose(conn, 1009, "message_too_big"); // 1009 = message too big
            return;
        }

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
                case "ping" -> {
                    if (st.deviceId() != null) {
                        KeeplyApi.touchAgentLastSeen(st.deviceId(), Instant.now().toString());
                    }
                    safeSend(conn, jsonOk("pong", Map.of("ts", Instant.now().toString()), requestId));
                }
                case "echo" -> {
                    JsonNode payload = root.path("payload");
                    // manda payload como JsonNode, preservando estrutura
                    safeSend(conn, jsonOk("echo", Map.of("payload", payload), requestId));
                }
                default -> safeSend(conn, jsonErr("unknown_type", "unsupported type: " + type, requestId));
            }

        } catch (Exception e) {
            safeSend(conn, jsonErr("bad_json", safeMsg(e), null));
        }
    }

    @Override
    public void onMessage(WebSocket conn, ByteBuffer bytes) {
        if (conn != null) safeClose(conn, 1003, "binary_not_supported"); // 1003 = unsupported data
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        if (conn != null) sessions.remove(conn);
        System.err.println("keeply-ws error: " + safeMsg(ex));
    }

    @Override
    public void onStart() {
        setConnectionLostTimeout(30);
        System.out.println("keeply-ws started on " + getAddress());
    }

    @Override
    public void onWebsocketPong(WebSocket conn, Framedata f) {
        if (conn == null) return;

        SessionState st = sessions.get(conn);
        if (st != null) {
            st.lastPongEpochMs().set(System.currentTimeMillis());
            if (st.deviceId() != null) {
                KeeplyApi.touchAgentLastSeen(st.deviceId(), Instant.now().toString());
            }
        }
    }

    int broadcastEvent(String type, Map<String, ?> payload) {
        return broadcastRaw(jsonOk(type, payload, null));
    }

    int broadcastRaw(String payload) {
        if (payload == null) return 0;

        int delivered = 0;
        for (var it = sessions.entrySet().iterator(); it.hasNext();) {
            var e = it.next();
            var ws = e.getKey();
            if (ws == null || !ws.isOpen()) {
                it.remove();
                continue;
            }
            if (safeSend(ws, payload)) delivered++;
        }
        return delivered;
    }

    // ----------------- helpers -----------------

    private static boolean utf8LenExceeds(String s, int maxBytes) {
        // heurística rápida: UTF-8 usa até 4 bytes por char
        if ((long) s.length() * 4L <= maxBytes) {
            // pode caber; mede exato
            return StandardCharsets.UTF_8.encode(s).remaining() > maxBytes;
        }
        // certamente excede (ou está perto; mesmo assim mede exato para evitar falsos positivos em ASCII)
        return StandardCharsets.UTF_8.encode(s).remaining() > maxBytes;
    }

    private static boolean isLocalPeer(WebSocket conn) {
        try {
            if (conn == null || conn.getRemoteSocketAddress() == null || conn.getRemoteSocketAddress().getAddress() == null) {
                return false;
            }
            InetAddress remote = conn.getRemoteSocketAddress().getAddress();
            if (remote.isLoopbackAddress()) return true;

            for (NetworkInterface nif : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                for (InetAddress localAddr : Collections.list(nif.getInetAddresses())) {
                    if (remote.equals(localAddr)) return true;
                }
            }
        } catch (Exception ignored) {
            return false;
        }
        return false;
    }

    private static void safeClose(WebSocket conn, int code, String reason) {
        try {
            conn.close(code, reason);
        } catch (Exception ignored) {}
    }

    private static boolean safeSend(WebSocket ws, String text) {
        try {
            if (ws != null && ws.isOpen()) {
                ws.send(text);
                return true;
            }
        } catch (Exception ignored) {}
        return false;
    }

    private static String jsonOk(String type, Map<String, ?> payload, String requestId) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("ok", true);
        o.put("type", type);
        if (requestId != null) o.put("requestId", requestId);

        ObjectNode p = o.putObject("payload");
        if (payload != null) {
            payload.forEach((k, v) -> {
                if (v == null) p.putNull(k);
                else p.set(k, MAPPER.valueToTree(v));
            });
        }
        return toJson(o);
    }

    private static String jsonErr(String code, String message, String requestId) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("ok", false);
        o.put("type", "error");
        if (requestId != null) o.put("requestId", requestId);

        ObjectNode err = o.putObject("error");
        err.put("code", code);
        err.put("message", message);
        return toJson(o);
    }

    private static String toJson(JsonNode node) {
        try {
            return MAPPER.writeValueAsString(node);
        } catch (Exception e) {
            return "{\"ok\":false,\"type\":\"error\",\"error\":{\"code\":\"json_encode\",\"message\":\"failed\"}}";
        }
    }

    private static String text(JsonNode node, String key) {
        JsonNode n = node == null ? null : node.get(key);
        if (n == null || n.isNull()) return null;
        String v = n.asText(null);
        return (v == null || v.isBlank()) ? null : v;
    }

    private static String safeMsg(Throwable t) {
        return (t.getMessage() == null || t.getMessage().isBlank())
                ? t.getClass().getSimpleName()
                : t.getMessage();
    }

    private static String extractToken(ClientHandshake hs, Map<String, String> query) {
        String qt = query.get("token");
        if (qt != null && !qt.isBlank()) return qt.trim();

        try {
            String auth = hs == null ? null : hs.getFieldValue("Authorization");
            if (auth == null || auth.isBlank()) return null;

            auth = auth.trim();
            if (auth.regionMatches(true, 0, "bearer ", 0, 7)) return auth.substring(7).trim();
            return auth;
        } catch (Exception ignored) {
            return null;
        }
    }

    private static UriParts parsePathAndQuery(String resourceDescriptor) {
        try {
            var u = URI.create("ws://localhost" + (resourceDescriptor == null ? "" : resourceDescriptor));
            String path = u.getPath() == null ? "" : u.getPath();
            Map<String, String> q = parseQuery(u.getRawQuery());
            return new UriParts(path, Map.copyOf(q));
        } catch (Exception e) {
            String p = resourceDescriptor == null ? "" : resourceDescriptor;
            return new UriParts(p, Map.of());
        }
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> out = new HashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) return out;

        for (String part : rawQuery.split("&")) {
            if (part.isBlank()) continue;
            int i = part.indexOf('=');
            if (i < 0) out.put(urlDecode(part), "");
            else out.put(urlDecode(part.substring(0, i)), urlDecode(part.substring(i + 1)));
        }
        return out;
    }

    private static String urlDecode(String s) {
        try {
            return java.net.URLDecoder.decode(s, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return s;
        }
    }

    private enum SessionRole { UI, AGENT }

    private record UriParts(String path, Map<String, String> query) {}

    private record SessionState(
            String path,
            String connectedAt,
            SessionRole role,
            String deviceId,
            FixedWindowRateLimiter rate,
            AtomicLong lastPongEpochMs
    ) {}

    /**
     * Rate limiter simples e leve (janela fixa).
     * Para WS (mensagens curtas) costuma ser suficiente e evita alocações de fila.
     */
    private static final class FixedWindowRateLimiter {
        private final int max;
        private final long windowMs;

        private final AtomicLong windowStartMs = new AtomicLong(System.currentTimeMillis());
        private final AtomicInteger count = new AtomicInteger(0);

        FixedWindowRateLimiter(int max, long windowMs) {
            this.max = Math.max(1, max);
            this.windowMs = Math.max(1L, windowMs);
        }

        boolean allow() {
            long now = System.currentTimeMillis();
            long start = windowStartMs.get();

            if (now - start >= windowMs) {
                // tenta “virar” a janela
                if (windowStartMs.compareAndSet(start, now)) {
                    count.set(0);
                }
            }
            return count.incrementAndGet() <= max;
        }
    }
}
