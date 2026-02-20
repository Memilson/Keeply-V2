package com.keeply.app.api;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

final class KeeplyHttp {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final int MAX_BODY_BYTES = 64 * 1024;

    // CORS (se você usa token no header, inclua Authorization)
    private static final String CORS_ORIGIN = "*";
    private static final String CORS_HEADERS = "Content-Type, Authorization";
    private static final String CORS_METHODS = "GET, POST, DELETE, OPTIONS";

    private KeeplyHttp() {}

    static ObjectMapper mapper() { return MAPPER; }

    // ----------------- Request basics -----------------

    static boolean isMethod(HttpExchange ex, String method) {
        return method.equalsIgnoreCase(ex.getRequestMethod());
    }

    static String clientOf(HttpExchange ex) {
        try {
            if (ex == null || ex.getRemoteAddress() == null) return "-";
            return String.valueOf(ex.getRemoteAddress());
        } catch (Exception ignored) {
            return "-";
        }
    }

    static String firstHeader(HttpExchange ex, String name) {
        if (ex == null) return null;
        return firstHeader(ex.getRequestHeaders(), name);
    }

    private static String firstHeader(Headers h, String name) {
        List<String> v = h.get(name);
        return (v == null || v.isEmpty()) ? null : v.get(0);
    }

    // ----------------- CORS / common headers -----------------

    static void addCommonHeaders(Headers h) {
        // idempotente: set() sobrescreve
        h.set("Access-Control-Allow-Origin", CORS_ORIGIN);
        h.set("Access-Control-Allow-Headers", CORS_HEADERS);
        h.set("Access-Control-Allow-Methods", CORS_METHODS);

        h.set("Cache-Control", "no-store");
        h.set("X-Content-Type-Options", "nosniff");
    }

    static boolean handlePreflightIfNeeded(HttpExchange ex) throws IOException {
        if (!isMethod(ex, "OPTIONS")) return false;
        addCommonHeaders(ex.getResponseHeaders());
        ex.sendResponseHeaders(204, -1);
        return true;
    }

    // ----------------- Responses -----------------

    static void methodNotAllowed(HttpExchange ex, String allow) throws IOException {
        ex.getResponseHeaders().set("Allow", allow);
        sendError(ex, 405, "method_not_allowed", "Método não permitido");
    }

    static void sendError(HttpExchange ex, int status, String code, String message) throws IOException {
        ObjectNode out = MAPPER.createObjectNode();
        out.put("ok", false);
        ObjectNode err = out.putObject("error");
        err.put("code", code);
        err.put("message", message);
        sendJson(ex, status, out);
    }

    static void sendJson(HttpExchange ex, int status, JsonNode body) throws IOException {
        byte[] bytes = MAPPER.writeValueAsBytes(body);

        Headers h = ex.getResponseHeaders();
        addCommonHeaders(h);
        h.set("Content-Type", "application/json; charset=utf-8");

        ex.sendResponseHeaders(status, bytes.length);
        ex.getResponseBody().write(bytes);
    }

    // ----------------- Body parsing -----------------

    static JsonNode readJsonBody(HttpExchange ex) throws IOException {
        String ct = firstHeader(ex.getRequestHeaders(), "Content-Type");
        if (ct != null) {
            String low = ct.toLowerCase(Locale.ROOT);
            // aceita "application/json; charset=utf-8"
            if (!low.startsWith("application/json")) {
                throw new IllegalArgumentException("Content-Type deve ser application/json");
            }
        }

        byte[] data = readAllBytesLimited(ex.getRequestBody(), MAX_BODY_BYTES);
        if (data.length == 0) return MAPPER.createObjectNode();

        // Jackson já valida JSON; se quebrar, vai subir exception
        return MAPPER.readTree(data);
    }

    private static byte[] readAllBytesLimited(InputStream in, int limit) throws IOException {
        if (in == null) return new byte[0];

        byte[] buf = new byte[8192];
        int read, total = 0;

        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.min(limit, 16 * 1024));
        while ((read = in.read(buf)) != -1) {
            total += read;
            if (total > limit) {
                throw new IllegalArgumentException("Body excede limite de " + limit + " bytes");
            }
            out.write(buf, 0, read);
        }
        return out.toByteArray();
    }

    // ----------------- Query parsing -----------------

    static Map<String, String> parseQuery(URI uri) {
        Map<String, String> out = new HashMap<>();
        String query = (uri == null) ? null : uri.getRawQuery();
        if (isBlank(query)) return out;

        for (String item : query.split("&")) {
            if (isBlank(item)) continue;

            int idx = item.indexOf('=');
            if (idx < 0) {
                out.put(urlDecode(item), "");
            } else {
                String k = urlDecode(item.substring(0, idx));
                String v = urlDecode(item.substring(idx + 1));
                out.put(k, v);
            }
        }
        return out;
    }

    private static String urlDecode(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return value;
        }
    }

    // ----------------- JSON helpers -----------------

    static String text(JsonNode body, String field) {
        JsonNode n = (body == null) ? null : body.get(field);
        return (n == null || n.isNull()) ? null : n.asText(null);
    }

    static Long longOrNull(JsonNode body, String field) {
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

    static int clampInt(String value, int fallback, int min, int max) {
        if (isBlank(value)) return fallback;
        try {
            int v = Integer.parseInt(value.trim());
            if (v < min) return min;
            if (v > max) return max;
            return v;
        } catch (Exception e) {
            return fallback;
        }
    }

    static void putNullable(ObjectNode obj, String key, String value) {
        if (value == null) obj.putNull(key);
        else obj.put(key, value);
    }

    // ----------------- Misc -----------------

    static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    static String nvl(String value) {
        return value == null ? "" : value;
    }

    static String safeMsg(Throwable t) {
        return (t.getMessage() == null || t.getMessage().isBlank())
                ? t.getClass().getSimpleName()
                : t.getMessage();
    }

    static void logInfo(String message) {
        System.out.println("[" + Instant.now() + "] [api] " + message);
    }

    static void logWarn(String message) {
        System.err.println("[" + Instant.now() + "] [api] " + message);
    }
}
