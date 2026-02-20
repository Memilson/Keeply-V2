package com.keeply.app.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Enumeration;
import java.util.Locale;

final class KeeplyPairingApi {
    private static final String PAIRING_BASE = "/api/keeply/pairing";
    private static final Duration PAIRING_TTL = Duration.ofMinutes(2);
    private static final SecureRandom RANDOM = new SecureRandom();

    private final AgentStateStore stateStore;

    KeeplyPairingApi(AgentStateStore stateStore) {
        this.stateStore = stateStore;
    }

    void printStartupPairingIfNeeded() {
        AgentStateStore.AgentLinkState startupLink = stateStore.loadAgentLinkState();
        if (startupLink.paired()) return;

        PairingIssued startupPairing = issuePairingCode();
        System.out.println("Pairing (agente local):");
        System.out.println("  Codigo: " + startupPairing.code());
        System.out.println("  Expira em: " + startupPairing.expiresAt());
        System.out.println("  Use este codigo no painel web para vincular.");
    }

    void handle(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        String rest = path.length() > PAIRING_BASE.length() ? path.substring(PAIRING_BASE.length()) : "";

        AgentStateStore.DeviceIdentity identity = stateStore.loadOrCreateDeviceIdentity();
        AgentStateStore.AgentLinkState linkState = stateStore.loadAgentLinkState();
        AgentStateStore.PairingState current = stateStore.loadPairingState();

        if (rest.isBlank() || "/".equals(rest)) {
            if (!KeeplyHttp.isMethod(ex, "GET")) {
                KeeplyHttp.methodNotAllowed(ex, "GET");
                return;
            }

            ObjectNode out = KeeplyHttp.mapper().createObjectNode();
            out.put("ok", true);
            out.put("paired", linkState.paired());
            out.put("requiresPairing", !linkState.paired());
            out.put("deviceId", identity.deviceId());
            out.put("publicKey", identity.publicKeyBase64());

            if (linkState.paired()) {
                out.putNull("code");
                out.putNull("createdAt");
                out.putNull("expiresAt");
                KeeplyHttp.putNullable(out, "linkedAt", linkState.linkedAt());
                if (linkState.machineId() == null) out.putNull("machineId"); else out.put("machineId", linkState.machineId());
                if (linkState.userId() == null) out.putNull("userId"); else out.put("userId", linkState.userId());
                KeeplyHttp.putNullable(out, "userEmail", linkState.userEmail());
                KeeplyHttp.putNullable(out, "lastSeenAt", linkState.lastSeenAt());
            } else {
                boolean hasActiveCode = current != null && !current.isExpired() && current.usedAt() == null;
                out.putNull("code");
                if (current == null) {
                    out.putNull("createdAt");
                    out.putNull("expiresAt");
                } else {
                    KeeplyHttp.putNullable(out, "createdAt", current.createdAt());
                    KeeplyHttp.putNullable(out, "expiresAt", current.expiresAt());
                }
                out.put("hasActiveCode", hasActiveCode);
            }

            out.set("machine", machineInfoNode());
            KeeplyHttp.sendJson(ex, 200, out);
            return;
        }

        if ("/rotate".equals(rest)) {
            if (!KeeplyHttp.isMethod(ex, "POST")) {
                KeeplyHttp.methodNotAllowed(ex, "POST");
                return;
            }
            if (linkState.paired()) {
                KeeplyHttp.sendError(ex, 409, "already_linked", "Agente ja vinculado; nao deve gerar novo codigo");
                return;
            }

            PairingIssued rotated = issuePairingCode();
            ObjectNode out = KeeplyHttp.mapper().createObjectNode();
            out.put("ok", true);
            out.put("code", rotated.code());
            out.put("createdAt", rotated.createdAt());
            out.put("expiresAt", rotated.expiresAt());
            out.put("deviceId", identity.deviceId());
            out.put("publicKey", identity.publicKeyBase64());
            KeeplyHttp.sendJson(ex, 200, out);
            return;
        }

        if ("/request".equals(rest)) {
            if (!KeeplyHttp.isMethod(ex, "POST")) {
                KeeplyHttp.methodNotAllowed(ex, "POST");
                return;
            }
            if (linkState.paired()) {
                KeeplyHttp.sendError(ex, 409, "already_linked", "Agente ja vinculado");
                return;
            }

            PairingIssued issued = issuePairingCode();
            ObjectNode out = KeeplyHttp.mapper().createObjectNode();
            out.put("ok", true);
            out.put("deviceId", identity.deviceId());
            out.put("publicKey", identity.publicKeyBase64());
            out.put("code", issued.code());
            out.put("createdAt", issued.createdAt());
            out.put("expiresAt", issued.expiresAt());
            out.set("machine", machineInfoNode());
            KeeplyHttp.sendJson(ex, 200, out);
            return;
        }

        if ("/approve".equals(rest)) {
            if (!KeeplyHttp.isMethod(ex, "POST")) {
                KeeplyHttp.methodNotAllowed(ex, "POST");
                return;
            }
            if (linkState.paired()) {
                KeeplyHttp.sendError(ex, 409, "already_linked", "Agente ja vinculado");
                return;
            }

            JsonNode body = KeeplyHttp.readJsonBody(ex);
            String informedCode = normalizePairingCode(KeeplyHttp.text(body, "code"));
            if (KeeplyHttp.isBlank(informedCode)) {
                KeeplyHttp.sendError(ex, 400, "bad_request", "Informe o codigo de pairing");
                return;
            }

            AgentStateStore.PairingState state = stateStore.loadPairingState();
            if (state == null || state.isExpired()) {
                KeeplyHttp.sendError(ex, 410, "pairing_expired", "Codigo expirado; gere um novo");
                return;
            }
            if (state.usedAt() != null) {
                KeeplyHttp.sendError(ex, 409, "pairing_already_used", "Codigo de pairing ja consumido");
                return;
            }

            String expected = sha256Hex(state.salt() + ":" + informedCode);
            if (!KeeplyApi.timingSafeEquals(expected, state.codeHash())) {
                KeeplyHttp.sendError(ex, 400, "pairing_code_invalid", "Codigo invalido");
                return;
            }

            AgentStateStore.PairingState used = state.markUsed(Instant.now().toString());
            stateStore.savePairingState(used);

            ObjectNode out = KeeplyHttp.mapper().createObjectNode();
            out.put("ok", true);
            out.put("deviceId", identity.deviceId());
            out.put("publicKey", identity.publicKeyBase64());
            out.put("approvedAt", Instant.now().toString());
            out.set("machine", machineInfoNode());
            KeeplyHttp.sendJson(ex, 200, out);
            return;
        }

        if ("/mark-linked".equals(rest)) {
            if (!KeeplyHttp.isMethod(ex, "POST")) {
                KeeplyHttp.methodNotAllowed(ex, "POST");
                return;
            }

            JsonNode body = KeeplyHttp.readJsonBody(ex);
            Long machineId = KeeplyHttp.longOrNull(body, "machineId");
            Long userId = KeeplyHttp.longOrNull(body, "userId");
            String userEmail = KeeplyHttp.text(body, "userEmail");

            AgentStateStore.AgentLinkState updated = new AgentStateStore.AgentLinkState(
                    true,
                    machineId,
                    userId,
                    userEmail,
                    Instant.now().toString(),
                    Instant.now().toString(),
                    identity.deviceId()
            );
            stateStore.saveAgentLinkState(updated);

            ObjectNode out = KeeplyHttp.mapper().createObjectNode();
            out.put("ok", true);
            out.put("paired", true);
            KeeplyHttp.putNullable(out, "linkedAt", updated.linkedAt());
            if (updated.machineId() == null) out.putNull("machineId"); else out.put("machineId", updated.machineId());
            if (updated.userId() == null) out.putNull("userId"); else out.put("userId", updated.userId());
            KeeplyHttp.putNullable(out, "userEmail", updated.userEmail());
            KeeplyHttp.putNullable(out, "lastSeenAt", updated.lastSeenAt());
            out.put("deviceId", identity.deviceId());
            KeeplyHttp.sendJson(ex, 200, out);
            return;
        }

        if ("/mark-unlinked".equals(rest)) {
            if (!KeeplyHttp.isMethod(ex, "POST")) {
                KeeplyHttp.methodNotAllowed(ex, "POST");
                return;
            }

            stateStore.saveAgentLinkState(AgentStateStore.AgentLinkState.unpaired());

            ObjectNode out = KeeplyHttp.mapper().createObjectNode();
            out.put("ok", true);
            out.put("paired", false);
            out.put("requiresPairing", true);
            out.putNull("code");
            out.putNull("createdAt");
            out.putNull("expiresAt");
            out.put("deviceId", identity.deviceId());
            out.put("publicKey", identity.publicKeyBase64());
            KeeplyHttp.sendJson(ex, 200, out);
            return;
        }

        KeeplyHttp.sendError(ex, 404, "not_found", "Rota de pairing invalida");
    }

    private PairingIssued issuePairingCode() {
        String code = newPairingCode();
        String createdAt = Instant.now().toString();
        String expiresAt = Instant.now().plus(PAIRING_TTL).toString();
        String salt = randomBase64Url(16);
        String codeHash = sha256Hex(salt + ":" + normalizePairingCode(code));
        stateStore.savePairingState(new AgentStateStore.PairingState(codeHash, salt, createdAt, expiresAt, null));
        return new PairingIssued(code, createdAt, expiresAt);
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

    private static ObjectNode machineInfoNode() {
        String hostname = resolveHostname();

        String osName = safe(System.getProperty("os.name"));
        String osVersion = safe(System.getProperty("os.version"));
        String arch = safe(System.getProperty("os.arch"));
        String userName = safe(System.getProperty("user.name"));
        String machineName = (KeeplyHttp.isBlank(userName) ? "usuario" : userName) + "@" + hostname;
        String machineAlias = hostname;

        String lastIp = detectFirstIpv4();
        String hardwareHashHex = sha256Hex(String.join("|",
                nvl(hostname), nvl(osName), nvl(osVersion), nvl(arch), nvl(lastIp)));

        ObjectNode out = KeeplyHttp.mapper().createObjectNode();
        out.put("machineName", machineName);
        out.put("machineAlias", machineAlias);
        KeeplyHttp.putNullable(out, "hostname", hostname);
        KeeplyHttp.putNullable(out, "osName", osName);
        KeeplyHttp.putNullable(out, "osVersion", osVersion);
        KeeplyHttp.putNullable(out, "arch", arch);
        KeeplyHttp.putNullable(out, "lastIp", lastIp);
        out.put("hardwareHashHex", hardwareHashHex);
        return out;
    }

    private static String resolveHostname() {
        String hostname = safe(System.getenv("COMPUTERNAME"));
        if (KeeplyHttp.isBlank(hostname)) hostname = safe(System.getenv("HOSTNAME"));
        if (KeeplyHttp.isBlank(hostname)) {
            try {
                hostname = safe(InetAddress.getLocalHost().getHostName());
            } catch (Exception ignored) {}
        }
        if (KeeplyHttp.isBlank(hostname)) {
            try {
                hostname = safe(java.nio.file.Files.readString(java.nio.file.Path.of("/etc/hostname"), java.nio.charset.StandardCharsets.UTF_8).trim());
            } catch (Exception ignored) {}
        }
        if (KeeplyHttp.isBlank(hostname)) hostname = "unknown-host";
        return hostname;
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
            byte[] bytes = digest.digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder out = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) out.append(String.format("%02x", b));
            return out.toString();
        } catch (Exception ignored) {
            return "";
        }
    }

    private static String safe(String value) {
        return KeeplyHttp.isBlank(value) ? null : value;
    }

    private static String nvl(String value) {
        return value == null ? "" : value;
    }

    private record PairingIssued(String code, String createdAt, String expiresAt) {}
}
