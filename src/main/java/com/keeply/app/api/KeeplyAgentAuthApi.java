package com.keeply.app.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class KeeplyAgentAuthApi {
    private static final Duration CHALLENGE_TTL = Duration.ofMinutes(1);
    private static final Duration AGENT_JWT_TTL = Duration.ofMinutes(15);
    private static final SecureRandom RANDOM = new SecureRandom();

    private final AgentStateStore stateStore;
    private final ConcurrentMap<String, ChallengeState> challenges = new ConcurrentHashMap<>();

    KeeplyAgentAuthApi(AgentStateStore stateStore) {
        this.stateStore = stateStore;
    }

    void handleChallenge(HttpExchange ex) throws IOException {
        if (!KeeplyHttp.isMethod(ex, "GET")) {
            KeeplyHttp.methodNotAllowed(ex, "GET");
            return;
        }

        String deviceId = KeeplyHttp.parseQuery(ex.getRequestURI()).get("deviceId");
        AgentStateStore.DeviceIdentity identity = stateStore.loadOrCreateDeviceIdentity();
        if (KeeplyHttp.isBlank(deviceId) || !KeeplyApi.timingSafeEquals(deviceId.trim(), identity.deviceId())) {
            KeeplyHttp.sendError(ex, 404, "device_not_found", "deviceId invalido");
            return;
        }

        String nonce = randomBase64Url(24);
        Instant expiresAt = Instant.now().plus(CHALLENGE_TTL);
        challenges.put(identity.deviceId(), new ChallengeState(nonce, expiresAt));

        ObjectNode out = KeeplyHttp.mapper().createObjectNode();
        out.put("ok", true);
        out.put("deviceId", identity.deviceId());
        out.put("nonce", nonce);
        out.put("expiresAt", expiresAt.toString());
        KeeplyHttp.sendJson(ex, 200, out);
    }

    void handleLogin(HttpExchange ex) throws IOException {
        if (!KeeplyHttp.isMethod(ex, "POST")) {
            KeeplyHttp.methodNotAllowed(ex, "POST");
            return;
        }

        JsonNode body = KeeplyHttp.readJsonBody(ex);
        String deviceId = KeeplyHttp.text(body, "deviceId");
        String nonce = KeeplyHttp.text(body, "nonce");
        String signatureB64 = KeeplyHttp.text(body, "signature");

        if (KeeplyHttp.isBlank(deviceId) || KeeplyHttp.isBlank(nonce) || KeeplyHttp.isBlank(signatureB64)) {
            KeeplyHttp.sendError(ex, 400, "bad_request", "Informe deviceId, nonce e signature");
            return;
        }

        AgentStateStore.DeviceIdentity identity = stateStore.loadOrCreateDeviceIdentity();
        if (!KeeplyApi.timingSafeEquals(deviceId, identity.deviceId())) {
            KeeplyHttp.sendError(ex, 404, "device_not_found", "deviceId invalido");
            return;
        }

        ChallengeState challenge = challenges.get(identity.deviceId());
        if (challenge == null || challenge.isExpired() || !KeeplyApi.timingSafeEquals(challenge.nonce(), nonce)) {
            KeeplyHttp.sendError(ex, 401, "invalid_challenge", "Challenge invalido ou expirado");
            return;
        }

        boolean signatureOk = verifyEd25519(identity.publicKeyBase64(), nonce + ":" + identity.deviceId(), signatureB64);
        if (!signatureOk) {
            KeeplyHttp.sendError(ex, 401, "invalid_signature", "Assinatura invalida");
            return;
        }

        challenges.remove(identity.deviceId(), challenge);

        String jwt = issueAgentJwt(identity.deviceId());
        Instant expiresAt = Instant.now().plus(AGENT_JWT_TTL);
        stateStore.touchAgentLastSeen(identity.deviceId(), Instant.now().toString());

        ObjectNode out = KeeplyHttp.mapper().createObjectNode();
        out.put("ok", true);
        out.put("deviceId", identity.deviceId());
        out.put("agentJwt", jwt);
        out.put("expiresAt", expiresAt.toString());
        KeeplyHttp.sendJson(ex, 200, out);
    }

    void cleanupChallenges() {
        for (var e : challenges.entrySet()) {
            ChallengeState st = e.getValue();
            if (st == null || st.isExpired()) challenges.remove(e.getKey(), st);
        }
    }

    Optional<KeeplyApi.AgentJwtClaims> verifyAgentJwt(String token) {
        try {
            if (KeeplyHttp.isBlank(token)) return Optional.empty();
            String[] parts = token.split("\\.");
            if (parts.length != 3) return Optional.empty();

            String data = parts[0] + "." + parts[1];
            String expectedSig = hmacSha256Base64Url(agentJwtSecret(), data);
            if (!KeeplyApi.timingSafeEquals(parts[2], expectedSig)) return Optional.empty();

            JsonNode payload = KeeplyHttp.mapper().readTree(Base64.getUrlDecoder().decode(parts[1]));
            String typ = KeeplyHttp.text(payload, "typ");
            String sub = KeeplyHttp.text(payload, "sub");
            long exp = payload == null ? 0 : payload.path("exp").asLong(0);

            if (!"agent".equals(typ) || KeeplyHttp.isBlank(sub) || exp <= Instant.now().getEpochSecond()) return Optional.empty();
            return Optional.of(new KeeplyApi.AgentJwtClaims(sub, exp));
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    private String issueAgentJwt(String deviceId) {
        ObjectNode header = KeeplyHttp.mapper().createObjectNode();
        header.put("alg", "HS256");
        header.put("typ", "JWT");

        long iat = Instant.now().getEpochSecond();
        long exp = iat + AGENT_JWT_TTL.toSeconds();

        ObjectNode payload = KeeplyHttp.mapper().createObjectNode();
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

    private static String randomBase64Url(int numBytes) {
        byte[] bytes = new byte[numBytes];
        RANDOM.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static String base64UrlJson(JsonNode node) {
        try {
            return Base64.getUrlEncoder().withoutPadding().encodeToString(KeeplyHttp.mapper().writeValueAsBytes(node));
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao serializar JWT: " + KeeplyHttp.safeMsg(e), e);
        }
    }

    private static String hmacSha256Base64Url(String secret, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] sig = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(sig);
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao assinar JWT: " + KeeplyHttp.safeMsg(e), e);
        }
    }

    private static Path agentJwtSecretFilePath() {
        String custom = System.getProperty("KEEPLY_AGENT_JWT_SECRET_FILE");
        if (!KeeplyHttp.isBlank(custom)) return Path.of(custom).toAbsolutePath().normalize();
        return Path.of(System.getProperty("user.dir"), ".keeply-agent-jwt.json").toAbsolutePath().normalize();
    }

    private static String agentJwtSecret() {
        Path path = agentJwtSecretFilePath();
        try {
            if (Files.exists(path)) {
                JsonNode node = KeeplyHttp.mapper().readTree(Files.readString(path, StandardCharsets.UTF_8));
                String secret = KeeplyHttp.text(node, "secret");
                if (!KeeplyHttp.isBlank(secret)) return secret;
            }
        } catch (Exception e) {
            KeeplyHttp.logWarn("Falha ao ler agent jwt secret: " + KeeplyHttp.safeMsg(e));
        }

        String generated = randomBase64Url(48);
        try {
            ObjectNode node = KeeplyHttp.mapper().createObjectNode();
            node.put("secret", generated);
            writeJsonAtomic(path, node);
        } catch (Exception e) {
            KeeplyHttp.logWarn("Falha ao salvar agent jwt secret: " + KeeplyHttp.safeMsg(e));
        }
        return generated;
    }

    private static void writeJsonAtomic(Path target, JsonNode node) throws IOException {
        Path parent = target.getParent();
        if (parent != null) Files.createDirectories(parent);

        Path dir = parent != null ? parent : Path.of(".").toAbsolutePath();
        String baseName = target.getFileName().toString();
        Path tmp = Files.createTempFile(dir, baseName, ".tmp");

        boolean moved = false;
        try {
            String json = KeeplyHttp.mapper().writerWithDefaultPrettyPrinter().writeValueAsString(node);
            Files.writeString(tmp, json, StandardCharsets.UTF_8,
                    java.nio.file.StandardOpenOption.CREATE,
                    java.nio.file.StandardOpenOption.TRUNCATE_EXISTING,
                    java.nio.file.StandardOpenOption.WRITE);

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

    private record ChallengeState(String nonce, Instant expiresAt) {
        boolean isExpired() {
            return expiresAt == null || Instant.now().isAfter(expiresAt);
        }
    }
}
