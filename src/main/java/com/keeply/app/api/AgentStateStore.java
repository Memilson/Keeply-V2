package com.keeply.app.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.keeply.app.database.DatabaseBackup;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;

final class AgentStateStore {
    private static final String STATE_KEY_DEVICE_IDENTITY = "agent.device_identity";
    private static final String STATE_KEY_PAIRING = "agent.pairing_state";
    private static final String STATE_KEY_LINK = "agent.link_state";

    void bootstrap() {
        ensureAgentStateStore();
    }

    DeviceIdentity loadOrCreateDeviceIdentity() {
        try {
            JsonNode dbNode = loadStateJson(STATE_KEY_DEVICE_IDENTITY);
            String deviceId = KeeplyHttp.text(dbNode, "deviceId");
            String publicKey = KeeplyHttp.text(dbNode, "publicKey");
            String privateKey = KeeplyHttp.text(dbNode, "privateKey");
            if (!KeeplyHttp.isBlank(deviceId) && !KeeplyHttp.isBlank(publicKey) && !KeeplyHttp.isBlank(privateKey)) {
                return new DeviceIdentity(deviceId, publicKey, privateKey);
            }
        } catch (Exception e) {
            KeeplyHttp.logWarn("Falha ao ler identidade do dispositivo no sqlite: " + KeeplyHttp.safeMsg(e));
        }

        Path path = deviceIdentityFilePath();
        try {
            if (Files.exists(path)) {
                JsonNode node = KeeplyHttp.mapper().readTree(Files.readString(path, StandardCharsets.UTF_8));
                String deviceId = KeeplyHttp.text(node, "deviceId");
                String publicKey = KeeplyHttp.text(node, "publicKey");
                String privateKey = KeeplyHttp.text(node, "privateKey");
                if (!KeeplyHttp.isBlank(deviceId) && !KeeplyHttp.isBlank(publicKey) && !KeeplyHttp.isBlank(privateKey)) {
                    DeviceIdentity legacy = new DeviceIdentity(deviceId, publicKey, privateKey);
                    saveDeviceIdentity(legacy);
                    tryDeleteLegacyFile(path);
                    return legacy;
                }
            }
        } catch (Exception e) {
            KeeplyHttp.logWarn("Falha ao ler identidade do dispositivo legado: " + KeeplyHttp.safeMsg(e));
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
            throw new IllegalStateException("Falha ao gerar identidade do dispositivo: " + KeeplyHttp.safeMsg(e), e);
        }
    }

    PairingState loadPairingState() {
        try {
            JsonNode node = loadStateJson(STATE_KEY_PAIRING);
            if (node == null || !node.isObject()) return null;
            return new PairingState(
                    KeeplyHttp.text(node, "codeHash"),
                    KeeplyHttp.text(node, "salt"),
                    KeeplyHttp.text(node, "createdAt"),
                    KeeplyHttp.text(node, "expiresAt"),
                    KeeplyHttp.text(node, "usedAt")
            );
        } catch (Exception e) {
            KeeplyHttp.logWarn("Falha ao ler pairing state no sqlite: " + KeeplyHttp.safeMsg(e));
        }

        Path path = pairingFilePath();
        try {
            if (!Files.exists(path)) return null;
            JsonNode node = KeeplyHttp.mapper().readTree(Files.readString(path, StandardCharsets.UTF_8));
            if (node == null || !node.isObject()) return null;
            PairingState legacy = new PairingState(
                    KeeplyHttp.text(node, "codeHash"),
                    KeeplyHttp.text(node, "salt"),
                    KeeplyHttp.text(node, "createdAt"),
                    KeeplyHttp.text(node, "expiresAt"),
                    KeeplyHttp.text(node, "usedAt")
            );
            savePairingState(legacy);
            tryDeleteLegacyFile(path);
            return legacy;
        } catch (Exception e) {
            KeeplyHttp.logWarn("Falha ao ler pairing state legado: " + KeeplyHttp.safeMsg(e));
            return null;
        }
    }

    void savePairingState(PairingState state) {
        if (state == null) return;
        try {
            ObjectNode node = KeeplyHttp.mapper().createObjectNode();
            KeeplyHttp.putNullable(node, "codeHash", state.codeHash());
            KeeplyHttp.putNullable(node, "salt", state.salt());
            KeeplyHttp.putNullable(node, "createdAt", state.createdAt());
            KeeplyHttp.putNullable(node, "expiresAt", state.expiresAt());
            KeeplyHttp.putNullable(node, "usedAt", state.usedAt());
            saveStateJson(STATE_KEY_PAIRING, node);
        } catch (Exception e) {
            KeeplyHttp.logWarn("Falha ao salvar pairing state no sqlite: " + KeeplyHttp.safeMsg(e));
        }
    }

    AgentLinkState loadAgentLinkState() {
        try {
            JsonNode node = loadStateJson(STATE_KEY_LINK);
            if (node != null && node.isObject()) {
                boolean paired = node.path("paired").asBoolean(false);
                Long machineId = KeeplyHttp.longOrNull(node, "machineId");
                Long userId = KeeplyHttp.longOrNull(node, "userId");
                String userEmail = KeeplyHttp.text(node, "userEmail");
                String linkedAt = KeeplyHttp.text(node, "linkedAt");
                String lastSeenAt = KeeplyHttp.text(node, "lastSeenAt");
                String deviceId = KeeplyHttp.text(node, "deviceId");
                return new AgentLinkState(paired, machineId, userId, userEmail, linkedAt, lastSeenAt, deviceId);
            }
        } catch (Exception e) {
            KeeplyHttp.logWarn("Falha ao ler agent link state no sqlite: " + KeeplyHttp.safeMsg(e));
        }

        Path path = agentStateFilePath();
        try {
            if (!Files.exists(path)) return AgentLinkState.unpaired();
            JsonNode node = KeeplyHttp.mapper().readTree(Files.readString(path, StandardCharsets.UTF_8));

            boolean paired = node != null && node.path("paired").asBoolean(false);
            Long machineId = KeeplyHttp.longOrNull(node, "machineId");
            Long userId = KeeplyHttp.longOrNull(node, "userId");
            String userEmail = KeeplyHttp.text(node, "userEmail");
            String linkedAt = KeeplyHttp.text(node, "linkedAt");
            String lastSeenAt = KeeplyHttp.text(node, "lastSeenAt");
            String deviceId = KeeplyHttp.text(node, "deviceId");

            AgentLinkState legacy = new AgentLinkState(paired, machineId, userId, userEmail, linkedAt, lastSeenAt, deviceId);
            saveAgentLinkState(legacy);
            tryDeleteLegacyFile(path);
            return legacy;
        } catch (Exception e) {
            KeeplyHttp.logWarn("Falha ao ler agent link state legado: " + KeeplyHttp.safeMsg(e));
            return AgentLinkState.unpaired();
        }
    }

    void saveAgentLinkState(AgentLinkState state) {
        if (state == null) return;
        try {
            ObjectNode node = KeeplyHttp.mapper().createObjectNode();
            node.put("paired", state.paired());
            if (state.machineId() == null) node.putNull("machineId"); else node.put("machineId", state.machineId());
            if (state.userId() == null) node.putNull("userId"); else node.put("userId", state.userId());
            KeeplyHttp.putNullable(node, "userEmail", state.userEmail());
            KeeplyHttp.putNullable(node, "linkedAt", state.linkedAt());
            KeeplyHttp.putNullable(node, "lastSeenAt", state.lastSeenAt());
            KeeplyHttp.putNullable(node, "deviceId", state.deviceId());
            saveStateJson(STATE_KEY_LINK, node);
        } catch (Exception e) {
            KeeplyHttp.logWarn("Falha ao salvar agent link state no sqlite: " + KeeplyHttp.safeMsg(e));
        }
    }

    boolean isAgentPaired() {
        AgentLinkState st = loadAgentLinkState();
        return st != null && st.paired();
    }

    Optional<String> getPairedDeviceId() {
        AgentLinkState st = loadAgentLinkState();
        if (st == null || !st.paired()) return Optional.empty();
        if (KeeplyHttp.isBlank(st.deviceId())) return Optional.empty();
        return Optional.of(st.deviceId());
    }

    void touchAgentLastSeen(String deviceId, String lastSeenAt) {
        if (KeeplyHttp.isBlank(deviceId) || KeeplyHttp.isBlank(lastSeenAt)) return;

        AgentLinkState state = loadAgentLinkState();
        if (state == null || !state.paired()) return;
        if (!KeeplyHttp.isBlank(state.deviceId()) && !KeeplyApi.timingSafeEquals(state.deviceId(), deviceId)) return;

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

    private void saveDeviceIdentity(DeviceIdentity identity) {
        try {
            ObjectNode node = KeeplyHttp.mapper().createObjectNode();
            node.put("deviceId", identity.deviceId());
            node.put("publicKey", identity.publicKeyBase64());
            node.put("privateKey", identity.privateKeyBase64());
            saveStateJson(STATE_KEY_DEVICE_IDENTITY, node);
        } catch (Exception e) {
            KeeplyHttp.logWarn("Falha ao salvar identidade do dispositivo no sqlite: " + KeeplyHttp.safeMsg(e));
        }
    }

    private JsonNode loadStateJson(String stateKey) {
        if (KeeplyHttp.isBlank(stateKey)) return null;
        try {
            ensureAgentStateStore();
            try (Connection c = DatabaseBackup.openSingleConnection();
                 PreparedStatement ps = c.prepareStatement("SELECT json_value FROM agent_state WHERE state_key = ? LIMIT 1")) {
                ps.setString(1, stateKey);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return null;
                    String raw = rs.getString(1);
                    if (KeeplyHttp.isBlank(raw)) return null;
                    return KeeplyHttp.mapper().readTree(raw);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao ler state_key=" + stateKey + " no sqlite: " + KeeplyHttp.safeMsg(e), e);
        }
    }

    private void saveStateJson(String stateKey, JsonNode node) {
        if (KeeplyHttp.isBlank(stateKey) || node == null) return;
        try {
            ensureAgentStateStore();
            String raw = KeeplyHttp.mapper().writeValueAsString(node);
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
            throw new IllegalStateException("Falha ao salvar state_key=" + stateKey + " no sqlite: " + KeeplyHttp.safeMsg(e), e);
        }
    }

    private void ensureAgentStateStore() {
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
            throw new IllegalStateException("Falha ao preparar tabela agent_state: " + KeeplyHttp.safeMsg(e), e);
        }
    }

    private static void tryDeleteLegacyFile(Path path) {
        try {
            if (path != null) Files.deleteIfExists(path);
        } catch (Exception ignored) {
            // cleanup best effort
        }
    }

    private static Path pairingFilePath() {
        String custom = System.getProperty("KEEPLY_PAIRING_FILE");
        if (!KeeplyHttp.isBlank(custom)) return Path.of(custom).toAbsolutePath().normalize();
        return Path.of(System.getProperty("user.dir"), ".keeply-pairing.json").toAbsolutePath().normalize();
    }

    private static Path agentStateFilePath() {
        String custom = System.getProperty("KEEPLY_AGENT_STATE_FILE");
        if (!KeeplyHttp.isBlank(custom)) return Path.of(custom).toAbsolutePath().normalize();
        return Path.of(System.getProperty("user.dir"), ".keeply-agent-state.json").toAbsolutePath().normalize();
    }

    private static Path deviceIdentityFilePath() {
        String custom = System.getProperty("KEEPLY_DEVICE_IDENTITY_FILE");
        if (!KeeplyHttp.isBlank(custom)) return Path.of(custom).toAbsolutePath().normalize();
        return Path.of(System.getProperty("user.dir"), ".keeply-device-identity.json").toAbsolutePath().normalize();
    }

    record PairingState(String codeHash, String salt, String createdAt, String expiresAt, String usedAt) {
        boolean isExpired() {
            if (KeeplyHttp.isBlank(expiresAt)) return true;
            try { return Instant.now().isAfter(Instant.parse(expiresAt)); }
            catch (Exception e) { return true; }
        }

        PairingState markUsed(String ts) {
            return new PairingState(codeHash, salt, createdAt, expiresAt, ts);
        }
    }

    record AgentLinkState(
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

    record DeviceIdentity(String deviceId, String publicKeyBase64, String privateKeyBase64) {}
}
