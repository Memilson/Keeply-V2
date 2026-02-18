package com.keeply.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.validation.constraints.NotBlank;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/keeply-ws")
@CrossOrigin(origins = "*")
@Validated
public class KeeplyWsController {

    private final ObjectMapper mapper;
    private final HttpClient httpClient;
    private final String authBaseUrl;
    private final String agentApiBaseUrl;
    private final String clientVersion;
    private final Path sessionPath;

    public KeeplyWsController(
            ObjectMapper mapper,
            @Value("${keeply.ws.auth-base-url:http://localhost:18081/api/auth}") String authBaseUrl,
            @Value("${keeply.ws.agent-api-base-url:http://localhost:25420/api/keeply}") String agentApiBaseUrl,
            @Value("${keeply.ws.client-version:keeply-ws/1.0.0}") String clientVersion,
            @Value("${keeply.ws.session-file:${user.dir}/.keeply-ws-session.json}") String sessionFile
    ) {
        this.mapper = mapper;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        this.authBaseUrl = stripSlash(authBaseUrl);
        this.agentApiBaseUrl = stripSlash(agentApiBaseUrl);
        this.clientVersion = clientVersion;
        this.sessionPath = Path.of(sessionFile).toAbsolutePath().normalize();
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        Optional<LocalSession> session = readSession();
        return Map.of(
                "ok", true,
                "service", "keeply-ws",
                "ts", Instant.now().toString(),
                "authBase", authBaseUrl,
                "agentApiBase", agentApiBaseUrl,
                "hasSession", session.map(LocalSession::token).filter(v -> !v.isBlank()).isPresent()
        );
    }

    @GetMapping("/session")
    public ResponseEntity<?> session() {
        Optional<LocalSession> sessionOpt = readSession();
        if (sessionOpt.isEmpty() || isBlank(sessionOpt.get().token())) {
            return error(HttpStatus.UNAUTHORIZED, "unauthorized", "Sessao local ausente");
        }

        LocalSession session = sessionOpt.get();
        ApiCallResult probe = request(authBaseUrl, "/machines", "GET", session.token(), null, "auth_unreachable", "Nao foi possivel conectar no auth-service");
        if (!probe.ok()) {
            clearSession();
            return error(HttpStatus.UNAUTHORIZED, "unauthorized", "Sessao local expirada");
        }

        return ResponseEntity.ok(Map.of("ok", true, "session", sessionSummary(session)));
    }

    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestBody LoginRequest request) {
        String email = clean(request.email());
        String password = clean(request.password());

        if (isBlank(email) || isBlank(password)) {
            return error(HttpStatus.BAD_REQUEST, "bad_request", "Informe email e senha");
        }

        ObjectNode payload = mapper.createObjectNode();
        payload.put("email", email.toLowerCase());
        payload.put("password", password);

        ApiCallResult login = request(authBaseUrl, "/login", "POST", null, payload, "auth_unreachable", "Nao foi possivel conectar no auth-service");
        if (!login.ok()) {
            return error(HttpStatus.valueOf(login.status()), nn(login.code(), "auth_error"), nn(login.message(), "Falha no login"));
        }

        String token = text(login.data(), "token");
        if (isBlank(token)) {
            return error(HttpStatus.BAD_GATEWAY, "bad_auth_response", "Auth retornou sem token");
        }

        JsonNode user = login.data() == null ? null : login.data().get("user");

        writeSession(new LocalSession(
                token,
                email.toLowerCase(),
                user,
                null,
                Instant.now().toString(),
                Instant.now().toString()
        ));

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("token", token);
        out.put("user", user);
        out.put("machine", null);
        return ResponseEntity.ok(out);
    }

    @PostMapping("/register")
    public ResponseEntity<?> register(@RequestBody RegisterRequest request) {
        String name = clean(request.name());
        String email = clean(request.email());
        String password = clean(request.password());

        if (isBlank(name) || isBlank(email) || isBlank(password)) {
            return error(HttpStatus.BAD_REQUEST, "bad_request", "Informe nome, email e senha");
        }

        ObjectNode payload = mapper.createObjectNode();
        payload.put("name", name);
        payload.put("email", email.toLowerCase());
        payload.put("password", password);

        ApiCallResult register = request(authBaseUrl, "/register", "POST", null, payload, "auth_unreachable", "Nao foi possivel conectar no auth-service");
        if (!register.ok()) {
            return error(HttpStatus.valueOf(register.status()), nn(register.code(), "auth_error"), nn(register.message(), "Falha no cadastro"));
        }

        String token = text(register.data(), "token");
        if (isBlank(token)) {
            return error(HttpStatus.BAD_GATEWAY, "bad_auth_response", "Auth retornou sem token");
        }

        JsonNode user = register.data() == null ? null : register.data().get("user");

        writeSession(new LocalSession(
                token,
                email.toLowerCase(),
                user,
                null,
                Instant.now().toString(),
                Instant.now().toString()
        ));

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("token", token);
        out.put("user", user);
        out.put("machine", null);
        return ResponseEntity.status(HttpStatus.CREATED).body(out);
    }

    @PostMapping("/logout")
    public Map<String, Object> logout() {
        clearSession();
        return Map.of("ok", true);
    }

    @GetMapping("/devices")
    public ResponseEntity<?> devices() {
        Optional<LocalSession> sessionOpt = readSession();
        if (sessionOpt.isEmpty() || isBlank(sessionOpt.get().token())) {
            return error(HttpStatus.UNAUTHORIZED, "unauthorized", "Faca login antes");
        }

        LocalSession session = sessionOpt.get();
        ApiCallResult out = request(authBaseUrl, "/machines", "GET", session.token(), null, "auth_unreachable", "Nao foi possivel conectar no auth-service");
        if (!out.ok()) {
            clearSession();
            return error(HttpStatus.valueOf(out.status()), nn(out.code(), "auth_error"), nn(out.message(), "Falha ao listar dispositivos"));
        }

        JsonNode itemsNode = out.data() == null ? null : out.data().get("items");
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ok", true);
        body.put("items", itemsNode != null && itemsNode.isArray() ? itemsNode : mapper.createArrayNode());
        body.put("session", sessionSummary(session));
        return ResponseEntity.ok(body);
    }

    @DeleteMapping("/devices/{deviceId}")
    public ResponseEntity<?> deleteDevice(@PathVariable("deviceId") long deviceId) {
        Optional<LocalSession> sessionOpt = readSession();
        if (sessionOpt.isEmpty() || isBlank(sessionOpt.get().token())) {
            return error(HttpStatus.UNAUTHORIZED, "unauthorized", "Faca login antes");
        }
        if (deviceId <= 0) {
            return error(HttpStatus.BAD_REQUEST, "bad_request", "deviceId invalido");
        }

        LocalSession session = sessionOpt.get();
        ApiCallResult devicesBefore = request(authBaseUrl, "/machines", "GET", session.token(), null, "auth_unreachable", "Nao foi possivel listar dispositivos");
        String deletedHwHash = null;
        if (devicesBefore.ok() && devicesBefore.data() != null && devicesBefore.data().path("items").isArray()) {
            for (JsonNode item : devicesBefore.data().path("items")) {
                if (item != null && item.path("id").asLong(-1) == deviceId) {
                    deletedHwHash = text(item, "hardwareHashHex");
                    break;
                }
            }
        }
        ApiCallResult out = request(
                authBaseUrl,
                "/machines/" + deviceId,
                "DELETE",
                session.token(),
                null,
                "auth_unreachable",
                "Nao foi possivel remover dispositivo"
        );
        if (!out.ok()) {
            return error(HttpStatus.valueOf(out.status()), nn(out.code(), "auth_error"), nn(out.message(), "Falha ao remover dispositivo"));
        }

        if (!isBlank(deletedHwHash)) {
            ApiCallResult pairing = request(agentApiBaseUrl, "/pairing", "GET", null, null, "agent_api_unreachable", "Falha ao consultar agente");
            String localHwHash = pairing.ok() && pairing.data() != null && pairing.data().get("machine") != null
                    ? text(pairing.data().get("machine"), "hardwareHashHex")
                    : null;
            if (!isBlank(localHwHash) && deletedHwHash.equalsIgnoreCase(localHwHash)) {
                request(agentApiBaseUrl, "/pairing/mark-unlinked", "POST", null, mapper.createObjectNode(), "agent_api_unreachable", "Falha ao marcar agente como desvinculado");
            }
        }

        return ResponseEntity.ok(Map.of("ok", true, "deleted", true, "deviceId", deviceId));
    }

    @PostMapping("/devices/register")
    public ResponseEntity<?> registerDevice() {
        return error(
                HttpStatus.GONE,
                "deprecated_endpoint",
                "Use /pairing/activate para vincular o agente local por codigo"
        );
    }

    @GetMapping("/agent/overview")
    public Map<String, Object> agentOverview() {
        Optional<LocalSession> sessionOpt = readSession();
        LocalAgentBinding binding = sessionOpt.isPresent() && !isBlank(sessionOpt.get().token())
                ? resolveLocalAgentBinding(sessionOpt.get().token())
                : LocalAgentBinding.notLinked("Sessao local ausente");
        Map<String, Object> backup = binding.linked() ? requestAgentHistory() : Map.of(
                "ok", false,
                "code", "agent_not_linked",
                "message", "Vincule este agente no painel antes de consultar metricas"
        );

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("machine", machineInfoMap(collectMachineInfo()));
        out.put("session", sessionOpt.map(this::sessionSummaryWithMachine).orElse(null));
        out.put("linked", binding.linked());
        out.put("linkedMachine", binding.machine() == null ? mapper.createObjectNode() : binding.machine());
        out.put("backup", backup);
        return out;
    }

    @GetMapping("/backups")
    public ResponseEntity<?> backups() {
        Optional<LocalSession> sessionOpt = readSession();
        if (sessionOpt.isEmpty() || isBlank(sessionOpt.get().token())) {
            return error(HttpStatus.UNAUTHORIZED, "unauthorized", "Faca login antes");
        }
        LocalAgentBinding binding = resolveLocalAgentBinding(sessionOpt.get().token());
        if (!binding.linked()) {
            return error(HttpStatus.CONFLICT, "agent_not_linked", nn(binding.message(), "Vincule este agente no painel antes de consultar metricas"));
        }

        Map<String, Object> backup = requestAgentHistory();
        boolean ok = Boolean.TRUE.equals(backup.get("ok"));
        return ResponseEntity.status(ok ? HttpStatus.OK : HttpStatus.BAD_GATEWAY).body(backup);
    }

    @GetMapping("/pairing/code")
    public ResponseEntity<?> pairingCode() {
        Optional<LocalSession> sessionOpt = readSession();
        if (sessionOpt.isEmpty() || isBlank(sessionOpt.get().token())) {
            return error(HttpStatus.UNAUTHORIZED, "unauthorized", "Faca login antes");
        }

        LocalSession session = sessionOpt.get();
        ApiCallResult agent = request(agentApiBaseUrl, "/pairing", "GET", null, null, "agent_api_unreachable", "Nao foi possivel consultar estado do agente");
        if (!agent.ok()) {
            return error(HttpStatus.BAD_GATEWAY, nn(agent.code(), "agent_api_error"), nn(agent.message(), "Falha ao consultar codigo do agente"));
        }
        ApiCallResult devices = request(authBaseUrl, "/machines", "GET", session.token(), null, "auth_unreachable", "Nao foi possivel listar dispositivos");
        if (!devices.ok()) {
            return error(HttpStatus.BAD_GATEWAY, nn(devices.code(), "auth_error"), nn(devices.message(), "Falha ao listar dispositivos"));
        }

        String localHash = null;
        if (agent.data() != null && agent.data().get("machine") != null) {
            localHash = text(agent.data().get("machine"), "hardwareHashHex");
        }
        boolean existsInPanel = false;
        JsonNode matchedMachine = null;
        if (!isBlank(localHash) && devices.data() != null && devices.data().path("items").isArray()) {
            for (JsonNode item : devices.data().path("items")) {
                String hash = text(item, "hardwareHashHex");
                if (!isBlank(hash) && localHash.equalsIgnoreCase(hash)) {
                    existsInPanel = true;
                    matchedMachine = item;
                    break;
                }
            }
        }

        JsonNode responseData = agent.data() == null ? mapper.createObjectNode() : agent.data().deepCopy();
        if (responseData instanceof ObjectNode on) {
            on.put("existsInPanel", existsInPanel);
            if (existsInPanel) {
                // Estado local do agente pode ter sido apagado; não force novo pairing se já existe no painel.
                on.put("paired", true);
                on.put("requiresPairing", false);
                on.putNull("code");
                if (isBlank(text(on, "linkedAt"))) {
                    putNullable(on, "linkedAt", Instant.now().toString());
                }

                ObjectNode markLinked = mapper.createObjectNode();
                if (matchedMachine != null && matchedMachine.get("id") != null) {
                    markLinked.set("machineId", matchedMachine.get("id"));
                }
                if (session.user() != null && session.user().isObject()) {
                    if (session.user().get("id") != null) markLinked.set("userId", session.user().get("id"));
                    putNullable(markLinked, "userEmail", text(session.user(), "email"));
                } else {
                    putNullable(markLinked, "userEmail", session.email());
                }
                request(agentApiBaseUrl, "/pairing/mark-linked", "POST", null, markLinked, "agent_api_unreachable", "Falha ao sincronizar estado local do agente");
            }
        }
        return ResponseEntity.ok(responseData == null ? mapper.createObjectNode() : responseData);
    }

    @PostMapping("/pairing/activate")
    public ResponseEntity<?> pairingActivate(@RequestBody PairingActivateRequest req) {
        Optional<LocalSession> sessionOpt = readSession();
        if (sessionOpt.isEmpty() || isBlank(sessionOpt.get().token())) {
            return error(HttpStatus.UNAUTHORIZED, "unauthorized", "Faca login antes");
        }

        String informedCode = normalizePairingCode(clean(req.code()));
        if (isBlank(informedCode)) {
            return error(HttpStatus.BAD_REQUEST, "bad_request", "Informe o codigo do agente");
        }
        String targetAgentBase = stripSlash(clean(req.agentUrl()));
        if (isBlank(targetAgentBase)) {
            targetAgentBase = agentApiBaseUrl;
        }

        ObjectNode pairingPayload = mapper.createObjectNode();
        pairingPayload.put("code", informedCode);
        ApiCallResult pairingOut = request(targetAgentBase, "/pairing/approve", "POST", null, pairingPayload, "agent_api_unreachable", "Nao foi possivel validar codigo do agente");
        if (!pairingOut.ok()) {
            return error(HttpStatus.BAD_GATEWAY, nn(pairingOut.code(), "agent_api_error"), nn(pairingOut.message(), "Falha ao validar codigo do agente"));
        }

        JsonNode machine = pairingOut.data() == null ? null : pairingOut.data().get("machine");
        if (machine == null || !machine.isObject()) {
            return error(HttpStatus.BAD_GATEWAY, "bad_agent_response", "Agente retornou dados de maquina invalidos");
        }
        String deviceId = text(pairingOut.data(), "deviceId");
        String publicKey = text(pairingOut.data(), "publicKey");
        String requestedAlias = clean(req.alias());

        ObjectNode payload = mapper.createObjectNode();
        putNullable(payload, "machineName", text(machine, "machineName"));
        putNullable(payload, "hostname", text(machine, "hostname"));
        putNullable(payload, "osName", text(machine, "osName"));
        putNullable(payload, "osVersion", text(machine, "osVersion"));
        putNullable(payload, "machineFingerprint", text(machine, "hardwareHashHex"));
        putNullable(payload, "machineAlias", isBlank(requestedAlias) ? text(machine, "machineAlias") : requestedAlias);
        payload.put("clientVersion", clientVersion);
        putNullable(payload, "lastIp", text(machine, "lastIp"));
        putNullable(payload, "publicKey", publicKey);
        putNullable(payload, "deviceId", deviceId);
        putNullable(payload, "hardwareHashHex", text(machine, "hardwareHashHex"));
        payload.put("hardwareHashAlgo", "sha256");

        LocalSession session = sessionOpt.get();
        ApiCallResult regOut = request(authBaseUrl, "/machines/register", "POST", session.token(), payload, "auth_unreachable", "Nao foi possivel registrar dispositivo");
        if (!regOut.ok()) {
            return error(HttpStatus.valueOf(regOut.status()), nn(regOut.code(), "auth_error"), nn(regOut.message(), "Falha ao vincular agente"));
        }

        JsonNode machineNode = regOut.data() == null ? null : regOut.data().get("machine");
        writeSession(new LocalSession(
                session.token(),
                session.email(),
                session.user(),
                machineNode,
                session.createdAt(),
                Instant.now().toString()
        ));

        // marca persistencia de vinculo no agente; best effort
        ObjectNode markLinked = mapper.createObjectNode();
        if (machineNode != null && machineNode.isObject() && machineNode.get("id") != null) {
            markLinked.set("machineId", machineNode.get("id"));
        }
        if (session.user() != null && session.user().isObject()) {
            if (session.user().get("id") != null) markLinked.set("userId", session.user().get("id"));
            String userEmail = text(session.user(), "email");
            putNullable(markLinked, "userEmail", userEmail);
        } else {
            putNullable(markLinked, "userEmail", session.email());
        }
        request(targetAgentBase, "/pairing/mark-linked", "POST", null, markLinked, "agent_api_unreachable", "Falha ao persistir vinculo no agente");

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("paired", true);
        out.put("agentBaseUrl", targetAgentBase);
        out.put("machine", machineNode == null ? mapper.createObjectNode() : machineNode);
        return ResponseEntity.ok(out);
    }

    private MachineRegistrationResult ensureMachineRegistration(String token) {
        MachineInfo machine = collectMachineInfo();

        ObjectNode payload = mapper.createObjectNode();
        payload.put("machineName", machine.machineName());
        payload.put("hostname", machine.hostname());
        payload.put("osName", machine.osName());
        payload.put("osVersion", machine.osVersion());
        payload.put("machineFingerprint", machine.hardwareHashHex());
        payload.put("machineAlias", machine.machineAlias());
        payload.put("clientVersion", clientVersion);
        putNullable(payload, "lastIp", machine.lastIp());
        payload.putNull("publicKey");
        payload.put("hardwareHashHex", machine.hardwareHashHex());
        payload.put("hardwareHashAlgo", "sha256");

        ApiCallResult out = request(authBaseUrl, "/machines/register", "POST", token, payload, "auth_unreachable", "Nao foi possivel conectar no auth-service");
        if (!out.ok()) {
            return new MachineRegistrationResult(false, null, out.status(), nn(out.code(), "auth_error"), nn(out.message(), "Falha ao registrar dispositivo"));
        }

        JsonNode machineNode = out.data() == null ? null : out.data().get("machine");
        return new MachineRegistrationResult(true, machineNode, 201, null, null);
    }

    private Map<String, Object> requestAgentHistory() {
        ApiCallResult out = request(agentApiBaseUrl, "/history?limit=20", "GET", null, null, "agent_api_unreachable", "Nao foi possivel consultar backup local");
        if (!out.ok()) {
            String code = "agent_api_error";
            String message = "Keeply API retornou " + out.status();
            if ("agent_api_unreachable".equals(out.code())) {
                code = out.code();
                message = out.message();
            }
            return Map.of(
                    "ok", false,
                    "code", code,
                    "message", message,
                    "details", out.data() == null ? mapper.createObjectNode() : out.data()
            );
        }

        JsonNode items = out.data() == null ? mapper.createArrayNode() : out.data().path("items");
        ArrayNode folders = mapper.createArrayNode();
        ObjectNode metrics = mapper.createObjectNode();
        int total = 0;
        int backups = 0;
        int restores = 0;
        int success = 0;
        int failed = 0;
        int running = 0;
        LinkedHashSet<String> paths = new LinkedHashSet<>();

        if (items.isArray()) {
            for (JsonNode it : items) {
                total++;
                String type = text(it, "backupType");
                String status = text(it, "status");
                String rootPath = text(it, "rootPath");
                String destPath = text(it, "destPath");
                if ("restore".equalsIgnoreCase(type)) restores++;
                else backups++;
                if ("ok".equalsIgnoreCase(status) || "success".equalsIgnoreCase(status)) success++;
                else if ("running".equalsIgnoreCase(status) || "pending".equalsIgnoreCase(status)) running++;
                else failed++;
                if (!isBlank(rootPath) || !isBlank(destPath)) {
                    paths.add(nn(rootPath, "-") + " -> " + nn(destPath, "-"));
                }

                ObjectNode folder = folders.addObject();
                copyOrNull(folder, "scanId", it, "scanId");
                copyOrNull(folder, "rootPath", it, "rootPath");
                copyOrNull(folder, "destPath", it, "destPath");
                copyOrNull(folder, "status", it, "status");
                copyOrNull(folder, "startedAt", it, "startedAt");
                copyOrNull(folder, "finishedAt", it, "finishedAt");
                copyOrNull(folder, "backupType", it, "backupType");
                copyOrNull(folder, "message", it, "message");
            }
        }

        ArrayNode sources = mapper.createArrayNode();
        for (String path : paths) sources.add(path);

        metrics.put("totalJobs", total);
        metrics.put("backupJobs", backups);
        metrics.put("restoreJobs", restores);
        metrics.put("successJobs", success);
        metrics.put("failedJobs", failed);
        metrics.put("runningJobs", running);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("ok", true);
        response.put("items", items);
        response.put("folders", folders);
        response.put("metrics", metrics);
        response.put("sourceDestinations", sources);
        return response;
    }

    private LocalAgentBinding resolveLocalAgentBinding(String token) {
        ApiCallResult pairing = request(agentApiBaseUrl, "/pairing", "GET", null, null, "agent_api_unreachable", "Nao foi possivel consultar agente local");
        if (!pairing.ok()) {
            return LocalAgentBinding.notLinked(nn(pairing.message(), "Falha ao consultar agente local"));
        }

        String localHash = pairing.data() != null && pairing.data().get("machine") != null
                ? text(pairing.data().get("machine"), "hardwareHashHex")
                : null;
        String localDeviceId = text(pairing.data(), "deviceId");

        ApiCallResult devices = request(authBaseUrl, "/machines", "GET", token, null, "auth_unreachable", "Nao foi possivel listar dispositivos");
        if (!devices.ok()) {
            return LocalAgentBinding.notLinked(nn(devices.message(), "Falha ao listar dispositivos"));
        }

        if (devices.data() == null || !devices.data().path("items").isArray()) {
            return LocalAgentBinding.notLinked("Lista de dispositivos vazia");
        }

        for (JsonNode item : devices.data().path("items")) {
            String hash = text(item, "hardwareHashHex");
            String deviceId = text(item, "deviceId");
            boolean hashMatch = !isBlank(localHash) && !isBlank(hash) && localHash.equalsIgnoreCase(hash);
            boolean deviceMatch = !isBlank(localDeviceId) && !isBlank(deviceId) && localDeviceId.equals(deviceId);
            if (hashMatch || deviceMatch) {
                return new LocalAgentBinding(true, item, null);
            }
        }
        return LocalAgentBinding.notLinked("Agente local ainda nao foi vinculado a esta conta");
    }

    private ApiCallResult request(
            String base,
            String path,
            String method,
            String token,
            JsonNode body,
            String unreachableCode,
            String unreachableMessage
    ) {
        try {
            String endpoint = stripSlash(base) + (path.startsWith("/") ? path : "/" + path);
            HttpRequest.BodyPublisher publisher = body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body));

            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .timeout(Duration.ofSeconds(20))
                    .method(method == null ? "GET" : method.toUpperCase(), publisher)
                    .header("Accept", "application/json");

            if (body != null) {
                builder.header("Content-Type", "application/json");
            }
            if (!isBlank(token)) {
                builder.header("Authorization", "Bearer " + token);
            }

            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            JsonNode data = parseJsonSafe(response.body());

            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                return new ApiCallResult(
                        false,
                        response.statusCode(),
                        data,
                        text(data, "code") == null ? "upstream_error" : text(data, "code"),
                        text(data, "message") == null ? ("Upstream retornou " + response.statusCode()) : text(data, "message")
                );
            }

            return new ApiCallResult(true, response.statusCode(), data, null, null);
        } catch (Exception e) {
            return new ApiCallResult(false, 502, null, unreachableCode, unreachableMessage + ": " + safeMsg(e));
        }
    }

    private Optional<LocalSession> readSession() {
        if (!Files.exists(sessionPath)) {
            return Optional.empty();
        }

        try {
            JsonNode node = mapper.readTree(Files.readString(sessionPath));
            if (node == null || !node.isObject()) {
                return Optional.empty();
            }

            return Optional.of(new LocalSession(
                    text(node, "token"),
                    text(node, "email"),
                    node.get("user"),
                    node.get("machine"),
                    text(node, "createdAt"),
                    text(node, "updatedAt")
            ));
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    private void writeSession(LocalSession session) {
        try {
            Path parent = sessionPath.getParent();
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }

            LocalSession payload = new LocalSession(
                    session.token(),
                    session.email(),
                    session.user(),
                    session.machine(),
                    session.createdAt() == null ? Instant.now().toString() : session.createdAt(),
                    Instant.now().toString()
            );

            Files.writeString(
                    sessionPath,
                    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
        } catch (IOException ignored) {
            // best effort
        }
    }

    private void clearSession() {
        try {
            Files.deleteIfExists(sessionPath);
        } catch (IOException ignored) {
            // best effort
        }
    }

    private MachineInfo collectMachineInfo() {
        String hostname = safe(System.getenv("COMPUTERNAME"));
        if (hostname == null) hostname = safe(System.getenv("HOSTNAME"));
        if (hostname == null) hostname = "unknown-host";

        String osName = safe(System.getProperty("os.name"));
        String osVersion = safe(System.getProperty("os.version"));
        String arch = safe(System.getProperty("os.arch"));
        String userName = safe(System.getProperty("user.name"));

        String machineName = (userName == null ? "usuario" : userName) + "@" + hostname;
        String machineAlias = hostname;
        String lastIp = firstIpv4();
        String hardwareHashHex = sha256Hex(String.join("|", nvl(hostname), nvl(osName), nvl(osVersion), nvl(arch), nvl(lastIp)));

        return new MachineInfo(machineName, machineAlias, hostname, osName, osVersion, arch, lastIp, hardwareHashHex);
    }

    private String firstIpv4() {
        try {
            for (NetworkInterface nif : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!nif.isUp() || nif.isLoopback()) continue;
                for (var addr : Collections.list(nif.getInetAddresses())) {
                    if (addr instanceof Inet4Address ip && !ip.isLoopbackAddress()) {
                        return ip.getHostAddress();
                    }
                }
            }
        } catch (Exception ignored) {
            return null;
        }
        return null;
    }

    private String sha256Hex(String value) {
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

    private JsonNode parseJsonSafe(String raw) {
        if (raw == null || raw.isBlank()) {
            return mapper.createObjectNode();
        }
        try {
            return mapper.readTree(raw);
        } catch (Exception ignored) {
            ObjectNode out = mapper.createObjectNode();
            out.put("raw", raw);
            return out;
        }
    }

    private Map<String, Object> machineInfoMap(MachineInfo info) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("machineName", info.machineName());
        out.put("machineAlias", info.machineAlias());
        out.put("hostname", info.hostname());
        out.put("osName", info.osName());
        out.put("osVersion", info.osVersion());
        out.put("arch", info.arch());
        out.put("lastIp", info.lastIp());
        out.put("hardwareHashHex", info.hardwareHashHex());
        return out;
    }

    private Map<String, Object> sessionSummary(LocalSession session) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("email", session.email());
        out.put("user", session.user());
        out.put("tokenPreview", tokenPreview(session.token()));
        out.put("createdAt", session.createdAt());
        out.put("updatedAt", session.updatedAt());
        return out;
    }

    private Map<String, Object> sessionSummaryWithMachine(LocalSession session) {
        Map<String, Object> out = new LinkedHashMap<>(sessionSummary(session));
        out.put("machine", session.machine());
        return out;
    }

    private void putNullable(ObjectNode node, String key, String value) {
        if (isBlank(value)) node.putNull(key);
        else node.put(key, value);
    }

    private void copyOrNull(ObjectNode out, String target, JsonNode source, String field) {
        JsonNode value = source.get(field);
        if (value == null || value.isNull()) out.putNull(target);
        else out.set(target, value);
    }

    private String tokenPreview(String token) {
        if (isBlank(token)) return null;
        if (token.length() < 12) return "***";
        return token.substring(0, 6) + "..." + token.substring(token.length() - 4);
    }

    private String text(JsonNode node, String field) {
        if (node == null) return null;
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) return null;
        String out = value.asText(null);
        return (out == null || out.isBlank()) ? null : out;
    }

    private String clean(String value) {
        if (value == null) return null;
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private String normalizePairingCode(String code) {
        if (code == null) return null;
        String raw = code.trim().toUpperCase();
        if (raw.isEmpty()) return null;
        return raw.replaceAll("[^A-Z0-9]", "");
    }

    private String safe(String value) {
        return isBlank(value) ? null : value;
    }

    private String stripSlash(String value) {
        if (value == null) return "";
        return value.replaceAll("/+$", "");
    }

    private String safeMsg(Throwable error) {
        String message = error.getMessage();
        return (message == null || message.isBlank()) ? error.getClass().getSimpleName() : message;
    }

    private String nvl(String value) {
        return value == null ? "" : value;
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private String nn(String value, String fallback) {
        return value == null ? fallback : value;
    }

    private ResponseEntity<Map<String, Object>> error(HttpStatus status, String code, String message) {
        return ResponseEntity.status(status).body(Map.of("ok", false, "code", code, "message", message));
    }

    public record LoginRequest(@NotBlank String email, @NotBlank String password) {}
    public record RegisterRequest(@NotBlank String name, @NotBlank String email, @NotBlank String password) {}
    public record PairingActivateRequest(String code, String alias, String agentUrl) {}

    private record LocalSession(
            String token,
            String email,
            JsonNode user,
            JsonNode machine,
            String createdAt,
            String updatedAt
    ) {}

    private record MachineInfo(
            String machineName,
            String machineAlias,
            String hostname,
            String osName,
            String osVersion,
            String arch,
            String lastIp,
            String hardwareHashHex
    ) {}

    private record ApiCallResult(boolean ok, int status, JsonNode data, String code, String message) {}

    private record MachineRegistrationResult(
            boolean ok,
            JsonNode machine,
            int status,
            String code,
            String message
    ) {
        JsonNode asNode(ObjectMapper mapper) {
            if (ok) return machine == null ? mapper.createObjectNode() : machine;

            ObjectNode out = mapper.createObjectNode();
            out.put("ok", false);
            out.put("status", status);
            out.put("code", code);
            out.put("message", message);
            return out;
        }
    }

    private record LocalAgentBinding(boolean linked, JsonNode machine, String message) {
        static LocalAgentBinding notLinked(String message) {
            return new LocalAgentBinding(false, null, message);
        }
    }
}
