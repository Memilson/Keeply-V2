package com.keeply.authservice.auth;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/auth")
@CrossOrigin(origins = "*")
public class AuthController {

    private final AuthService authService;

    public AuthController(AuthService authService) {
        this.authService = authService;
    }

    @PostMapping("/register")
    public ResponseEntity<?> register(@RequestBody RegisterRequest req) {
        try {
            AuthService.AuthResult out = authService.register(req.name(), req.email(), req.password());
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                    "ok", true,
                    "token", out.token(),
                    "user", out.user()
            ));
        } catch (AuthService.AuthException e) {
            HttpStatus status = switch (e.code()) {
                case "email_exists" -> HttpStatus.CONFLICT;
                case "auth_disabled" -> HttpStatus.SERVICE_UNAVAILABLE;
                case "bad_request" -> HttpStatus.BAD_REQUEST;
                default -> HttpStatus.INTERNAL_SERVER_ERROR;
            };
            return ResponseEntity.status(status).body(Map.of(
                    "ok", false,
                    "code", e.code(),
                    "message", e.getMessage()
            ));
        }
    }

    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestBody LoginRequest req) {
        try {
            AuthService.AuthResult out = authService.login(req.email(), req.password());
            return ResponseEntity.ok(Map.of(
                    "ok", true,
                    "token", out.token(),
                    "user", out.user()
            ));
        } catch (AuthService.AuthException e) {
            HttpStatus status = switch (e.code()) {
                case "invalid_credentials" -> HttpStatus.UNAUTHORIZED;
                case "auth_disabled" -> HttpStatus.SERVICE_UNAVAILABLE;
                case "bad_request" -> HttpStatus.BAD_REQUEST;
                default -> HttpStatus.INTERNAL_SERVER_ERROR;
            };
            return ResponseEntity.status(status).body(Map.of(
                    "ok", false,
                    "code", e.code(),
                    "message", e.getMessage()
            ));
        }
    }

    @GetMapping("/status")
    public ResponseEntity<?> status() {
        return ResponseEntity.ok(Map.of(
                "ok", true,
                "enabled", authService.enabled()
        ));
    }

    @PostMapping("/machines/register")
    public ResponseEntity<?> registerMachine(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestBody RegisterMachineRequest req
    ) {
        try {
            AuthService.MachineView machine = authService.registerMachine(
                    extractToken(authorization),
                    req.machineName(),
                    req.hostname(),
                    req.osName(),
                    req.osVersion(),
                    req.machineFingerprint(),
                    req.machineAlias(),
                    req.clientVersion(),
                    req.lastIp(),
                    req.publicKey(),
                    req.hardwareHashHex(),
                    req.hardwareHashAlgo()
            );
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                    "ok", true,
                    "machine", machine
            ));
        } catch (AuthService.AuthException e) {
            return ResponseEntity.status(mapAuthStatus(e.code())).body(Map.of(
                    "ok", false,
                    "code", e.code(),
                    "message", e.getMessage()
            ));
        }
    }

    @GetMapping("/machines")
    public ResponseEntity<?> listMachines(
            @RequestHeader(value = "Authorization", required = false) String authorization
    ) {
        try {
            List<AuthService.MachineView> machines = authService.listMachines(extractToken(authorization));
            return ResponseEntity.ok(Map.of(
                    "ok", true,
                    "items", machines
            ));
        } catch (AuthService.AuthException e) {
            return ResponseEntity.status(mapAuthStatus(e.code())).body(Map.of(
                    "ok", false,
                    "code", e.code(),
                    "message", e.getMessage()
            ));
        }
    }

    private HttpStatus mapAuthStatus(String code) {
        return switch (code) {
            case "email_exists" -> HttpStatus.CONFLICT;
            case "invalid_credentials", "unauthorized" -> HttpStatus.UNAUTHORIZED;
            case "auth_disabled" -> HttpStatus.SERVICE_UNAVAILABLE;
            case "bad_request" -> HttpStatus.BAD_REQUEST;
            default -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
    }

    private String extractToken(String authorization) {
        if (authorization == null || authorization.isBlank()) return null;
        String token = authorization.trim();
        if (token.toLowerCase().startsWith("bearer ")) {
            token = token.substring(7).trim();
        }
        return token;
    }

    public record RegisterRequest(String name, String email, String password) {}
    public record LoginRequest(String email, String password) {}
    public record RegisterMachineRequest(
            String machineName,
            String hostname,
            String osName,
            String osVersion,
            String machineFingerprint,
            String machineAlias,
            String clientVersion,
            String lastIp,
            String publicKey,
            String hardwareHashHex,
            String hardwareHashAlgo
    ) {}
}
