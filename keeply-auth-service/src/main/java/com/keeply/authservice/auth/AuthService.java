package com.keeply.authservice.auth;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class AuthService {

    private final DataSource authDataSource;
    private final BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();
    private final Map<String, SessionUser> sessions = new ConcurrentHashMap<>();

    public AuthService(@Qualifier("authDataSource") DataSource authDataSource) {
        this.authDataSource = authDataSource;
        ensureSchema();
    }

    public boolean enabled() {
        return authDataSource != null;
    }

    public AuthResult register(String fullName, String email, String password) {
        requireEnabled();
        validate(email, password);

        String normalizedEmail = email.trim().toLowerCase();
        String normalizedName = (fullName == null || fullName.isBlank()) ? "Usuario Keeply" : fullName.trim();

        if (existsEmail(normalizedEmail)) {
            throw new AuthException("email_exists", "Este email ja esta cadastrado");
        }

        String hash = encoder.encode(password);
        long userId;

        String sql = "INSERT INTO keeply_users(full_name, email, password_hash, created_at, updated_at) VALUES(?,?,?,now(),now()) RETURNING id";
        try (Connection c = authDataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, normalizedName);
            ps.setString(2, normalizedEmail);
            ps.setString(3, hash);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new AuthException("register_failed", "Falha ao criar usuario");
                }
                userId = rs.getLong(1);
            }
        } catch (AuthException e) {
            throw e;
        } catch (Exception e) {
            throw new AuthException("db_error", safeMsg(e));
        }

        String token = issueToken(userId, normalizedName, normalizedEmail);
        return new AuthResult(token, new UserView(userId, normalizedName, normalizedEmail));
    }

    public AuthResult login(String email, String password) {
        requireEnabled();
        validate(email, password);

        String normalizedEmail = email.trim().toLowerCase();
        String sql = "SELECT id, full_name, email, password_hash FROM keeply_users WHERE email = ? AND disabled_at IS NULL LIMIT 1";

        try (Connection c = authDataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, normalizedEmail);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new AuthException("invalid_credentials", "Email ou senha invalidos");
                }
                long id = rs.getLong("id");
                String name = rs.getString("full_name");
                String foundEmail = rs.getString("email");
                String hash = rs.getString("password_hash");

                if (!encoder.matches(password, hash)) {
                    throw new AuthException("invalid_credentials", "Email ou senha invalidos");
                }

                try (PreparedStatement upd = c.prepareStatement("UPDATE keeply_users SET last_login_at = now() WHERE id = ?")) {
                    upd.setLong(1, id);
                    upd.executeUpdate();
                }

                String token = issueToken(id, name, foundEmail);
                return new AuthResult(token, new UserView(id, name, foundEmail));
            }
        } catch (AuthException e) {
            throw e;
        } catch (Exception e) {
            throw new AuthException("db_error", safeMsg(e));
        }
    }

    public MachineView registerMachine(
            String token,
            String machineName,
            String hostname,
            String osName,
            String osVersion,
            String legacyMachineFingerprint,
            String machineAlias,
            String clientVersion,
            String lastIp,
            String publicKey,
            String hardwareHashHex,
            String hardwareHashAlgo
    ) {
        requireEnabled();
        SessionUser session = requireSession(token);

        String normalizedHex = normalizeHardwareHashHex(hardwareHashHex, legacyMachineFingerprint);
        String algo = (hardwareHashAlgo == null || hardwareHashAlgo.isBlank()) ? "sha256" : hardwareHashAlgo.trim().toLowerCase();
        if ("sha256".equals(algo) && normalizedHex.length() != 64) {
            throw new AuthException("bad_request", "hardware hash sha256 deve ter 64 caracteres hex");
        }

        String name = (machineName == null || machineName.isBlank()) ? "Minha maquina" : machineName.trim();
        String host = (hostname == null || hostname.isBlank()) ? null : hostname.trim();
        String os = (osName == null || osName.isBlank()) ? null : osName.trim();
        String osv = (osVersion == null || osVersion.isBlank()) ? null : osVersion.trim();
        String alias = (machineAlias == null || machineAlias.isBlank()) ? null : machineAlias.trim();
        String version = (clientVersion == null || clientVersion.isBlank()) ? null : clientVersion.trim();
        String ip = (lastIp == null || lastIp.isBlank()) ? null : lastIp.trim();
        String pub = (publicKey == null || publicKey.isBlank()) ? null : publicKey.trim();

        Long existingOwner = findMachineOwnerByHardwareHash(normalizedHex);
        if (existingOwner != null && existingOwner.longValue() != session.id()) {
            throw new AuthException("machine_already_linked", "Esta maquina ja esta vinculada a outro usuario");
        }

        String sql = """
                INSERT INTO keeply_machines(
                  user_id, machine_name, hostname, os_name, os_version,
                  client_version, last_ip, public_key,
                  hardware_hash, hardware_hash_algo, machine_alias,
                  registered_at, last_seen_at, created_at, updated_at
                )
                VALUES(?,?,?,?,?,?,?::inet,?,decode(?, 'hex'),?,?, now(), now(), now(), now())
                ON CONFLICT (hardware_hash) DO UPDATE SET
                  user_id = EXCLUDED.user_id,
                  machine_name = EXCLUDED.machine_name,
                  hostname = EXCLUDED.hostname,
                  os_name = EXCLUDED.os_name,
                  os_version = EXCLUDED.os_version,
                  client_version = EXCLUDED.client_version,
                  last_ip = EXCLUDED.last_ip,
                  public_key = COALESCE(EXCLUDED.public_key, keeply_machines.public_key),
                  machine_alias = COALESCE(EXCLUDED.machine_alias, keeply_machines.machine_alias),
                  hardware_hash_algo = EXCLUDED.hardware_hash_algo,
                  last_seen_at = EXCLUDED.last_seen_at
                RETURNING id, user_id, machine_name, hostname, os_name, os_version,
                          machine_alias, client_version, last_ip::text AS last_ip,
                          public_key, hardware_hash_hex, hardware_hash_algo,
                          registered_at, last_seen_at
                """;

        try (Connection c = authDataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, session.id());
            ps.setString(2, name);
            ps.setString(3, host);
            ps.setString(4, os);
            ps.setString(5, osv);
            ps.setString(6, version);
            ps.setString(7, ip);
            ps.setString(8, pub);
            ps.setString(9, normalizedHex);
            ps.setString(10, algo);
            ps.setString(11, alias);

            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new AuthException("register_machine_failed", "Falha ao registrar maquina");
                }
                return new MachineView(
                        rs.getLong("id"),
                        rs.getLong("user_id"),
                        rs.getString("machine_name"),
                        rs.getString("hostname"),
                        rs.getString("os_name"),
                        rs.getString("os_version"),
                        rs.getString("machine_alias"),
                        rs.getString("client_version"),
                        rs.getString("last_ip"),
                        rs.getString("public_key"),
                        rs.getString("hardware_hash_hex"),
                        rs.getString("hardware_hash_algo"),
                        String.valueOf(rs.getObject("registered_at")),
                        String.valueOf(rs.getObject("last_seen_at"))
                );
            }
        } catch (AuthException e) {
            throw e;
        } catch (Exception e) {
            throw new AuthException("db_error", safeMsg(e));
        }
    }

    public List<MachineView> listMachines(String token) {
        requireEnabled();
        SessionUser session = requireSession(token);

        String sql = """
                SELECT id, user_id, machine_name, hostname, os_name, os_version,
                       machine_alias, client_version, last_ip::text AS last_ip,
                       public_key, hardware_hash_hex, hardware_hash_algo,
                       registered_at, last_seen_at
                FROM keeply_machines
                WHERE user_id = ?
                ORDER BY id DESC
                """;

        List<MachineView> out = new ArrayList<>();
        try (Connection c = authDataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, session.id());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new MachineView(
                            rs.getLong("id"),
                            rs.getLong("user_id"),
                            rs.getString("machine_name"),
                            rs.getString("hostname"),
                            rs.getString("os_name"),
                            rs.getString("os_version"),
                            rs.getString("machine_alias"),
                            rs.getString("client_version"),
                            rs.getString("last_ip"),
                            rs.getString("public_key"),
                            rs.getString("hardware_hash_hex"),
                            rs.getString("hardware_hash_algo"),
                            String.valueOf(rs.getObject("registered_at")),
                            String.valueOf(rs.getObject("last_seen_at"))
                    ));
                }
            }
        } catch (AuthException e) {
            throw e;
        } catch (Exception e) {
            throw new AuthException("db_error", safeMsg(e));
        }
        return out;
    }

    public void deleteMachine(String token, long machineId) {
        requireEnabled();
        SessionUser session = requireSession(token);
        if (machineId <= 0) {
            throw new AuthException("bad_request", "machineId invalido");
        }

        String sql = "DELETE FROM keeply_machines WHERE id = ? AND user_id = ?";
        try (Connection c = authDataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, machineId);
            ps.setLong(2, session.id());
            int rows = ps.executeUpdate();
            if (rows == 0) {
                throw new AuthException("not_found", "Dispositivo nao encontrado");
            }
        } catch (AuthException e) {
            throw e;
        } catch (Exception e) {
            throw new AuthException("db_error", safeMsg(e));
        }
    }

    private boolean existsEmail(String email) {
        String sql = "SELECT 1 FROM keeply_users WHERE email = ? LIMIT 1";
        try (Connection c = authDataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, email);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (Exception e) {
            throw new AuthException("db_error", safeMsg(e));
        }
    }

    private Long findMachineOwnerByHardwareHash(String hardwareHashHex) {
        String sql = "SELECT user_id FROM keeply_machines WHERE hardware_hash = decode(?, 'hex') LIMIT 1";
        try (Connection c = authDataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, hardwareHashHex);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return rs.getLong("user_id");
            }
        } catch (Exception e) {
            throw new AuthException("db_error", safeMsg(e));
        }
    }

    private void validate(String email, String password) {
        if (email == null || email.isBlank()) {
            throw new AuthException("bad_request", "Informe email");
        }
        if (password == null || password.isBlank()) {
            throw new AuthException("bad_request", "Informe senha");
        }
        if (password.length() < 6) {
            throw new AuthException("bad_request", "Senha deve ter pelo menos 6 caracteres");
        }
    }

    private void requireEnabled() {
        if (authDataSource == null) {
            throw new AuthException("auth_disabled", "Auth Postgres nao configurado");
        }
    }

    private SessionUser requireSession(String token) {
        if (token == null || token.isBlank()) {
            throw new AuthException("unauthorized", "Token ausente");
        }
        SessionUser s = sessions.get(token.trim());
        if (s == null) {
            throw new AuthException("unauthorized", "Sessao invalida");
        }
        return s;
    }

    private String issueToken(long userId, String fullName, String email) {
        String token = UUID.randomUUID().toString();
        sessions.put(token, new SessionUser(userId, fullName, email, Instant.now()));
        return token;
    }

    private void ensureSchema() {
        if (authDataSource == null) {
            return;
        }

        String[] ddls = new String[] {
                "CREATE EXTENSION IF NOT EXISTS citext",
                "CREATE EXTENSION IF NOT EXISTS pgcrypto",
                """
                CREATE OR REPLACE FUNCTION set_updated_at()
                RETURNS trigger AS $$
                BEGIN
                  NEW.updated_at = now();
                  RETURN NEW;
                END;
                $$ LANGUAGE plpgsql
                """,
                """
                CREATE TABLE IF NOT EXISTS keeply_users (
                  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                  full_name VARCHAR(120) NOT NULL,
                  email CITEXT NOT NULL UNIQUE,
                  password_hash TEXT NOT NULL,
                  email_verified_at TIMESTAMPTZ,
                  disabled_at TIMESTAMPTZ,
                  last_login_at TIMESTAMPTZ,
                  password_changed_at TIMESTAMPTZ,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  CONSTRAINT keeply_users_full_name_nonempty CHECK (length(trim(full_name)) > 0),
                  CONSTRAINT keeply_users_password_hash_nonempty CHECK (length(trim(password_hash)) > 0),
                  CONSTRAINT keeply_users_disabled_after_created CHECK (disabled_at IS NULL OR disabled_at >= created_at)
                )
                """,
                "DROP TRIGGER IF EXISTS trg_keeply_users_updated_at ON keeply_users",
                """
                CREATE TRIGGER trg_keeply_users_updated_at
                BEFORE UPDATE ON keeply_users
                FOR EACH ROW EXECUTE FUNCTION set_updated_at()
                """,
                """
                CREATE TABLE IF NOT EXISTS keeply_machines (
                  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                  user_id BIGINT NOT NULL REFERENCES keeply_users(id) ON DELETE CASCADE,
                  machine_name VARCHAR(160) NOT NULL,
                  hostname VARCHAR(190),
                  os_name VARCHAR(120),
                  os_version VARCHAR(120),
                  client_version VARCHAR(60),
                  last_ip INET,
                  public_key TEXT,
                  hardware_hash BYTEA NOT NULL,
                  hardware_hash_algo TEXT NOT NULL DEFAULT 'sha256',
                  hardware_hash_hex TEXT GENERATED ALWAYS AS (encode(hardware_hash, 'hex')) STORED,
                  registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  CONSTRAINT keeply_machines_machine_name_nonempty CHECK (length(trim(machine_name)) > 0),
                  CONSTRAINT keeply_machines_hw_hash_len_sha256 CHECK (hardware_hash_algo <> 'sha256' OR length(hardware_hash) = 32)
                )
                """,
                "ALTER TABLE keeply_machines ADD COLUMN IF NOT EXISTS machine_alias CITEXT",
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_keeply_machines_hardware_hash ON keeply_machines(hardware_hash)",
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_keeply_machines_user_alias
                  ON keeply_machines(user_id, machine_alias)
                  WHERE machine_alias IS NOT NULL
                """,
                "CREATE INDEX IF NOT EXISTS idx_keeply_machines_user_id ON keeply_machines(user_id)",
                "CREATE INDEX IF NOT EXISTS idx_keeply_machines_user_last_seen ON keeply_machines(user_id, last_seen_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_keeply_machines_last_seen ON keeply_machines(last_seen_at DESC)",
                "DROP TRIGGER IF EXISTS trg_keeply_machines_updated_at ON keeply_machines",
                """
                CREATE TRIGGER trg_keeply_machines_updated_at
                BEFORE UPDATE ON keeply_machines
                FOR EACH ROW EXECUTE FUNCTION set_updated_at()
                """
        };

        try (Connection c = authDataSource.getConnection();
             Statement st = c.createStatement()) {
            for (String ddl : ddls) {
                st.execute(ddl);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao inicializar schema de auth: " + safeMsg(e), e);
        }
    }

    private String normalizeHardwareHashHex(String hardwareHashHex, String legacyMachineFingerprint) {
        String raw = (hardwareHashHex == null || hardwareHashHex.isBlank()) ? legacyMachineFingerprint : hardwareHashHex;
        if (raw == null || raw.isBlank()) {
            throw new AuthException("bad_request", "Informe hardwareHashHex");
        }
        String normalized = raw.trim().toLowerCase().replaceAll("[^0-9a-f]", "");
        try {
            HexFormat.of().parseHex(normalized);
            return normalized;
        } catch (Exception e) {
            throw new AuthException("bad_request", "hardwareHashHex invalido");
        }
    }

    private String safeMsg(Throwable t) {
        return (t == null || t.getMessage() == null || t.getMessage().isBlank())
                ? "erro"
                : t.getMessage();
    }

    public record AuthResult(String token, UserView user) {}
    public record UserView(long id, String name, String email) {}
    public record MachineView(
            long id,
            long userId,
            String machineName,
            String hostname,
            String osName,
            String osVersion,
            String machineAlias,
            String clientVersion,
            String lastIp,
            String publicKey,
            String hardwareHashHex,
            String hardwareHashAlgo,
            String registeredAt,
            String lastSeenAt
    ) {}
    private record SessionUser(long id, String name, String email, Instant createdAt) {}

    public static class AuthException extends RuntimeException {
        private final String code;

        public AuthException(String code, String message) {
            super(message);
            this.code = code;
        }

        public String code() {
            return code;
        }
    }
}
