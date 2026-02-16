CREATE SCHEMA IF NOT EXISTS auth;

CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA auth;

SET search_path TO auth, public;

-- trigger padrão
CREATE OR REPLACE FUNCTION auth.set_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- tabela de usuários
CREATE TABLE IF NOT EXISTS auth.keeply_users (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

  full_name TEXT NOT NULL,
  email CITEXT NOT NULL UNIQUE,

  password_hash TEXT NOT NULL,

  email_verified_at TIMESTAMPTZ,
  disabled_at TIMESTAMPTZ,

  last_login_at TIMESTAMPTZ,
  password_changed_at TIMESTAMPTZ,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT keeply_users_full_name_nonempty
    CHECK (length(trim(full_name)) > 0),

  CONSTRAINT keeply_users_password_hash_nonempty
    CHECK (length(trim(password_hash)) > 0),

  CONSTRAINT keeply_users_disabled_after_created
    CHECK (disabled_at IS NULL OR disabled_at >= created_at),

  CONSTRAINT keeply_users_email_format
    CHECK (position('@' in email) > 1)
);

DROP TRIGGER IF EXISTS trg_keeply_users_updated_at
ON auth.keeply_users;

CREATE TRIGGER trg_keeply_users_updated_at
BEFORE UPDATE ON auth.keeply_users
FOR EACH ROW
EXECUTE FUNCTION auth.set_updated_at();

-- tabela de sessões
CREATE TABLE IF NOT EXISTS auth.keeply_sessions (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

  user_id BIGINT NOT NULL
    REFERENCES auth.keeply_users(id)
    ON DELETE CASCADE,

  refresh_token_hash BYTEA NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ,

  last_used_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ip INET,
  user_agent TEXT,

  CONSTRAINT keeply_sessions_hash_len
    CHECK (length(refresh_token_hash) = 32),

  CONSTRAINT keeply_sessions_revoked_after_created
    CHECK (revoked_at IS NULL OR revoked_at >= created_at)
);

-- índices
CREATE UNIQUE INDEX IF NOT EXISTS uq_keeply_sessions_refresh_token_hash
ON auth.keeply_sessions(refresh_token_hash);

CREATE INDEX IF NOT EXISTS idx_keeply_sessions_user_id
ON auth.keeply_sessions(user_id);

CREATE INDEX IF NOT EXISTS idx_keeply_sessions_expires_at
ON auth.keeply_sessions(expires_at);

CREATE INDEX IF NOT EXISTS idx_keeply_sessions_active
ON auth.keeply_sessions(user_id, expires_at)
WHERE revoked_at IS NULL;
