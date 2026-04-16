-- ============================================================
-- AgencyMCP — Supabase SQL Migration
-- Run this in the Supabase SQL Editor to create all tables.
-- Matches SQLAlchemy models in database.py exactly.
-- ============================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
-- 1. USERS
-- ============================================================
CREATE TABLE IF NOT EXISTS users (
    id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    email       TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL DEFAULT '',
    role        TEXT NOT NULL DEFAULT 'viewer',           -- viewer | editor | admin
    is_active   BOOLEAN NOT NULL DEFAULT true,

    -- Encrypted tokens (Fernet AES-128, stored as base64 text)
    google_refresh_token_enc  TEXT,
    meta_access_token_enc     TEXT,
    meta_token_expires_at     TIMESTAMPTZ,

    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);

-- ============================================================
-- 2. OAUTH STATES  (CSRF protection for OAuth flows)
-- ============================================================
CREATE TABLE IF NOT EXISTS oauth_states (
    state       TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- ============================================================
-- 3. OAUTH CODES  (OAuth2 authorization code grant for Claude)
-- ============================================================
CREATE TABLE IF NOT EXISTS oauth_codes (
    code                    TEXT PRIMARY KEY,
    user_id                 TEXT,
    redirect_uri            TEXT NOT NULL,
    code_challenge          TEXT,
    code_challenge_method   TEXT,
    client_id               TEXT NOT NULL DEFAULT '',
    original_state          TEXT NOT NULL DEFAULT '',
    status                  TEXT NOT NULL DEFAULT 'pending',  -- pending | ready | used
    created_at              TIMESTAMPTZ DEFAULT now()
);

-- ============================================================
-- 4. SUBSCRIPTIONS  (billing & plan status per user)
-- ============================================================
CREATE TABLE IF NOT EXISTS subscriptions (
    id                          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    user_id                     TEXT NOT NULL,
    status                      TEXT NOT NULL DEFAULT 'trialing',  -- active | trialing | past_due | canceled | paused

    -- Airwallex payment IDs
    airwallex_customer_id       TEXT,
    airwallex_subscription_id   TEXT,

    -- Billing period
    trial_ends_at               TIMESTAMPTZ,
    current_period_start        TIMESTAMPTZ,
    current_period_end          TIMESTAMPTZ,
    canceled_at                 TIMESTAMPTZ,

    created_at                  TIMESTAMPTZ DEFAULT now(),
    updated_at                  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_user_id ON subscriptions (user_id);

-- ============================================================
-- 5. USER SESSIONS  (JWT session tracking & anti-sharing)
-- ============================================================
CREATE TABLE IF NOT EXISTS user_sessions (
    id              TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    user_id         TEXT NOT NULL,
    jti             TEXT NOT NULL UNIQUE,                  -- JWT ID
    ip_address      TEXT,
    user_agent_hash TEXT,                                  -- SHA-256 of User-Agent
    last_seen       TIMESTAMPTZ DEFAULT now(),
    created_at      TIMESTAMPTZ DEFAULT now(),
    revoked         BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON user_sessions (user_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_sessions_jti ON user_sessions (jti);

-- ============================================================
-- 6. AUDIT LOGS  (immutable record of every MCP tool call)
-- ============================================================
CREATE TABLE IF NOT EXISTS audit_logs (
    id               TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    user_id          TEXT NOT NULL,
    tool_name        TEXT NOT NULL,
    ip_address       TEXT,
    response_time_ms INTEGER,
    success          BOOLEAN NOT NULL DEFAULT true,
    error_message    TEXT,
    created_at       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id    ON audit_logs (user_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_tool_name  ON audit_logs (tool_name);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs (created_at);

-- ============================================================
-- Auto-update updated_at on users & subscriptions
-- ============================================================
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_subscriptions_updated_at
    BEFORE UPDATE ON subscriptions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ============================================================
-- Row Level Security (optional — enable if using Supabase Auth)
-- ============================================================
-- ALTER TABLE users ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE subscriptions ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE user_sessions ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE audit_logs ENABLE ROW LEVEL SECURITY;

-- ============================================================
-- Done. All 6 tables created with indexes and triggers.
-- ============================================================
