-- phish-game schema, version 3.
--
-- Phase 4b: optional magic-link email auth.
--
-- Adds:
--   - users.email_lower (case-insensitive uniqueness via partial unique index)
--     plus a unique index on lower(email) where email is not null.
--     (users.email + users.email_verified_at columns already exist from 001.)
--   - auth_tokens table for short-lived single-use magic-link tokens. Tokens
--     are stored as their SHA-256 hex digest; the plaintext lives only in the
--     emailed link and the user's inbox.
--
-- Idempotent. Safe to apply on a v2 DB.

BEGIN;

-- ---------------------------------------------------------------------------
-- users.email + email_verified_at landed in 001. We add the case-insensitive
-- uniqueness index here. We do NOT add a `email_lower` STORED generated
-- column because Postgres 16 supports functional indexes for this purpose
-- with no additional column. Equivalent semantics, smaller schema diff.
-- ---------------------------------------------------------------------------

-- Unique on email (raw form). 001 created `users.email TEXT` without a
-- UNIQUE constraint; add it now (partial so multiple NULLs are still
-- allowed for anonymous-only users).
CREATE UNIQUE INDEX IF NOT EXISTS users_email_unique_idx
    ON users (email)
    WHERE email IS NOT NULL;

-- Functional unique index for case-insensitive email lookup (the canonical
-- duplicate check is on `lower(email)`).
CREATE UNIQUE INDEX IF NOT EXISTS users_email_lower_unique_idx
    ON users (lower(email))
    WHERE email IS NOT NULL;

-- ---------------------------------------------------------------------------
-- auth_tokens — short-lived single-use tokens for magic-link flows.
--
-- Token plaintext is generated via secrets.token_urlsafe(32) and stored ONLY
-- as its SHA-256 hex digest. Plaintext is emailed to the user and lives in
-- the URL they click; it never touches the DB. consumed_at flips on first
-- use (single-use enforcement). expires_at is enforced at lookup time.
-- ip_first_seen records the verifier's IP for audit.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS auth_tokens (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    purpose TEXT NOT NULL CHECK (purpose IN ('email_verify', 'login')),
    token_hash TEXT NOT NULL UNIQUE,                -- sha256(plaintext) hex
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,                        -- single-use; NULL until verified
    ip_first_seen INET,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS auth_tokens_user_purpose_idx
    ON auth_tokens (user_id, purpose);

-- Speed up the "outstanding token" rate-limit query.
CREATE INDEX IF NOT EXISTS auth_tokens_outstanding_idx
    ON auth_tokens (user_id, purpose, expires_at)
    WHERE consumed_at IS NULL;

INSERT INTO schema_version (version) VALUES (3)
    ON CONFLICT (version) DO NOTHING;

COMMIT;
