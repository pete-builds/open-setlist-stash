-- phish-game schema, version 7.
--
-- Phase 1 (Google SSO): optional "Sign in with Google" upgrade for the
-- anonymous handle identity.
--
-- Adds:
--   - users.google_sub — the Google OpenID Connect ``sub`` claim (the stable,
--     opaque per-user identifier). NULL for handle-only / email-only users.
--   - partial unique index so at most one users row can own a given google_sub
--     while still allowing many NULLs (anonymous + email-only players).
--
-- No data migration: existing rows keep google_sub NULL and are untouched.
-- Idempotent. Safe to apply on a v6 DB.

BEGIN;

-- Nullable so handle-only and email-only accounts stay valid.
ALTER TABLE users ADD COLUMN IF NOT EXISTS google_sub TEXT;

-- One Google account maps to at most one users row. Partial (WHERE NOT NULL)
-- so the many anonymous/email-only rows with a NULL google_sub don't collide.
CREATE UNIQUE INDEX IF NOT EXISTS users_google_sub_unique_idx
    ON users (google_sub)
    WHERE google_sub IS NOT NULL;

INSERT INTO schema_version (version) VALUES (7)
    ON CONFLICT (version) DO NOTHING;

COMMIT;
