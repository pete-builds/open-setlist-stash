-- phish-game schema, version 1.
--
-- Game-side state ONLY. Vault data (shows, songs, venues, gaps) is read via
-- the mcp-phish HTTP endpoint. Never join across these databases.
--
-- Naming convention: snake_case tables, plural. Surrogate `id` PKs unless
-- a natural key is unambiguous. All timestamps are TIMESTAMPTZ.

BEGIN;

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- users — anonymous handle by default. Magic-link email is Phase 4b.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    handle TEXT NOT NULL UNIQUE,                -- public display name
    handle_lower TEXT NOT NULL UNIQUE,          -- case-insensitive lookup
    email TEXT,                                 -- NULL until verified (Phase 4b)
    email_verified_at TIMESTAMPTZ,              -- non-NULL = magic-link confirmed
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT users_handle_format CHECK (handle ~ '^[A-Za-z0-9_-]{2,32}$')
);

-- ---------------------------------------------------------------------------
-- prediction_locks — per-show showtime cutoff. Created lazily when the first
-- prediction comes in for a given show. Override with `lock_at_override` if
-- the venue tz / start time deviates from the default.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS prediction_locks (
    show_date DATE PRIMARY KEY,                 -- ISO show date (vault key)
    show_id TEXT,                               -- mcp-phish show id, denormalized
    lock_at TIMESTAMPTZ NOT NULL,               -- effective cutoff (UTC)
    lock_at_override TIMESTAMPTZ,               -- non-NULL = manual override
    venue_tz TEXT,                              -- e.g. America/New_York; NULL = default
    resolved_at TIMESTAMPTZ,                    -- non-NULL = setlist scored
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS prediction_locks_resolved_at_idx
    ON prediction_locks(resolved_at)
    WHERE resolved_at IS NULL;

-- ---------------------------------------------------------------------------
-- predictions — one row per (user, show). The picks are stored as ordered
-- arrays of song slugs. We store slugs (not foreign keys) because songs live
-- in the vault, not here. The resolve job validates slugs against mcp-phish.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS predictions (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    show_date DATE NOT NULL REFERENCES prediction_locks(show_date),
    -- The three "any-set" picks. Order is irrelevant for scoring; keep
    -- sorted at write time for deterministic uniqueness checks.
    pick_song_slugs TEXT[] NOT NULL CHECK (cardinality(pick_song_slugs) = 3),
    -- Slot picks. NULL is allowed (user can skip a slot for partial score).
    opener_slug TEXT,
    closer_slug TEXT,
    encore_slug TEXT,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Score lives here once the show resolves. NULL until then.
    score INTEGER,
    score_breakdown JSONB,                      -- {"rarity": ..., "slot": ...}
    UNIQUE (user_id, show_date)
);

CREATE INDEX IF NOT EXISTS predictions_show_date_idx ON predictions(show_date);
CREATE INDEX IF NOT EXISTS predictions_user_id_idx ON predictions(user_id);

-- Hard rule: no writes after lock. Enforced via trigger so application bugs
-- can't slip a late prediction in.
CREATE OR REPLACE FUNCTION reject_post_lock_predictions()
RETURNS TRIGGER AS $$
DECLARE
    lock_ts TIMESTAMPTZ;
BEGIN
    SELECT lock_at INTO lock_ts
        FROM prediction_locks
        WHERE show_date = NEW.show_date;
    IF lock_ts IS NOT NULL AND now() > lock_ts THEN
        RAISE EXCEPTION 'show % is locked (cutoff %)', NEW.show_date, lock_ts
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS predictions_lock_guard ON predictions;
CREATE TRIGGER predictions_lock_guard
    BEFORE INSERT OR UPDATE ON predictions
    FOR EACH ROW
    EXECUTE FUNCTION reject_post_lock_predictions();

-- ---------------------------------------------------------------------------
-- scoring_runs — audit table mirroring phish-vault's etl_runs. Every
-- resolve-cron invocation writes one row.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS scoring_runs (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'success', 'partial', 'error', 'noop')),
    shows_resolved INTEGER NOT NULL DEFAULT 0,
    predictions_scored INTEGER NOT NULL DEFAULT 0,
    summary JSONB,                              -- per-show counts, errors
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS scoring_runs_started_at_idx
    ON scoring_runs(started_at DESC);

-- ---------------------------------------------------------------------------
-- leaderboard_snapshots — materialized leaderboards for cheap reads.
-- Refreshed after each scoring_run. Three scopes: 'weekly', 'tour', 'all_time'.
-- The `scope_key` is the bucket id (e.g. '2026-W19' for weekly, 'fall-2026'
-- for tour, 'all' for all-time). One row per (scope, scope_key, user).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS leaderboard_snapshots (
    id BIGSERIAL PRIMARY KEY,
    scope TEXT NOT NULL CHECK (scope IN ('weekly', 'tour', 'all_time')),
    scope_key TEXT NOT NULL,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    handle TEXT NOT NULL,                       -- denormalized for cheap reads
    total_score INTEGER NOT NULL,
    shows_played INTEGER NOT NULL,
    rank INTEGER NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (scope, scope_key, user_id)
);

CREATE INDEX IF NOT EXISTS leaderboard_scope_rank_idx
    ON leaderboard_snapshots(scope, scope_key, rank);

-- ---------------------------------------------------------------------------
-- Stamp version 1.
-- ---------------------------------------------------------------------------
INSERT INTO schema_version (version) VALUES (1)
    ON CONFLICT (version) DO NOTHING;

COMMIT;
