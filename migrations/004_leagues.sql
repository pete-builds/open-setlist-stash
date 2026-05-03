-- phish-game schema, version 4.
--
-- Phase 4c: private leagues.
--
-- Adds two tables (leagues, league_members) and a fourth scope ("league")
-- to the existing leaderboard_snapshots check constraint. Predictions are
-- still global -- one prediction per user per show -- but each prediction
-- now scores in every league the user belongs to (in addition to global).
--
-- Idempotent. Safe to apply on a v3 DB.

BEGIN;

-- ---------------------------------------------------------------------------
-- leagues -- private prediction leagues. Discovery is by URL share only;
-- there is no public directory in Phase 4c.
--
-- - slug: 6+ char readable URL token (e.g. "tweezer-7k"). Doubles as the
--   invite. Anyone with the URL can join. Hosts can rotate the slug; on
--   rotate the old slug stops resolving immediately, but existing members
--   keep their membership.
-- - host_user_id: the league creator. host can't leave (must transfer or
--   delete; host transfer is deferred to v2).
-- - member_cap: soft cap, default 500 (also configurable via env). Enforced
--   at join time.
-- - start_date / end_date: optional tour window. When set, the league
--   leaderboard only counts shows whose date falls inside [start, end]
--   inclusive. NULL = score every show forever.
-- - deleted_at: soft delete. Predictions are NOT cascade-deleted; they
--   still count globally. The league simply disappears from member views.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS leagues (
    id              BIGSERIAL PRIMARY KEY,
    slug            TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    host_user_id    BIGINT NOT NULL REFERENCES users(id),
    member_cap      INTEGER NOT NULL DEFAULT 500,
    start_date      DATE,
    end_date        DATE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at      TIMESTAMPTZ,
    CONSTRAINT leagues_name_length CHECK (char_length(name) BETWEEN 1 AND 80),
    CONSTRAINT leagues_member_cap_positive CHECK (member_cap > 0),
    CONSTRAINT leagues_date_window CHECK (
        start_date IS NULL OR end_date IS NULL OR start_date <= end_date
    )
);

CREATE INDEX IF NOT EXISTS leagues_host_idx
    ON leagues (host_user_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS leagues_slug_active_idx
    ON leagues (slug) WHERE deleted_at IS NULL;

-- ---------------------------------------------------------------------------
-- league_members -- many-to-many. The host is also a member (role='host').
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS league_members (
    league_id   BIGINT NOT NULL REFERENCES leagues(id) ON DELETE CASCADE,
    user_id     BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role        TEXT NOT NULL CHECK (role IN ('host','member')),
    joined_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (league_id, user_id)
);
CREATE INDEX IF NOT EXISTS league_members_user_idx
    ON league_members (user_id);

-- ---------------------------------------------------------------------------
-- Add the 'league' scope to leaderboard_snapshots' CHECK constraint.
--
-- 001 declared CHECK (scope IN ('weekly','tour','all_time')). We need a
-- fourth scope without losing the existing rows. Drop + recreate the
-- constraint with the wider list. Postgres names the constraint
-- ``leaderboard_snapshots_scope_check`` by convention, but if 001 happened
-- to land under a different name we look it up dynamically.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    cname text;
BEGIN
    SELECT conname INTO cname
      FROM pg_constraint
     WHERE conrelid = 'leaderboard_snapshots'::regclass
       AND contype  = 'c'
       AND pg_get_constraintdef(oid) ILIKE '%scope%';
    IF cname IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE leaderboard_snapshots DROP CONSTRAINT %I',
            cname
        );
    END IF;
END
$$;

ALTER TABLE leaderboard_snapshots
    ADD CONSTRAINT leaderboard_snapshots_scope_check
        CHECK (scope IN ('weekly','tour','all_time','league'));

INSERT INTO schema_version (version) VALUES (4)
    ON CONFLICT (version) DO NOTHING;

COMMIT;
