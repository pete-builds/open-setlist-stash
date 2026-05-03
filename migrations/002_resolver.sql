-- phish-game schema, version 2.
--
-- Resolver session additions. Idempotent. Safe to apply on a DB that already
-- ran migration 001.
--
-- Adds:
--   - scoring_runs.shows_scanned (counter for how many open locks the
--     resolver inspected, regardless of whether they were resolvable yet)
--
-- Migration 001 already created the rest of the scoring_runs table and the
-- prediction_locks.summary column referenced by the resolver. Build session 2
-- only needs this single column to land cleanly on top of an existing v1 DB.

BEGIN;

ALTER TABLE scoring_runs
    ADD COLUMN IF NOT EXISTS shows_scanned INTEGER NOT NULL DEFAULT 0;

-- prediction_locks needs a summary JSONB column for cancelled-show sentinels
-- and per-resolve audit info ({"setlist_song_count": N, "predictions_scored": M}).
-- Migration 001 didn't include this; add it idempotently.
ALTER TABLE prediction_locks
    ADD COLUMN IF NOT EXISTS summary JSONB;

-- The session 1 lock-guard trigger fired on every INSERT or UPDATE regardless
-- of which columns changed. The resolver needs to write `score` and
-- `score_breakdown` AFTER lock_at — which is the whole point of post-lock
-- scoring. Tighten the guard to only block when one of the user-controlled
-- pick columns is touched. Score writes (resolver-only path) pass through.
CREATE OR REPLACE FUNCTION reject_post_lock_predictions()
RETURNS TRIGGER AS $$
DECLARE
    lock_ts TIMESTAMPTZ;
    picks_changed BOOLEAN;
BEGIN
    SELECT lock_at INTO lock_ts
        FROM prediction_locks
        WHERE show_date = NEW.show_date;
    IF lock_ts IS NULL OR now() <= lock_ts THEN
        RETURN NEW;
    END IF;
    -- Past lock. Only block if a pick column is being written. INSERT always
    -- counts as a pick write because TG_OP = 'INSERT' implies fresh picks.
    IF TG_OP = 'INSERT' THEN
        RAISE EXCEPTION 'show % is locked (cutoff %)', NEW.show_date, lock_ts
            USING ERRCODE = 'check_violation';
    END IF;
    -- UPDATE: compare each user-controlled pick column.
    picks_changed :=
        (NEW.pick_song_slugs IS DISTINCT FROM OLD.pick_song_slugs)
        OR (NEW.opener_slug IS DISTINCT FROM OLD.opener_slug)
        OR (NEW.closer_slug IS DISTINCT FROM OLD.closer_slug)
        OR (NEW.encore_slug IS DISTINCT FROM OLD.encore_slug);
    IF picks_changed THEN
        RAISE EXCEPTION 'show % is locked (cutoff %)', NEW.show_date, lock_ts
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

INSERT INTO schema_version (version) VALUES (2)
    ON CONFLICT (version) DO NOTHING;

COMMIT;
