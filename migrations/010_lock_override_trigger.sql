-- setlist-stash schema, version 10.
--
-- Lock guard honors lock_at_override. Idempotent. Safe to apply on a DB that
-- already ran migrations 001-009.
--
-- WHY: `prediction_locks.lock_at_override` is the documented operator escape
-- hatch for a show whose real downbeat differs from the computed default (a
-- co-bill where the headliner closes, a festival slot, a late curfew). Every
-- APPLICATION read honors it — `locks.get_or_create_lock` / `read_lock`,
-- `resolve.py`, and `web_helpers.py` all resolve the cutoff as
-- `COALESCE(lock_at_override, lock_at)`.
--
-- This trigger did not. It read the raw `lock_at` column, so the database
-- backstop kept enforcing the ORIGINAL cutoff after an operator moved it. The
-- two layers disagreed, and the failure was invisible in exactly the wrong
-- direction: the page rendered an open form with a live countdown off the
-- override, and then every submit was rejected with
-- `show % is locked (cutoff %)` naming a time the UI never showed. The
-- override has therefore never worked end to end since it was introduced —
-- setting one produced a game that looked open and silently refused picks.
--
-- Found live on 2026-08-15 (Umphrey's at Ravinia, a moe. co-bill where the
-- default 18:55 venue-local cutoff landed ~90 minutes before the band went
-- on). Reading the same effective cutoff the app reads is the whole fix.
--
-- The column-level logic from migration 002 is preserved verbatim: the guard
-- still blocks only writes that touch a user-controlled pick column, so the
-- resolver's post-lock `score` / `score_breakdown` writes continue to pass
-- through. The RAISE now reports the cutoff it actually enforced, so an
-- operator debugging a refusal sees the effective instant rather than a
-- superseded one.

BEGIN;

CREATE OR REPLACE FUNCTION reject_post_lock_predictions()
RETURNS TRIGGER AS $$
DECLARE
    lock_ts TIMESTAMPTZ;
    picks_changed BOOLEAN;
BEGIN
    -- The effective cutoff, matching every application-side read. An operator
    -- override always wins; NULL falls back to the computed lock_at.
    SELECT COALESCE(lock_at_override, lock_at) INTO lock_ts
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

INSERT INTO schema_version (version) VALUES (10)
    ON CONFLICT (version) DO NOTHING;

COMMIT;
