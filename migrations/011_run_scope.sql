-- setlist-stash schema, version 11.
--
-- Add the 'run' scope to leaderboard_snapshots' CHECK constraint. Idempotent.
-- Safe to apply on a DB that already ran migrations 001-010.
--
-- WHY: a "run" is a named set of show dates that earns its own board — a
-- residency, a festival, a venue stand (e.g. the five-night MSG summer run).
-- Which runs exist is deployment config (LEADERBOARD_RUNS), never code, since
-- a Phish residency means nothing on an Umphrey's deployment. The scope_key is
-- the run's slug.
--
-- 004 widened the constraint from 001's ('weekly','tour','all_time') to add
-- 'league'. Same drop-and-recreate dance here, and the same dynamic lookup of
-- the constraint name in case it did not land under the conventional one.

BEGIN;

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
        CHECK (scope IN ('weekly','tour','all_time','league','run'));

INSERT INTO schema_version (version) VALUES (11)
    ON CONFLICT (version) DO NOTHING;

COMMIT;
