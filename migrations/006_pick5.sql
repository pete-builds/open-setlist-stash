-- 006_pick5.sql
-- Relax the pick_song_slugs cardinality CHECK from exactly 3 to 1..5 so a
-- prediction can carry up to five song picks (and a friend can pick fewer).
--
-- Migration-safe for existing rows: every prior row holds exactly 3 picks,
-- which falls inside BETWEEN 1 AND 5.
--
-- The inline CHECK in 001_initial.sql is auto-named by Postgres as
-- predictions_pick_song_slugs_check. We drop it by that name if present, and
-- defensively drop any other CHECK on the column that enforces the old
-- cardinality, then add the new named constraint.

DO $$
DECLARE
    con_name text;
BEGIN
    -- Drop the known auto-generated name if it exists.
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'predictions'::regclass
          AND conname = 'predictions_pick_song_slugs_check'
    ) THEN
        ALTER TABLE predictions
            DROP CONSTRAINT predictions_pick_song_slugs_check;
    END IF;

    -- Belt-and-suspenders: drop any remaining CHECK constraint whose source
    -- still enforces cardinality(pick_song_slugs) = 3.
    FOR con_name IN
        SELECT conname
          FROM pg_constraint
         WHERE conrelid = 'predictions'::regclass
           AND contype = 'c'
           AND pg_get_constraintdef(oid) ILIKE '%cardinality(pick_song_slugs) = 3%'
    LOOP
        EXECUTE format(
            'ALTER TABLE predictions DROP CONSTRAINT %I', con_name
        );
    END LOOP;
END
$$;

ALTER TABLE predictions
    ADD CONSTRAINT predictions_pick_song_slugs_check
    CHECK (cardinality(pick_song_slugs) BETWEEN 1 AND 5);

INSERT INTO schema_version (version) VALUES (6)
    ON CONFLICT (version) DO NOTHING;
