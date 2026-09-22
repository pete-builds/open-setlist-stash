-- setlist-stash schema, version 14.
--
-- Add prediction_locks.tour_season_key: the season bucket derived from the
-- UPSTREAM tour name rather than the show's calendar month. Idempotent, and
-- additive only, so it is safe to apply to live data.
--
-- WHY: rebuild_season bucketed purely on the month (Sep/Oct/Nov -> fall). The
-- upstream tour boundary does not respect month boundaries, so Phish's Dick's
-- Labor Day run (2026-09-04..06), which phish.net tags "2026 Summer Tour",
-- landed in 2026-fall and WAS the entire Fall Tour board until the real fall
-- tour started in October. A tab pinned to the fall bucket therefore showed
-- the wrong run while looking perfectly healthy.
--
-- NULL means "no usable tour information for this show", not "not computed
-- yet after the backfill": rebuild_season COALESCEs a NULL back to the same
-- month expression it has always used. That fallback is load-bearing, and not
-- only for history. The Umphrey's tenant's MCP returns the placeholder
-- "No Tour Name" for every show, so on that deployment every row stays NULL
-- and the season boards keep behaving exactly as they did before.
--
-- The key SHAPE is deliberately unchanged (YYYY-<season>). Switching to raw
-- upstream tour slugs would have re-keyed every existing bucket and silently
-- emptied the tour tabs configured in LEADERBOARD_TABS, which name scope_keys
-- like '2026-summer'.

BEGIN;

ALTER TABLE prediction_locks
    ADD COLUMN IF NOT EXISTS tour_season_key TEXT;

INSERT INTO schema_version (version) VALUES (14)
    ON CONFLICT (version) DO NOTHING;

COMMIT;
