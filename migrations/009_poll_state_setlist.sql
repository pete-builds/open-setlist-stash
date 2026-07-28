-- setlist-stash schema, version 9.
--
-- Setlist snapshot on poll_state. Idempotent. Safe to apply on a DB that
-- already ran migrations 001-008.
--
-- WHY: the live show page used to render its setlist from a fresh mcp-phish
-- `get_show` at request time, while the standings next to it came from
-- `predictions.score`, written by the resolver on its own (slower) tick. Two
-- sources on two clocks, so a viewer could see a song in the setlist that the
-- scores did not yet reflect, and the gap widened with the resolver interval.
--
-- The resolver already fetches the setlist it scores against. Persisting that
-- exact list here makes the page render the setlist and the scores from the
-- SAME resolver tick, so they can never disagree. It also takes the per-render
-- upstream call off the page: viewers no longer generate load on phish.net /
-- allthings.umphreys.com, so client refresh cadence is decoupled from upstream
-- politeness.
--
-- Nullable + fail-soft by design: a show with no snapshot (anything resolved
-- before this migration, or a show whose first tick has not run yet) falls
-- back to the old live-read path in the route.

BEGIN;

ALTER TABLE poll_state
    ADD COLUMN IF NOT EXISTS setlist_json JSONB;

-- When the snapshot above was taken AND scored. This is the "as of" the page
-- shows, and it is the same instant the scores in `predictions.score` were
-- computed for.
ALTER TABLE poll_state
    ADD COLUMN IF NOT EXISTS scored_at TIMESTAMPTZ;

INSERT INTO schema_version (version) VALUES (9)
    ON CONFLICT (version) DO NOTHING;

COMMIT;
