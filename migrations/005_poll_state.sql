-- setlist-stash schema, version 5.
--
-- Setlist-completeness gate. Idempotent. Safe to apply on a DB that already
-- ran migrations 001-004.
--
-- Adds the `poll_state` table: durable per-show poll bookkeeping for the
-- resolver's completeness gate. Before this, the resolver scored a show on
-- the FIRST tick that returned ANY non-empty setlist and stamped
-- `prediction_locks.resolved_at` permanently. On a live show night that
-- scores everyone's encore pick against the end of Set 1 and locks the wrong
-- scores in forever. The gate now waits until the setlist looks final
-- (encore seen AND track count stable across N polls, or a time backstop)
-- before scoring.
--
-- This state MUST survive resolver restarts: a container bounce mid-show
-- can't be allowed to reset the stable-poll counter and re-arm a premature
-- score. Hence a durable table rather than in-memory counters or a JSONB
-- read-modify-write on `prediction_locks.summary` (which is a terminal audit
-- blob, overwritten on resolve/cancel). Columns mirror the phish-vault
-- `poll_state` table so the two platforms stay coherent.

BEGIN;

CREATE TABLE IF NOT EXISTS poll_state (
    show_date DATE PRIMARY KEY
        REFERENCES prediction_locks(show_date) ON DELETE CASCADE,
    -- Track count observed on the most recent poll. Used to detect growth.
    last_track_count INTEGER NOT NULL DEFAULT 0,
    -- TRUE once any poll has seen an Encore set. Latches: a later poll that
    -- (transiently) drops the encore can't un-see it.
    encore_seen BOOLEAN NOT NULL DEFAULT FALSE,
    -- Consecutive polls where last_track_count did not change. Reset to 0 the
    -- moment the count grows.
    stable_polls INTEGER NOT NULL DEFAULT 0,
    -- Latched once the completeness heuristic (or backstop) fires. Lets the
    -- resolver short-circuit and helps observability.
    complete BOOLEAN NOT NULL DEFAULT FALSE,
    last_polled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO schema_version (version) VALUES (5)
    ON CONFLICT (version) DO NOTHING;

COMMIT;
