-- phish-game schema, version 8.
--
-- Per-show comment threads. A single global thread per ``show_date`` for the
-- deployment (NOT per-league / per-user). Read-open to anyone; posting is
-- gated on having a handle (the same identity gate as making picks).
--
-- show_date design note:
--   ``predictions.show_date`` FKs to ``prediction_locks(show_date)`` because a
--   prediction only exists once the lock row is lazily created. Comments are
--   deliberately NOT FK'd to ``prediction_locks``: a thread must work on a show
--   that has zero predictions (no lock row yet), and creating a lock row as a
--   side effect of a comment would wrongly pull that date into the resolver's
--   scoring lifecycle. So we store a bare ``DATE`` and let the read path scope
--   by it. No parent row to cascade from; prediction_locks rows are never
--   deleted anyway. (This is the "simpler, don't FK to the lock table" branch
--   of the approved design.)
--
-- Idempotent. Safe to apply on a v7 DB.

BEGIN;

CREATE TABLE IF NOT EXISTS comments (
    id          BIGSERIAL PRIMARY KEY,
    show_date   DATE NOT NULL,                              -- ISO show date (vault key); no FK, see note above
    user_id     BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    body        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at  TIMESTAMPTZ,                                -- non-NULL = soft-deleted, hidden from reads
    CONSTRAINT comments_body_length CHECK (char_length(body) BETWEEN 1 AND 1000)
);

-- The read path lists a single show's live (non-deleted) comments in time
-- order, so a partial index on (show_date, created_at) filtered to live rows
-- is exactly the access pattern.
CREATE INDEX IF NOT EXISTS comments_show_date_created_idx
    ON comments (show_date, created_at)
    WHERE deleted_at IS NULL;

INSERT INTO schema_version (version) VALUES (8)
    ON CONFLICT (version) DO NOTHING;

COMMIT;
