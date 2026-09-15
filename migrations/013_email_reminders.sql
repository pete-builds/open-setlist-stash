-- setlist-stash schema, version 13.
--
-- Show-day pick reminders + the unsubscribe state they honor.
-- Idempotent. Safe on a DB that ran 001-012.
--
-- Two additions, and the second is the load-bearing one:
--
-- 1. ``users.email_opt_out_at``. NULL means subscribed; a timestamp means the
--    player unsubscribed and we never mail them a reminder again. Deliberately
--    a timestamp rather than a boolean so we can answer "when did they leave",
--    which is the question that actually comes up when someone reports mail
--    they did not expect. It does NOT gate magic-link / sign-in mail: that is
--    transactional, the user asked for it in the same second, and suppressing
--    it would lock people out of their own account.
--
-- 2. ``email_sends``. The reminder job is a loop in a restartable container,
--    so "did we already mail this person about this show" cannot live in
--    memory. The UNIQUE (user_id, kind, dedupe_key) is the whole safety story:
--    the job INSERTs the claim BEFORE it sends, and a conflicting insert means
--    somebody already has this message. Without it, a container restart at
--    09:00 mails the same 10 people twice, which is exactly the failure that
--    makes people unsubscribe.
--
-- Note the row is written pre-send and then updated with the outcome. A row
-- with status='failed' is retained rather than deleted so a repeated SMTP
-- outage is visible in the table instead of silently re-attempting forever.

BEGIN;

-- ---------------------------------------------------------------------------
-- users.email_opt_out_at
-- ---------------------------------------------------------------------------
ALTER TABLE users ADD COLUMN IF NOT EXISTS email_opt_out_at TIMESTAMPTZ;

-- ---------------------------------------------------------------------------
-- email_sends — one row per (person, message kind, thing the message is about)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS email_sends (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- Message family. 'pick_reminder' today; a future digest gets its own
    -- kind and its own dedupe namespace for free.
    kind TEXT NOT NULL,
    -- What this message was about. For 'pick_reminder' it is the show date in
    -- ISO form, so one reminder per player per show, forever.
    dedupe_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'sent', 'failed')),
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ,
    UNIQUE (user_id, kind, dedupe_key)
);

-- "Who did we mail about show X" — the query the operator runs when someone
-- says they got nothing.
CREATE INDEX IF NOT EXISTS email_sends_kind_key_idx
    ON email_sends (kind, dedupe_key);

INSERT INTO schema_version (version) VALUES (13)
    ON CONFLICT (version) DO NOTHING;

COMMIT;
