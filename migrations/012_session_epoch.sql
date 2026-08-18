-- setlist-stash schema, version 12.
--
-- Add users.session_epoch. Idempotent. Safe on a DB that ran 001-011.
--
-- WHY: the session cookie is a signed user id and nothing else, so the only
-- fact the server could check was "is this signature ours". A cookie captured
-- from a shared machine, a synced browser profile, or a proxy log stayed valid
-- for its full 365-day lifetime and there was no way to end it. Rotating
-- SESSION_SECRET was the only lever and it signs out every user on the
-- deployment at once, so in practice nobody would ever pull it for one person.
--
-- The epoch is that missing lever, per user. It is minted into the cookie at
-- sign-in and re-checked on every request; bumping the column invalidates every
-- cookie already issued to that user while leaving everyone else signed in.
--
-- Starts at 0 so existing rows need no backfill and tests that sign a token
-- without consulting the DB still line up with a freshly inserted user.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS session_epoch INTEGER NOT NULL DEFAULT 0;
