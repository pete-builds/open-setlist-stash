-- setlist-stash schema, version 13.
--
-- Bind each magic-link token to the address it was emailed to.
--
-- WHY: ``auth_tokens`` recorded only the user. ``request_email_link`` writes
-- the submitted address onto ``users.email`` immediately and mints a token;
-- ``verify_token`` then stamped ``email_verified_at`` on whatever address the
-- row held at click time. A signed-in player could request a link for their
-- own inbox, submit a second address they do not control, and click the first
-- link: the second address came out verified on their account. From there the
-- Google resolver's verified-email match would attach the real owner's Google
-- identity to the attacker's row on the owner's first Google sign-in.
--
-- With the address on the token, verification only succeeds when the token's
-- address is still the address on the row. Tokens minted before this
-- migration have NULL here and are refused; they expire within 24h anyway.
--
-- Idempotent. Safe to apply on a v12 DB.

BEGIN;

ALTER TABLE auth_tokens ADD COLUMN IF NOT EXISTS email TEXT;

INSERT INTO schema_version (version) VALUES (13)
    ON CONFLICT (version) DO NOTHING;

COMMIT;
