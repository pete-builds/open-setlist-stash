"""Signed, non-expiring unsubscribe links for reminder email.

A player who wants out of reminder mail is very often not signed in on the
device holding the message, and may not remember which handle the account
uses. Any unsubscribe that starts with "first, log in" is one they will not
complete: they will hit the spam button instead, which costs the sending
domain's reputation and takes the magic-link sign-in mail down with it. So the
link carries its own proof of who it is for.

Three deliberate differences from the session cookie in ``auth.py``:

* **Untimed.** ``URLSafeSerializer``, not ``URLSafeTimedSerializer``. An email
  from eight months ago must still unsubscribe correctly; a token that expires
  turns an old message into a dead link and, again, a spam report.
* **Its own salt.** Same ``SESSION_SECRET`` key material, different salt, so an
  unsubscribe token can never be replayed as a session cookie and vice versa
  even though both are minted from one secret.
* **Not tied to ``session_epoch``.** Signing every device out of an account must
  not silently invalidate the unsubscribe links already sitting in that
  person's inbox.

Rotating ``SESSION_SECRET`` does break every outstanding link. That is
acceptable and is the one lever that revokes them all at once; it is also why
the unsubscribe page always offers the signed-in route as a fallback.
"""

from __future__ import annotations

import logging
from typing import Any

import asyncpg
from itsdangerous import BadSignature, URLSafeSerializer

from setlist_stash.config import Settings

logger = logging.getLogger("setlist_stash.unsubscribe")

__all__ = [
    "is_opted_out",
    "mint_unsubscribe_token",
    "read_unsubscribe_token",
    "set_opt_out",
    "unsubscribe_url",
]

_SALT = "setlist-stash-unsubscribe"


def _serializer(settings: Settings) -> URLSafeSerializer:
    return URLSafeSerializer(
        settings.session_secret.get_secret_value(), salt=_SALT
    )


def mint_unsubscribe_token(settings: Settings, user_id: int) -> str:
    """Sign ``user_id`` into an opaque token for an unsubscribe URL."""
    return _serializer(settings).dumps(str(user_id))


def read_unsubscribe_token(settings: Settings, token: str) -> int | None:
    """Return the ``user_id`` a token proves, or None if it proves nothing.

    Every rejection collapses to None on purpose: forged, tampered, truncated
    by a mail client that wrapped the line, or minted under a rotated secret
    all lead the caller to the same place, which is a page saying "we could not
    read that link, here is how to unsubscribe while signed in".
    """
    try:
        raw = _serializer(settings).loads(token)
    except BadSignature:
        return None
    try:
        return int(str(raw))
    except (TypeError, ValueError):
        return None


def unsubscribe_url(settings: Settings, user_id: int) -> str:
    """The absolute one-click unsubscribe URL for ``user_id``.

    Absolute because it is going into an email: a relative path resolves
    against the mail client, where it means nothing. ``BASE_URL`` is per tenant,
    so the Wappy message links to wappypicks.com and the Tweezer one does not.
    """
    token = mint_unsubscribe_token(settings, user_id)
    return f"{settings.base_url.rstrip('/')}/email/unsubscribe?t={token}"


# ----- state ----------------------------------------------------------------


async def set_opt_out(
    pool: asyncpg.Pool[Any], user_id: int, *, opted_out: bool
) -> bool:
    """Set or clear ``users.email_opt_out_at``. Returns True if the row exists.

    Idempotent in both directions: unsubscribing twice keeps the FIRST
    timestamp rather than refreshing it, because the useful fact is when the
    person left, not when their mail client last prefetched the link.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE users
               SET email_opt_out_at = CASE
                       WHEN $2 THEN COALESCE(email_opt_out_at, now())
                       ELSE NULL
                   END
             WHERE id = $1
            RETURNING id
            """,
            user_id,
            opted_out,
        )
    return row is not None


async def is_opted_out(pool: asyncpg.Pool[Any], user_id: int) -> bool:
    """True when this user has unsubscribed from reminder mail."""
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            "SELECT email_opt_out_at FROM users WHERE id = $1", user_id
        )
    return value is not None
