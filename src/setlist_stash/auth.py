"""Anonymous handle auth.

Phase 4: a user picks a public handle (2-32 chars, A-Z 0-9 _ -). We create
a ``users`` row and set a signed cookie. No password, no email yet.

Phase 4b will add magic-link email.

The cookie is a single signed string — ``"<user_id>.<session_epoch>"``, signed
with ``itsdangerous.URLSafeTimedSerializer``. We don't put the handle in the
cookie because users can theoretically lose their handle if it's ever
moderated; the canonical lookup is by id.

Two things the bare ``URLSafeSerializer`` could not do, both of which started
mattering the day this went public:

* **Expiry.** The untimed serializer stamps no issue time, so its signature is
  valid forever and ``max_age`` on the cookie was only ever a request to the
  browser. A captured cookie outlived any plausible session. The timed
  serializer carries the timestamp inside the signed payload and the server
  enforces ``SESSION_MAX_AGE_DAYS`` on every request.
* **Revocation.** ``session_epoch`` (migration 012) is minted into the token and
  re-checked against the row. Bumping it drops every cookie already issued to
  that one user; previously the only lever was rotating SESSION_SECRET, which
  signs out the entire deployment.

Changing the payload format invalidates every cookie issued by an earlier
build: everyone is signed out once on the deploy that ships this. That is the
intended cost and it happens exactly once.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

import asyncpg
from fastapi import Request
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from setlist_stash.config import Settings

logger = logging.getLogger("setlist_stash.auth")

COOKIE_NAME = "phishgame_session"
# Browser-side hint only. The authoritative lifetime is the signed timestamp
# checked server-side against ``Settings.session_max_age_days``; this constant
# just stops the browser holding a cookie the server would refuse anyway, so
# the two are derived from the same number at call time (see deps.py).
COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 365
HANDLE_REGEX = re.compile(r"^[A-Za-z0-9_-]{2,32}$")
HANDLE_HELP = "2-32 characters: letters, digits, underscore, hyphen."


class HandleError(ValueError):
    """Invalid handle (format / taken / reserved)."""


@dataclass(frozen=True)
class CurrentUser:
    id: int
    handle: str
    session_epoch: int = 0


@dataclass(frozen=True)
class SessionToken:
    """What a valid session cookie decodes to."""

    user_id: int
    session_epoch: int


def _serializer(settings: Settings) -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(
        settings.session_secret.get_secret_value(), salt="setlist-stash-session"
    )


def sign_user_id(settings: Settings, user_id: int, session_epoch: int = 0) -> str:
    """Mint a session cookie value for ``user_id`` at ``session_epoch``.

    The epoch defaults to 0 to match a freshly inserted ``users`` row, so a
    caller that has not read the row back still mints a token that validates.
    """
    return _serializer(settings).dumps(f"{user_id}.{session_epoch}")


def unsign_session(settings: Settings, token: str) -> SessionToken | None:
    """Verify signature and age, returning the token's claims.

    ``None`` covers every rejection — forged, tampered, expired, malformed —
    because the caller's only correct response to any of them is the same:
    treat the request as signed out.
    """
    max_age = settings.session_max_age_days * 86400
    try:
        raw = _serializer(settings).loads(token, max_age=max_age)
    except SignatureExpired:
        logger.info("rejected expired session cookie")
        return None
    except BadSignature:
        return None
    user_id_str, _, epoch_str = str(raw).partition(".")
    try:
        return SessionToken(user_id=int(user_id_str), session_epoch=int(epoch_str))
    except (TypeError, ValueError):
        return None


def unsign_user_id(settings: Settings, token: str) -> int | None:
    """The user id from a valid session cookie, or None. Convenience wrapper."""
    claims = unsign_session(settings, token)
    return None if claims is None else claims.user_id


def validate_handle(handle: str) -> str:
    """Return the canonical (trimmed) handle or raise ``HandleError``."""
    handle = handle.strip()
    if not handle:
        raise HandleError("Handle cannot be empty.")
    if not HANDLE_REGEX.match(handle):
        raise HandleError(HANDLE_HELP)
    return handle


async def create_user(pool: asyncpg.Pool[Any], handle: str) -> int:
    """Insert a fresh users row. Caller has already validated."""
    canonical = validate_handle(handle)
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO users (handle, handle_lower)
                VALUES ($1, $2)
                RETURNING id
                """,
                canonical,
                canonical.lower(),
            )
        except asyncpg.UniqueViolationError as exc:
            raise HandleError(f"Handle '{canonical}' is already taken.") from exc
        if row is None:
            raise HandleError("Could not create user (no row returned).")
        return int(row["id"])


async def update_handle(
    pool: asyncpg.Pool[Any], user_id: int, new_handle: str
) -> str:
    """Rename a user's handle. Returns the canonical (trimmed) handle.

    Mirrors ``create_user``: validate, then update, translating a unique
    violation into a friendly ``HandleError``. Lets any signed-in user (handle,
    email, or Google) choose their own handle.
    """
    canonical = validate_handle(new_handle)
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                UPDATE users
                   SET handle = $2, handle_lower = $3
                 WHERE id = $1
                RETURNING id
                """,
                user_id,
                canonical,
                canonical.lower(),
            )
        except asyncpg.UniqueViolationError as exc:
            raise HandleError(f"Handle '{canonical}' is already taken.") from exc
        if row is None:
            raise HandleError("Could not update handle (user not found).")
    return canonical


async def get_user_by_id(
    pool: asyncpg.Pool[Any], user_id: int
) -> CurrentUser | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, handle, session_epoch FROM users WHERE id = $1", user_id
        )
    if row is None:
        return None
    return CurrentUser(
        id=int(row["id"]),
        handle=str(row["handle"]),
        session_epoch=int(row["session_epoch"]),
    )


async def revoke_sessions(pool: asyncpg.Pool[Any], user_id: int) -> int:
    """Invalidate every session cookie already issued to ``user_id``.

    Returns the new epoch. Only this user is affected; everyone else stays
    signed in. The caller is responsible for re-issuing a cookie at the new
    epoch if it wants to keep the *current* browser signed in.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE users
               SET session_epoch = session_epoch + 1
             WHERE id = $1
            RETURNING session_epoch
            """,
            user_id,
        )
    if row is None:
        raise LookupError(f"no user {user_id}")
    return int(row["session_epoch"])


async def touch_last_seen(pool: asyncpg.Pool[Any], user_id: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET last_seen_at = now() WHERE id = $1", user_id
        )


async def current_user(
    request: Request, pool: asyncpg.Pool[Any], settings: Settings
) -> CurrentUser | None:
    """Resolve the current user from the signed cookie. Returns None if absent."""
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return None
    claims = unsign_session(settings, token)
    if claims is None:
        logger.warning("rejected bad session cookie")
        return None
    user = await get_user_by_id(pool, claims.user_id)
    if user is None:
        return None
    if claims.session_epoch != user.session_epoch:
        # Signed and unexpired, but minted before a "sign out everywhere".
        logger.info("rejected revoked session cookie")
        return None
    await touch_last_seen(pool, claims.user_id)
    return user
