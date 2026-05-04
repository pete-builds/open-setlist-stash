"""Anonymous handle auth.

Phase 4: a user picks a public handle (2-32 chars, A-Z 0-9 _ -). We create
a ``users`` row and set a signed cookie. No password, no email yet.

Phase 4b will add magic-link email.

The cookie is a single signed string — the user_id as ASCII digits, signed
with ``itsdangerous.URLSafeSerializer``. We don't put the handle in the
cookie because users can theoretically lose their handle if it's ever
moderated; the canonical lookup is by id.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

import asyncpg
from fastapi import Request
from itsdangerous import BadSignature, URLSafeSerializer

from setlist_stash.config import Settings

logger = logging.getLogger("setlist_stash.auth")

COOKIE_NAME = "phishgame_session"
# 365 days; cookies are LAN/Tailscale only through Phase 5.
COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 365
HANDLE_REGEX = re.compile(r"^[A-Za-z0-9_-]{2,32}$")
HANDLE_HELP = "2-32 characters: letters, digits, underscore, hyphen."


class HandleError(ValueError):
    """Invalid handle (format / taken / reserved)."""


@dataclass(frozen=True)
class CurrentUser:
    id: int
    handle: str


def _serializer(settings: Settings) -> URLSafeSerializer:
    return URLSafeSerializer(
        settings.session_secret.get_secret_value(), salt="setlist-stash-session"
    )


def sign_user_id(settings: Settings, user_id: int) -> str:
    return _serializer(settings).dumps(str(user_id))


def unsign_user_id(settings: Settings, token: str) -> int | None:
    try:
        raw = _serializer(settings).loads(token)
    except BadSignature:
        return None
    try:
        return int(str(raw))
    except (TypeError, ValueError):
        return None


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


async def get_user_by_id(
    pool: asyncpg.Pool[Any], user_id: int
) -> CurrentUser | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, handle FROM users WHERE id = $1", user_id
        )
    if row is None:
        return None
    return CurrentUser(id=int(row["id"]), handle=str(row["handle"]))


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
    user_id = unsign_user_id(settings, token)
    if user_id is None:
        logger.warning("rejected bad session cookie")
        return None
    user = await get_user_by_id(pool, user_id)
    if user is None:
        return None
    await touch_last_seen(pool, user_id)
    return user
