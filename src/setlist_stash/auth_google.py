"""Google OIDC (SSO) account linking.

Phase 1: an optional "Sign in with Google" upgrade for the anonymous handle
identity. Authlib (in the route layer) owns the OIDC discovery, the
``state``/``nonce`` round-trip, and — critically — the ``id_token`` signature
and claims verification against Google's JWKS, so we never hand-roll token
validation.

This module owns only the *account resolution*: mapping a verified Google
identity (``sub`` + ``email``) onto a setlist-stash ``users`` row without ever
stranding or duplicating an existing handle account. The resolution order in
``resolve_google_identity`` is the whole "don't break existing users" contract:

1. Caller already signed in (handle cookie present) -> link Google to THAT
   row, so the player keeps their handle, picks, scores, and leagues.
2. A row already owns this ``google_sub`` -> returning user, return it.
3. A row has a VERIFIED email matching Google's verified email and no
   ``google_sub`` -> same person who arrived earlier via magic-link; link it.
4. Otherwise create a fresh user with an auto-generated unique handle.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import asyncpg

from setlist_stash.auth import (
    CurrentUser,
    HandleError,
    create_user,
    validate_handle,
)

logger = logging.getLogger("setlist_stash.auth_google")

_HANDLE_MAX = 32
# Fallback stem when a Google email/name yields nothing handle-legal.
_FALLBACK_SEED = "phan"


class GoogleLinkConflict(ValueError):
    """Linking would collide with an existing account.

    Raised when a signed-in handle tries to adopt a Google account that already
    belongs to a *different* user, or when the current handle is already linked
    to a different Google account. Surfaced to the player as a friendly error.
    """


def _seed_to_base(seed: str) -> str:
    """Turn an arbitrary seed (email local-part or name) into a handle stem.

    Keeps only handle-legal characters (``A-Z a-z 0-9 _ -``), collapsing any
    run of other characters to a single hyphen, then trims stray separators and
    caps the length. Falls back to a fixed stem when nothing legal remains.
    """
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "-", (seed or "").strip())
    cleaned = cleaned.strip("-_")
    if len(cleaned) < 2:
        cleaned = _FALLBACK_SEED
    return cleaned[:_HANDLE_MAX]


async def generate_unique_handle(pool: asyncpg.Pool[Any], seed: str) -> str:
    """Return a valid handle derived from ``seed`` that is not yet taken.

    The first candidate is the cleaned seed itself; collisions get ``-2``,
    ``-3`` ... suffixes (trimming the stem so the whole handle stays within the
    32-char limit). This is a best-effort pre-check; ``create_user`` still
    enforces uniqueness at the DB via its unique-violation guard, so callers
    that insert should retry on ``HandleError``.
    """
    base = _seed_to_base(seed)
    try:
        base = validate_handle(base)
    except HandleError:
        base = _FALLBACK_SEED
    candidate = base
    n = 1
    async with pool.acquire() as conn:
        while True:
            taken = await conn.fetchval(
                "SELECT 1 FROM users WHERE handle_lower = $1", candidate.lower()
            )
            if not taken:
                return candidate
            n += 1
            suffix = f"-{n}"
            stem = base[: _HANDLE_MAX - len(suffix)]
            candidate = f"{stem}{suffix}"


async def _link_sub(
    conn: asyncpg.Connection[Any],
    *,
    user_id: int,
    google_sub: str,
    email: str | None,
    email_verified: bool,
) -> None:
    """Attach ``google_sub`` to a user row.

    Populates ``email`` / ``email_verified_at`` from Google ONLY when the row
    currently has no email — an existing address (verified or not) is never
    clobbered. The email is also skipped if another row already owns it, so we
    never trip the partial-unique email indexes.
    """
    email_to_set: str | None = None
    if email:
        clash = await conn.fetchval(
            "SELECT 1 FROM users WHERE lower(email) = lower($1) AND id <> $2 LIMIT 1",
            email,
            user_id,
        )
        if not clash:
            email_to_set = email
    await conn.execute(
        """
        UPDATE users
           SET google_sub = $2,
               email = COALESCE(email, $3),
               email_verified_at = CASE
                   WHEN email IS NULL AND $3 IS NOT NULL AND $4 THEN now()
                   ELSE email_verified_at
               END
         WHERE id = $1
        """,
        user_id,
        google_sub,
        email_to_set,
        email_verified,
    )


async def resolve_google_identity(
    pool: asyncpg.Pool[Any],
    *,
    google_sub: str,
    email: str | None,
    email_verified: bool,
    current: CurrentUser | None,
) -> int:
    """Resolve a verified Google identity to a ``users.id``. See module docs.

    Raises ``GoogleLinkConflict`` when the requested link collides with another
    account. Raises ``ValueError`` if ``google_sub`` is empty.
    """
    google_sub = (google_sub or "").strip()
    if not google_sub:
        raise ValueError("google_sub is required")
    norm_email = (email or "").strip().lower() or None

    async with pool.acquire() as conn:
        owner = await conn.fetchrow(
            "SELECT id FROM users WHERE google_sub = $1", google_sub
        )
        owner_id = int(owner["id"]) if owner is not None else None

        # --- Case 1: caller already signed in -> link to that handle row ---
        if current is not None:
            if owner_id is not None and owner_id != current.id:
                raise GoogleLinkConflict(
                    "That Google account is already linked to another handle."
                )
            if owner_id == current.id:
                return current.id  # already linked; idempotent
            row = await conn.fetchrow(
                "SELECT google_sub FROM users WHERE id = $1", current.id
            )
            existing_sub = row["google_sub"] if row is not None else None
            if existing_sub not in (None, google_sub):
                raise GoogleLinkConflict(
                    "This handle is already linked to a different Google account."
                )
            await _link_sub(
                conn,
                user_id=current.id,
                google_sub=google_sub,
                email=norm_email,
                email_verified=email_verified,
            )
            logger.info("linked google to existing handle", extra={"user_id": current.id})
            return current.id

        # --- Case 2: returning Google user ---
        if owner_id is not None:
            return owner_id

        # --- Case 3: verified-email match on an un-linked row ---
        if norm_email and email_verified:
            match = await conn.fetchrow(
                """
                SELECT id FROM users
                 WHERE lower(email) = $1
                   AND email_verified_at IS NOT NULL
                   AND google_sub IS NULL
                 LIMIT 1
                """,
                norm_email,
            )
            if match is not None:
                user_id = int(match["id"])
                await _link_sub(
                    conn,
                    user_id=user_id,
                    google_sub=google_sub,
                    email=norm_email,
                    email_verified=email_verified,
                )
                logger.info("linked google via verified email", extra={"user_id": user_id})
                return user_id

    # --- Case 4: brand-new user ---
    seed = norm_email.split("@", 1)[0] if norm_email else google_sub
    last_exc: HandleError | None = None
    for _ in range(5):
        handle = await generate_unique_handle(pool, seed)
        try:
            user_id = await create_user(pool, handle)
        except HandleError as exc:  # lost a race for the handle; retry
            last_exc = exc
            continue
        async with pool.acquire() as conn:
            await _link_sub(
                conn,
                user_id=user_id,
                google_sub=google_sub,
                email=norm_email,
                email_verified=email_verified,
            )
        logger.info("created new user via google", extra={"user_id": user_id})
        return user_id
    raise last_exc or HandleError("could not allocate a unique handle")
