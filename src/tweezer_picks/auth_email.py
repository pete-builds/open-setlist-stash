"""Magic-link email auth flows.

Phase 4b: optional upgrade for an anonymous handle to a verified email
address. Two flows share the same token table:

- ``email_verify``: signed-in user adds their email; click confirms it.
  Sets ``users.email`` + ``users.email_verified_at``.
- ``login``: NOT-signed-in user enters their email; click sets the session
  cookie to the matching ``users.id``. Cross-browser carry-over.

Tokens:

- 256-bit URL-safe random via ``secrets.token_urlsafe(32)``.
- DB stores SHA-256 hex of the token only — plaintext lives only in the
  emailed link and the user's inbox.
- Single-use: ``consumed_at`` flips on first verify; subsequent uses fail.
- 24h TTL by default; configurable via ``magic_link_ttl_hours``.
- Max 3 outstanding tokens per (user, purpose); oldest get expired before
  a new one is minted.

The email sender is injected as a provider (see ``email.py``); routes get
to stay transport-agnostic.
"""

from __future__ import annotations

import hashlib
import logging
import re
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from tweezer_picks.auth import CurrentUser
from tweezer_picks.config import Settings
from tweezer_picks.email import EmailProvider, EmailSendError

logger = logging.getLogger("tweezer_picks.auth_email")


# Cheap email regex per the plan: don't try to be perfect. RFC-5322 is a
# rabbit hole; we only block obvious garbage. Real validation = "the user
# clicked the link in their inbox".
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_EMAIL_MAX_LEN = 320  # RFC-5321 hard cap; emails longer than this are spam


class EmailFormatError(ValueError):
    """The submitted address is not a plausible email."""


class EmailTakenError(ValueError):
    """That email is already attached to another verified account."""


@dataclass(frozen=True)
class VerifyResult:
    """Outcome of a successful ``verify_*_link`` call."""

    user_id: int
    handle: str
    purpose: str  # 'email_verify' or 'login'
    email: str | None  # the address now attached to the user (post-verify)


def validate_email(raw: str) -> str:
    """Return a normalized email or raise ``EmailFormatError``.

    Lowercase + strip; cheap regex check. Anything past this is the
    inbox-click test.
    """
    s = (raw or "").strip().lower()
    if not s:
        raise EmailFormatError("Email cannot be empty.")
    if len(s) > _EMAIL_MAX_LEN:
        raise EmailFormatError("Email is too long.")
    if not _EMAIL_RE.match(s):
        raise EmailFormatError(
            "That doesn't look like an email. Format: user@example.com."
        )
    return s


def _hash_token(token: str) -> str:
    """SHA-256 hex digest. Never store the plaintext."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _now_utc() -> datetime:
    """Wallclock UTC. Pulled out so tests can monkeypatch this module."""
    return datetime.now(UTC)


def generate_token() -> str:
    """Cryptographically random URL-safe token. ~43 chars from 32 bytes."""
    return secrets.token_urlsafe(32)


def build_magic_link(base_url: str, token: str) -> str:
    """Build the full /auth/verify URL the user receives by email."""
    base = (base_url or "").rstrip("/")
    return f"{base}/auth/verify?token={token}"


def render_email_verify_body(
    *, handle: str, link: str, ttl_hours: int
) -> tuple[str, str]:
    """Return (subject, body) for an email_verify message."""
    subject = "Confirm your tweezer-picks email"
    body = (
        f"Hi {handle},\n\n"
        f"Click this link to attach this email to your tweezer-picks handle:\n\n"
        f"  {link}\n\n"
        f"The link expires in {ttl_hours} hours and can only be used once.\n\n"
        f"If you didn't request this, ignore this email.\n"
    )
    return subject, body


def render_login_body(*, link: str, ttl_hours: int) -> tuple[str, str]:
    """Return (subject, body) for a cross-browser login message."""
    subject = "Sign in to tweezer-picks"
    body = (
        f"Click this link to sign in to tweezer-picks:\n\n"
        f"  {link}\n\n"
        f"The link expires in {ttl_hours} hours and can only be used once.\n\n"
        f"If you didn't request this, ignore this email.\n"
    )
    return subject, body


async def _expire_outstanding(
    conn: asyncpg.Connection[Any],
    *,
    user_id: int,
    purpose: str,
    keep: int,
) -> int:
    """Expire un-consumed un-expired tokens past the rate-limit cap.

    Returns count of rows expired. Idempotent.
    """
    # Sort outstanding by created_at DESC, keep the first `keep-1` (we're
    # about to mint one more), expire the rest.
    rows = await conn.fetch(
        """
        SELECT id FROM auth_tokens
         WHERE user_id = $1
           AND purpose = $2
           AND consumed_at IS NULL
           AND expires_at > now()
         ORDER BY created_at DESC
        """,
        user_id,
        purpose,
    )
    overflow = rows[max(keep - 1, 0):]
    if not overflow:
        return 0
    ids = [r["id"] for r in overflow]
    # Mark them consumed (single-use semantics) so they can never be
    # exchanged for a session.
    await conn.execute(
        """
        UPDATE auth_tokens
           SET consumed_at = now()
         WHERE id = ANY($1::bigint[])
        """,
        ids,
    )
    return len(ids)


async def request_email_link(
    pool: asyncpg.Pool[Any],
    *,
    user: CurrentUser,
    email: str,
    settings: Settings,
    provider: EmailProvider,
) -> str:
    """Verify-email flow.

    Validates the address, mints a token, attaches the email to the user
    (without flipping email_verified_at — that happens on verify), and
    sends the magic link.

    Returns the masked email (e.g. ``p******@gmail.com``) for the
    "we emailed you" page.

    Raises:
      - ``EmailFormatError`` for invalid input
      - ``EmailTakenError`` if another VERIFIED user already owns the email
      - ``EmailSendError`` if the provider fails (don't expose token in DB)
    """
    canonical = validate_email(email)
    ttl = timedelta(hours=settings.magic_link_ttl_hours)
    expires_at = _now_utc() + ttl
    token = generate_token()
    token_hash = _hash_token(token)

    async with pool.acquire() as conn, conn.transaction():
        # Refuse if a DIFFERENT verified user already owns this email.
        # Unverified ownership is fine to clobber: the rightful owner has
        # 24h to claim it via their inbox.
        clash = await conn.fetchrow(
            """
            SELECT id FROM users
             WHERE lower(email) = $1
               AND email_verified_at IS NOT NULL
               AND id <> $2
             LIMIT 1
            """,
            canonical,
            user.id,
        )
        if clash is not None:
            raise EmailTakenError(
                "That email is already attached to another verified handle."
            )
        # Attach the email (no verification yet). If the user is changing
        # their email mid-flow, this overwrites the previous unverified one
        # and we expire any older outstanding tokens.
        await conn.execute(
            """
            UPDATE users
               SET email = $2,
                   email_verified_at = CASE
                       WHEN lower(COALESCE(email, '')) = $3 THEN email_verified_at
                       ELSE NULL
                   END
             WHERE id = $1
            """,
            user.id,
            canonical,
            canonical,
        )
        # Rate-limit: max N outstanding for (user_id, 'email_verify').
        await _expire_outstanding(
            conn,
            user_id=user.id,
            purpose="email_verify",
            keep=settings.magic_link_max_outstanding,
        )
        await conn.execute(
            """
            INSERT INTO auth_tokens
                (user_id, purpose, token_hash, expires_at)
            VALUES ($1, 'email_verify', $2, $3)
            """,
            user.id,
            token_hash,
            expires_at,
        )

    # Send the email AFTER the DB commit so a transient SMTP failure
    # doesn't leave us with a dangling unverified address. If send fails
    # the user can retry; the row update above is harmless.
    link = build_magic_link(settings.base_url, token)
    subject, body = render_email_verify_body(
        handle=user.handle, link=link, ttl_hours=settings.magic_link_ttl_hours
    )
    try:
        await provider.send(to=canonical, subject=subject, body=body)
    except EmailSendError:
        raise

    return mask_email(canonical)


async def request_login_link(
    pool: asyncpg.Pool[Any],
    *,
    email: str,
    settings: Settings,
    provider: EmailProvider,
) -> str:
    """Cross-browser login flow.

    Looks up the user by verified email; mints a 'login'-purpose token;
    sends the link. Returns the masked email regardless of whether a user
    exists (no enumeration via response shape — only the inbox owner can
    distinguish).

    Raises ``EmailFormatError`` on bad input. ``EmailSendError`` propagates
    from the provider.
    """
    canonical = validate_email(email)
    ttl = timedelta(hours=settings.magic_link_ttl_hours)
    expires_at = _now_utc() + ttl

    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            """
            SELECT id, handle FROM users
             WHERE lower(email) = $1
               AND email_verified_at IS NOT NULL
             LIMIT 1
            """,
            canonical,
        )
        if row is None:
            # No verified user with that email. Don't leak; pretend we sent
            # a link. Real users see the link in their inbox; attackers
            # can't enumerate verified accounts.
            logger.info(
                "login_link request for unknown email (silently dropped)"
            )
            return mask_email(canonical)
        user_id = int(row["id"])
        token = generate_token()
        token_hash = _hash_token(token)
        await _expire_outstanding(
            conn,
            user_id=user_id,
            purpose="login",
            keep=settings.magic_link_max_outstanding,
        )
        await conn.execute(
            """
            INSERT INTO auth_tokens
                (user_id, purpose, token_hash, expires_at)
            VALUES ($1, 'login', $2, $3)
            """,
            user_id,
            token_hash,
            expires_at,
        )

    link = build_magic_link(settings.base_url, token)
    subject, body = render_login_body(
        link=link, ttl_hours=settings.magic_link_ttl_hours
    )
    try:
        await provider.send(to=canonical, subject=subject, body=body)
    except EmailSendError:
        raise
    return mask_email(canonical)


async def verify_token(
    pool: asyncpg.Pool[Any],
    *,
    token: str,
    ip: str | None = None,
) -> VerifyResult:
    """Consume a magic-link token (either purpose).

    Single-use + expiry enforced. On success:
      - For 'email_verify': stamps users.email_verified_at = now().
      - For 'login': leaves email_verified_at as-is (user must already be
        verified — request_login_link only mints for verified users).
      - In both cases: stamps consumed_at and ip_first_seen.

    Raises ``LookupError`` if the token is unknown / expired / consumed.
    """
    if not token or not isinstance(token, str):
        raise LookupError("Empty token.")
    token_hash = _hash_token(token)

    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            """
            SELECT t.id, t.user_id, t.purpose, t.expires_at, t.consumed_at,
                   u.handle, u.email
              FROM auth_tokens t
              JOIN users u ON u.id = t.user_id
             WHERE t.token_hash = $1
             FOR UPDATE OF t
            """,
            token_hash,
        )
        if row is None:
            raise LookupError("Invalid token.")
        if row["consumed_at"] is not None:
            raise LookupError("Token has already been used.")
        # Compare against DB's now() so we don't disagree with the trigger
        # layer's clock if the app and Postgres run in different containers.
        now_db = await conn.fetchval("SELECT now()")
        if not isinstance(now_db, datetime):
            raise RuntimeError("could not read DB now()")
        if now_db.tzinfo is None:
            now_db = now_db.replace(tzinfo=UTC)
        expires_at = row["expires_at"]
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        if expires_at <= now_db:
            raise LookupError("Token has expired.")

        purpose = str(row["purpose"])
        user_id = int(row["user_id"])
        await conn.execute(
            """
            UPDATE auth_tokens
               SET consumed_at = now(),
                   ip_first_seen = COALESCE(ip_first_seen, $2::inet)
             WHERE id = $1
            """,
            row["id"],
            ip,
        )
        if purpose == "email_verify":
            await conn.execute(
                """
                UPDATE users
                   SET email_verified_at = now()
                 WHERE id = $1
                """,
                user_id,
            )
        # Re-read the email after any update so the caller has the truth.
        email_after = await conn.fetchval(
            "SELECT email FROM users WHERE id = $1", user_id
        )
        handle = await conn.fetchval(
            "SELECT handle FROM users WHERE id = $1", user_id
        )

    logger.info(
        "magic link verified",
        extra={"user_id": user_id, "purpose": purpose},
    )
    return VerifyResult(
        user_id=user_id,
        handle=str(handle) if handle is not None else "",
        purpose=purpose,
        email=str(email_after) if email_after is not None else None,
    )


def mask_email(email: str) -> str:
    """Return a privacy-preserving display form: ``a***@example.com``.

    Only the first character of the local-part is preserved, the domain is
    shown verbatim. Used in "we emailed you" confirmations so a stray
    screenshot doesn't fully leak the address.
    """
    s = (email or "").strip().lower()
    if "@" not in s:
        return s
    local, domain = s.split("@", 1)
    if len(local) <= 1:
        return f"{local}***@{domain}"
    return f"{local[0]}***@{domain}"


async def get_email_status(
    pool: asyncpg.Pool[Any], user_id: int
) -> dict[str, Any]:
    """Return the user's email status for the /account page.

    Shape: ``{"email": str | None, "verified": bool, "pending": bool}``

    pending = email is set but email_verified_at is null (a verify link
    was requested but not yet consumed).
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT email, email_verified_at FROM users WHERE id = $1
            """,
            user_id,
        )
    if row is None:
        return {"email": None, "verified": False, "pending": False}
    email = row["email"]
    verified = row["email_verified_at"] is not None
    return {
        "email": email,
        "verified": verified,
        "pending": email is not None and not verified,
        "masked": mask_email(email) if email else None,
    }
