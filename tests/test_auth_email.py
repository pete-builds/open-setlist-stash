"""Unit + DB-backed tests for the magic-link auth flows.

Coverage targets:
- Token generation: 256-bit random, URL-safe, never persisted in plaintext
- Hash storage: only sha256(token) lives in DB
- 24h expiry enforcement
- Single-use enforcement
- Rate limiting (max N outstanding per user/purpose)
- Email format validation
- Email taken (verified-elsewhere) collision
- Login flow returns success-shaped response even for unknown emails
"""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from pydantic import SecretStr

from phish_game.auth import CurrentUser
from phish_game.auth_email import (
    EmailFormatError,
    EmailTakenError,
    _hash_token,
    build_magic_link,
    generate_token,
    get_email_status,
    mask_email,
    render_email_verify_body,
    render_login_body,
    request_email_link,
    request_login_link,
    validate_email,
    verify_token,
)
from phish_game.config import Settings
from phish_game.email import EmailSendError
from tests.conftest import requires_pg

# ---------- pure unit tests (no DB) ----------


def test_validate_email_accepts_lowercase_normalized() -> None:
    assert validate_email("Pete@Example.com") == "pete@example.com"
    assert validate_email("  user@x.io  ") == "user@x.io"


def test_validate_email_rejects_garbage() -> None:
    for bad in ["", "no-at-sign", "@no-local", "no-domain@", "spaces in@x.com"]:
        with pytest.raises(EmailFormatError):
            validate_email(bad)


def test_validate_email_rejects_overlong() -> None:
    too_long = "a" * 320 + "@x.com"
    with pytest.raises(EmailFormatError):
        validate_email(too_long)


def test_generate_token_is_random_and_long() -> None:
    a = generate_token()
    b = generate_token()
    assert a != b
    # token_urlsafe(32) yields ~43 base64-url chars. Sanity: length > 30.
    assert len(a) > 30
    # Only URL-safe chars: a-z A-Z 0-9 _ -
    allowed = set(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
    )
    assert set(a) <= allowed


def test_hash_token_is_deterministic_sha256() -> None:
    token = "the-secret-token"
    expected = hashlib.sha256(token.encode("utf-8")).hexdigest()
    assert _hash_token(token) == expected
    assert _hash_token("other") != expected


def test_build_magic_link_strips_trailing_slash() -> None:
    assert (
        build_magic_link("http://nix1:3706/", "abc")
        == "http://nix1:3706/auth/verify?token=abc"
    )
    assert (
        build_magic_link("http://nix1:3706", "abc")
        == "http://nix1:3706/auth/verify?token=abc"
    )


def test_render_email_bodies_include_link_and_handle() -> None:
    subj, body = render_email_verify_body(
        handle="tweezerfan", link="http://x/auth/verify?token=AAA", ttl_hours=24
    )
    assert "phish-game" in subj
    assert "tweezerfan" in body
    assert "http://x/auth/verify?token=AAA" in body
    assert "24" in body

    subj2, body2 = render_login_body(
        link="http://x/auth/verify?token=BBB", ttl_hours=12
    )
    assert "Sign in" in subj2 or "sign in" in subj2.lower()
    assert "http://x/auth/verify?token=BBB" in body2
    assert "12" in body2


def test_mask_email_preserves_domain() -> None:
    assert mask_email("pete@example.com") == "p***@example.com"
    assert mask_email("a@example.com") == "a***@example.com"
    assert mask_email("PETE@EXAMPLE.COM") == "p***@example.com"
    assert mask_email("notanemail") == "notanemail"
    assert mask_email("") == ""


# ---------- shared DB helpers ----------


def _settings(**overrides: Any) -> Settings:
    defaults = {
        "session_secret": SecretStr("test-secret"),
        "pg_password": SecretStr("test-pw"),
        "smtp_pass": SecretStr(""),
        "base_url": "http://test.local:3706",
        "magic_link_ttl_hours": 24,
        "magic_link_max_outstanding": 3,
    }
    defaults.update(overrides)
    return Settings(**defaults)  # type: ignore[arg-type]


@dataclass
class CapturedEmail:
    to: str
    subject: str
    body: str


class CapturingProvider:
    """Test double that records every send call."""

    name = "test-capture"

    def __init__(self) -> None:
        self.sent: list[CapturedEmail] = []
        self.fail_with: Exception | None = None

    async def send(self, *, to: str, subject: str, body: str) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self.sent.append(CapturedEmail(to=to, subject=subject, body=body))


async def _create_user(
    pool: asyncpg.Pool[Any],
    handle: str,
    *,
    email: str | None = None,
    verified: bool = False,
) -> int:
    async with pool.acquire() as conn:
        user_id = await conn.fetchval(
            """
            INSERT INTO users (handle, handle_lower, email, email_verified_at)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            """,
            handle,
            handle.lower(),
            email,
            datetime.now(UTC) if verified else None,
        )
    return int(user_id)


# ---------- DB-backed flows ----------


@requires_pg
async def test_request_email_link_persists_hash_not_plaintext(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """The token plaintext must NEVER be persisted. Only sha256 lives in DB."""
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "alice")
    user = CurrentUser(id=user_id, handle="alice")
    provider = CapturingProvider()

    masked = await request_email_link(
        pg_pool,
        user=user,
        email="alice@example.com",
        settings=_settings(),
        provider=provider,
    )
    assert masked == "a***@example.com"

    # Extract the magic link from the captured body.
    assert len(provider.sent) == 1
    sent = provider.sent[0]
    assert sent.to == "alice@example.com"
    # Token from the link.
    assert "/auth/verify?token=" in sent.body
    token = sent.body.split("/auth/verify?token=")[1].split()[0]
    expected_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id, purpose, token_hash, expires_at, consumed_at "
            "FROM auth_tokens WHERE user_id = $1",
            user_id,
        )
    assert len(rows) == 1
    row = rows[0]
    assert row["user_id"] == user_id
    assert row["purpose"] == "email_verify"
    # Hash matches; plaintext does NOT appear anywhere.
    assert row["token_hash"] == expected_hash
    assert row["token_hash"] != token
    # Confirm plaintext token nowhere in the row by string search.
    for col_value in row.values():
        assert token not in str(col_value)
    assert row["consumed_at"] is None
    # Expiry ~24h from now.
    delta = row["expires_at"] - datetime.now(UTC)
    assert timedelta(hours=23) < delta <= timedelta(hours=24)


@requires_pg
async def test_request_email_link_validates_format(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "bob")
    user = CurrentUser(id=user_id, handle="bob")
    provider = CapturingProvider()
    with pytest.raises(EmailFormatError):
        await request_email_link(
            pg_pool, user=user, email="not-an-email",
            settings=_settings(), provider=provider,
        )
    # No token created.
    async with pg_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT count(*) FROM auth_tokens WHERE user_id = $1", user_id
        )
    assert count == 0


@requires_pg
async def test_request_email_link_blocks_taken_email(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    # Alice already verified with this email.
    await _create_user(
        pg_pool, "alice", email="shared@example.com", verified=True
    )
    bob_id = await _create_user(pg_pool, "bob")
    bob = CurrentUser(id=bob_id, handle="bob")
    provider = CapturingProvider()
    with pytest.raises(EmailTakenError):
        await request_email_link(
            pg_pool, user=bob, email="SHARED@example.com",
            settings=_settings(), provider=provider,
        )
    # Bob's user row should NOT have email set.
    async with pg_pool.acquire() as conn:
        bob_email = await conn.fetchval(
            "SELECT email FROM users WHERE id = $1", bob_id
        )
    assert bob_email is None


@requires_pg
async def test_verify_email_link_happy_path(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "carol")
    user = CurrentUser(id=user_id, handle="carol")
    provider = CapturingProvider()

    await request_email_link(
        pg_pool, user=user, email="carol@example.com",
        settings=_settings(), provider=provider,
    )
    token = provider.sent[0].body.split("/auth/verify?token=")[1].split()[0]

    result = await verify_token(pg_pool, token=token, ip="10.0.0.1")
    assert result.user_id == user_id
    assert result.handle == "carol"
    assert result.purpose == "email_verify"
    assert result.email == "carol@example.com"

    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT email, email_verified_at FROM users WHERE id = $1", user_id
        )
        token_row = await conn.fetchrow(
            "SELECT consumed_at, ip_first_seen FROM auth_tokens WHERE user_id = $1",
            user_id,
        )
    assert row["email"] == "carol@example.com"
    assert row["email_verified_at"] is not None
    assert token_row["consumed_at"] is not None
    assert str(token_row["ip_first_seen"]) == "10.0.0.1"


@requires_pg
async def test_verify_token_single_use(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "dave")
    user = CurrentUser(id=user_id, handle="dave")
    provider = CapturingProvider()
    await request_email_link(
        pg_pool, user=user, email="dave@example.com",
        settings=_settings(), provider=provider,
    )
    token = provider.sent[0].body.split("/auth/verify?token=")[1].split()[0]

    # First use succeeds.
    await verify_token(pg_pool, token=token, ip=None)
    # Second use fails — single-use enforced.
    with pytest.raises(LookupError, match="already been used"):
        await verify_token(pg_pool, token=token, ip=None)


@requires_pg
async def test_verify_token_rejects_expired(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "eve")
    user = CurrentUser(id=user_id, handle="eve")
    provider = CapturingProvider()
    await request_email_link(
        pg_pool, user=user, email="eve@example.com",
        settings=_settings(), provider=provider,
    )
    token = provider.sent[0].body.split("/auth/verify?token=")[1].split()[0]
    token_hash = _hash_token(token)
    # Force-expire the token.
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE auth_tokens SET expires_at = now() - interval '1 hour' "
            "WHERE token_hash = $1",
            token_hash,
        )
    with pytest.raises(LookupError, match="expired"):
        await verify_token(pg_pool, token=token, ip=None)


@requires_pg
async def test_verify_token_rejects_unknown(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    with pytest.raises(LookupError, match="Invalid"):
        await verify_token(pg_pool, token="not-a-real-token", ip=None)
    with pytest.raises(LookupError, match="Empty"):
        await verify_token(pg_pool, token="", ip=None)


@requires_pg
async def test_request_email_link_rate_limit_expires_oldest(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """At most N outstanding tokens per (user, purpose). New requests
    consume the oldest beyond the cap."""
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "fred")
    user = CurrentUser(id=user_id, handle="fred")
    provider = CapturingProvider()
    settings = _settings(magic_link_max_outstanding=2)

    # 1st request.
    await request_email_link(
        pg_pool, user=user, email="fred@example.com",
        settings=settings, provider=provider,
    )
    # tiny sleep so created_at is monotonically distinct.
    await asyncio.sleep(0.01)
    # 2nd.
    await request_email_link(
        pg_pool, user=user, email="fred@example.com",
        settings=settings, provider=provider,
    )
    await asyncio.sleep(0.01)
    # 3rd: should expire the oldest, leaving exactly 2 outstanding.
    await request_email_link(
        pg_pool, user=user, email="fred@example.com",
        settings=settings, provider=provider,
    )
    async with pg_pool.acquire() as conn:
        outstanding = await conn.fetchval(
            "SELECT count(*) FROM auth_tokens "
            "WHERE user_id = $1 AND consumed_at IS NULL",
            user_id,
        )
        total = await conn.fetchval(
            "SELECT count(*) FROM auth_tokens WHERE user_id = $1", user_id
        )
    assert outstanding == 2
    assert total == 3  # 1 of 3 consumed (the rate-limit eviction)


@requires_pg
async def test_request_login_link_silently_drops_unknown_email(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """Unknown email returns a masked-success shape with no token minted.

    Anti-enumeration: attacker can't probe for verified accounts."""
    assert pg_pool is not None
    provider = CapturingProvider()
    masked = await request_login_link(
        pg_pool,
        email="ghost@example.com",
        settings=_settings(),
        provider=provider,
    )
    assert masked == "g***@example.com"
    # No email actually sent.
    assert provider.sent == []
    async with pg_pool.acquire() as conn:
        token_count = await conn.fetchval("SELECT count(*) FROM auth_tokens")
    assert token_count == 0


@requires_pg
async def test_request_login_link_only_for_verified_users(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """A user with email but no email_verified_at should NOT get a login link."""
    assert pg_pool is not None
    await _create_user(
        pg_pool, "pendingusr", email="pending@example.com", verified=False
    )
    provider = CapturingProvider()
    await request_login_link(
        pg_pool,
        email="pending@example.com",
        settings=_settings(),
        provider=provider,
    )
    assert provider.sent == []


@requires_pg
async def test_login_round_trip_attaches_session_to_correct_user(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """Verified user requests login link, clicks it, gets a VerifyResult
    pointing at their user_id (which the route then puts into the cookie)."""
    assert pg_pool is not None
    user_id = await _create_user(
        pg_pool, "gina", email="gina@example.com", verified=True
    )
    provider = CapturingProvider()
    await request_login_link(
        pg_pool,
        email="gina@example.com",
        settings=_settings(),
        provider=provider,
    )
    assert len(provider.sent) == 1
    token = provider.sent[0].body.split("/auth/verify?token=")[1].split()[0]
    result = await verify_token(pg_pool, token=token, ip=None)
    assert result.user_id == user_id
    assert result.purpose == "login"
    # email_verified_at unchanged (already verified).
    async with pg_pool.acquire() as conn:
        verified_at_after = await conn.fetchval(
            "SELECT email_verified_at FROM users WHERE id = $1", user_id
        )
    assert verified_at_after is not None


@requires_pg
async def test_get_email_status_three_states(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    none_uid = await _create_user(pg_pool, "noemail")
    pending_uid = await _create_user(
        pg_pool, "pendingbob", email="p@x.com", verified=False
    )
    verified_uid = await _create_user(
        pg_pool, "verifiedalice", email="v@x.com", verified=True
    )

    none_st = await get_email_status(pg_pool, none_uid)
    pending_st = await get_email_status(pg_pool, pending_uid)
    verified_st = await get_email_status(pg_pool, verified_uid)

    assert none_st["email"] is None
    assert none_st["verified"] is False
    assert none_st["pending"] is False

    assert pending_st["email"] == "p@x.com"
    assert pending_st["verified"] is False
    assert pending_st["pending"] is True
    assert pending_st["masked"] == "p***@x.com"

    assert verified_st["email"] == "v@x.com"
    assert verified_st["verified"] is True
    assert verified_st["pending"] is False


@requires_pg
async def test_email_send_failure_propagates(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """If the provider fails, raise EmailSendError. The DB row was committed
    (idempotent retry is safe) but the user sees an error and can resend."""
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "henry")
    user = CurrentUser(id=user_id, handle="henry")
    provider = CapturingProvider()
    provider.fail_with = EmailSendError("smtp boom")
    with pytest.raises(EmailSendError):
        await request_email_link(
            pg_pool, user=user, email="henry@example.com",
            settings=_settings(), provider=provider,
        )
