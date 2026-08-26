"""Session cookie expiry and per-user revocation.

These cover the two properties the pre-012 cookie could not offer: a signed
token that stops being valid on its own, and a way to end one user's sessions
without ending everyone's. Both were previously untestable because there was
nothing to assert on -- the old ``URLSafeSerializer`` token was valid forever
and carried no epoch.
"""

from __future__ import annotations

import time
from typing import Any

import asyncpg
import pytest
from httpx import ASGITransport, AsyncClient
from itsdangerous.timed import TimestampSigner

from setlist_stash.auth import (
    COOKIE_NAME,
    current_user,
    get_user_by_id,
    revoke_sessions,
    sign_user_id,
    unsign_session,
    unsign_user_id,
)
from setlist_stash.config import Settings, get_settings
from tests.conftest import build_app_with_pool, requires_pg

_SECRET = "test-secret-for-session-lifecycle-tests"


def _cfg(**kw: Any) -> Settings:
    return Settings(session_secret=_SECRET, **kw)


async def _insert_user(pool: asyncpg.Pool[Any], handle: str) -> int:
    async with pool.acquire() as conn:
        user_id = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ($1, $2) RETURNING id",
            handle,
            handle.lower(),
        )
    return int(user_id)


# --- token round trip ---------------------------------------------------------


def test_token_round_trips_id_and_epoch() -> None:
    cfg = _cfg()
    claims = unsign_session(cfg, sign_user_id(cfg, 42, 7))
    assert claims is not None
    assert claims.user_id == 42
    assert claims.session_epoch == 7


def test_epoch_defaults_to_zero() -> None:
    cfg = _cfg()
    claims = unsign_session(cfg, sign_user_id(cfg, 42))
    assert claims is not None
    assert claims.session_epoch == 0


def test_unsign_user_id_wrapper_still_returns_id() -> None:
    cfg = _cfg()
    assert unsign_user_id(cfg, sign_user_id(cfg, 42, 3)) == 42


def test_token_from_a_different_secret_is_rejected() -> None:
    forged = sign_user_id(Settings(session_secret="some-other-secret"), 42)
    assert unsign_session(_cfg(), forged) is None


def test_tampered_token_is_rejected() -> None:
    """A modified signature must not validate.

    The mutated character is deliberately NOT the last one. These tokens are 43
    base64url characters carrying a 256-bit signature, and 43 * 6 = 258, so the
    final character has two bits that decode to nothing. Flipping it can leave
    the decoded signature bytes identical, in which case the "tampered" token is
    genuinely valid and this test fails through no fault of the code.

    That is not hypothetical. URLSafeTimedSerializer embeds the current time, so
    the token differs every run; sweeping 2062 timestamps, the old last-character
    flip left the signature unchanged for 135 of them. A roughly 6.5% failure
    rate on a security test, which is exactly the kind that gets re-run until it
    passes. A character inside the signature carries all six bits, and the same
    sweep detects the tamper in 2062 of 2062.
    """
    cfg = _cfg()
    token = sign_user_id(cfg, 42)
    i = len(token) - 8
    tampered = token[:i] + ("A" if token[i] != "A" else "B") + token[i + 1 :]
    assert unsign_session(cfg, tampered) is None


def test_garbage_is_rejected() -> None:
    assert unsign_session(_cfg(), "not-a-token") is None


# --- expiry -------------------------------------------------------------------


def test_token_older_than_max_age_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The property the untimed serializer could not express.

    Signed correctly, never tampered with, simply too old. Before 012 this
    token stayed valid for as long as SESSION_SECRET did.
    """
    cfg = _cfg(session_max_age_days=1)
    stale = int(time.time()) - (2 * 86400)
    monkeypatch.setattr(TimestampSigner, "get_timestamp", lambda self: stale)
    token = sign_user_id(cfg, 42)
    monkeypatch.undo()
    assert unsign_session(cfg, token) is None


def test_token_inside_max_age_is_accepted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _cfg(session_max_age_days=30)
    recent = int(time.time()) - (29 * 86400)
    monkeypatch.setattr(TimestampSigner, "get_timestamp", lambda self: recent)
    token = sign_user_id(cfg, 42)
    monkeypatch.undo()
    claims = unsign_session(cfg, token)
    assert claims is not None and claims.user_id == 42


def test_max_age_is_read_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Same token, two policies: only the stricter one rejects it."""
    stale = int(time.time()) - (10 * 86400)
    monkeypatch.setattr(TimestampSigner, "get_timestamp", lambda self: stale)
    token = sign_user_id(_cfg(), 42)
    monkeypatch.undo()
    assert unsign_session(_cfg(session_max_age_days=7), token) is None
    assert unsign_session(_cfg(session_max_age_days=30), token) is not None


# --- revocation (DB-backed) ---------------------------------------------------


@requires_pg
async def test_revoke_bumps_epoch(pg_pool: asyncpg.Pool[Any]) -> None:
    user_id = await _insert_user(pg_pool, "revoker1")
    user = await get_user_by_id(pg_pool, user_id)
    assert user is not None and user.session_epoch == 0
    assert await revoke_sessions(pg_pool, user_id) == 1
    after = await get_user_by_id(pg_pool, user_id)
    assert after is not None and after.session_epoch == 1


@requires_pg
async def test_revoke_only_affects_that_user(
    pg_pool: asyncpg.Pool[Any],
) -> None:
    """The whole point: one user's cookies die, everyone else stays signed in."""
    victim = await _insert_user(pg_pool, "revoker2")
    bystander = await _insert_user(pg_pool, "bystander2")
    await revoke_sessions(pg_pool, victim)
    other = await get_user_by_id(pg_pool, bystander)
    assert other is not None and other.session_epoch == 0


@requires_pg
async def test_cookie_minted_before_revocation_stops_resolving(
    pg_pool: asyncpg.Pool[Any],
) -> None:
    from starlette.requests import Request

    cfg = _cfg()
    user_id = await _insert_user(pg_pool, "revoker3")
    token = sign_user_id(cfg, user_id, 0)
    scope = {
        "type": "http",
        "headers": [(b"cookie", f"{COOKIE_NAME}={token}".encode())],
        "client": ("10.0.0.5", 1234),
    }
    resolved = await current_user(Request(scope), pg_pool, cfg)
    assert resolved is not None and resolved.id == user_id

    await revoke_sessions(pg_pool, user_id)
    assert await current_user(Request(scope), pg_pool, cfg) is None


@requires_pg
async def test_new_cookie_after_revocation_works(
    pg_pool: asyncpg.Pool[Any],
) -> None:
    """Revocation must not lock the user out permanently."""
    from starlette.requests import Request

    cfg = _cfg()
    user_id = await _insert_user(pg_pool, "revoker4")
    new_epoch = await revoke_sessions(pg_pool, user_id)
    token = sign_user_id(cfg, user_id, new_epoch)
    scope = {
        "type": "http",
        "headers": [(b"cookie", f"{COOKIE_NAME}={token}".encode())],
        "client": ("10.0.0.5", 1234),
    }
    resolved = await current_user(Request(scope), pg_pool, cfg)
    assert resolved is not None and resolved.id == user_id


@requires_pg
async def test_revoke_route_signs_out_the_calling_browser(
    pg_pool: asyncpg.Pool[Any],
) -> None:
    user_id = await _insert_user(pg_pool, "revoker5")
    app = build_app_with_pool(pg_pool)
    cfg = get_settings()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as client:
        client.cookies.set(COOKIE_NAME, sign_user_id(cfg, user_id, 0))
        resp = await client.post(
            "/account/sessions/revoke", follow_redirects=False
        )
        assert resp.status_code == 303
        # The account page must no longer recognise the old cookie.
        after = await client.get("/account", follow_redirects=False)
        assert after.status_code in (302, 303)


@requires_pg
async def test_revoke_route_is_a_noop_when_signed_out(
    pg_pool: asyncpg.Pool[Any],
) -> None:
    app = build_app_with_pool(pg_pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as client:
        resp = await client.post(
            "/account/sessions/revoke", follow_redirects=False
        )
        assert resp.status_code == 303
