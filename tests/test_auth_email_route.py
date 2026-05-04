"""Route tests for the magic-link email auth endpoints.

Uses the same ASGITransport pattern as ``test_handle_route.py`` and
``test_assist_route.py`` (see ``conftest.py`` for why we don't use
Starlette's TestClient).

Coverage:
- /auth/email     (form + POST happy path with capturing provider)
- /auth/verify    (valid / expired / consumed / invalid)
- /auth/login     (form + POST + verified-user round-trip)
- /account        (none / pending / verified email states)
- 503 path when EMAIL_PROVIDER=disabled
- Integration: full round-trip from POST /auth/email -> log capture ->
  GET /auth/verify (using the LogProvider, mimicking the nix1 dev flow)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import asyncpg
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from tweezer_picks.auth import sign_user_id
from tweezer_picks.auth_email import _hash_token
from tweezer_picks.config import get_settings
from tweezer_picks.email import DisabledProvider, EmailProvider, LogProvider
from tests.conftest import requires_pg


@dataclass
class CapturedEmail:
    to: str
    subject: str
    body: str


class CapturingProvider:
    """Records every sent message; matches the EmailProvider Protocol."""

    name = "test-capture"

    def __init__(self) -> None:
        self.sent: list[CapturedEmail] = []

    async def send(self, *, to: str, subject: str, body: str) -> None:
        self.sent.append(CapturedEmail(to=to, subject=subject, body=body))


def _build_app_with_provider(
    pool: asyncpg.Pool[Any], provider: EmailProvider
) -> FastAPI:
    """Same pattern as conftest.build_app_with_pool but lets us inject a
    fake email provider directly.
    """
    from tweezer_picks import db as db_module
    from tweezer_picks.server import build_app

    db_module._pool = pool  # type: ignore[attr-defined]
    return build_app(get_settings(), email_provider=provider)


async def _client_with_provider(
    pool: asyncpg.Pool[Any], provider: EmailProvider
) -> AsyncClient:
    app = _build_app_with_provider(pool, provider)
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


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


def _set_session(client: AsyncClient, user_id: int) -> None:
    client.cookies.set("phishgame_session", sign_user_id(get_settings(), user_id))


# ---------- /auth/email form ----------


@requires_pg
async def test_auth_email_get_form_requires_signin(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    async with client:
        resp = await client.get("/auth/email", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/"


@requires_pg
async def test_auth_email_get_form_renders_for_signed_in_user(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "alice")
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    _set_session(client, user_id)
    async with client:
        resp = await client.get("/auth/email")
    assert resp.status_code == 200
    assert "Attach email" in resp.text
    assert "<input" in resp.text


@requires_pg
async def test_auth_email_post_happy_path_sends_link(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "bob")
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    _set_session(client, user_id)
    async with client:
        resp = await client.post(
            "/auth/email", data={"email": "bob@example.com"}
        )
    assert resp.status_code == 200
    assert "Check your inbox" in resp.text
    assert "b***@example.com" in resp.text
    # One email captured.
    assert len(provider.sent) == 1
    assert provider.sent[0].to == "bob@example.com"
    assert "/auth/verify?token=" in provider.sent[0].body


@requires_pg
async def test_auth_email_post_rejects_bad_format(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "carol")
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    _set_session(client, user_id)
    async with client:
        resp = await client.post(
            "/auth/email", data={"email": "not-an-email"}
        )
    assert resp.status_code == 400
    assert provider.sent == []


# ---------- /auth/verify ----------


@requires_pg
async def test_auth_verify_happy_path_sets_cookie_and_flash(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "dave")
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    _set_session(client, user_id)
    async with client:
        resp = await client.post(
            "/auth/email", data={"email": "dave@example.com"}
        )
        assert resp.status_code == 200
        token = (
            provider.sent[0].body.split("/auth/verify?token=")[1].split()[0]
        )
        # Wipe the cookie so /auth/verify exercises the cross-browser path:
        # the cookie is set fresh from the verify token.
        client.cookies.clear()
        resp2 = await client.get(
            f"/auth/verify?token={token}", follow_redirects=False
        )
    assert resp2.status_code == 303
    assert resp2.headers["location"] == "/account"
    assert "phishgame_session" in resp2.cookies
    assert "phishgame_flash" in resp2.cookies

    # email_verified_at now set.
    async with pg_pool.acquire() as conn:
        verified_at = await conn.fetchval(
            "SELECT email_verified_at FROM users WHERE id = $1", user_id
        )
    assert verified_at is not None


@requires_pg
async def test_auth_verify_rejects_invalid(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    async with client:
        resp = await client.get("/auth/verify?token=does-not-exist")
    assert resp.status_code == 400
    assert "didn't work" in resp.text or "invalid" in resp.text.lower()


@requires_pg
async def test_auth_verify_rejects_expired(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "eve")
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    _set_session(client, user_id)
    async with client:
        await client.post(
            "/auth/email", data={"email": "eve@example.com"}
        )
        token = provider.sent[0].body.split("/auth/verify?token=")[1].split()[0]
        # Force-expire.
        async with pg_pool.acquire() as conn:
            await conn.execute(
                "UPDATE auth_tokens SET expires_at = now() - interval '1 hour'"
            )
        resp = await client.get(f"/auth/verify?token={token}")
    assert resp.status_code == 400
    assert "expired" in resp.text.lower()


@requires_pg
async def test_auth_verify_rejects_already_used(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "fred")
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    _set_session(client, user_id)
    async with client:
        await client.post(
            "/auth/email", data={"email": "fred@example.com"}
        )
        token = provider.sent[0].body.split("/auth/verify?token=")[1].split()[0]
        first = await client.get(
            f"/auth/verify?token={token}", follow_redirects=False
        )
        assert first.status_code == 303
        # Second use should now reject.
        client.cookies.clear()
        second = await client.get(f"/auth/verify?token={token}")
    assert second.status_code == 400
    assert "already" in second.text.lower() or "used" in second.text.lower()


# ---------- /account ----------


@requires_pg
async def test_account_redirects_when_signed_out(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    async with client:
        resp = await client.get("/account", follow_redirects=False)
    assert resp.status_code == 303


@requires_pg
async def test_account_renders_no_email_state(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "george")
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    _set_session(client, user_id)
    async with client:
        resp = await client.get("/account")
    assert resp.status_code == 200
    assert "Add email" in resp.text
    assert "<em>none</em>" in resp.text


@requires_pg
async def test_account_renders_pending_state(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    user_id = await _create_user(
        pg_pool, "henry", email="henry@example.com", verified=False
    )
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    _set_session(client, user_id)
    async with client:
        resp = await client.get("/account")
    assert resp.status_code == 200
    assert "pending verification" in resp.text.lower()
    assert "h***@example.com" in resp.text


@requires_pg
async def test_account_renders_verified_state(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    user_id = await _create_user(
        pg_pool, "ida", email="ida@example.com", verified=True
    )
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    _set_session(client, user_id)
    async with client:
        resp = await client.get("/account")
    assert resp.status_code == 200
    assert "verified" in resp.text.lower()
    assert "i***@example.com" in resp.text


# ---------- /auth/login ----------


@requires_pg
async def test_auth_login_form_renders(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    async with client:
        resp = await client.get("/auth/login")
    assert resp.status_code == 200
    assert "Sign in" in resp.text


@requires_pg
async def test_auth_login_form_redirects_when_signed_in(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "jack")
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    _set_session(client, user_id)
    async with client:
        resp = await client.get("/auth/login", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/account"


@requires_pg
async def test_auth_login_round_trip_signs_in_new_browser(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """Verified user → submit email on /auth/login → click verify link
    from the captured email → cookie now points at that user's id."""
    assert pg_pool is not None
    user_id = await _create_user(
        pg_pool, "kim", email="kim@example.com", verified=True
    )
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    async with client:
        resp = await client.post(
            "/auth/login", data={"email": "kim@example.com"}
        )
        assert resp.status_code == 200
        assert "Check your inbox" in resp.text
        assert len(provider.sent) == 1
        token = provider.sent[0].body.split("/auth/verify?token=")[1].split()[0]

        verify_resp = await client.get(
            f"/auth/verify?token={token}", follow_redirects=False
        )
    assert verify_resp.status_code == 303
    assert "phishgame_session" in verify_resp.cookies
    # The cookie must carry the same user_id as the verified email's owner.
    from tweezer_picks.auth import unsign_user_id
    raw_cookie = verify_resp.cookies["phishgame_session"]
    assert unsign_user_id(get_settings(), raw_cookie) == user_id


@requires_pg
async def test_auth_login_unknown_email_returns_masked_success(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """No leak: unknown email returns the same shape as a real one."""
    assert pg_pool is not None
    provider = CapturingProvider()
    client = await _client_with_provider(pg_pool, provider)
    async with client:
        resp = await client.post(
            "/auth/login", data={"email": "ghost@example.com"}
        )
    assert resp.status_code == 200
    assert "Check your inbox" in resp.text
    assert "g***@example.com" in resp.text
    assert provider.sent == []


# ---------- 503 / disabled provider ----------


@requires_pg
async def test_auth_email_post_returns_503_when_disabled(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "leo")
    client = await _client_with_provider(pg_pool, DisabledProvider())
    _set_session(client, user_id)
    async with client:
        resp = await client.post(
            "/auth/email", data={"email": "leo@example.com"}
        )
    assert resp.status_code == 503


@requires_pg
async def test_auth_login_post_returns_503_when_disabled(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    client = await _client_with_provider(pg_pool, DisabledProvider())
    async with client:
        resp = await client.post(
            "/auth/login", data={"email": "anybody@example.com"}
        )
    assert resp.status_code == 503


# ---------- Integration: LogProvider round-trip via container-log capture --


@requires_pg
async def test_integration_log_provider_full_round_trip(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """End-to-end smoke of the nix1 dev flow:

    1. User signs in (anonymous handle).
    2. User POSTs their email to /auth/email.
    3. LogProvider writes the magic link to the logger (the same place
       container logs would surface it).
    4. We extract the token from the captured log, call /auth/verify, and
       confirm email_verified_at flips.

    This is the exact flow Pete will smoke-test on nix1 with
    EMAIL_PROVIDER=log set.

    Note: we attach our own logging handler instead of using ``caplog``
    because ``build_app`` calls ``configure_logging`` which replaces the
    root handlers (clobbering caplog's). Attaching after the app is built
    is the simplest fix and mirrors how container logs work in production
    (the JSON handler is already wired; we add a sibling).
    """
    assert pg_pool is not None
    user_id = await _create_user(pg_pool, "morpheus")
    client = await _client_with_provider(pg_pool, LogProvider())
    _set_session(client, user_id)

    captured: list[str] = []

    class _ListHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record.getMessage())

    handler = _ListHandler(level=logging.INFO)
    email_logger = logging.getLogger("tweezer_picks.email")
    email_logger.addHandler(handler)
    email_logger.setLevel(logging.INFO)
    try:
        async with client:
            resp = await client.post(
                "/auth/email", data={"email": "morpheus@example.com"}
            )
            assert resp.status_code == 200

            # Extract token from the captured log lines.
            body_lines = [m for m in captured if "EMAIL body" in m]
            joined = "\n".join(body_lines)
            match = re.search(r"/auth/verify\?token=([\w-]+)", joined)
            assert match is not None, f"no token in captured log: {body_lines!r}"
            token = match.group(1)

            # Confirm DB stored the hash, NOT the plaintext.
            async with pg_pool.acquire() as conn:
                stored = await conn.fetchval(
                    "SELECT token_hash FROM auth_tokens WHERE user_id = $1",
                    user_id,
                )
            assert stored == _hash_token(token)
            assert stored != token

            # Now click the link.
            client.cookies.clear()
            verify = await client.get(
                f"/auth/verify?token={token}", follow_redirects=False
            )
    finally:
        email_logger.removeHandler(handler)
    assert verify.status_code == 303
    async with pg_pool.acquire() as conn:
        verified_at = await conn.fetchval(
            "SELECT email_verified_at FROM users WHERE id = $1", user_id
        )
    assert verified_at is not None
