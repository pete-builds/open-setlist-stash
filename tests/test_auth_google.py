"""Unit + DB-backed tests for the Google SSO account-linking flow.

The resolver + handle-generation tests need a real Postgres and are gated on
``TEST_PG_DSN`` (see ``conftest.py``). The two route tests here — Google start
when disabled, and /logout — deliberately need NO database, so they run
everywhere: they exercise the wiring that must never touch the DB.

Coverage:
- ``_seed_to_base`` handle-stem cleaning (pure unit)
- ``generate_unique_handle`` collision suffixing (DB)
- ``resolve_google_identity`` cases:
    1. caller already signed in -> link to that handle row
    1b. GoogleLinkConflict when the sub belongs to a different user
    2. returning user (row already owns the sub)
    3. verified-email match on an un-linked row -> link
    4. brand-new user create (+ email populated from a verified Google email)
- Route: /auth/google/start redirects home when Google SSO is disabled
- Route: /logout clears the session cookie
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import asyncpg
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from setlist_stash.auth import CurrentUser, sign_user_id
from setlist_stash.auth_google import (
    GoogleLinkConflict,
    _seed_to_base,
    generate_unique_handle,
    resolve_google_identity,
)
from setlist_stash.config import Settings, get_settings
from tests.conftest import requires_pg

# ---------- pure unit tests (no DB) ----------


def test_seed_to_base_cleans_illegal_chars() -> None:
    assert _seed_to_base("pete.stergion") == "pete-stergion"
    assert _seed_to_base("weird!!name") == "weird-name"
    assert _seed_to_base("  spaced out  ") == "spaced-out"
    assert _seed_to_base("keep_me-9") == "keep_me-9"


def test_seed_to_base_falls_back_when_too_short() -> None:
    assert _seed_to_base("a") == "phan"
    assert _seed_to_base("") == "phan"
    assert _seed_to_base("!!!") == "phan"
    # Leading/trailing separators are trimmed.
    assert _seed_to_base(".pete.") == "pete"


def test_seed_to_base_caps_length() -> None:
    long = "x" * 80
    assert len(_seed_to_base(long)) == 32


# ---------- route tests that need NO database ----------


def _build_app(settings: Settings) -> FastAPI:
    from setlist_stash.server import build_app

    return build_app(settings)


async def test_google_start_redirects_home_when_disabled() -> None:
    """With no Google client configured, /auth/google/start must not attempt
    an OAuth redirect — it sends the player home instead.
    """
    app = _build_app(get_settings())  # google disabled by default
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/auth/google/start", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/"


async def test_google_callback_redirects_home_when_disabled() -> None:
    app = _build_app(get_settings())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/auth/google/callback", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/"


async def test_logout_clears_session_cookie() -> None:
    app = _build_app(get_settings())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/logout", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/"
    set_cookie = resp.headers.get("set-cookie", "")
    assert "phishgame_session=" in set_cookie
    assert "max-age=0" in set_cookie.lower()


async def test_logout_secure_flag_follows_cookie_secure() -> None:
    app = _build_app(
        Settings(session_secret="x" * 32, cookie_secure=True)  # type: ignore[arg-type]
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/logout", follow_redirects=False)
    assert "secure" in resp.headers.get("set-cookie", "").lower()


# ---------- DB-backed resolver + handle tests ----------


async def _insert_user(
    pool: asyncpg.Pool[Any],
    handle: str,
    *,
    email: str | None = None,
    verified: bool = False,
    google_sub: str | None = None,
) -> int:
    async with pool.acquire() as conn:
        user_id = await conn.fetchval(
            """
            INSERT INTO users (handle, handle_lower, email, email_verified_at, google_sub)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            """,
            handle,
            handle.lower(),
            email,
            datetime.now(UTC) if verified else None,
            google_sub,
        )
    return int(user_id)


async def _fetch_user(pool: asyncpg.Pool[Any], user_id: int) -> Any:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT id, handle, email, email_verified_at, google_sub "
            "FROM users WHERE id = $1",
            user_id,
        )


@requires_pg
async def test_generate_unique_handle_suffixes_on_collision(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    # First is free.
    h1 = await generate_unique_handle(pg_pool, "tweezer")
    assert h1 == "tweezer"
    await _insert_user(pg_pool, "tweezer")
    # Now it collides -> -2.
    h2 = await generate_unique_handle(pg_pool, "tweezer")
    assert h2 == "tweezer-2"
    await _insert_user(pg_pool, "tweezer-2")
    h3 = await generate_unique_handle(pg_pool, "tweezer")
    assert h3 == "tweezer-3"


@requires_pg
async def test_resolve_case1_links_to_current_handle(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """A signed-in handle user linking Google keeps their SAME row."""
    assert pg_pool is not None
    uid = await _insert_user(pg_pool, "existing_fan")
    current = CurrentUser(id=uid, handle="existing_fan")
    resolved = await resolve_google_identity(
        pg_pool,
        google_sub="google-sub-111",
        email="fan@example.com",
        email_verified=True,
        current=current,
    )
    assert resolved.user_id == uid  # same row, not a new user
    assert resolved.is_new is False
    row = await _fetch_user(pg_pool, uid)
    assert row["google_sub"] == "google-sub-111"
    # Row had no email, so Google's verified email populates it.
    assert row["email"] == "fan@example.com"
    assert row["email_verified_at"] is not None


@requires_pg
async def test_resolve_case1_conflict_when_sub_owned_by_other(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """If the incoming google_sub already belongs to another user, linking it
    to the current handle raises GoogleLinkConflict.
    """
    assert pg_pool is not None
    other = await _insert_user(pg_pool, "other_fan", google_sub="google-sub-222")
    me = await _insert_user(pg_pool, "me_fan")
    current = CurrentUser(id=me, handle="me_fan")
    raised = False
    try:
        await resolve_google_identity(
            pg_pool,
            google_sub="google-sub-222",
            email=None,
            email_verified=False,
            current=current,
        )
    except GoogleLinkConflict:
        raised = True
    assert raised
    # Neither row changed ownership.
    assert (await _fetch_user(pg_pool, other))["google_sub"] == "google-sub-222"
    assert (await _fetch_user(pg_pool, me))["google_sub"] is None


@requires_pg
async def test_resolve_case2_returns_existing_sub_owner(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """A not-signed-in caller whose google_sub already exists returns that
    same user (returning-user path).
    """
    assert pg_pool is not None
    uid = await _insert_user(pg_pool, "returning_fan", google_sub="google-sub-333")
    resolved = await resolve_google_identity(
        pg_pool,
        google_sub="google-sub-333",
        email="returning@example.com",
        email_verified=True,
        current=None,
    )
    assert resolved.user_id == uid
    assert resolved.is_new is False


@requires_pg
async def test_resolve_case3_links_via_verified_email(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """A not-signed-in caller with no matching sub, but a verified email that
    matches an un-linked user, links to that user (same person, arrived via
    magic-link earlier).
    """
    assert pg_pool is not None
    uid = await _insert_user(
        pg_pool, "email_fan", email="match@example.com", verified=True
    )
    resolved = await resolve_google_identity(
        pg_pool,
        google_sub="google-sub-444",
        email="Match@Example.com",  # case-insensitive match
        email_verified=True,
        current=None,
    )
    assert resolved.user_id == uid
    assert resolved.is_new is False
    row = await _fetch_user(pg_pool, uid)
    assert row["google_sub"] == "google-sub-444"


@requires_pg
async def test_resolve_case3_skipped_when_email_unverified(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """An UN-verified Google email must NOT auto-link to an existing account;
    it falls through to creating a new user instead.
    """
    assert pg_pool is not None
    existing = await _insert_user(
        pg_pool, "verified_fan", email="taken@example.com", verified=True
    )
    resolved = await resolve_google_identity(
        pg_pool,
        google_sub="google-sub-555",
        email="taken@example.com",
        email_verified=False,  # Google says not verified
        current=None,
    )
    assert resolved.user_id != existing  # a fresh user was created
    assert resolved.is_new is True
    new_row = await _fetch_user(pg_pool, resolved.user_id)
    assert new_row["google_sub"] == "google-sub-555"
    # The existing user's email was never appropriated.
    assert new_row["email"] != "taken@example.com"


@requires_pg
async def test_resolve_case4_creates_new_user_from_email(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """No sub, no signed-in caller, no email match -> new user with a handle
    derived from the Google email local-part, and the verified email attached.
    """
    assert pg_pool is not None
    resolved = await resolve_google_identity(
        pg_pool,
        google_sub="google-sub-666",
        email="brandnew@example.com",
        email_verified=True,
        current=None,
    )
    assert resolved.is_new is True
    row = await _fetch_user(pg_pool, resolved.user_id)
    assert row["google_sub"] == "google-sub-666"
    assert row["handle"].lower().startswith("brandnew")
    assert row["email"] == "brandnew@example.com"
    assert row["email_verified_at"] is not None


@requires_pg
async def test_resolve_case4_new_user_handle_dedupes(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """If the derived handle is taken, the new user gets a suffixed handle."""
    assert pg_pool is not None
    await _insert_user(pg_pool, "dupe")
    resolved = await resolve_google_identity(
        pg_pool,
        google_sub="google-sub-777",
        email="dupe@example.com",
        email_verified=True,
        current=None,
    )
    row = await _fetch_user(pg_pool, resolved.user_id)
    assert row["handle"] == "dupe-2"


@requires_pg
async def test_account_page_shows_google_when_enabled(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """The /account page reflects Google-linked status when SSO is enabled."""
    assert pg_pool is not None
    from setlist_stash import db as db_module
    from setlist_stash.server import build_app

    db_module._pool = pg_pool  # type: ignore[attr-defined]
    settings = Settings(  # type: ignore[call-arg]
        session_secret="x" * 32,  # type: ignore[arg-type]
        google_client_id="cid.apps.googleusercontent.com",
        google_client_secret="secret",  # type: ignore[arg-type]
    )
    app = build_app(settings)
    uid = await _insert_user(pg_pool, "linked_fan", google_sub="google-sub-888")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        client.cookies.set("phishgame_session", sign_user_id(settings, uid))
        resp = await client.get("/account", follow_redirects=False)
    assert resp.status_code == 200
    assert "Google" in resp.text
    assert "linked" in resp.text.lower()
