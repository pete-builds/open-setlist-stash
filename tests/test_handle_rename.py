"""Tests for handle renaming + the "choose your handle" step.

Covers the new-scope requirement that users (a) choose their own handle and
(b) are never forced to log in:

- ``update_handle`` (auth.py): success, taken-handle conflict, invalid format.
- ``GET/POST /account/handle``: the choose/change-handle page and rename route,
  including the first-time-Google ``?new=1`` welcome variant.
- Regression: a handle-only user (no email, no Google) can still reach the
  leaderboard and their account — nothing new is gated behind login.

All tests are DB-backed (gated on ``TEST_PG_DSN`` via ``conftest``). The
new-Google-user branch that redirects first-timers to this page is exercised by
the ``resolve_google_identity`` ``is_new`` flag tests in ``test_auth_google.py``
(the callback simply routes on that flag); here we prove the destination page
and rename actually work.
"""

from __future__ import annotations

from typing import Any

import asyncpg
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from setlist_stash.auth import HandleError, sign_user_id, update_handle
from setlist_stash.config import get_settings
from tests.conftest import requires_pg


async def _insert_user(pool: asyncpg.Pool[Any], handle: str) -> int:
    async with pool.acquire() as conn:
        user_id = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ($1, $2) RETURNING id",
            handle,
            handle.lower(),
        )
    return int(user_id)


async def _handle_of(pool: asyncpg.Pool[Any], user_id: int) -> Any:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT handle, handle_lower FROM users WHERE id = $1", user_id
        )


def _build_app(pool: asyncpg.Pool[Any]) -> FastAPI:
    from setlist_stash import db as db_module
    from setlist_stash.server import build_app

    db_module._pool = pool  # type: ignore[attr-defined]
    return build_app(get_settings())


def _client(app: FastAPI) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def _set_session(client: AsyncClient, user_id: int) -> None:
    client.cookies.set("phishgame_session", sign_user_id(get_settings(), user_id))


# ---------- update_handle (unit-ish, DB-backed) ----------


@requires_pg
async def test_update_handle_success(pg_pool: asyncpg.Pool[Any] | None) -> None:
    assert pg_pool is not None
    uid = await _insert_user(pg_pool, "old_handle")
    new = await update_handle(pg_pool, uid, "New_Handle")
    assert new == "New_Handle"
    row = await _handle_of(pg_pool, uid)
    assert row["handle"] == "New_Handle"
    assert row["handle_lower"] == "new_handle"


@requires_pg
async def test_update_handle_conflict(pg_pool: asyncpg.Pool[Any] | None) -> None:
    assert pg_pool is not None
    await _insert_user(pg_pool, "taken")
    me = await _insert_user(pg_pool, "mine")
    with pytest.raises(HandleError):
        await update_handle(pg_pool, me, "taken")
    # Case-insensitive: handle_lower unique index also blocks a case variant.
    with pytest.raises(HandleError):
        await update_handle(pg_pool, me, "TAKEN")
    # My handle is unchanged after the failed renames.
    assert (await _handle_of(pg_pool, me))["handle"] == "mine"


@requires_pg
async def test_update_handle_invalid_format(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    uid = await _insert_user(pg_pool, "ok_handle")
    with pytest.raises(HandleError):
        await update_handle(pg_pool, uid, "no spaces allowed")
    with pytest.raises(HandleError):
        await update_handle(pg_pool, uid, "")


# ---------- /account/handle routes ----------


@requires_pg
async def test_account_handle_form_requires_signin(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    app = _build_app(pg_pool)
    async with _client(app) as client:
        resp = await client.get("/account/handle", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/"


@requires_pg
async def test_account_handle_new_shows_welcome_prefilled(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """First-time Google users land here with ?new=1: welcome copy + the
    suggested handle pre-filled and editable.
    """
    assert pg_pool is not None
    uid = await _insert_user(pg_pool, "brandnew-7")
    app = _build_app(pg_pool)
    async with _client(app) as client:
        _set_session(client, uid)
        resp = await client.get("/account/handle?new=1", follow_redirects=False)
    assert resp.status_code == 200
    assert "Pick your handle" in resp.text
    # Suggestion pre-filled so it's editable, not auto-finalized.
    assert 'value="brandnew-7"' in resp.text


@requires_pg
async def test_account_handle_post_renames_and_flashes(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    uid = await _insert_user(pg_pool, "before")
    app = _build_app(pg_pool)
    async with _client(app) as client:
        _set_session(client, uid)
        resp = await client.post(
            "/account/handle", data={"handle": "after"}, follow_redirects=False
        )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/account"
    assert "phishgame_flash" in resp.headers.get("set-cookie", "")
    assert (await _handle_of(pg_pool, uid))["handle"] == "after"


@requires_pg
async def test_account_handle_post_conflict(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    await _insert_user(pg_pool, "occupied")
    me = await _insert_user(pg_pool, "me_fan")
    app = _build_app(pg_pool)
    async with _client(app) as client:
        _set_session(client, me)
        resp = await client.post(
            "/account/handle", data={"handle": "occupied"}, follow_redirects=False
        )
    assert resp.status_code == 400
    assert "already taken" in resp.text.lower()
    assert (await _handle_of(pg_pool, me))["handle"] == "me_fan"


@requires_pg
async def test_account_handle_post_invalid_format(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    uid = await _insert_user(pg_pool, "valid_one")
    app = _build_app(pg_pool)
    async with _client(app) as client:
        _set_session(client, uid)
        resp = await client.post(
            "/account/handle",
            data={"handle": "bad handle!"},
            follow_redirects=False,
        )
    assert resp.status_code == 400
    assert (await _handle_of(pg_pool, uid))["handle"] == "valid_one"


# ---------- regression: anonymous play is never gated ----------


@requires_pg
async def test_handle_only_user_not_forced_to_login(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """A handle-only user (no email, no Google) can view the leaderboard, the
    home page, and their account without any login step.
    """
    assert pg_pool is not None
    uid = await _insert_user(pg_pool, "anon_player")
    app = _build_app(pg_pool)
    async with _client(app) as client:
        _set_session(client, uid)
        lb = await client.get("/leaderboard", follow_redirects=False)
        home = await client.get("/", follow_redirects=False)
        acct = await client.get("/account", follow_redirects=False)
    assert lb.status_code == 200
    assert home.status_code == 200
    assert acct.status_code == 200
    # And the account page offers the handle change (choose-your-own-handle).
    assert "/account/handle" in acct.text
