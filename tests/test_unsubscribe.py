"""Unsubscribe tokens and the routes that consume them.

The behavior under test that is easy to get wrong and expensive to get wrong:

* a GET on the unsubscribe URL must NOT unsubscribe anybody (mail scanners
  prefetch links, and a GET that acts silently drops real players);
* a token minted under one secret must not verify under another;
* an unsubscribe token must never be usable as a session cookie.

DB-backed arms are skipped unless ``TEST_PG_DSN`` is set (see conftest).
"""

from __future__ import annotations

from typing import Any

import asyncpg
import pytest
from httpx import AsyncClient
from pydantic import SecretStr

from setlist_stash.auth import unsign_session
from setlist_stash.config import Settings, get_settings
from setlist_stash.unsubscribe import (
    is_opted_out,
    mint_unsubscribe_token,
    read_unsubscribe_token,
    set_opt_out,
    unsubscribe_url,
)
from tests.conftest import requires_pg


def _settings(**over: Any) -> Settings:
    base: dict[str, Any] = {
        "session_secret": SecretStr("test-secret-aaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        "base_url": "https://example.test",
    }
    base.update(over)
    return Settings(**base)


# ----- token ----------------------------------------------------------------


def test_token_round_trips() -> None:
    cfg = _settings()
    token = mint_unsubscribe_token(cfg, 4242)
    assert read_unsubscribe_token(cfg, token) == 4242


def test_token_from_another_secret_is_rejected() -> None:
    minted = mint_unsubscribe_token(_settings(), 7)
    other = _settings(
        session_secret=SecretStr("different-secret-bbbbbbbbbbbbbbbbbbbbbbbb")
    )
    assert read_unsubscribe_token(other, minted) is None


@pytest.mark.parametrize("bad", ["", "garbage", "a.b.c", "!!!"])
def test_malformed_tokens_return_none(bad: str) -> None:
    assert read_unsubscribe_token(_settings(), bad) is None


def test_unsubscribe_token_is_not_a_session_cookie() -> None:
    """Same key material, different salt: neither token opens the other door.

    Without the salt separation an unsubscribe link mailed to a player would
    be a bearer credential for their account, which is the difference between
    a nuisance and an account takeover.
    """
    cfg = _settings()
    token = mint_unsubscribe_token(cfg, 11)
    assert unsign_session(cfg, token) is None


def test_unsubscribe_url_is_absolute_and_tenant_scoped() -> None:
    cfg = _settings(base_url="https://wappypicks.com/")
    url = unsubscribe_url(cfg, 3)
    assert url.startswith("https://wappypicks.com/email/unsubscribe?t=")
    # No double slash from the trailing slash on base_url.
    assert "//email" not in url.removeprefix("https://")


# ----- state ----------------------------------------------------------------


async def _make_user(pool: asyncpg.Pool[Any], handle: str) -> int:
    async with pool.acquire() as conn:
        uid = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower, email, email_verified_at) "
            "VALUES ($1, lower($1), $2, now()) RETURNING id",
            handle,
            f"{handle}@example.test",
        )
    return int(uid)


@requires_pg
@pytest.mark.asyncio
async def test_set_opt_out_toggles_both_ways(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    uid = await _make_user(pg_pool, "optout")
    assert await is_opted_out(pg_pool, uid) is False
    assert await set_opt_out(pg_pool, uid, opted_out=True) is True
    assert await is_opted_out(pg_pool, uid) is True
    assert await set_opt_out(pg_pool, uid, opted_out=False) is True
    assert await is_opted_out(pg_pool, uid) is False


@requires_pg
@pytest.mark.asyncio
async def test_repeat_unsubscribe_keeps_the_first_timestamp(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """A prefetching mail client hitting the button twice must not rewrite it.

    The stored instant answers "when did this person leave", which is the
    question that comes up when someone reports unexpected mail. Refreshing it
    on every touch would destroy that answer.
    """
    assert pg_pool is not None
    uid = await _make_user(pg_pool, "twice")
    await set_opt_out(pg_pool, uid, opted_out=True)
    async with pg_pool.acquire() as conn:
        first = await conn.fetchval(
            "SELECT email_opt_out_at FROM users WHERE id = $1", uid
        )
    await set_opt_out(pg_pool, uid, opted_out=True)
    async with pg_pool.acquire() as conn:
        second = await conn.fetchval(
            "SELECT email_opt_out_at FROM users WHERE id = $1", uid
        )
    assert first == second


@requires_pg
@pytest.mark.asyncio
async def test_set_opt_out_reports_unknown_user(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    assert await set_opt_out(pg_pool, 999_999, opted_out=True) is False


# ----- routes ---------------------------------------------------------------


@requires_pg
@pytest.mark.asyncio
async def test_get_unsubscribe_does_not_unsubscribe(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    """The control arm for the whole design.

    A version of this route that acted on GET would also pass every other test
    in this file. Only this one fails against it, and it is the exact failure
    that would silently drop players whose mail gateway scans links.
    """
    assert pg_pool is not None
    uid = await _make_user(pg_pool, "scanner")
    # ``build_app_with_pool`` builds the app from ``get_settings()``, so this
    # is the same Settings instance the route will verify against.
    cfg = get_settings()
    token = mint_unsubscribe_token(cfg, uid)

    resp = await async_client.get(f"/email/unsubscribe?t={token}")
    assert resp.status_code == 200
    assert "scanner" in resp.text
    assert await is_opted_out(pg_pool, uid) is False


@requires_pg
@pytest.mark.asyncio
async def test_post_unsubscribe_from_the_form(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    assert pg_pool is not None
    uid = await _make_user(pg_pool, "former")
    # ``build_app_with_pool`` builds the app from ``get_settings()``, so this
    # is the same Settings instance the route will verify against.
    cfg = get_settings()
    token = mint_unsubscribe_token(cfg, uid)

    resp = await async_client.post("/email/unsubscribe", data={"token": token})
    assert resp.status_code == 200
    assert await is_opted_out(pg_pool, uid) is True


@requires_pg
@pytest.mark.asyncio
async def test_one_click_post_carries_the_token_in_the_query(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    """RFC 8058: the mail client POSTs the List-Unsubscribe URL unchanged.

    It sends no form body of ours, so the token arrives only in the query
    string. A route that read the body alone would render Gmail's native
    unsubscribe button inert while looking perfectly fine in manual testing.
    """
    assert pg_pool is not None
    uid = await _make_user(pg_pool, "oneclick")
    # ``build_app_with_pool`` builds the app from ``get_settings()``, so this
    # is the same Settings instance the route will verify against.
    cfg = get_settings()
    token = mint_unsubscribe_token(cfg, uid)

    resp = await async_client.post(
        f"/email/unsubscribe?t={token}",
        data={"List-Unsubscribe": "One-Click"},
    )
    assert resp.status_code == 200
    assert await is_opted_out(pg_pool, uid) is True


@requires_pg
@pytest.mark.asyncio
async def test_unreadable_token_gets_a_400_not_a_crash(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    resp = await async_client.post("/email/unsubscribe", data={"token": "nope"})
    assert resp.status_code == 400
    assert "could not be read" in resp.text
