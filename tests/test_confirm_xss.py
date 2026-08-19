"""Regression tests for the league-name XSS and the ``next=`` redirect guard.

Both were found in the 2026-08-19 review. They share a theme: a value was
escaped correctly for one context and then parsed in a different one.
"""

from __future__ import annotations

from typing import Any

import asyncpg
from httpx import AsyncClient

from setlist_stash.auth import sign_user_id
from setlist_stash.config import get_settings
from setlist_stash.web_helpers import safe_next
from tests.conftest import requires_pg

# A name that breaks out of a JS string literal the moment the HTML parser
# decodes &#39; back to a quote, which is exactly what an onsubmit attribute
# does before the browser compiles it.
HOSTILE_NAME = "x'); window.__pwned = 1; ('"


def _cookie_for(client: AsyncClient, user_id: int) -> None:
    client.cookies.set("phishgame_session", sign_user_id(get_settings(), user_id))


async def _make_user(pool: asyncpg.Pool[Any], handle: str) -> int:
    async with pool.acquire() as conn:
        return int(
            await conn.fetchval(
                "INSERT INTO users (handle, handle_lower) VALUES ($1, $2) RETURNING id",
                handle,
                handle,
            )
        )


@requires_pg
async def test_league_name_never_reaches_a_js_parser(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    """A hostile league name renders as inert text, not as script.

    The host creates the league; a SECOND user joins and loads the dashboard,
    because the leave form (the sink) only renders for non-hosts. That is the
    cross-user path that made this stored XSS rather than self-XSS.
    """
    assert pg_pool is not None
    host_id = await _make_user(pg_pool, "xss_host")
    _cookie_for(async_client, host_id)
    resp = await async_client.post(
        "/leagues/new",
        data={"name": HOSTILE_NAME, "start_date": "", "end_date": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    slug = resp.headers["location"].removeprefix("/league/")

    victim_id = await _make_user(pg_pool, "xss_victim")
    _cookie_for(async_client, victim_id)
    await async_client.post(f"/league/{slug}/join", follow_redirects=False)
    page = await async_client.get(f"/league/{slug}")
    assert page.status_code == 200

    # The leave form is present (we are a non-host member), so the sink is
    # genuinely on the page and this test is exercising it.
    assert "/leave" in page.text
    # No inline handler anywhere: the message moved to data-confirm.
    assert "onsubmit" not in page.text.lower()
    # The name IS on the page (inside data-confirm) but its quotes are entity
    # encoded, so the raw payload never appears. In a plain attribute that is
    # inert; it only became executable when the attribute was ALSO a script.
    assert "data-confirm=" in page.text
    assert "&#39;" in page.text
    assert "x');" not in page.text


@requires_pg
async def test_destructive_forms_use_data_confirm(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    """The host's settings page carries data-confirm, never onsubmit."""
    assert pg_pool is not None
    host_id = await _make_user(pg_pool, "confirm_host")
    _cookie_for(async_client, host_id)
    resp = await async_client.post(
        "/leagues/new",
        data={"name": "Plain Name", "start_date": "", "end_date": ""},
        follow_redirects=False,
    )
    slug = resp.headers["location"].removeprefix("/league/")
    page = await async_client.get(f"/league/{slug}/settings")
    assert page.status_code == 200
    assert "data-confirm=" in page.text
    assert "onsubmit" not in page.text.lower()


def test_safe_next_rejects_backslash_authority() -> None:
    """``/\\evil.tld`` is an open redirect: browsers read ``\\`` as ``/``."""
    assert safe_next("/\\evil.tld") == "/"
    assert safe_next("/\\\\evil.tld") == "/"
    assert safe_next("/\\/evil.tld") == "/"


def test_safe_next_rejects_control_characters() -> None:
    """Browsers strip tab/CR/LF mid-URL, reassembling past a prefix check."""
    assert safe_next("/\tevil.tld") == "/"
    assert safe_next("/\n/evil.tld") == "/"
    assert safe_next("/\r\n/evil.tld") == "/"
    assert safe_next("/x\x7f") == "/"


def test_safe_next_still_allows_real_paths() -> None:
    """The guard must not break the invite hand-off it exists to serve."""
    assert safe_next("/league/tweezer-7kq4mfxb") == "/league/tweezer-7kq4mfxb"
    assert safe_next("/predict/2026-08-20") == "/predict/2026-08-20"
    assert safe_next("/game/ghost-aa22bb33?ref=x") == "/game/ghost-aa22bb33?ref=x"
    assert safe_next("//evil.tld") == "/"
    assert safe_next("https://evil.tld/x") == "/"
    assert safe_next("") == "/"
