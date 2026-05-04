"""Server-side song-slug validation on POST /predict/<show_date>.

The picker UI is a UX guardrail. The trust boundary is here: even a curl
or JS-disabled submission cannot land a row with a bogus slug. These tests
mock mcp-phish via respx so we can exercise the rejection + acceptance
paths without touching the live MCP server.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from typing import Any

import asyncpg
import httpx
import pytest
import respx
from httpx import AsyncClient

from setlist_stash.auth import sign_user_id
from setlist_stash.config import get_settings
from tests.conftest import requires_pg

MCP_URL = "http://mcp-phish:3705/mcp"


def _mcp_response(payload: Any, request_id: str = "abc") -> dict[str, Any]:
    """FastMCP-shaped JSON-RPC response."""
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "content": [
                {"type": "text", "text": json.dumps({"data": payload})}
            ],
            "isError": False,
        },
    }


def _init_response() -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": "1",
        "result": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "serverInfo": {"name": "phish-mcp", "version": "0.1"},
        },
    }


def _handshake_responses() -> list[httpx.Response]:
    """The two responses needed for the FastMCP handshake."""
    return [
        httpx.Response(
            200,
            headers={
                "content-type": "application/json",
                "mcp-session-id": "test-session",
            },
            json=_init_response(),
        ),
        httpx.Response(202, json={"jsonrpc": "2.0", "result": {}}),
    ]


def _validate_ok(valid: list[str], unknown: list[str] | None = None) -> httpx.Response:
    """Single mcp-phish response for the batch validate_song_slugs tool."""
    return httpx.Response(
        200,
        headers={"content-type": "application/json"},
        json=_mcp_response({"valid": valid, "unknown": unknown or []}),
    )


async def _seed_user_and_lock(
    pg_pool: asyncpg.Pool[Any], handle: str = "validation_fan"
) -> tuple[int, date]:
    """Create a user and a future-locked prediction_locks row."""
    show_date = date.today() + timedelta(days=7)
    future_lock = datetime.now(UTC) + timedelta(days=8)
    async with pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO prediction_locks (show_date, lock_at)
            VALUES ($1, $2)
            ON CONFLICT (show_date) DO UPDATE SET lock_at = EXCLUDED.lock_at
            """,
            show_date,
            future_lock,
        )
        user_id = await conn.fetchval(
            """
            INSERT INTO users (handle, handle_lower) VALUES ($1, $2)
            RETURNING id
            """,
            handle,
            handle.lower(),
        )
    return int(user_id), show_date


@requires_pg
@pytest.mark.asyncio
@respx.mock
async def test_post_with_bogus_slug_rejected_no_row_written(
    pg_pool: asyncpg.Pool[Any] | None,
    async_client: AsyncClient,
) -> None:
    """Bogus slug -> 400, no predictions row, error rendered."""
    assert pg_pool is not None
    user_id, show_date = await _seed_user_and_lock(pg_pool)

    # Sequence: handshake (init + initialized), then a single batch
    # validate_song_slugs call. The pick set after normalize_picks is
    # alphabetized: blarghhh, fluffhead, tweezer. Only blarghhh is unknown.
    route = respx.post(MCP_URL)
    route.side_effect = [
        *_handshake_responses(),
        _validate_ok(valid=["fluffhead", "tweezer"], unknown=["blarghhh"]),
    ]

    async_client.cookies.set(
        "phishgame_session", sign_user_id(get_settings(), user_id)
    )
    resp = await async_client.post(
        f"/predict/{show_date.isoformat()}",
        data={
            "pick_1": "tweezer",
            "pick_2": "fluffhead",
            "pick_3": "blarghhh",
            "opener_slug": "",
            "closer_slug": "",
            "encore_slug": "",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 400
    assert "blarghhh" in resp.text.lower()
    # Apostrophe is HTML-escaped in Jinja autoescape, so check the
    # un-apostrophe substring of the error message.
    assert "real phish songs" in resp.text.lower()

    async with pg_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM predictions WHERE user_id = $1", user_id
        )
    assert count == 0


@requires_pg
@pytest.mark.asyncio
@respx.mock
async def test_post_preserves_valid_picks_in_form_after_error(
    pg_pool: asyncpg.Pool[Any] | None,
    async_client: AsyncClient,
) -> None:
    """When one pick is bad, the other (valid) picks stay in the form."""
    assert pg_pool is not None
    user_id, show_date = await _seed_user_and_lock(pg_pool, handle="preserves")

    # Sorted picks: fluffhead, tweezer, wat. wat is unknown.
    route = respx.post(MCP_URL)
    route.side_effect = [
        *_handshake_responses(),
        _validate_ok(valid=["fluffhead", "tweezer"], unknown=["wat"]),
    ]

    async_client.cookies.set(
        "phishgame_session", sign_user_id(get_settings(), user_id)
    )
    resp = await async_client.post(
        f"/predict/{show_date.isoformat()}",
        data={
            "pick_1": "tweezer",
            "pick_2": "fluffhead",
            "pick_3": "wat",
            "opener_slug": "",
            "closer_slug": "",
            "encore_slug": "",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 400
    # The two valid picks survive the re-render (visible in the value attrs).
    assert 'value="tweezer"' in resp.text
    assert 'value="fluffhead"' in resp.text
    # The bad slug is also echoed back so the user sees what to fix.
    assert "wat" in resp.text


@requires_pg
@pytest.mark.asyncio
@respx.mock
async def test_post_with_all_valid_slugs_creates_row(
    pg_pool: asyncpg.Pool[Any] | None,
    async_client: AsyncClient,
) -> None:
    """All 6 slugs valid -> prediction lands."""
    assert pg_pool is not None
    user_id, show_date = await _seed_user_and_lock(pg_pool, handle="all_valid")

    # 6 distinct slugs => one batch validate_song_slugs call after the
    # handshake. The picks are alphabetized by normalize_picks before
    # validation: fluffhead, harry-hood, tweezer, then opener/closer/encore
    # in original order.
    route = respx.post(MCP_URL)
    route.side_effect = [
        *_handshake_responses(),
        _validate_ok(
            valid=[
                "fluffhead",
                "harry-hood",
                "tweezer",
                "wilson",
                "slave-to-the-traffic-light",
                "tweezer-reprise",
            ]
        ),
    ]

    async_client.cookies.set(
        "phishgame_session", sign_user_id(get_settings(), user_id)
    )
    resp = await async_client.post(
        f"/predict/{show_date.isoformat()}",
        data={
            "pick_1": "tweezer",
            "pick_2": "fluffhead",
            "pick_3": "harry-hood",
            "opener_slug": "wilson",
            "closer_slug": "slave-to-the-traffic-light",
            "encore_slug": "tweezer-reprise",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 200, resp.text
    # The predicted.html template renders on success.
    body = resp.text.lower()
    assert "submitted" in body or "thanks" in body or "predict" in body

    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT pick_song_slugs, opener_slug, closer_slug, encore_slug "
            "FROM predictions WHERE user_id = $1",
            user_id,
        )
    assert row is not None
    assert sorted(row["pick_song_slugs"]) == sorted(
        ["tweezer", "fluffhead", "harry-hood"]
    )
    assert row["opener_slug"] == "wilson"
    assert row["closer_slug"] == "slave-to-the-traffic-light"
    assert row["encore_slug"] == "tweezer-reprise"


@requires_pg
@pytest.mark.asyncio
@respx.mock
async def test_post_curl_bypass_attempt_rejected(
    pg_pool: asyncpg.Pool[Any] | None,
    async_client: AsyncClient,
) -> None:
    """A direct POST (no JS) with all-bogus slugs gets rejected."""
    assert pg_pool is not None
    user_id, show_date = await _seed_user_and_lock(pg_pool, handle="curl_bypass")

    # Sorted picks: blarghhh, bogusone, fakey. All unknown.
    route = respx.post(MCP_URL)
    route.side_effect = [
        *_handshake_responses(),
        _validate_ok(
            valid=[], unknown=["blarghhh", "bogusone", "fakey"]
        ),
    ]

    async_client.cookies.set(
        "phishgame_session", sign_user_id(get_settings(), user_id)
    )
    resp = await async_client.post(
        f"/predict/{show_date.isoformat()}",
        data={
            "pick_1": "blarghhh",
            "pick_2": "fakey",
            "pick_3": "bogusone",
            "opener_slug": "",
            "closer_slug": "",
            "encore_slug": "",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 400
    async with pg_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM predictions WHERE user_id = $1", user_id
        )
    assert count == 0
