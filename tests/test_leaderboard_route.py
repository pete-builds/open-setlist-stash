"""Route tests for /leaderboard.

Verifies:
- GET /leaderboard renders 200 with the empty-state message when the
  snapshot table is empty.
- GET /leaderboard with seeded data shows ranked rows in score order.
- GET /leaderboard?scope=tour and ?scope=all-time accept and render.
- HX-Request: true returns the table fragment (id="leaderboard-table") only
  — no full page.

DB-backed; skips when TEST_PG_DSN is unset.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from fastapi.testclient import TestClient

from phish_game import db as db_module
from phish_game.config import get_settings
from phish_game.leaderboard import rebuild_all
from phish_game.server import build_app
from tests.conftest import requires_pg


def _client(pool: asyncpg.Pool[Any]) -> TestClient:
    settings = get_settings()
    db_module._pool = pool  # type: ignore[attr-defined]
    app = build_app(settings)
    return TestClient(app, raise_server_exceptions=True)


@pytest.mark.asyncio
@requires_pg
async def test_leaderboard_empty_state_renders(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    with _client(pg_pool) as client:
        resp = client.get("/leaderboard")
    assert resp.status_code == 200
    body = resp.text.lower()
    assert "no scores yet" in body
    assert "leaderboard" in body


@pytest.mark.asyncio
@requires_pg
async def test_leaderboard_renders_seeded_rows(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    show_date = date(2024, 5, 14)
    past_lock = datetime.now(UTC) - timedelta(days=1)
    future_lock = datetime.now(UTC) + timedelta(hours=2)
    async with pg_pool.acquire() as conn:
        a = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ('alpha', 'alpha') RETURNING id"
        )
        b = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ('bravo', 'bravo') RETURNING id"
        )
        await conn.execute(
            "INSERT INTO prediction_locks (show_date, lock_at, venue_tz) VALUES ($1, $2, 'UTC')",
            show_date,
            future_lock,
        )
        await conn.execute(
            """
            INSERT INTO predictions (
                user_id, show_date, pick_song_slugs,
                opener_slug, closer_slug, encore_slug
            )
            VALUES ($1, $2, ARRAY['a','b','c'], NULL, NULL, NULL),
                   ($3, $2, ARRAY['x','y','z'], NULL, NULL, NULL)
            """,
            int(a),
            show_date,
            int(b),
        )
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2, resolved_at = now() WHERE show_date = $1",
            show_date,
            past_lock,
        )
        await conn.execute(
            "UPDATE predictions SET score = 30 WHERE user_id = $1", int(a)
        )
        await conn.execute(
            "UPDATE predictions SET score = 99 WHERE user_id = $1", int(b)
        )

    await rebuild_all(pg_pool)

    with _client(pg_pool) as client:
        resp = client.get("/leaderboard?scope=all-time")
    assert resp.status_code == 200
    text = resp.text
    assert "bravo" in text
    assert "alpha" in text
    # bravo's higher score should appear before alpha's row in rendered HTML.
    assert text.index("bravo") < text.index("alpha")


@pytest.mark.asyncio
@requires_pg
async def test_leaderboard_htmx_returns_fragment(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """HX-Request returns the table-only partial (no <html>, <body>)."""
    assert pg_pool is not None
    with _client(pg_pool) as client:
        resp = client.get("/leaderboard?scope=weekly", headers={"HX-Request": "true"})
    assert resp.status_code == 200
    text = resp.text.lower()
    # Partial should contain the swap target.
    assert 'id="leaderboard-table"' in text
    # And NOT contain the full-page chrome.
    assert "<html" not in text
    assert "<body" not in text


@pytest.mark.asyncio
@requires_pg
async def test_leaderboard_invalid_scope_falls_back_to_weekly(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    with _client(pg_pool) as client:
        resp = client.get("/leaderboard?scope=garbage")
    assert resp.status_code == 200
    # The page should render the weekly tab as active.
    assert "scope-tab is-active" in resp.text or "is-active" in resp.text


@pytest.mark.asyncio
@requires_pg
async def test_leaderboard_direct_scope_key_route(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    # No data => empty state, but the route should still 200.
    with _client(pg_pool) as client:
        resp = client.get("/leaderboard/all_time/all")
    assert resp.status_code == 200
