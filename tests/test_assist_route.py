"""Route tests for /show/{date}/assist and /show/{date}/predictions.

Covers PHASE-4-PLAN.md §7 (assist gating):
    - Pre-lock with default config: assist returns the locked message,
      gap chart and venue history are NOT in the response.
    - Pre-lock with ``ASSIST_PRE_LOCK=true`` override: assist renders
      (we patch settings rather than invoking mcp-phish; the gate is
      what matters, not the data quality).
    - Post-lock: assist gate opens.

And the read-only predictions visibility rule:
    - Pre-lock GET /show/{date}/predictions returns the "hidden until lock"
      panel (no rows leaked).
    - Post-lock returns the rows, scores hidden until ``resolved_at`` is set,
      then shown.

We do NOT hit mcp-phish in these tests; the routes' MCP calls all live
inside try/except blocks that degrade gracefully, so the page renders even
when upstream is unreachable. The assertions check the gate, not the data.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any
from unittest.mock import patch

import asyncpg
from httpx import AsyncClient

from phish_game.config import get_settings
from tests.conftest import requires_pg


async def _seed_lock(
    pool: asyncpg.Pool[Any],
    show_date: date,
    *,
    locked: bool,
    resolved: bool = False,
) -> None:
    """Insert a prediction_locks row with lock_at in the past (locked)
    or future (pre-lock).
    """
    lock_at = (
        datetime.now(UTC) - timedelta(hours=1)
        if locked
        else datetime.now(UTC) + timedelta(hours=2)
    )
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO prediction_locks (show_date, lock_at)
            VALUES ($1, $2)
            ON CONFLICT (show_date) DO UPDATE SET lock_at = EXCLUDED.lock_at
            """,
            show_date,
            lock_at,
        )
        if resolved:
            await conn.execute(
                "UPDATE prediction_locks SET resolved_at = now() WHERE show_date = $1",
                show_date,
            )


# ---------- /show/{date}/assist ---------------------------------------------


@requires_pg
async def test_assist_pre_lock_shows_locked_message(
    pg_pool: asyncpg.Pool[Any] | None,
    async_client: AsyncClient,
) -> None:
    """Pre-lock + ASSIST_PRE_LOCK=false: assist returns the locked panel."""
    assert pg_pool is not None
    show_date = date.today() + timedelta(days=2)
    await _seed_lock(pg_pool, show_date, locked=False)

    resp = await async_client.get(f"/show/{show_date.isoformat()}/assist")
    assert resp.status_code == 200
    body = resp.text.lower()
    assert "assist unlocks at showtime" in body
    # Must NOT render the assist data tables (the locked-panel copy mentions
    # the topics, but the actual <h2> section headers should be absent).
    assert "<h2>songs by gap" not in body
    assert "<h2>venue history" not in body
    assert "<h2>recent setlists" not in body


@requires_pg
async def test_assist_no_lock_row_treated_as_pre_lock(
    pg_pool: asyncpg.Pool[Any] | None,
    async_client: AsyncClient,
) -> None:
    """No prediction_locks row at all = treat as pre-lock (closed assist)."""
    assert pg_pool is not None
    show_date = date.today() + timedelta(days=10)
    # No lock row inserted on purpose.

    resp = await async_client.get(f"/show/{show_date.isoformat()}/assist")
    assert resp.status_code == 200
    assert "assist unlocks at showtime" in resp.text.lower()


@requires_pg
async def test_assist_post_lock_renders_assist_page(
    pg_pool: asyncpg.Pool[Any] | None,
    async_client: AsyncClient,
) -> None:
    """Once lock_at is past, the assist page renders even without mcp-phish data.

    The page degrades gracefully when songs_by_gap / venue_history /
    recent_shows fail (e.g. mcp-phish unreachable in the test env). We
    assert on the headers that prove the gate opened.
    """
    assert pg_pool is not None
    show_date = date.today() - timedelta(days=1)
    await _seed_lock(pg_pool, show_date, locked=True)

    resp = await async_client.get(f"/show/{show_date.isoformat()}/assist")
    assert resp.status_code == 200
    body = resp.text.lower()
    # Locked message must NOT appear.
    assert "assist unlocks at showtime" not in body
    # The post-lock view shows these section headers regardless of data.
    assert "<h2>songs by gap" in body
    assert "<h2>venue history" in body
    assert "<h2>recent setlists" in body


@requires_pg
async def test_assist_pre_lock_with_admin_override_unlocks(
    pg_pool: asyncpg.Pool[Any] | None,
    async_client: AsyncClient,
) -> None:
    """Pre-lock + ASSIST_PRE_LOCK=true: assist data unlocks (dev override).

    We patch get_settings on the locks module to return a Settings instance
    with ``assist_pre_lock=True``. The gate accepts the override and the
    page header switches to the unlocked layout.
    """
    assert pg_pool is not None
    show_date = date.today() + timedelta(days=2)
    await _seed_lock(pg_pool, show_date, locked=False)

    base = get_settings()
    overridden = base.model_copy(update={"assist_pre_lock": True})

    # server.py does ``from phish_game.locks import assist_allowed`` at import
    # time, so the route resolves the symbol from the server module's
    # namespace. Patch THAT binding (not locks.assist_allowed).
    from phish_game import locks as locks_module
    from phish_game import server as server_module
    real_assist = locks_module.assist_allowed

    async def _gated(pool: Any, sd: Any, settings: Any) -> bool:
        return await real_assist(pool, sd, overridden)

    with patch.object(server_module, "assist_allowed", _gated):
        resp = await async_client.get(f"/show/{show_date.isoformat()}/assist")
    assert resp.status_code == 200
    body = resp.text.lower()
    # With override: locked message gone, assist sections render.
    assert "assist unlocks at showtime" not in body
    assert "<h2>songs by gap" in body


# ---------- /show/{date}/predictions ----------------------------------------


@requires_pg
async def test_predictions_pre_lock_returns_hidden_panel(
    pg_pool: asyncpg.Pool[Any] | None,
    async_client: AsyncClient,
) -> None:
    """Pre-lock: never list other players' picks."""
    assert pg_pool is not None
    show_date = date.today() + timedelta(days=2)
    await _seed_lock(pg_pool, show_date, locked=False)

    # Seed a user + prediction so there's something to leak if the gate breaks.
    async with pg_pool.acquire() as conn:
        uid = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) "
            "VALUES ('phan_one', 'phan_one') RETURNING id"
        )
        await conn.execute(
            """
            INSERT INTO predictions (
                user_id, show_date, pick_song_slugs,
                opener_slug, closer_slug, encore_slug
            ) VALUES ($1, $2, ARRAY['tweezer','sand','wilson'],
                      'tweezer', 'wilson', 'sand')
            """,
            int(uid),
            show_date,
        )

    resp = await async_client.get(f"/show/{show_date.isoformat()}/predictions")
    assert resp.status_code == 200
    body = resp.text.lower()
    assert "predictions hidden until lock" in body
    # Critical: no handle leak pre-lock.
    assert "phan_one" not in body
    assert "tweezer" not in body


@requires_pg
async def test_predictions_post_lock_lists_picks_no_score(
    pg_pool: asyncpg.Pool[Any] | None,
    async_client: AsyncClient,
) -> None:
    """Post-lock + unresolved: list handles + slugs, hide score column."""
    assert pg_pool is not None
    show_date = date.today() - timedelta(days=1)
    # Seed BEFORE flipping lock to past, so trigger doesn't reject the insert.
    future_lock = datetime.now(UTC) + timedelta(hours=2)
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
        uid = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) "
            "VALUES ('phan_two', 'phan_two') RETURNING id"
        )
        await conn.execute(
            """
            INSERT INTO predictions (
                user_id, show_date, pick_song_slugs,
                opener_slug, closer_slug, encore_slug
            ) VALUES ($1, $2, ARRAY['tweezer','sand','wilson'],
                      NULL, NULL, NULL)
            """,
            int(uid),
            show_date,
        )
        # Now flip the lock to the past; resolved_at stays NULL (unresolved).
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2 WHERE show_date = $1",
            show_date,
            datetime.now(UTC) - timedelta(hours=1),
        )

    resp = await async_client.get(f"/show/{show_date.isoformat()}/predictions")
    assert resp.status_code == 200
    body = resp.text
    body_lower = body.lower()
    assert "predictions hidden until lock" not in body_lower
    assert "phan_two" in body
    assert "tweezer" in body
    # Awaiting-resolver footer should be present.
    assert "awaiting resolver" in body_lower


@requires_pg
async def test_predictions_post_resolve_shows_score(
    pg_pool: asyncpg.Pool[Any] | None,
    async_client: AsyncClient,
) -> None:
    """Resolved show: the Score column appears alongside picks."""
    assert pg_pool is not None
    show_date = date.today() - timedelta(days=2)
    future_lock = datetime.now(UTC) + timedelta(hours=2)
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
        uid = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) "
            "VALUES ('phan_three', 'phan_three') RETURNING id"
        )
        await conn.execute(
            """
            INSERT INTO predictions (
                user_id, show_date, pick_song_slugs,
                opener_slug, closer_slug, encore_slug
            ) VALUES ($1, $2, ARRAY['tweezer','sand','wilson'],
                      NULL, NULL, NULL)
            """,
            int(uid),
            show_date,
        )
        # Flip lock to past, set resolved_at, populate score.
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2, resolved_at = now() "
            "WHERE show_date = $1",
            show_date,
            datetime.now(UTC) - timedelta(hours=1),
        )
        await conn.execute(
            "UPDATE predictions SET score = 47 WHERE user_id = $1", int(uid)
        )

    resp = await async_client.get(f"/show/{show_date.isoformat()}/predictions")
    assert resp.status_code == 200
    body = resp.text
    body_lower = body.lower()
    assert "phan_three" in body
    assert ">Score<" in body or ">score<" in body_lower  # column header
    assert "47" in body
    assert "awaiting resolver" not in body_lower
