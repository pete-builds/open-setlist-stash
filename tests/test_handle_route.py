"""Route tests: handle creation + locked-show submission rejection.

These hit the real FastAPI app via ``httpx.AsyncClient`` over an
``ASGITransport``. That keeps the app on the same event loop as the
pytest-asyncio test (and therefore the asyncpg pool). The DB fixture is
``pg_pool`` from ``conftest.py``; the suite skips when ``TEST_PG_DSN`` is
not set in env.

Why not Starlette's ``TestClient``? See ``conftest.py`` docstring: the
TestClient runs the lifespan on a separate event loop via anyio's
BlockingPortal, which conflicts with the asyncpg pool that the fixture
creates on the test's own loop.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from httpx import AsyncClient

from tweezer_picks.auth import sign_user_id
from tweezer_picks.config import get_settings
from tests.conftest import requires_pg


@requires_pg
async def test_post_handle_creates_user_and_sets_cookie(
    pg_pool: asyncpg.Pool[Any] | None,
    async_client: AsyncClient,
) -> None:
    assert pg_pool is not None
    resp = await async_client.post(
        "/handle",
        data={"handle": "tweezer_fan"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/"
    assert "phishgame_session" in resp.cookies

    # Verify the row landed and is queryable by handle.
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, handle FROM users WHERE handle_lower = $1", "tweezer_fan"
        )
    assert row is not None
    assert row["handle"] == "tweezer_fan"


@requires_pg
async def test_post_handle_rejects_bad_format(
    pg_pool: asyncpg.Pool[Any] | None,
    async_client: AsyncClient,
) -> None:
    assert pg_pool is not None
    resp = await async_client.post(
        "/handle",
        data={"handle": "no spaces allowed"},
        follow_redirects=False,
    )
    assert resp.status_code == 200  # re-renders the index with the error
    assert "letters, digits" in resp.text.lower() or "characters" in resp.text.lower()


@requires_pg
async def test_locked_show_rejects_submission(
    pg_pool: asyncpg.Pool[Any] | None,
    async_client: AsyncClient,
) -> None:
    """If lock_at is in the past, the form returns 409 and the trigger blocks
    direct DB inserts as a backstop.
    """
    assert pg_pool is not None
    show_date = date.today()
    past_lock = datetime.now(UTC) - timedelta(hours=1)

    # Pre-create a locked-prediction-locks row (operator override path).
    async with pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO prediction_locks (show_date, lock_at)
            VALUES ($1, $2)
            ON CONFLICT (show_date) DO UPDATE SET lock_at = EXCLUDED.lock_at
            """,
            show_date,
            past_lock,
        )
        user_id = await conn.fetchval(
            """
            INSERT INTO users (handle, handle_lower)
            VALUES ('locked_fan', 'locked_fan') RETURNING id
            """
        )

    # Sanity: the trigger blocks a direct INSERT past lock.
    async with pg_pool.acquire() as conn:
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await conn.execute(
                """
                INSERT INTO predictions
                  (user_id, show_date, pick_song_slugs)
                VALUES ($1, $2, ARRAY['a','b','c'])
                """,
                user_id,
                show_date,
            )

    # The HTTP route returns 409 when the user posts after lock.
    async_client.cookies.set(
        "phishgame_session",
        sign_user_id(get_settings(), user_id),
    )
    resp = await async_client.post(
        f"/predict/{show_date.isoformat()}",
        data={
            "pick_1": "tweezer",
            "pick_2": "fluffhead",
            "pick_3": "harry-hood",
            "opener_slug": "",
            "closer_slug": "",
            "encore_slug": "",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 409
    assert "locked" in resp.text.lower()
