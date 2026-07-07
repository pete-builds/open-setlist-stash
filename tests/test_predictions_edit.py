"""Editable-until-lock behavior for predictions.

Picks are editable while the show is open and frozen once locked:

1. A returning user before lock can resubmit; the upsert overwrites their
   existing row (picks + encore change persist).
2. On or after lock, a pick change is rejected server-side by the
   migration-002 lock-guard trigger, surfaced as ``PredictionLocked`` — even
   when the caller bypasses the route's own lock check. The stored row is
   left untouched.

DB-backed; skipped unless ``TEST_PG_DSN`` is set (see conftest).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import asyncpg

from setlist_stash.predictions import (
    PredictionLocked,
    get_user_prediction,
    insert_prediction,
)
from tests.conftest import requires_pg


async def _make_user(pool: asyncpg.Pool[Any], handle: str) -> int:
    async with pool.acquire() as conn:
        uid = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ($1, lower($1)) "
            "RETURNING id",
            handle,
        )
    return int(uid)


async def _make_lock(
    pool: asyncpg.Pool[Any], show_date: date, lock_at: datetime
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO prediction_locks (show_date, lock_at) VALUES ($1, $2) "
            "ON CONFLICT (show_date) DO UPDATE SET lock_at = EXCLUDED.lock_at",
            show_date,
            lock_at,
        )


@requires_pg
async def test_pre_lock_edit_overwrites(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    show_date = date(2030, 6, 1)
    await _make_lock(pg_pool, show_date, datetime.now(UTC) + timedelta(hours=2))
    uid = await _make_user(pg_pool, "editor_pre")

    await insert_prediction(
        pg_pool,
        user_id=uid,
        show_date=show_date,
        pick_song_slugs=["tweezer", "possum", "wilson"],
        encore_slug="tweezer",
    )
    await insert_prediction(
        pg_pool,
        user_id=uid,
        show_date=show_date,
        pick_song_slugs=["ghost", "reba", "antelope"],
        encore_slug="reba",
    )

    row = await get_user_prediction(pg_pool, uid, show_date)
    assert row is not None
    assert sorted(row.pick_song_slugs) == ["antelope", "ghost", "reba"]
    assert row.encore_slug == "reba"


@requires_pg
async def test_post_lock_edit_blocked(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    show_date = date(2030, 6, 2)
    # Open first so the initial submit is allowed, then move the cutoff to
    # the past to simulate a locked show.
    await _make_lock(pg_pool, show_date, datetime.now(UTC) + timedelta(hours=2))
    uid = await _make_user(pg_pool, "editor_post")
    await insert_prediction(
        pg_pool,
        user_id=uid,
        show_date=show_date,
        pick_song_slugs=["tweezer", "possum", "wilson"],
        encore_slug="tweezer",
    )
    await _make_lock(pg_pool, show_date, datetime.now(UTC) - timedelta(hours=1))

    try:
        await insert_prediction(
            pg_pool,
            user_id=uid,
            show_date=show_date,
            pick_song_slugs=["ghost", "reba", "antelope"],
            encore_slug="reba",
        )
        raise AssertionError("post-lock pick change was not blocked")
    except PredictionLocked:
        pass

    row = await get_user_prediction(pg_pool, uid, show_date)
    assert row is not None
    # Original picks survive the rejected edit.
    assert sorted(row.pick_song_slugs) == ["possum", "tweezer", "wilson"]
