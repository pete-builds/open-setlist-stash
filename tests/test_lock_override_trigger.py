"""The DB lock guard must honor ``prediction_locks.lock_at_override``.

``lock_at_override`` is the operator escape hatch for a show whose real
downbeat differs from the computed default (a co-bill where the headliner
closes, a festival slot, a late curfew). Every application-side read resolves
the cutoff as ``COALESCE(lock_at_override, lock_at)``; before migration 010
the trigger read the raw ``lock_at``, so the two layers disagreed and the
override was inert: the page rendered an open form and a live countdown while
every submit was rejected against the superseded cutoff.

These are deliberately a CONTRAST, not a single assertion. Arm A alone would
also pass against a trigger that had simply been disabled, and arm B alone
passes against the old buggy trigger. Only the pair distinguishes "reads the
effective cutoff" from "stopped guarding".

DB-backed; skipped unless ``TEST_PG_DSN`` is set (see conftest).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import asyncpg
import pytest

from setlist_stash.predictions import PredictionLocked, insert_prediction
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
    pool: asyncpg.Pool[Any],
    show_date: date,
    lock_at: datetime,
    override: datetime | None,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO prediction_locks (show_date, lock_at, lock_at_override)
            VALUES ($1, $2, $3)
            ON CONFLICT (show_date) DO UPDATE
                SET lock_at = EXCLUDED.lock_at,
                    lock_at_override = EXCLUDED.lock_at_override
            """,
            show_date,
            lock_at,
            override,
        )


@requires_pg
async def test_future_override_reopens_a_passed_lock(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """Arm A: lock_at passed, override still ahead -> picks accepted.

    This is the regression. Against the pre-010 trigger it raises
    PredictionLocked, because the guard read lock_at and ignored the override.
    """
    assert pg_pool is not None
    show_date = date(2030, 7, 1)
    now = datetime.now(UTC)
    await _make_lock(
        pg_pool,
        show_date,
        lock_at=now - timedelta(hours=2),
        override=now + timedelta(hours=2),
    )
    uid = await _make_user(pg_pool, "override_open")

    pid = await insert_prediction(
        pg_pool,
        user_id=uid,
        show_date=show_date,
        pick_song_slugs=["all-in-time", "bridgeless", "hajimemashite"],
        encore_slug="bridgeless",
    )
    assert pid > 0


@requires_pg
async def test_passed_lock_without_override_still_blocked(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """Arm B: lock_at passed, no override -> still rejected.

    Proves 010 taught the guard to read the effective cutoff rather than
    simply loosening it.
    """
    assert pg_pool is not None
    show_date = date(2030, 7, 2)
    await _make_lock(
        pg_pool,
        show_date,
        lock_at=datetime.now(UTC) - timedelta(hours=2),
        override=None,
    )
    uid = await _make_user(pg_pool, "override_absent")

    with pytest.raises(PredictionLocked):
        await insert_prediction(
            pg_pool,
            user_id=uid,
            show_date=show_date,
            pick_song_slugs=["all-in-time", "bridgeless", "hajimemashite"],
            encore_slug="bridgeless",
        )


@requires_pg
async def test_override_can_tighten_an_open_lock(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """Arm C: lock_at still ahead, override already passed -> rejected.

    The override is the effective cutoff in BOTH directions. An operator
    pulling a cutoff earlier must bind, not just one pushing it later.
    """
    assert pg_pool is not None
    show_date = date(2030, 7, 3)
    now = datetime.now(UTC)
    await _make_lock(
        pg_pool,
        show_date,
        lock_at=now + timedelta(hours=2),
        override=now - timedelta(hours=1),
    )
    uid = await _make_user(pg_pool, "override_tighten")

    with pytest.raises(PredictionLocked):
        await insert_prediction(
            pg_pool,
            user_id=uid,
            show_date=show_date,
            pick_song_slugs=["all-in-time", "bridgeless", "hajimemashite"],
            encore_slug="bridgeless",
        )
