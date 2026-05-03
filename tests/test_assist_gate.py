"""Unit tests for the ``assist_allowed`` gate.

Pure-DB checks (no HTTP). The gate is the single source of truth for the
smart-pick assist policy from PHASE-4-PLAN.md §7. Three branches:

1. ASSIST_PRE_LOCK=true (dev override): always returns True.
2. No prediction_locks row + override off: returns False (closed default).
3. lock_at in the past: returns True. lock_at in the future: returns False.

Lock_at_override is honored: if it's set, it wins over lock_at.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import asyncpg

from phish_game.config import get_settings
from phish_game.locks import assist_allowed
from tests.conftest import requires_pg


def _settings_with(assist: bool) -> Any:
    base = get_settings()
    return base.model_copy(update={"assist_pre_lock": assist})


@requires_pg
async def test_assist_gate_dev_override_always_open(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    show_date = date(2030, 1, 1)
    settings = _settings_with(True)
    # No row in prediction_locks; override should still open the gate.
    assert await assist_allowed(pg_pool, show_date, settings) is True


@requires_pg
async def test_assist_gate_no_lock_row_closed(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    show_date = date(2030, 1, 1)
    settings = _settings_with(False)
    assert await assist_allowed(pg_pool, show_date, settings) is False


@requires_pg
async def test_assist_gate_pre_lock_closed(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    show_date = date(2030, 1, 1)
    settings = _settings_with(False)
    future = datetime.now(UTC) + timedelta(hours=3)
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO prediction_locks (show_date, lock_at) VALUES ($1, $2)",
            show_date,
            future,
        )
    assert await assist_allowed(pg_pool, show_date, settings) is False


@requires_pg
async def test_assist_gate_post_lock_open(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    show_date = date(2020, 1, 1)
    settings = _settings_with(False)
    past = datetime.now(UTC) - timedelta(hours=3)
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO prediction_locks (show_date, lock_at) VALUES ($1, $2)",
            show_date,
            past,
        )
    assert await assist_allowed(pg_pool, show_date, settings) is True


@requires_pg
async def test_assist_gate_override_takes_precedence(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """``lock_at_override`` (operator override) wins over ``lock_at``."""
    assert pg_pool is not None
    show_date = date(2030, 6, 15)
    settings = _settings_with(False)
    # lock_at far in the past — would normally open the gate ...
    far_past = datetime.now(UTC) - timedelta(days=30)
    # ... but override pushes it to the future.
    future_override = datetime.now(UTC) + timedelta(hours=2)
    async with pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO prediction_locks (show_date, lock_at, lock_at_override)
            VALUES ($1, $2, $3)
            """,
            show_date,
            far_past,
            future_override,
        )
    # Override is in the future, so we are pre-lock => gate is closed.
    assert await assist_allowed(pg_pool, show_date, settings) is False
