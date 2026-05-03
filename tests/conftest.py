"""Shared pytest fixtures.

Route + DB tests need a real Postgres. We don't spin one up in the test
process; instead we look for ``TEST_PG_DSN`` in the env. If absent, the DB
fixtures yield None and the marked tests skip.

To run the full suite against the live nix1 phish-game-pg container:

    ssh -L 5434:127.0.0.1:5434 pete@192.168.86.20 -N &
    export TEST_PG_DSN="postgresql://phish_game:<pw>@127.0.0.1:5434/phish_game_test"
    psql "$TEST_PG_DSN" -c 'SELECT 1'   # pre-create the test DB
    pytest

Or against any local Postgres with the migrations applied. The fixtures
truncate the relevant tables between tests, so the DB does NOT need to be
ephemeral — it only needs to be writable and to have the schema.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any

import asyncpg
import pytest
import pytest_asyncio


def _test_pg_dsn() -> str | None:
    return os.getenv("TEST_PG_DSN")


@pytest_asyncio.fixture
async def pg_pool() -> AsyncIterator[asyncpg.Pool[Any] | None]:
    dsn = _test_pg_dsn()
    if not dsn:
        yield None
        return
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=3)
    if pool is None:
        raise RuntimeError("asyncpg.create_pool returned None")
    # Ensure schema is present. We import the migrate module from the
    # package and run it. Tests run in development; safe.
    from phish_game.migrate import run_migrations
    await run_migrations(pool)
    # Truncate game tables before each test for a clean slate. We don't
    # touch schema_version.
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE predictions, prediction_locks, users, "
            "leaderboard_snapshots, scoring_runs RESTART IDENTITY CASCADE"
        )
    try:
        yield pool
    finally:
        await pool.close()


def requires_pg(fn: Any) -> Any:
    """Decorator: skip a test if TEST_PG_DSN is not set."""
    return pytest.mark.skipif(
        _test_pg_dsn() is None,
        reason="set TEST_PG_DSN to run DB-backed tests",
    )(fn)
