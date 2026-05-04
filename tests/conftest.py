"""Shared pytest fixtures.

Route + DB tests need a real Postgres. We don't spin one up in the test
process; instead we look for ``TEST_PG_DSN`` in the env. If absent, the DB
fixtures yield None and the marked tests skip.

To run the full suite against the live nix1 setlist-stash-pg container:

    ssh -L 5434:127.0.0.1:5434 pete@192.168.86.20 -N &
    export TEST_PG_DSN="postgresql://setlist_stash:<pw>@127.0.0.1:5434/setlist_stash_test"
    psql "$TEST_PG_DSN" -c 'SELECT 1'   # pre-create the test DB
    pytest

Or against any local Postgres with the migrations applied. The fixtures
truncate the relevant tables between tests, so the DB does NOT need to be
ephemeral; it only needs to be writable and to have the schema.

Route-test gotcha (fixed in build session 7):
    Starlette's ``TestClient`` runs the FastAPI lifespan via
    ``anyio.from_thread.BlockingPortal`` on a *different* event loop than the
    pytest-asyncio one that owns the asyncpg pool. The result is the
    notorious ``got Future <...> attached to a different loop`` error.

    The fix is ``httpx.AsyncClient(transport=ASGITransport(app))``: it runs
    the ASGI app inline on the test's loop. The ``async_client`` fixture
    builds the app with the test pool injected and skips the lifespan
    (migrations are already applied by ``pg_pool``).
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any

import asyncpg
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient


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
    from setlist_stash.migrate import run_migrations
    await run_migrations(pool)
    # Truncate game tables before each test for a clean slate. We don't
    # touch schema_version. ``league_members`` is CASCADEd from leagues +
    # users, but list it explicitly for clarity.
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE league_members, leagues, predictions, prediction_locks, "
            "users, leaderboard_snapshots, scoring_runs RESTART IDENTITY CASCADE"
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


def build_app_with_pool(pool: asyncpg.Pool[Any]) -> FastAPI:
    """Build a FastAPI app with the test pool already injected.

    Critically, we do NOT enter the app's lifespan (which would try to create
    its own pool on a fresh event loop). Migrations have already been applied
    by the ``pg_pool`` fixture, and the pool is shared via the ``db`` module's
    private global.
    """
    from setlist_stash import db as db_module
    from setlist_stash.config import get_settings
    from setlist_stash.server import build_app
    db_module._pool = pool  # type: ignore[attr-defined]
    return build_app(get_settings())


@pytest_asyncio.fixture
async def async_client(
    pg_pool: asyncpg.Pool[Any] | None,
) -> AsyncIterator[AsyncClient]:
    """An ``httpx.AsyncClient`` wired to the FastAPI app via ASGITransport.

    Unlike Starlette's ``TestClient``, this runs the ASGI app on the same
    event loop as the test (and the asyncpg pool). The lifespan is skipped
    (``lifespan='off'``) because the test pool is already set up.

    Tests requiring a DB should depend on both ``pg_pool`` (skip gate +
    schema) and ``async_client``.
    """
    if pg_pool is None:
        # The test will skip via @requires_pg before hitting this client,
        # but we still need to yield something to satisfy the fixture
        # protocol when the test isn't gated on the pool.
        async with AsyncClient(base_url="http://test") as client:
            yield client
        return
    app = build_app_with_pool(pg_pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://test"
    ) as client:
        yield client
