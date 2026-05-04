"""asyncpg connection pool + lifecycle helpers.

Single global pool, created at app startup, closed at shutdown. Keep the
surface small — modules that need DB access call ``get_pool()`` and use
``async with pool.acquire() as conn:`` directly.
"""

from __future__ import annotations

import logging
from typing import Any

import asyncpg

from tweezer_picks.config import Settings

logger = logging.getLogger("tweezer_picks.db")

_pool: asyncpg.Pool[Any] | None = None


async def init_pool(settings: Settings) -> asyncpg.Pool[Any]:
    """Create the global asyncpg pool. Idempotent."""
    global _pool
    if _pool is not None:
        return _pool
    _pool = await asyncpg.create_pool(
        dsn=settings.pg_dsn,
        min_size=1,
        max_size=10,
        command_timeout=10.0,
    )
    if _pool is None:  # asyncpg type hint says Optional
        raise RuntimeError("asyncpg.create_pool returned None")
    logger.info("tweezer-picks db pool ready", extra={"db": settings.pg_db})
    return _pool


async def close_pool() -> None:
    """Close the global pool (called from FastAPI shutdown)."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("tweezer-picks db pool closed")


def get_pool() -> asyncpg.Pool[Any]:
    """Return the global pool. Raises if not initialized."""
    if _pool is None:
        raise RuntimeError("db pool not initialized — init_pool() first")
    return _pool
