"""DB-backed tests for ``home_show_pointers`` (home-page live / recent links).

The helper drives two home-page entry points:

- **live**: the "watch the setlist" button that only appears while a show is
  actually playing (locked, unfinalized, inside ``LIVE_SHOW_WINDOW_HOURS``).
- **recent**: the "last show's setlist" link, which is what players reach for
  the morning after. Never the same show as ``live``.

Both read ``prediction_locks`` only — no upstream MCP call — so these tests
pin the SQL, including that ``lock_at_override`` wins over ``lock_at``.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import asyncpg

from setlist_stash.web_helpers import LIVE_SHOW_WINDOW_HOURS, home_show_pointers
from tests.conftest import requires_pg


async def _insert_lock(
    pool: asyncpg.Pool[Any],
    show_date: date,
    lock_at: datetime,
    *,
    resolved: bool = False,
    override: datetime | None = None,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO prediction_locks
                (show_date, lock_at, lock_at_override, resolved_at)
            VALUES ($1, $2, $3, $4)
            """,
            show_date,
            lock_at,
            override,
            datetime.now(UTC) if resolved else None,
        )


@requires_pg
async def test_empty_table_returns_no_pointers(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    assert await home_show_pointers(pg_pool) == (None, None)


@requires_pg
async def test_show_in_progress_is_live(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """Locked an hour ago, not finalized => the band is on stage."""
    assert pg_pool is not None
    await _insert_lock(
        pg_pool, date(2026, 7, 24), datetime.now(UTC) - timedelta(hours=1)
    )
    live, recent = await home_show_pointers(pg_pool)
    assert live == date(2026, 7, 24)
    # The only show on the board is the live one, so there is no "last show".
    assert recent is None


@requires_pg
async def test_pre_lock_show_is_not_live(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    await _insert_lock(
        pg_pool, date(2030, 1, 1), datetime.now(UTC) + timedelta(hours=3)
    )
    assert await home_show_pointers(pg_pool) == (None, None)


@requires_pg
async def test_finalized_show_is_not_live_but_is_recent(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """A resolved show is over: it drops off live and becomes the recent one."""
    assert pg_pool is not None
    await _insert_lock(
        pg_pool,
        date(2026, 7, 23),
        datetime.now(UTC) - timedelta(hours=2),
        resolved=True,
    )
    live, recent = await home_show_pointers(pg_pool)
    assert live is None
    assert recent == date(2026, 7, 23)


@requires_pg
async def test_stale_unresolved_show_falls_out_of_the_live_window(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """The resolver can lag for hours; the live badge must still time out."""
    assert pg_pool is not None
    stale = datetime.now(UTC) - timedelta(hours=LIVE_SHOW_WINDOW_HOURS + 1)
    await _insert_lock(pg_pool, date(2026, 7, 22), stale)
    live, recent = await home_show_pointers(pg_pool)
    assert live is None
    assert recent == date(2026, 7, 22)


@requires_pg
async def test_recent_excludes_the_live_show_and_picks_the_newest_past(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    now = datetime.now(UTC)
    await _insert_lock(pg_pool, date(2026, 7, 20), now - timedelta(days=4))
    await _insert_lock(pg_pool, date(2026, 7, 22), now - timedelta(days=2))
    # Playing right now.
    await _insert_lock(pg_pool, date(2026, 7, 24), now - timedelta(hours=1))
    # Announced, not yet locked.
    await _insert_lock(pg_pool, date(2026, 8, 1), now + timedelta(days=8))
    live, recent = await home_show_pointers(pg_pool)
    assert live == date(2026, 7, 24)
    assert recent == date(2026, 7, 22)


@requires_pg
async def test_lock_at_override_wins(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """An operator override pushing lock into the future un-lives the show."""
    assert pg_pool is not None
    now = datetime.now(UTC)
    await _insert_lock(
        pg_pool,
        date(2026, 7, 24),
        now - timedelta(hours=1),
        override=now + timedelta(hours=2),
    )
    assert await home_show_pointers(pg_pool) == (None, None)
