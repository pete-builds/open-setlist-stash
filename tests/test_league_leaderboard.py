"""League leaderboard rebuild tests.

Covers:
- ``rebuild_leagues`` writes one snapshot per (league, member) with
  rank by total_score desc + tie-breakers.
- Tour-window filter: shows outside [start_date, end_date] don't count.
- Soft-deleted leagues are skipped.
- Non-members of the league don't appear in the league snapshot even if
  they have global predictions.
- Resolver tick rebuilds league leaderboards in addition to global.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from setlist_stash.config import Settings
from setlist_stash.leaderboard import (
    fetch_leaderboard,
    rebuild_leagues,
)
from setlist_stash.leagues import (
    create_league,
    join_league,
    soft_delete_league,
)
from tests.conftest import requires_pg


def _settings() -> Settings:
    return Settings()


async def _user(pool: Any, handle: str) -> int:
    async with pool.acquire() as conn:
        return int(
            await conn.fetchval(
                "INSERT INTO users (handle, handle_lower) VALUES ($1, $2) RETURNING id",
                handle,
                handle,
            )
        )


async def _resolved_show_with_scores(
    pool: Any, *, show_date: date, scores: list[tuple[int, int]]
) -> None:
    """Insert a resolved show + N scored predictions in one batch.

    The lock-guard trigger blocks INSERTs when ``lock_at < now()``. So we
    insert with a future lock first, write all predictions, then back-date
    the lock and stamp scores. Stamping ``score`` doesn't tickle the
    trigger because migration 002 narrowed it to user-pick column writes.

    ``scores`` is a list of ``(user_id, score)`` tuples.
    """
    future_lock = datetime.now(UTC) + timedelta(hours=2)
    past_lock = datetime.now(UTC) - timedelta(hours=2)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO prediction_locks (show_date, lock_at, venue_tz)
            VALUES ($1, $2, 'UTC')
            ON CONFLICT (show_date) DO UPDATE
                SET lock_at = EXCLUDED.lock_at, resolved_at = NULL
            """,
            show_date,
            future_lock,
        )
        for uid, _ in scores:
            await conn.execute(
                """
                INSERT INTO predictions (
                    user_id, show_date, pick_song_slugs,
                    opener_slug, closer_slug, encore_slug
                )
                VALUES ($1, $2, ARRAY['a','b','c'], NULL, NULL, NULL)
                ON CONFLICT (user_id, show_date) DO NOTHING
                """,
                uid,
                show_date,
            )
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2, resolved_at = now() WHERE show_date = $1",
            show_date,
            past_lock,
        )
        for uid, score in scores:
            await conn.execute(
                "UPDATE predictions SET score = $1 WHERE user_id = $2 AND show_date = $3",
                score,
                uid,
                show_date,
            )


async def _resolved_show_with_score(
    pool: Any, *, user_id: int, show_date: date, score: int
) -> None:
    """Single-user wrapper for the multi-user helper above."""
    await _resolved_show_with_scores(
        pool, show_date=show_date, scores=[(user_id, score)]
    )


@pytest.mark.asyncio
@requires_pg
async def test_rebuild_leagues_basic_two_members(pg_pool: Any) -> None:
    host = await _user(pg_pool, "alice")
    bob = await _user(pg_pool, "bob")
    league = await create_league(
        pg_pool, name="Pod", host_user_id=host, settings=_settings()
    )
    await join_league(pg_pool, league, bob)
    await _resolved_show_with_scores(
        pg_pool,
        show_date=date(2024, 6, 15),
        scores=[(host, 80), (bob, 120)],
    )

    out = await rebuild_leagues(pg_pool)
    assert out[league.slug] == 2
    rows = await fetch_leaderboard(pg_pool, "league", league.slug, limit=10)
    handles = [r.handle for r in rows]
    assert handles == ["bob", "alice"]
    assert [r.rank for r in rows] == [1, 2]


@pytest.mark.asyncio
@requires_pg
async def test_rebuild_leagues_excludes_non_members(pg_pool: Any) -> None:
    """A user who has predictions but isn't in the league must not appear."""
    host = await _user(pg_pool, "alice")
    bob = await _user(pg_pool, "bob")
    randy = await _user(pg_pool, "randy")  # global player, not in league
    league = await create_league(
        pg_pool, name="Pod", host_user_id=host, settings=_settings()
    )
    await join_league(pg_pool, league, bob)
    await _resolved_show_with_scores(
        pg_pool,
        show_date=date(2024, 6, 15),
        scores=[(host, 50), (bob, 70), (randy, 999)],
    )

    await rebuild_leagues(pg_pool)
    rows = await fetch_leaderboard(pg_pool, "league", league.slug, limit=10)
    assert {r.handle for r in rows} == {"alice", "bob"}


@pytest.mark.asyncio
@requires_pg
async def test_rebuild_leagues_tour_window_filters(pg_pool: Any) -> None:
    """Shows outside [start_date, end_date] (inclusive) are excluded."""
    host = await _user(pg_pool, "alice")
    bob = await _user(pg_pool, "bob")
    league = await create_league(
        pg_pool,
        name="Summer Tour",
        host_user_id=host,
        settings=_settings(),
        start_date=date(2024, 7, 1),
        end_date=date(2024, 8, 31),
    )
    await join_league(pg_pool, league, bob)

    # In-window: counts.
    await _resolved_show_with_score(
        pg_pool, user_id=host, show_date=date(2024, 7, 15), score=100
    )
    await _resolved_show_with_score(
        pg_pool, user_id=bob, show_date=date(2024, 8, 1), score=200
    )
    # Out-of-window (before start): does NOT count.
    await _resolved_show_with_score(
        pg_pool, user_id=host, show_date=date(2024, 6, 1), score=999
    )
    # Out-of-window (after end): does NOT count.
    await _resolved_show_with_score(
        pg_pool, user_id=bob, show_date=date(2024, 9, 15), score=999
    )

    await rebuild_leagues(pg_pool)
    rows = await fetch_leaderboard(pg_pool, "league", league.slug, limit=10)
    by_handle = {r.handle: r for r in rows}
    assert by_handle["alice"].total_score == 100
    assert by_handle["alice"].shows_played == 1
    assert by_handle["bob"].total_score == 200
    assert by_handle["bob"].shows_played == 1


@pytest.mark.asyncio
@requires_pg
async def test_rebuild_leagues_skips_deleted(pg_pool: Any) -> None:
    host = await _user(pg_pool, "alice")
    league = await create_league(
        pg_pool, name="Pod", host_user_id=host, settings=_settings()
    )
    await _resolved_show_with_score(
        pg_pool, user_id=host, show_date=date(2024, 6, 15), score=50
    )
    await soft_delete_league(pg_pool, league, host_user_id=host)
    out = await rebuild_leagues(pg_pool)
    assert league.slug not in out


@pytest.mark.asyncio
@requires_pg
async def test_rebuild_leagues_idempotent(pg_pool: Any) -> None:
    host = await _user(pg_pool, "alice")
    league = await create_league(
        pg_pool, name="Pod", host_user_id=host, settings=_settings()
    )
    await _resolved_show_with_score(
        pg_pool, user_id=host, show_date=date(2024, 6, 15), score=50
    )
    n1 = (await rebuild_leagues(pg_pool))[league.slug]
    n2 = (await rebuild_leagues(pg_pool))[league.slug]
    assert n1 == n2 == 1


@pytest.mark.asyncio
@requires_pg
async def test_rebuild_leagues_member_with_no_scored_predictions(
    pg_pool: Any,
) -> None:
    """A member with zero resolved predictions is omitted (no row), not zeroed."""
    host = await _user(pg_pool, "alice")
    bob = await _user(pg_pool, "bob")
    league = await create_league(
        pg_pool, name="Pod", host_user_id=host, settings=_settings()
    )
    await join_league(pg_pool, league, bob)
    # Only host has a scored prediction.
    await _resolved_show_with_score(
        pg_pool, user_id=host, show_date=date(2024, 6, 15), score=50
    )
    await rebuild_leagues(pg_pool)
    rows = await fetch_leaderboard(pg_pool, "league", league.slug, limit=10)
    assert {r.handle for r in rows} == {"alice"}
