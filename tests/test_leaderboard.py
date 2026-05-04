"""Leaderboard rebuilder + read tests.

DB-backed via the ``pg_pool`` fixture; skipped when ``TEST_PG_DSN`` is unset.

Coverage:
- Rebuilders create rows for each scope with correct ranking.
- Ties broken by earlier ``submitted_at`` first, then handle.
- Cancelled-show predictions (score=0) count toward shows_played but add 0
  to total_score.
- ``rebuild_all`` runs all three scopes and returns counts per scope.
- ``derive_season_key`` covers boundary months including the Jan/Feb
  rollback to the previous winter.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from tweezer_picks.leaderboard import (
    derive_season_key,
    fetch_leaderboard,
    fetch_user_rank,
    list_scope_keys,
    normalize_scope,
    rebuild_all,
    rebuild_all_time,
    rebuild_season,
    rebuild_weekly,
)
from tests.conftest import requires_pg

# ----- pure unit tests (always run) -----------------------------------------


def test_derive_season_key_spring() -> None:
    assert derive_season_key(date(2026, 4, 15)) == "2026-spring"


def test_derive_season_key_summer() -> None:
    assert derive_season_key(date(2026, 7, 1)) == "2026-summer"


def test_derive_season_key_fall() -> None:
    assert derive_season_key(date(2026, 10, 31)) == "2026-fall"


def test_derive_season_key_december_winter() -> None:
    assert derive_season_key(date(2026, 12, 5)) == "2026-winter"


def test_derive_season_key_january_rolls_back_to_prior_winter() -> None:
    assert derive_season_key(date(2027, 1, 10)) == "2026-winter"


def test_derive_season_key_february_rolls_back() -> None:
    assert derive_season_key(date(2027, 2, 28)) == "2026-winter"


def test_normalize_scope_aliases() -> None:
    assert normalize_scope("weekly") == "weekly"
    assert normalize_scope("week") == "weekly"
    assert normalize_scope("tour") == "tour"
    assert normalize_scope("season") == "tour"
    assert normalize_scope("all-time") == "all_time"
    assert normalize_scope("all_time") == "all_time"
    assert normalize_scope("alltime") == "all_time"


# ----- DB-backed tests ------------------------------------------------------


async def _seed_users_and_show(pool: Any, show_date: date) -> dict[str, int]:
    """Helper: create three users and one resolved prediction_locks row."""
    past_lock = datetime.now(UTC) - timedelta(days=2)
    user_ids: dict[str, int] = {}
    async with pool.acquire() as conn:
        for h in ("alice", "bob", "carol"):
            row = await conn.fetchrow(
                "INSERT INTO users (handle, handle_lower) VALUES ($1, $2) RETURNING id",
                h,
                h,
            )
            assert row is not None
            user_ids[h] = int(row["id"])
        # Future lock first so inserts pass the trigger; then back-date.
        future_lock = datetime.now(UTC) + timedelta(hours=2)
        await conn.execute(
            """
            INSERT INTO prediction_locks (show_date, lock_at, venue_tz)
            VALUES ($1, $2, 'UTC')
            """,
            show_date,
            future_lock,
        )
        # Bob earliest, alice next, carol last — to test tie-breaking.
        for handle, submitted in (
            ("bob", datetime.now(UTC) - timedelta(hours=4)),
            ("alice", datetime.now(UTC) - timedelta(hours=3)),
            ("carol", datetime.now(UTC) - timedelta(hours=2)),
        ):
            await conn.execute(
                """
                INSERT INTO predictions (
                    user_id, show_date, pick_song_slugs,
                    opener_slug, closer_slug, encore_slug, submitted_at
                )
                VALUES ($1, $2, ARRAY['x','y','z'], NULL, NULL, NULL, $3)
                """,
                user_ids[handle],
                show_date,
                submitted,
            )
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2, resolved_at = now() WHERE show_date = $1",
            show_date,
            past_lock,
        )
    return user_ids


async def _stamp_score(pool: Any, user_id: int, show_date: date, score: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE predictions SET score = $1 WHERE user_id = $2 AND show_date = $3",
            score,
            user_id,
            show_date,
        )


@pytest.mark.asyncio
@requires_pg
async def test_rebuild_all_time_ranks_by_score(pg_pool: Any) -> None:
    show_date = date(2024, 6, 15)
    user_ids = await _seed_users_and_show(pg_pool, show_date)
    await _stamp_score(pg_pool, user_ids["alice"], show_date, 80)
    await _stamp_score(pg_pool, user_ids["bob"], show_date, 120)
    await _stamp_score(pg_pool, user_ids["carol"], show_date, 40)

    written = await rebuild_all_time(pg_pool)
    assert written == 3

    rows = await fetch_leaderboard(pg_pool, "all_time", "all", limit=10)
    assert [r.handle for r in rows] == ["bob", "alice", "carol"]
    assert [r.rank for r in rows] == [1, 2, 3]
    assert [r.total_score for r in rows] == [120, 80, 40]
    for r in rows:
        assert r.shows_played == 1


@pytest.mark.asyncio
@requires_pg
async def test_rebuild_all_time_ties_break_by_submitted_at_then_handle(
    pg_pool: Any,
) -> None:
    show_date = date(2024, 6, 16)
    user_ids = await _seed_users_and_show(pg_pool, show_date)
    # Same score for everyone.
    for h in ("alice", "bob", "carol"):
        await _stamp_score(pg_pool, user_ids[h], show_date, 50)

    await rebuild_all_time(pg_pool)
    rows = await fetch_leaderboard(pg_pool, "all_time", "all", limit=10)
    # Tie-break: bob submitted earliest, then alice, then carol.
    assert [r.handle for r in rows] == ["bob", "alice", "carol"]
    # RANK() gives 1,1,1 with full ties. We use stable ORDER BY but Postgres
    # RANK still allows tied ranks. Check ranks are non-decreasing.
    ranks = [r.rank for r in rows]
    assert ranks == sorted(ranks)


@pytest.mark.asyncio
@requires_pg
async def test_rebuild_weekly_buckets_by_iso_week(pg_pool: Any) -> None:
    # Two shows in different ISO weeks.
    week_a = date(2024, 6, 10)  # ISO 2024-W24
    week_b = date(2024, 6, 17)  # ISO 2024-W25
    ids_a = await _seed_users_and_show(pg_pool, week_a)
    await _stamp_score(pg_pool, ids_a["alice"], week_a, 100)
    # New users for the second week to keep things isolated.
    async with pg_pool.acquire() as conn:
        bob2 = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ('bobtwo','bobtwo') RETURNING id"
        )
        future_lock = datetime.now(UTC) + timedelta(hours=2)
        past_lock = datetime.now(UTC) - timedelta(days=1)
        await conn.execute(
            "INSERT INTO prediction_locks (show_date, lock_at, venue_tz) VALUES ($1, $2, 'UTC')",
            week_b,
            future_lock,
        )
        await conn.execute(
            """
            INSERT INTO predictions (
                user_id, show_date, pick_song_slugs,
                opener_slug, closer_slug, encore_slug
            )
            VALUES ($1, $2, ARRAY['a','b','c'], NULL, NULL, NULL)
            """,
            int(bob2),
            week_b,
        )
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2, resolved_at = now() WHERE show_date = $1",
            week_b,
            past_lock,
        )
        await conn.execute(
            "UPDATE predictions SET score = 75 WHERE user_id = $1 AND show_date = $2",
            int(bob2),
            week_b,
        )

    await rebuild_weekly(pg_pool)
    keys = await list_scope_keys(pg_pool, "weekly")
    assert "2024-W24" in keys
    assert "2024-W25" in keys

    rows_a = await fetch_leaderboard(pg_pool, "weekly", "2024-W24", limit=10)
    rows_b = await fetch_leaderboard(pg_pool, "weekly", "2024-W25", limit=10)
    assert any(r.handle == "alice" for r in rows_a)
    assert all(r.handle != "alice" for r in rows_b)


@pytest.mark.asyncio
@requires_pg
async def test_rebuild_season_buckets_by_meteorological_season(pg_pool: Any) -> None:
    summer = date(2024, 7, 4)
    fall = date(2024, 10, 31)
    ids_s = await _seed_users_and_show(pg_pool, summer)
    await _stamp_score(pg_pool, ids_s["alice"], summer, 90)
    async with pg_pool.acquire() as conn:
        carol2 = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ('caroltwo','caroltwo') RETURNING id"
        )
        future_lock = datetime.now(UTC) + timedelta(hours=2)
        past_lock = datetime.now(UTC) - timedelta(days=1)
        await conn.execute(
            "INSERT INTO prediction_locks (show_date, lock_at, venue_tz) VALUES ($1, $2, 'UTC')",
            fall,
            future_lock,
        )
        await conn.execute(
            """
            INSERT INTO predictions (
                user_id, show_date, pick_song_slugs,
                opener_slug, closer_slug, encore_slug
            )
            VALUES ($1, $2, ARRAY['a','b','c'], NULL, NULL, NULL)
            """,
            int(carol2),
            fall,
        )
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2, resolved_at = now() WHERE show_date = $1",
            fall,
            past_lock,
        )
        await conn.execute(
            "UPDATE predictions SET score = 30 WHERE user_id = $1 AND show_date = $2",
            int(carol2),
            fall,
        )

    await rebuild_season(pg_pool)
    keys = await list_scope_keys(pg_pool, "tour")
    assert "2024-summer" in keys
    assert "2024-fall" in keys


@pytest.mark.asyncio
@requires_pg
async def test_cancelled_predictions_count_for_shows_played(pg_pool: Any) -> None:
    """score=0 (cancelled-show sentinel) should add to shows_played but 0 to total."""
    show_a = date(2024, 8, 1)
    show_b = date(2024, 8, 8)
    ids_a = await _seed_users_and_show(pg_pool, show_a)
    await _stamp_score(pg_pool, ids_a["alice"], show_a, 50)
    await _stamp_score(pg_pool, ids_a["bob"], show_a, 50)
    await _stamp_score(pg_pool, ids_a["carol"], show_a, 50)
    # Add a cancelled show — only alice plays.
    async with pg_pool.acquire() as conn:
        future_lock = datetime.now(UTC) + timedelta(hours=2)
        past_lock = datetime.now(UTC) - timedelta(days=1)
        await conn.execute(
            "INSERT INTO prediction_locks (show_date, lock_at, venue_tz) VALUES ($1, $2, 'UTC')",
            show_b,
            future_lock,
        )
        await conn.execute(
            """
            INSERT INTO predictions (
                user_id, show_date, pick_song_slugs,
                opener_slug, closer_slug, encore_slug
            )
            VALUES ($1, $2, ARRAY['a','b','c'], NULL, NULL, NULL)
            """,
            ids_a["alice"],
            show_b,
        )
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2, resolved_at = now() WHERE show_date = $1",
            show_b,
            past_lock,
        )
        await conn.execute(
            "UPDATE predictions SET score = 0 WHERE user_id = $1 AND show_date = $2",
            ids_a["alice"],
            show_b,
        )

    await rebuild_all_time(pg_pool)
    rows = await fetch_leaderboard(pg_pool, "all_time", "all", limit=10)
    by_handle = {r.handle: r for r in rows}
    assert by_handle["alice"].total_score == 50
    assert by_handle["alice"].shows_played == 2
    assert by_handle["bob"].shows_played == 1


@pytest.mark.asyncio
@requires_pg
async def test_rebuild_all_runs_every_scope(pg_pool: Any) -> None:
    show_date = date(2024, 9, 21)
    ids = await _seed_users_and_show(pg_pool, show_date)
    await _stamp_score(pg_pool, ids["alice"], show_date, 100)

    counts = await rebuild_all(pg_pool)
    assert counts["weekly"] >= 1
    assert counts["tour"] >= 1
    assert counts["all_time"] >= 1
    # No exception keys.
    assert all(v >= 0 for v in counts.values())


@pytest.mark.asyncio
@requires_pg
async def test_fetch_user_rank_returns_none_when_absent(pg_pool: Any) -> None:
    # Empty DB after fixture truncate.
    row = await fetch_user_rank(pg_pool, "all_time", "all", user_id=999)
    assert row is None


@pytest.mark.asyncio
@requires_pg
async def test_unscored_predictions_are_excluded(pg_pool: Any) -> None:
    """Predictions with score=NULL must not appear in any scope."""
    show_date = date(2024, 10, 5)
    ids = await _seed_users_and_show(pg_pool, show_date)
    await _stamp_score(pg_pool, ids["alice"], show_date, 30)
    # bob & carol left at NULL score.
    await rebuild_all_time(pg_pool)
    rows = await fetch_leaderboard(pg_pool, "all_time", "all", limit=10)
    handles = [r.handle for r in rows]
    assert "alice" in handles
    assert "bob" not in handles
    assert "carol" not in handles


@pytest.mark.asyncio
@requires_pg
async def test_rebuild_is_idempotent(pg_pool: Any) -> None:
    """Running rebuild twice yields the same row count."""
    show_date = date(2024, 11, 11)
    ids = await _seed_users_and_show(pg_pool, show_date)
    await _stamp_score(pg_pool, ids["alice"], show_date, 60)
    n1 = await rebuild_all_time(pg_pool)
    n2 = await rebuild_all_time(pg_pool)
    assert n1 == n2


# ----- resolver integration -------------------------------------------------


def _row(pos: int, set_name: str, slug: str) -> dict[str, Any]:
    return {"position": pos, "set_name": set_name, "song_slug": slug, "song_title": slug}


class _FakeMcpForLeaderboard:
    """Minimal mcp stub that returns a published setlist for a single show."""

    def __init__(self) -> None:
        self.shows = {
            "2024-04-15": {
                "setlist": [
                    _row(1, "Set 1", "tweezer"),
                    _row(2, "Set 2", "harry-hood"),
                    _row(3, "Encore", "loving-cup"),
                ]
            }
        }
        self.songs = {
            "tweezer": {"slug": "tweezer", "gap_current": 5, "times_played": 400},
            "harry-hood": {"slug": "harry-hood", "gap_current": 10, "times_played": 300},
            "loving-cup": {"slug": "loving-cup", "gap_current": 25, "times_played": 100},
        }

    async def __aenter__(self) -> _FakeMcpForLeaderboard:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def get_show(self, date_or_id: str) -> dict[str, Any]:
        from tweezer_picks.mcp_client import McpPhishNotFound
        if date_or_id not in self.shows:
            raise McpPhishNotFound(date_or_id)
        return self.shows[date_or_id]

    async def get_song(self, slug: str) -> dict[str, Any]:
        from tweezer_picks.mcp_client import McpPhishNotFound
        if slug not in self.songs:
            raise McpPhishNotFound(slug)
        return self.songs[slug]


@pytest.mark.asyncio
@requires_pg
async def test_resolver_tick_rebuilds_leaderboard(
    pg_pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A resolver tick that resolves >=1 show should leave leaderboard rows behind."""
    from tweezer_picks import db, resolve
    from tweezer_picks.config import Settings

    monkeypatch.setattr(db, "_pool", pg_pool)

    show_date = date(2024, 4, 15)
    future_lock = datetime.now(UTC) + timedelta(hours=2)
    past_lock = datetime.now(UTC) - timedelta(hours=2)
    async with pg_pool.acquire() as conn:
        uid = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ('integ', 'integ') RETURNING id"
        )
        await conn.execute(
            "INSERT INTO prediction_locks (show_date, lock_at, venue_tz) VALUES ($1, $2, 'UTC')",
            show_date,
            future_lock,
        )
        await conn.execute(
            """
            INSERT INTO predictions (
                user_id, show_date, pick_song_slugs,
                opener_slug, closer_slug, encore_slug
            )
            VALUES ($1, $2, ARRAY['harry-hood','loving-cup','tweezer'],
                    'tweezer', 'harry-hood', 'loving-cup')
            """,
            int(uid),
            show_date,
        )
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2 WHERE show_date = $1",
            show_date,
            past_lock,
        )

    monkeypatch.setattr(resolve, "McpPhishClient", lambda *a, **kw: _FakeMcpForLeaderboard())

    settings = Settings(
        mcp_phish_url="http://test/mcp",
        mcp_phish_timeout_seconds=2.0,
        resolver_cancel_after_hours=72,
        resolver_interval_seconds=60,
    )
    result = await resolve.run_tick(settings)
    assert result.status == "success"
    assert result.shows_resolved == 1

    # Leaderboard rows now exist for all three scopes.
    rows_at = await fetch_leaderboard(pg_pool, "all_time", "all", limit=10)
    assert len(rows_at) == 1
    assert rows_at[0].handle == "integ"
    assert rows_at[0].rank == 1
    assert rows_at[0].total_score > 0

    weekly_keys = await list_scope_keys(pg_pool, "weekly")
    assert any(k.startswith("2024-W") for k in weekly_keys)

    season_keys = await list_scope_keys(pg_pool, "tour")
    assert "2024-spring" in season_keys
