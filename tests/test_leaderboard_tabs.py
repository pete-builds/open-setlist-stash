"""Per-deployment leaderboard tabs and the ``run`` scope.

Both are deployment-level config (oss-platform-split): which boards a game
shows, and which named runs of shows get their own board, are editorial calls
that differ per tenant. The platform default must be untouched so the OSS
image, any third-party self-host, and the Phish demo keep the original
Weekly / Season / All-time bar unless they opt in.

The parser tests are pure; ``test_rebuild_runs_*`` are DB-backed and skip
unless ``TEST_PG_DSN`` is set (see conftest).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import asyncpg

from setlist_stash.leaderboard import (
    DEFAULT_TABS,
    LeaderboardTab,
    fetch_leaderboard,
    parse_runs,
    parse_tabs,
    rebuild_runs,
)
from tests.conftest import requires_pg

MSG = "msg-summer-26=2026-07-22,2026-07-24,2026-07-25,2026-07-27,2026-07-29"


# ----- parse_runs -----------------------------------------------------------


def test_parse_runs_reads_a_residency() -> None:
    assert parse_runs(MSG) == {
        "msg-summer-26": [
            date(2026, 7, 22),
            date(2026, 7, 24),
            date(2026, 7, 25),
            date(2026, 7, 27),
            date(2026, 7, 29),
        ]
    }


def test_parse_runs_handles_multiple_runs() -> None:
    parsed = parse_runs(f"{MSG};fenway-26=2026-07-31,2026-08-01")
    assert set(parsed) == {"msg-summer-26", "fenway-26"}
    assert parsed["fenway-26"] == [date(2026, 7, 31), date(2026, 8, 1)]


def test_parse_runs_dedupes_and_sorts() -> None:
    assert parse_runs("r=2026-07-25,2026-07-22,2026-07-25") == {
        "r": [date(2026, 7, 22), date(2026, 7, 25)]
    }


def test_parse_runs_is_fail_soft() -> None:
    """A malformed run is dropped; valid siblings survive.

    A typo in one env var must not take the deployment down, and must not
    silently poison the runs that parsed fine.
    """
    assert parse_runs("") == {}
    assert parse_runs("noequals") == {}
    assert parse_runs("BAD KEY=2026-07-22") == {}
    assert parse_runs("r=not-a-date") == {}
    assert parse_runs(f"r=nope;{MSG}") == {
        "msg-summer-26": [
            date(2026, 7, 22),
            date(2026, 7, 24),
            date(2026, 7, 25),
            date(2026, 7, 27),
            date(2026, 7, 29),
        ]
    }


# ----- parse_tabs -----------------------------------------------------------


def test_parse_tabs_empty_keeps_platform_default() -> None:
    """The whole point of the default: existing tenants are unchanged."""
    assert parse_tabs("") == DEFAULT_TABS
    assert parse_tabs("   ") == DEFAULT_TABS


def test_parse_tabs_reads_petes_bar() -> None:
    spec = (
        "Summer Tour|tour|2026-summer,"
        "MSG Summer 26|run|msg-summer-26,"
        "All Time|all_time,"
        "Fall Tour|tour|2026-fall"
    )
    assert parse_tabs(spec) == (
        LeaderboardTab("Summer Tour", "tour", "2026-summer"),
        LeaderboardTab("MSG Summer 26", "run", "msg-summer-26"),
        LeaderboardTab("All Time", "all_time", None),
        LeaderboardTab("Fall Tour", "tour", "2026-fall"),
    )


def test_parse_tabs_two_tabs_may_share_a_scope() -> None:
    """Summer and Fall both ride ``tour``; only the pinned key separates them."""
    tabs = parse_tabs("Summer|tour|2026-summer,Fall|tour|2026-fall")
    assert [t.scope for t in tabs] == ["tour", "tour"]
    assert [t.scope_key for t in tabs] == ["2026-summer", "2026-fall"]


def test_parse_tabs_drops_bad_entries_but_keeps_good_ones() -> None:
    tabs = parse_tabs("Good|all_time,Nope|not_a_scope,Alsobad,|tour")
    assert tabs == (LeaderboardTab("Good", "all_time", None),)


def test_parse_tabs_all_bad_falls_back_to_default() -> None:
    """Never render a leaderboard with an empty tab bar."""
    assert parse_tabs("Nope|not_a_scope") == DEFAULT_TABS


# ----- rebuild_runs (DB) ----------------------------------------------------


async def _seed(pool: asyncpg.Pool[Any], handle: str, days: dict[date, int]) -> None:
    async with pool.acquire() as conn:
        uid = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ($1, lower($1)) "
            "RETURNING id",
            handle,
        )
        for show_date, score in days.items():
            await conn.execute(
                "INSERT INTO prediction_locks (show_date, lock_at) VALUES ($1, $2) "
                "ON CONFLICT (show_date) DO NOTHING",
                show_date,
                datetime.now(UTC) + timedelta(days=365),
            )
            await conn.execute(
                "INSERT INTO predictions "
                "(user_id, show_date, pick_song_slugs, encore_slug, score) "
                "VALUES ($1, $2, $3, $4, $5)",
                uid,
                show_date,
                ["a", "b"],
                "a",
                score,
            )


@requires_pg
async def test_rebuild_runs_counts_only_the_runs_dates(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """A run board is the run's shows and nothing else.

    The contrast that matters: a show OUTSIDE the run is scored in the same
    season, so a board that merely bucketed by season would sweep it in.
    """
    assert pg_pool is not None
    in_run = date(2026, 7, 22)
    also_in_run = date(2026, 7, 24)
    outside = date(2026, 7, 7)  # same summer, not the residency
    await _seed(pg_pool, "runner", {in_run: 10, also_in_run: 5, outside: 100})

    written = await rebuild_runs(pg_pool, {"msg-summer-26": [in_run, also_in_run]})
    assert written == 1

    rows = await fetch_leaderboard(pg_pool, "run", "msg-summer-26", limit=10)
    assert len(rows) == 1
    # 10 + 5 only. The 100-point outside show must not appear.
    assert rows[0].total_score == 15
    assert rows[0].shows_played == 2


@requires_pg
async def test_rebuild_runs_keeps_runs_separate(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """Two runs rebuilt in one pass must not clobber each other.

    ``_rebuild_bucketed`` deletes by scope, so building runs one call at a
    time would leave only the last one standing. This is that regression.
    """
    assert pg_pool is not None
    await _seed(
        pg_pool,
        "twofer",
        {date(2026, 7, 22): 10, date(2026, 7, 31): 7},
    )

    await rebuild_runs(
        pg_pool,
        {
            "msg-summer-26": [date(2026, 7, 22)],
            "fenway-26": [date(2026, 7, 31)],
        },
    )

    msg = await fetch_leaderboard(pg_pool, "run", "msg-summer-26", limit=10)
    fenway = await fetch_leaderboard(pg_pool, "run", "fenway-26", limit=10)
    assert [r.total_score for r in msg] == [10]
    assert [r.total_score for r in fenway] == [7]


@requires_pg
async def test_rebuild_runs_noop_without_config(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """No configured runs means no run rows — the OSS/self-host default."""
    assert pg_pool is not None
    await _seed(pg_pool, "nobody", {date(2026, 7, 22): 10})
    assert await rebuild_runs(pg_pool, {}) == 0
    assert await fetch_leaderboard(pg_pool, "run", "msg-summer-26", limit=10) == []
