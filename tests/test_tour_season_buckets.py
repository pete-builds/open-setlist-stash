"""Season buckets derived from the real upstream tour, not the calendar month.

The bug these pin: ``rebuild_season`` bucketed purely on the show's month, so
Phish's Dick's Labor Day run (2026-09-04..06) landed in ``2026-fall`` even
though phish.net tags it "2026 Summer Tour". A Fall Tour tab pinned to that
bucket therefore rendered the Dick's standings and looked perfectly healthy
doing it.

The fallback matters as much as the fix. The Umphrey's tenant's MCP returns
the placeholder "No Tour Name" for every show, so on that deployment every
``tour_season_key`` stays NULL and the season boards must keep behaving
exactly as they did before this change.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from setlist_stash.leaderboard import (
    derive_season_key,
    derive_tour_season_key,
    list_scope_keys,
    rebuild_season,
)
from tests.conftest import requires_pg

# ----- pure unit tests (always run) -----------------------------------------

DICKS = date(2026, 9, 4)


def test_the_regression_dicks_is_summer_tour_despite_a_september_date() -> None:
    """The exact case that broke the Fall Tour tab."""
    assert derive_season_key(DICKS) == "2026-fall"
    assert derive_tour_season_key("2026 Summer Tour", DICKS) == "2026-summer"


def test_fall_tour_still_resolves_to_fall() -> None:
    assert derive_tour_season_key("2026 Fall Tour", date(2026, 10, 2)) == "2026-fall"


def test_year_may_trail_the_season_word() -> None:
    assert derive_tour_season_key("Fall Tour 1995", date(1995, 10, 1)) == "1995-fall"


def test_multi_season_tour_takes_its_first_season_and_stays_one_bucket() -> None:
    """A tour split across buckets by month is the behavior being replaced."""
    key = derive_tour_season_key("Winter/Spring Tour 1993", date(1993, 3, 1))
    assert key == "1993-winter"
    assert derive_tour_season_key("Winter/Spring Tour 1993", date(1993, 5, 8)) == key


def test_autumn_normalizes_to_fall() -> None:
    assert derive_tour_season_key("Autumn Tour 2001", date(2001, 10, 5)) == "2001-fall"


@pytest.mark.parametrize(
    "placeholder",
    ["No Tour Name", "Not Part of a Tour", "", "   ", "none", "N/A", "unknown"],
)
def test_placeholders_decline_rather_than_inventing_a_bucket(placeholder: str) -> None:
    assert derive_tour_season_key(placeholder, DICKS) is None


def test_none_tour_name_declines() -> None:
    """ADMIN_SHOW_DATE builds a ShowTarget with tour_name=None every night."""
    assert derive_tour_season_key(None, DICKS) is None


def test_a_year_without_a_season_word_declines() -> None:
    """"1990 Tour" must not be guessed at from the show's month."""
    assert derive_tour_season_key("1990 Tour", date(1990, 5, 1)) is None


def test_a_season_word_without_a_year_declines() -> None:
    assert derive_tour_season_key("Summer Tour", date(2026, 7, 1)) is None


def test_a_stray_number_is_not_read_as_a_year() -> None:
    assert derive_tour_season_key("Fall Run 12345", date(2026, 10, 1)) is None


# ----- DB-backed: the rebuild actually uses the column ----------------------


async def _seed_scored_show(
    pool: Any, show_date: date, handle: str, score: int, tour_key: str | None
) -> None:
    """One resolved show with one scored prediction, at a given tour key."""
    async with pool.acquire() as conn:
        uid = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ($1, $2) RETURNING id",
            handle,
            handle.lower(),
        )
        await conn.execute(
            "INSERT INTO prediction_locks (show_date, lock_at, venue_tz, tour_season_key)"
            " VALUES ($1, $2, 'UTC', $3)",
            show_date,
            datetime.now(UTC) + timedelta(hours=2),
            tour_key,
        )
        await conn.execute(
            """
            INSERT INTO predictions (
                user_id, show_date, pick_song_slugs,
                opener_slug, closer_slug, encore_slug
            )
            VALUES ($1, $2, ARRAY['a','b','c'], NULL, NULL, NULL)
            """,
            int(uid),
            show_date,
        )
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2, resolved_at = now()"
            " WHERE show_date = $1",
            show_date,
            datetime.now(UTC) - timedelta(days=1),
        )
        await conn.execute(
            "UPDATE predictions SET score = $2 WHERE user_id = $1",
            int(uid),
            score,
        )


@pytest.mark.asyncio
@requires_pg
async def test_rebuild_season_honors_the_stored_tour_key(pg_pool: Any) -> None:
    """The September Dick's show must NOT create a fall bucket."""
    await _seed_scored_show(pg_pool, DICKS, "dicksfan", 11, "2026-summer")
    await rebuild_season(pg_pool)
    keys = await list_scope_keys(pg_pool, "tour")
    assert "2026-summer" in keys
    assert "2026-fall" not in keys


@pytest.mark.asyncio
@requires_pg
async def test_null_tour_key_falls_back_to_the_month(pg_pool: Any) -> None:
    """The Umphrey's case: every row NULL must reproduce the old behavior."""
    await _seed_scored_show(pg_pool, DICKS, "wappyfan", 7, None)
    await rebuild_season(pg_pool)
    keys = await list_scope_keys(pg_pool, "tour")
    assert "2026-fall" in keys
    assert "2026-summer" not in keys


@pytest.mark.asyncio
@requires_pg
async def test_tour_keyed_and_month_fallback_shows_coexist(pg_pool: Any) -> None:
    """A mixed deployment must bucket each show by its own best information."""
    await _seed_scored_show(pg_pool, DICKS, "tourfan", 11, "2026-summer")
    await _seed_scored_show(pg_pool, date(2026, 10, 2), "monthfan", 5, None)
    await rebuild_season(pg_pool)
    keys = await list_scope_keys(pg_pool, "tour")
    assert "2026-summer" in keys
    assert "2026-fall" in keys


# ----- DB-backed: the backfill, which is what makes the fix visible ---------


class _FakeMcpTours:
    """Minimal MCP stand-in returning one year of tour-tagged shows."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.years_asked: list[int] = []

    async def __aenter__(self) -> _FakeMcpTours:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def search_shows(self, *, year: int, limit: int = 60) -> list[dict[str, Any]]:
        self.years_asked.append(year)
        return [r for r in self._rows if str(r["date"]).startswith(str(year))]


@pytest.mark.asyncio
@requires_pg
async def test_backfill_rekeys_rows_that_predate_the_column(
    pg_pool: Any, monkeypatch: Any
) -> None:
    """Without this, the fix ships and Dick's keeps its wrong bucket forever."""
    from setlist_stash import resolve
    from setlist_stash.config import get_settings

    await _seed_scored_show(pg_pool, DICKS, "legacyfan", 11, None)
    fake = _FakeMcpTours(
        [{"date": "2026-09-04", "tour_name": "2026 Summer Tour"}]
    )
    monkeypatch.setattr(resolve, "McpPhishClient", lambda *a, **kw: fake)

    updated = await resolve.backfill_tour_season_keys(pg_pool, get_settings())
    assert updated == 1
    async with pg_pool.acquire() as conn:
        key = await conn.fetchval(
            "SELECT tour_season_key FROM prediction_locks WHERE show_date = $1",
            DICKS,
        )
    assert key == "2026-summer"

    await rebuild_season(pg_pool)
    keys = await list_scope_keys(pg_pool, "tour")
    assert "2026-summer" in keys
    assert "2026-fall" not in keys


@pytest.mark.asyncio
@requires_pg
async def test_backfill_is_a_noop_once_every_row_is_filled(
    pg_pool: Any, monkeypatch: Any
) -> None:
    """It runs on every resolver start, so it must not re-query forever."""
    from setlist_stash import resolve
    from setlist_stash.config import get_settings

    await _seed_scored_show(pg_pool, DICKS, "donefan", 11, "2026-summer")
    fake = _FakeMcpTours([])
    monkeypatch.setattr(resolve, "McpPhishClient", lambda *a, **kw: fake)

    assert await resolve.backfill_tour_season_keys(pg_pool, get_settings()) == 0
    assert fake.years_asked == []


@pytest.mark.asyncio
@requires_pg
async def test_backfill_leaves_placeholder_tour_rows_null(
    pg_pool: Any, monkeypatch: Any
) -> None:
    """The Umphrey's case: 'No Tour Name' must not become a bucket."""
    from setlist_stash import resolve
    from setlist_stash.config import get_settings

    await _seed_scored_show(pg_pool, DICKS, "wappy2", 7, None)
    fake = _FakeMcpTours([{"date": "2026-09-04", "tour_name": "No Tour Name"}])
    monkeypatch.setattr(resolve, "McpPhishClient", lambda *a, **kw: fake)

    assert await resolve.backfill_tour_season_keys(pg_pool, get_settings()) == 0
    async with pg_pool.acquire() as conn:
        key = await conn.fetchval(
            "SELECT tour_season_key FROM prediction_locks WHERE show_date = $1",
            DICKS,
        )
    assert key is None


@pytest.mark.asyncio
@requires_pg
async def test_backfill_survives_an_unreachable_mcp(
    pg_pool: Any, monkeypatch: Any
) -> None:
    """A backfill is never worth taking the resolver down for."""
    from setlist_stash import resolve
    from setlist_stash.config import get_settings
    from setlist_stash.mcp_client import McpPhishError

    await _seed_scored_show(pg_pool, DICKS, "offlinefan", 11, None)

    class _Dead:
        async def __aenter__(self) -> Any:
            raise McpPhishError("upstream down")

        async def __aexit__(self, *exc: Any) -> None:
            return None

    monkeypatch.setattr(resolve, "McpPhishClient", lambda *a, **kw: _Dead())
    assert await resolve.backfill_tour_season_keys(pg_pool, get_settings()) == 0
