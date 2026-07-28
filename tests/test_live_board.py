"""Tests for the live show board: one refresh cycle, one source of truth.

Two properties are being defended here, and they are the whole point of the
feature:

1. **The setlist and the standings never disagree.** They are rendered from a
   single request against the resolver's setlist snapshot — the exact list the
   scores were computed from. A viewer must never see a song the scores beside
   it don't count.
2. **The poll turns itself off.** Auto-refresh runs only while the show is
   genuinely live. When it finalizes, the replacement fragment carries no
   trigger and the browser goes quiet on its own.

The snapshot round-trip tests are DB-backed; the rest are pure.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
from pydantic import SecretStr

from setlist_stash.completeness import (
    PollState,
    read_setlist_snapshot,
    save_setlist_snapshot,
    upsert_poll_state,
)
from setlist_stash.config import Settings
from setlist_stash.locks import live_board_active

from .conftest import requires_pg

SHOW = date(2026, 7, 27)

SETLIST: list[dict[str, Any]] = [
    {"set_name": "Set 1", "song_title": "Tweezer", "song_slug": "tweezer"},
    {"set_name": "Encore", "song_title": "Tweezer Reprise", "song_slug": "tweprise"},
]


async def _seed_lock(pool: Any, show_date: date) -> None:
    """poll_state FKs to prediction_locks, so a lock row must exist first."""
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO prediction_locks (show_date, lock_at) VALUES ($1, now()) "
            "ON CONFLICT (show_date) DO NOTHING",
            show_date,
        )


# ----- snapshot persistence --------------------------------------------------


@requires_pg
@pytest.mark.asyncio
async def test_snapshot_absent_returns_none(pg_pool: Any) -> None:
    """No snapshot yet -> caller must fall back to a live upstream read.

    This is the path every show resolved before migration 009 takes, so it has
    to stay a clean (None, None) rather than an error.
    """
    setlist, scored_at = await read_setlist_snapshot(pg_pool, date(1995, 12, 31))
    assert setlist is None
    assert scored_at is None


@requires_pg
@pytest.mark.asyncio
async def test_snapshot_round_trips_with_timestamp(pg_pool: Any) -> None:
    await _seed_lock(pg_pool, SHOW)
    await upsert_poll_state(pg_pool, PollState(show_date=SHOW))
    await save_setlist_snapshot(pg_pool, SHOW, SETLIST)

    setlist, scored_at = await read_setlist_snapshot(pg_pool, SHOW)
    assert setlist == SETLIST
    assert scored_at is not None
    # Stamped by the DB at write time, so it is the instant the scores in
    # `predictions.score` were computed for.
    assert datetime.now(UTC) - scored_at < timedelta(minutes=5)


@requires_pg
@pytest.mark.asyncio
async def test_snapshot_overwrites_on_next_tick(pg_pool: Any) -> None:
    """Each tick republishes. A shrinking setlist must not leave stale songs."""
    await _seed_lock(pg_pool, SHOW)
    await upsert_poll_state(pg_pool, PollState(show_date=SHOW))
    await save_setlist_snapshot(pg_pool, SHOW, SETLIST)
    await save_setlist_snapshot(pg_pool, SHOW, SETLIST[:1])

    setlist, _ = await read_setlist_snapshot(pg_pool, SHOW)
    assert setlist == SETLIST[:1]


@requires_pg
@pytest.mark.asyncio
async def test_snapshot_write_without_poll_state_row_is_noop(pg_pool: Any) -> None:
    """Fail-soft: a missing row must not raise inside the scoring path."""
    await save_setlist_snapshot(pg_pool, date(1994, 6, 18), SETLIST)
    setlist, _ = await read_setlist_snapshot(pg_pool, date(1994, 6, 18))
    assert setlist is None


# ----- the refresh window ----------------------------------------------------
#
# ``live_board_active`` is the exact predicate the route calls, not a copy of
# it, so these cases can't drift away from shipped behavior.


def _live_now(
    *,
    lock_at: datetime,
    resolved: bool,
    now: datetime,
    window_hours: int = 6,
) -> bool:
    return live_board_active(
        lock_at=lock_at,
        resolved=resolved,
        now=now,
        active_window_hours=window_hours,
    )


LOCK = datetime(2026, 7, 27, 23, 0, tzinfo=UTC)  # 19:00 ET


def _settings() -> Settings:
    """Bare Settings built from code defaults, ignoring any ambient .env."""
    return Settings(
        session_secret=SecretStr("x" * 32),
        _env_file=None,  # type: ignore[call-arg]
    )


def test_refresh_on_during_the_show() -> None:
    assert _live_now(lock_at=LOCK, resolved=False, now=LOCK + timedelta(hours=2))


def test_refresh_off_before_lock() -> None:
    # Pre-lock the page shows no standings at all; nothing to refresh.
    assert not _live_now(lock_at=LOCK, resolved=False, now=LOCK - timedelta(minutes=1))


def test_refresh_off_once_finalized() -> None:
    # The reason the fragment owns its own wrapper: this render is what stops
    # the browser polling, mid-show, without a page reload.
    assert not _live_now(lock_at=LOCK, resolved=True, now=LOCK + timedelta(hours=2))


def test_refresh_off_after_the_active_window() -> None:
    # An unresolved show whose setlist never published must not leave open tabs
    # polling all night.
    assert not _live_now(lock_at=LOCK, resolved=False, now=LOCK + timedelta(hours=7))


# ----- cadence coherence -----------------------------------------------------


def test_default_client_refresh_is_not_faster_than_the_resolver() -> None:
    """Polling faster than the data changes is theater, not freshness.

    The board renders the resolver's snapshot, so the resolver's active cadence
    is a hard floor on how fresh the page can possibly be. This guards the
    defaults against drifting apart.
    """
    s = _settings()
    assert (
        s.live_refresh_seconds == 0
        or s.live_refresh_seconds >= s.resolver_active_interval_seconds
    )


def test_stable_quiet_window_stays_around_thirty_minutes() -> None:
    """The completeness guard is a DURATION, expressed as polls x interval.

    Speeding up the cadence without raising the poll count silently shrinks the
    window that keeps a gap between encore songs from reading as "final". If a
    future change breaks this, it breaks scoring in a way that only shows up as
    a wrong leaderboard weeks later — so assert it here instead.
    """
    s = _settings()
    quiet = s.resolver_stable_polls_required * s.resolver_active_interval_seconds
    assert 20 * 60 <= quiet <= 45 * 60


# ----- route: the two renders must agree -------------------------------------


async def _seed_live_show(pool: Any) -> None:
    """A show that is locked, unresolved, and 2h into its window."""
    async with pool.acquire() as conn:
        # Open the lock in the future first: `predictions_lock_guard` refuses
        # pick writes on a locked show, so the prediction has to land before
        # the cutoff moves into the past.
        await conn.execute(
            "INSERT INTO prediction_locks (show_date, lock_at) "
            "VALUES ($1, now() + interval '1 hour') "
            "ON CONFLICT (show_date) DO UPDATE "
            "SET lock_at = EXCLUDED.lock_at, resolved_at = NULL",
            SHOW,
        )
        user_id = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) "
            "VALUES ('tweezerhead', 'tweezerhead') RETURNING id"
        )
        await conn.execute(
            "INSERT INTO predictions "
            "(show_date, user_id, pick_song_slugs, encore_slug, score) "
            "VALUES ($1, $2, $3, 'tweprise', 4)",
            SHOW,
            user_id,
            ["tweezer", "sand", "ghost"],
        )
        # Now put the show 2h into its live window.
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = now() - interval '2 hours' "
            "WHERE show_date = $1",
            SHOW,
        )
    await upsert_poll_state(pool, PollState(show_date=SHOW))
    await save_setlist_snapshot(pool, SHOW, SETLIST)


@requires_pg
@pytest.mark.asyncio
async def test_live_page_polls_and_shows_the_snapshot_setlist(
    pg_pool: Any, async_client: Any
) -> None:
    await _seed_live_show(pg_pool)
    r = await async_client.get(f"/show/{SHOW}/predictions")
    assert r.status_code == 200
    body = r.text
    # Polling is armed, on the board wrapper, at the configured cadence.
    assert 'id="live-board"' in body
    assert 'hx-trigger="every 60s"' in body
    assert 'hx-swap="outerHTML"' in body
    assert f'hx-get="/show/{SHOW}/predictions"' in body
    # Setlist came from the resolver snapshot, NOT a live upstream read (no MCP
    # is mocked in this test — if the route still called out, it would degrade
    # to the "not posted yet" placeholder instead of showing these songs).
    assert "Tweezer Reprise" in body
    assert "Setlist not posted yet" not in body
    # ...and the standings from the same request.
    assert "tweezerhead" in body


@requires_pg
@pytest.mark.asyncio
async def test_htmx_refresh_returns_the_board_fragment_only(
    pg_pool: Any, async_client: Any
) -> None:
    """The refresh must be the SAME render, minus the page chrome.

    This is what keeps setlist and scores in lockstep: one request, one
    snapshot, both halves. If this ever returned a full document the swap
    would nest a whole page inside the board.
    """
    await _seed_live_show(pg_pool)
    full = await async_client.get(f"/show/{SHOW}/predictions")
    frag = await async_client.get(
        f"/show/{SHOW}/predictions", headers={"HX-Request": "true"}
    )
    assert frag.status_code == 200
    assert "<html" not in frag.text.lower()
    assert "<nav" not in frag.text.lower()
    # Both halves present in the fragment, and identical to the full render.
    assert "Tweezer Reprise" in frag.text
    assert "tweezerhead" in frag.text
    assert frag.text.strip() in full.text
    # The replacement re-arms its own trigger, or polling would stop after one.
    assert 'hx-trigger="every 60s"' in frag.text


@requires_pg
@pytest.mark.asyncio
async def test_finalized_show_stops_polling(
    pg_pool: Any, async_client: Any
) -> None:
    """Once resolved, the swapped-in board carries no trigger and goes quiet."""
    await _seed_live_show(pg_pool)
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE prediction_locks SET resolved_at = now() WHERE show_date = $1",
            SHOW,
        )
    r = await async_client.get(
        f"/show/{SHOW}/predictions", headers={"HX-Request": "true"}
    )
    assert r.status_code == 200
    assert "hx-trigger" not in r.text
    # Still renders the setlist and the final standings, just statically.
    assert "Tweezer Reprise" in r.text
    assert "Final." in r.text
