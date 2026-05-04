"""Resolver tick tests.

Two flavors:
- DB-backed integration tests gated on ``TEST_PG_DSN`` (skip if absent).
  These cover: resolver scoring path, no-op path, watchdog stale-running
  flip, cancelled-show path.
- Pure unit tests (always run): setlist parsing lives in
  ``test_resolve_setlist.py``; this file owns the resolver-level behavior.

The MCP client is stubbed via a tiny in-memory fake. We don't go over the
wire here — that's covered in test_mcp_client.py.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from setlist_stash.config import Settings
from setlist_stash.resolve import (
    parse_setlist,
    run_tick,
    watchdog_stale_running,
)
from tests.conftest import requires_pg


class _FakeMcpPhishClient:
    """In-memory mcp-phish double for resolver tests.

    The real ``McpPhishClient`` is an async context manager. Mirror that
    shape so the resolver can swap us in via monkeypatch.
    """

    def __init__(
        self,
        shows: dict[str, dict[str, Any]],
        songs: dict[str, dict[str, Any]],
    ) -> None:
        self._shows = shows
        self._songs = songs

    async def __aenter__(self) -> _FakeMcpPhishClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def get_show(self, date_or_id: str) -> dict[str, Any]:
        if date_or_id not in self._shows:
            from setlist_stash.mcp_client import McpPhishNotFound
            raise McpPhishNotFound(f"unknown show {date_or_id}")
        return self._shows[date_or_id]

    async def get_song(self, slug: str) -> dict[str, Any]:
        if slug not in self._songs:
            from setlist_stash.mcp_client import McpPhishNotFound
            raise McpPhishNotFound(f"unknown song {slug}")
        return self._songs[slug]


def _setlist_row(position: int, set_name: str, slug: str) -> dict[str, Any]:
    return {
        "position": position,
        "set_name": set_name,
        "song_slug": slug,
        "song_title": slug,
        "transition": "",
        "footnote": "",
    }


def _make_settings() -> Settings:
    """Build a Settings with safe in-test defaults."""
    return Settings(
        mcp_phish_url="http://test/mcp",
        mcp_phish_timeout_seconds=2.0,
        resolver_cancel_after_hours=72,
        resolver_interval_seconds=60,
    )


@pytest.mark.asyncio
@requires_pg
async def test_run_tick_noop_when_no_open_locks(
    pg_pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from setlist_stash import db, resolve

    monkeypatch.setattr(db, "_pool", pg_pool)

    # No mcp call should happen, but inject a placeholder anyway.
    monkeypatch.setattr(
        resolve, "McpPhishClient",
        lambda *a, **kw: _FakeMcpPhishClient({}, {}),
    )

    result = await run_tick(_make_settings())
    assert result.status == "noop"
    assert result.shows_scanned == 0

    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, shows_scanned, summary FROM scoring_runs ORDER BY id DESC LIMIT 1"
        )
    assert row is not None
    assert row["status"] == "noop"
    assert row["shows_scanned"] == 0


@pytest.mark.asyncio
@requires_pg
async def test_run_tick_scores_published_setlist(
    pg_pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Seed a prediction whose lock_at is in the past, mock a published
    setlist, run the tick, assert score + breakdown + resolved_at."""
    from setlist_stash import db, resolve

    monkeypatch.setattr(db, "_pool", pg_pool)

    show_date = "2024-12-31"
    future_lock = datetime.now(UTC) + timedelta(hours=2)
    past_lock = datetime.now(UTC) - timedelta(hours=2)

    async with pg_pool.acquire() as conn:
        user_id = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ($1, $2) RETURNING id",
            "tester", "tester",
        )
        # Seed the lock as future so the predictions trigger lets us insert,
        # then back-date lock_at to simulate showtime having passed.
        await conn.execute(
            """
            INSERT INTO prediction_locks (show_date, lock_at, venue_tz)
            VALUES ($1, $2, 'America/New_York')
            """,
            datetime.fromisoformat(show_date).date(),
            future_lock,
        )
        await conn.execute(
            """
            INSERT INTO predictions (
                user_id, show_date, pick_song_slugs,
                opener_slug, closer_slug, encore_slug
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            user_id,
            datetime.fromisoformat(show_date).date(),
            ["tweezer", "fluffhead", "harry-hood"],
            "tweezer",
            "harry-hood",
            "loving-cup",
        )
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2 WHERE show_date = $1",
            datetime.fromisoformat(show_date).date(),
            past_lock,
        )

    setlist = [
        _setlist_row(1, "Set 1", "tweezer"),
        _setlist_row(2, "Set 1", "fluffhead"),
        _setlist_row(3, "Set 2", "harry-hood"),
        _setlist_row(4, "Encore", "loving-cup"),
    ]
    fake = _FakeMcpPhishClient(
        shows={show_date: {"setlist": setlist}},
        songs={
            "tweezer": {"slug": "tweezer", "gap_current": 5, "times_played": 400},
            "fluffhead": {"slug": "fluffhead", "gap_current": 50, "times_played": 200},
            "harry-hood": {"slug": "harry-hood", "gap_current": 10, "times_played": 300},
        },
    )
    monkeypatch.setattr(resolve, "McpPhishClient", lambda *a, **kw: fake)

    result = await run_tick(_make_settings())
    assert result.status == "success"
    assert result.shows_resolved == 1
    assert result.predictions_scored == 1

    async with pg_pool.acquire() as conn:
        pred = await conn.fetchrow("SELECT score, score_breakdown FROM predictions LIMIT 1")
        lock = await conn.fetchrow("SELECT resolved_at, summary FROM prediction_locks LIMIT 1")
        run = await conn.fetchrow(
            "SELECT status, shows_resolved, predictions_scored, shows_scanned, summary "
            "FROM scoring_runs ORDER BY id DESC LIMIT 1"
        )

    assert pred is not None
    assert pred["score"] is not None
    assert pred["score"] > 0  # all three picks played + every slot bonus
    breakdown = json.loads(pred["score_breakdown"])
    assert set(breakdown.keys()) == {"picks", "opener", "closer", "encore", "total"}
    assert breakdown["opener"]["bonus"] == 25
    assert breakdown["closer"]["bonus"] == 25
    assert breakdown["encore"]["bonus"] == 30

    assert lock is not None
    assert lock["resolved_at"] is not None
    summary = json.loads(lock["summary"])
    assert summary["setlist_song_count"] == 4
    assert summary["predictions_scored"] == 1

    assert run["status"] == "success"
    assert run["shows_resolved"] == 1
    assert run["predictions_scored"] == 1
    assert run["shows_scanned"] == 1


@pytest.mark.asyncio
@requires_pg
async def test_run_tick_skips_when_setlist_not_yet_published(
    pg_pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Lock is past but mcp-phish has no setlist yet (within cancel window).

    Behaviour: leave resolved_at NULL, predictions.score NULL, run status='noop'.
    """
    from setlist_stash import db, resolve

    monkeypatch.setattr(db, "_pool", pg_pool)

    show_date = "2099-01-01"  # future date so cancel window can't fire
    future_lock = datetime.now(UTC) + timedelta(hours=2)
    past_lock = datetime.now(UTC) - timedelta(hours=2)

    async with pg_pool.acquire() as conn:
        user_id = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ('skip', 'skip') RETURNING id"
        )
        await conn.execute(
            """
            INSERT INTO prediction_locks (show_date, lock_at, venue_tz)
            VALUES ($1, $2, 'UTC')
            """,
            datetime.fromisoformat(show_date).date(),
            future_lock,
        )
        await conn.execute(
            """
            INSERT INTO predictions (
                user_id, show_date, pick_song_slugs,
                opener_slug, closer_slug, encore_slug
            )
            VALUES ($1, $2, $3, NULL, NULL, NULL)
            """,
            user_id,
            datetime.fromisoformat(show_date).date(),
            ["a", "b", "c"],
        )
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2 WHERE show_date = $1",
            datetime.fromisoformat(show_date).date(),
            past_lock,
        )

    fake = _FakeMcpPhishClient(
        shows={show_date: {"setlist": []}},  # vault knows the show, no setlist
        songs={},
    )
    monkeypatch.setattr(resolve, "McpPhishClient", lambda *a, **kw: fake)

    result = await run_tick(_make_settings())
    # The show was scanned but not resolved.
    assert result.status == "noop"
    assert result.shows_scanned == 1
    assert result.shows_resolved == 0

    async with pg_pool.acquire() as conn:
        pred = await conn.fetchrow("SELECT score FROM predictions LIMIT 1")
        lock = await conn.fetchrow("SELECT resolved_at FROM prediction_locks LIMIT 1")
    assert pred["score"] is None
    assert lock["resolved_at"] is None


@pytest.mark.asyncio
@requires_pg
async def test_run_tick_cancels_show_past_threshold(
    pg_pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lock older than the cancel window with no setlist gets stamped cancelled."""
    from setlist_stash import db, resolve

    monkeypatch.setattr(db, "_pool", pg_pool)

    # Use 2024 so the show date is comfortably in the past and far older than
    # the cancel threshold. lock_at is also old.
    show_date = "2024-01-01"
    future_lock = datetime.now(UTC) + timedelta(hours=2)
    very_old_lock = datetime.now(UTC) - timedelta(days=10)

    async with pg_pool.acquire() as conn:
        user_id = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ('canc', 'canc') RETURNING id"
        )
        await conn.execute(
            """
            INSERT INTO prediction_locks (show_date, lock_at, venue_tz)
            VALUES ($1, $2, 'UTC')
            """,
            datetime.fromisoformat(show_date).date(),
            future_lock,
        )
        await conn.execute(
            """
            INSERT INTO predictions (
                user_id, show_date, pick_song_slugs,
                opener_slug, closer_slug, encore_slug
            )
            VALUES ($1, $2, $3, NULL, NULL, NULL)
            """,
            user_id,
            datetime.fromisoformat(show_date).date(),
            ["a", "b", "c"],
        )
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2 WHERE show_date = $1",
            datetime.fromisoformat(show_date).date(),
            very_old_lock,
        )

    # No setlist available (show was cancelled IRL).
    fake = _FakeMcpPhishClient(shows={show_date: {"setlist": []}}, songs={})
    monkeypatch.setattr(resolve, "McpPhishClient", lambda *a, **kw: fake)

    result = await run_tick(_make_settings())
    assert result.status == "success"

    async with pg_pool.acquire() as conn:
        pred = await conn.fetchrow("SELECT score, score_breakdown FROM predictions LIMIT 1")
        lock = await conn.fetchrow("SELECT resolved_at, summary FROM prediction_locks LIMIT 1")
    assert pred["score"] == 0
    breakdown = json.loads(pred["score_breakdown"])
    assert breakdown["cancelled"] is True
    assert lock["resolved_at"] is not None
    assert json.loads(lock["summary"])["cancelled"] is True


@pytest.mark.asyncio
@requires_pg
async def test_watchdog_flips_stale_running_rows(pg_pool: Any) -> None:
    """A 'running' row older than the threshold becomes 'error'."""
    async with pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO scoring_runs (started_at, status)
            VALUES (now() - interval '20 minutes', 'running')
            """
        )
        # A fresh running row should NOT get flipped.
        await conn.execute(
            "INSERT INTO scoring_runs (started_at, status) VALUES (now(), 'running')"
        )

    flipped = await watchdog_stale_running(pg_pool, stale_after_minutes=15)
    assert flipped == 1

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT status, error_message FROM scoring_runs ORDER BY id ASC")
    assert rows[0]["status"] == "error"
    assert "watchdog" in (rows[0]["error_message"] or "")
    assert rows[1]["status"] == "running"


# ----- pure tests (always run) ---------------------------------------------


def test_parse_setlist_smoke_invariants() -> None:
    """Sanity check: parse_setlist is exposed for resolver consumers."""
    parsed = parse_setlist([
        {"position": 1, "set_name": "Set 1", "song_slug": "x"},
        {"position": 2, "set_name": "Encore", "song_slug": "y"},
    ])
    assert parsed.opener_slug == "x"
    assert parsed.closer_slug == "x"
    assert parsed.encore_slugs == ["y"]
    assert parsed.all_slugs == {"x", "y"}
