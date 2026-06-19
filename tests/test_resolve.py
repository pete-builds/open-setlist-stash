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


class _CountingMcpPhishClient(_FakeMcpPhishClient):
    """A fake that counts how many times a show was fetched (get_show calls).

    Used to assert a complete setlist is scored exactly once: re-running the
    tick after a resolve must be a no-op (the lock is resolved), so get_show
    is never called a second time for that show.
    """

    def __init__(
        self,
        shows: dict[str, dict[str, Any]],
        songs: dict[str, dict[str, Any]],
    ) -> None:
        super().__init__(shows, songs)
        self.get_show_calls = 0

    async def get_show(self, date_or_id: str) -> dict[str, Any]:
        self.get_show_calls += 1
        return await super().get_show(date_or_id)


def _setlist_row(position: int, set_name: str, slug: str) -> dict[str, Any]:
    return {
        "position": position,
        "set_name": set_name,
        "song_slug": slug,
        "song_title": slug,
        "transition": "",
        "footnote": "",
    }


def _make_settings(
    *,
    stable_polls_required: int = 1,
    backstop_hours: int = 6,
) -> Settings:
    """Build a Settings with safe in-test defaults.

    ``stable_polls_required`` defaults to 1 so a single tick on a complete
    (encore-bearing, stable) setlist scores immediately — that keeps the
    pre-gate scoring tests one-tick. The completeness-gate tests override it.
    """
    return Settings(
        mcp_phish_url="http://test/mcp",
        mcp_phish_timeout_seconds=2.0,
        resolver_cancel_after_hours=72,
        resolver_interval_seconds=60,
        resolver_stable_polls_required=stable_polls_required,
        resolver_backstop_hours=backstop_hours,
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
    assert pred["score"] > 0  # all three picks played + encore bonus
    breakdown = json.loads(pred["score_breakdown"])
    assert set(breakdown.keys()) == {"picks", "encore", "total"}
    assert breakdown["encore"]["bonus"] == 5
    # 3 played picks (2 each = 6) + encore loving-cup in the encore (5) = 11.
    assert all(p["points"] == 2 for p in breakdown["picks"])
    assert breakdown["total"] == 11
    assert pred["score"] == 11

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


# ----- completeness gate (DB-backed) ---------------------------------------


async def _seed_lock_and_prediction(
    pg_pool: Any,
    *,
    show_date: str,
    handle: str,
    lock_at: datetime,
    picks: list[str],
    opener: str | None,
    closer: str | None,
    encore: str | None,
) -> None:
    """Seed one prediction + a back-dated lock so the resolver sees it open.

    The lock is inserted in the future (so the predictions trigger allows the
    INSERT), then back-dated to ``lock_at`` to simulate showtime having passed.
    """
    future_lock = datetime.now(UTC) + timedelta(hours=2)
    async with pg_pool.acquire() as conn:
        user_id = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ($1, $2) RETURNING id",
            handle, handle.lower(),
        )
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
            picks,
            opener,
            closer,
            encore,
        )
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2 WHERE show_date = $1",
            datetime.fromisoformat(show_date).date(),
            lock_at,
        )


@pytest.mark.asyncio
@requires_pg
async def test_gate_partial_setlist_scored_live(
    pg_pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End of Set 1, no encore: scored LIVE (running total) but NOT finalized.

    The additive model scores played picks every tick; the encore bonus stays 0
    until the encore lands. The lock stays open (resolved_at NULL) so later ticks
    keep re-scoring, and poll_state records the observation.
    """
    from setlist_stash import db, resolve

    monkeypatch.setattr(db, "_pool", pg_pool)

    show_date = "2099-02-01"  # future so the cancel window can't fire
    past_lock = datetime.now(UTC) - timedelta(hours=1)
    await _seed_lock_and_prediction(
        pg_pool,
        show_date=show_date,
        handle="partial",
        lock_at=past_lock,
        picks=["tweezer", "fluffhead", "harry-hood"],
        opener="tweezer",
        closer="harry-hood",
        encore="loving-cup",
    )

    # Set 1 only: no encore. Require 6 stable polls + a 6h backstop that hasn't
    # fired (lock was only 1h ago).
    partial = [
        _setlist_row(1, "Set 1", "tweezer"),
        _setlist_row(2, "Set 1", "fluffhead"),
        _setlist_row(3, "Set 1", "harry-hood"),
    ]
    fake = _FakeMcpPhishClient(shows={show_date: {"setlist": partial}}, songs={})
    monkeypatch.setattr(resolve, "McpPhishClient", lambda *a, **kw: fake)

    result = await run_tick(
        _make_settings(stable_polls_required=6, backstop_hours=6)
    )
    # Live scoring happened, but the show is not finalized.
    assert result.status == "success"
    assert result.shows_scanned == 1
    assert result.shows_resolved == 0

    async with pg_pool.acquire() as conn:
        pred = await conn.fetchrow("SELECT score, score_breakdown FROM predictions LIMIT 1")
        lock = await conn.fetchrow("SELECT resolved_at FROM prediction_locks LIMIT 1")
        ps = await conn.fetchrow(
            "SELECT last_track_count, encore_seen, stable_polls, complete "
            "FROM poll_state WHERE show_date = $1",
            datetime.fromisoformat(show_date).date(),
        )
    # 3 picks played (2 each) = 6; encore (loving-cup) hasn't landed yet → +0.
    assert pred["score"] == 6
    breakdown = json.loads(pred["score_breakdown"])
    assert breakdown["encore"]["bonus"] == 0
    # Not finalized: lock stays open so later ticks keep re-scoring.
    assert lock["resolved_at"] is None
    assert ps is not None
    assert ps["last_track_count"] == 3
    assert ps["encore_seen"] is False
    assert ps["complete"] is False


@pytest.mark.asyncio
@requires_pg
async def test_gate_scores_once_when_encore_and_stable(
    pg_pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Encore + the required stable poll scores exactly once.

    Pre-seed poll_state at stable_polls = required-1 (encore already seen) so
    the next stable poll is the one that completes. Then run a SECOND tick and
    assert it's a no-op (lock resolved) — i.e. scored exactly once.
    """
    from setlist_stash import db, resolve

    monkeypatch.setattr(db, "_pool", pg_pool)

    show_date = "2099-02-02"
    past_lock = datetime.now(UTC) - timedelta(hours=1)
    await _seed_lock_and_prediction(
        pg_pool,
        show_date=show_date,
        handle="stable",
        lock_at=past_lock,
        picks=["tweezer", "fluffhead", "harry-hood"],
        opener="tweezer",
        closer="harry-hood",
        encore="loving-cup",
    )

    full = [
        _setlist_row(1, "Set 1", "tweezer"),
        _setlist_row(2, "Set 1", "fluffhead"),
        _setlist_row(3, "Set 2", "harry-hood"),
        _setlist_row(4, "Encore", "loving-cup"),
    ]
    show_date_obj = datetime.fromisoformat(show_date).date()

    # Pre-seed: 4-track count already observed, encore seen, 5 stable polls.
    # The required-th (6th) stable poll lands this tick.
    async with pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO poll_state (
                show_date, last_track_count, encore_seen, stable_polls, complete
            ) VALUES ($1, 4, TRUE, 5, FALSE)
            """,
            show_date_obj,
        )

    fake = _CountingMcpPhishClient(
        shows={show_date: {"setlist": full}},
        songs={
            "tweezer": {"slug": "tweezer", "gap_current": 5, "times_played": 400},
            "fluffhead": {"slug": "fluffhead", "gap_current": 50, "times_played": 200},
            "harry-hood": {"slug": "harry-hood", "gap_current": 10, "times_played": 300},
        },
    )
    monkeypatch.setattr(resolve, "McpPhishClient", lambda *a, **kw: fake)

    result = await run_tick(
        _make_settings(stable_polls_required=6, backstop_hours=6)
    )
    assert result.status == "success"
    assert result.shows_resolved == 1
    assert result.predictions_scored == 1
    assert fake.get_show_calls == 1

    async with pg_pool.acquire() as conn:
        pred = await conn.fetchrow("SELECT score FROM predictions LIMIT 1")
        lock = await conn.fetchrow("SELECT resolved_at, summary FROM prediction_locks LIMIT 1")
        ps = await conn.fetchrow(
            "SELECT complete, stable_polls FROM poll_state WHERE show_date = $1",
            show_date_obj,
        )
    assert pred["score"] is not None
    assert pred["score"] > 0
    assert lock["resolved_at"] is not None
    assert ps["complete"] is True
    assert ps["stable_polls"] == 6

    # Second tick: the lock is resolved, so it's not even an open lock anymore.
    # The show must NOT be fetched or scored again.
    result2 = await run_tick(
        _make_settings(stable_polls_required=6, backstop_hours=6)
    )
    assert result2.status == "noop"
    assert result2.shows_scanned == 0
    assert fake.get_show_calls == 1  # unchanged — scored exactly once


@pytest.mark.asyncio
@requires_pg
async def test_gate_backstop_scores_partial_setlist(
    pg_pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Past the backstop, a still-partial setlist gets scored anyway.

    This is the safety net that holds even while the mcp-phish 24h hot-window
    cache freezes the stability signal: the time backstop fires regardless of
    cache state.
    """
    from setlist_stash import db, resolve

    monkeypatch.setattr(db, "_pool", pg_pool)

    show_date = "2099-02-03"
    # Lock 7h ago, backstop is 6h -> backstop has fired. Show date is future so
    # the cancel window (72h) can't pre-empt it.
    old_lock = datetime.now(UTC) - timedelta(hours=7)
    await _seed_lock_and_prediction(
        pg_pool,
        show_date=show_date,
        handle="backstop",
        lock_at=old_lock,
        picks=["tweezer", "fluffhead", "harry-hood"],
        opener="tweezer",
        closer="harry-hood",
        encore=None,
    )

    # Partial: no encore, no stability accrued. Only the backstop can save it.
    partial = [
        _setlist_row(1, "Set 1", "tweezer"),
        _setlist_row(2, "Set 1", "fluffhead"),
        _setlist_row(3, "Set 2", "harry-hood"),
    ]
    fake = _FakeMcpPhishClient(
        shows={show_date: {"setlist": partial}},
        songs={
            "tweezer": {"slug": "tweezer", "gap_current": 5, "times_played": 400},
            "fluffhead": {"slug": "fluffhead", "gap_current": 50, "times_played": 200},
            "harry-hood": {"slug": "harry-hood", "gap_current": 10, "times_played": 300},
        },
    )
    monkeypatch.setattr(resolve, "McpPhishClient", lambda *a, **kw: fake)

    result = await run_tick(
        _make_settings(stable_polls_required=6, backstop_hours=6)
    )
    assert result.status == "success"
    assert result.shows_resolved == 1
    assert result.predictions_scored == 1

    async with pg_pool.acquire() as conn:
        pred = await conn.fetchrow("SELECT score FROM predictions LIMIT 1")
        lock = await conn.fetchrow("SELECT resolved_at FROM prediction_locks LIMIT 1")
        ps = await conn.fetchrow(
            "SELECT complete FROM poll_state WHERE show_date = $1",
            datetime.fromisoformat(show_date).date(),
        )
    assert pred["score"] is not None
    assert lock["resolved_at"] is not None
    assert ps["complete"] is True


@pytest.mark.asyncio
@requires_pg
async def test_gate_empty_setlist_cancel_window_unchanged(
    pg_pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Empty setlist still routes through the cancel window, not the gate.

    Regression guard: the completeness gate must not intercept empty setlists.
    A lock older than the cancel window with no setlist data is still cancelled,
    and no poll_state row is created for it (the gate is never reached).
    """
    from setlist_stash import db, resolve

    monkeypatch.setattr(db, "_pool", pg_pool)

    show_date = "2024-03-01"  # well in the past
    very_old_lock = datetime.now(UTC) - timedelta(days=10)
    await _seed_lock_and_prediction(
        pg_pool,
        show_date=show_date,
        handle="emptycanc",
        lock_at=very_old_lock,
        picks=["a", "b", "c"],
        opener=None,
        closer=None,
        encore=None,
    )

    fake = _FakeMcpPhishClient(shows={show_date: {"setlist": []}}, songs={})
    monkeypatch.setattr(resolve, "McpPhishClient", lambda *a, **kw: fake)

    result = await run_tick(_make_settings())
    assert result.status == "success"

    async with pg_pool.acquire() as conn:
        pred = await conn.fetchrow("SELECT score, score_breakdown FROM predictions LIMIT 1")
        lock = await conn.fetchrow("SELECT resolved_at, summary FROM prediction_locks LIMIT 1")
        ps = await conn.fetchrow(
            "SELECT show_date FROM poll_state WHERE show_date = $1",
            datetime.fromisoformat(show_date).date(),
        )
    assert pred["score"] == 0
    assert json.loads(pred["score_breakdown"])["cancelled"] is True
    assert lock["resolved_at"] is not None
    assert json.loads(lock["summary"])["cancelled"] is True
    # The gate was never reached for an empty setlist: no poll_state row.
    assert ps is None


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
