"""Setlist-completeness gate — pure heuristic tests.

These never touch the DB. They drive ``evaluate_completeness`` directly,
folding successive poll observations into poll state and asserting when the
gate flips to complete.

The companion DB-backed gate tests (the resolver actually skipping a partial
setlist, scoring exactly once on a complete one, scoring on backstop) live in
``test_resolve.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from setlist_stash.completeness import PollState, evaluate_completeness
from setlist_stash.resolve_types import ParsedSetlist

_LOCK = datetime(2026, 6, 8, 2, 0, tzinfo=UTC)  # 22:00 ET show date
_REQUIRED = 6
_BACKSTOP = timedelta(hours=6)


def _setlist(*, tracks: int, encore: bool) -> ParsedSetlist:
    """Build a ParsedSetlist with ``tracks`` songs, optionally with an encore.

    The completeness gate only looks at ``song_count`` and ``encore_slugs``,
    so the slug values are placeholders.
    """
    encore_slugs = ["loving-cup"] if encore else []
    all_slugs = {f"song-{i}" for i in range(tracks)}
    return ParsedSetlist(
        opener_slug="song-0" if tracks else None,
        closer_slug=f"song-{tracks - 1}" if tracks else None,
        encore_slugs=encore_slugs,
        all_slugs=all_slugs,
        song_count=tracks,
    )


def _evaluate(
    parsed: ParsedSetlist, prior: PollState, *, now: datetime
) -> tuple[bool, PollState, str]:
    d = evaluate_completeness(
        parsed=parsed,
        prior=prior,
        now=now,
        effective_lock=_LOCK,
        stable_polls_required=_REQUIRED,
        backstop=_BACKSTOP,
    )
    return d.complete, d.next_state, d.reason


def test_partial_no_encore_is_not_complete() -> None:
    """End of Set 1, no encore: never complete no matter how stable."""
    state = PollState(show_date="2026-06-08")
    now = _LOCK + timedelta(minutes=30)
    # Same 8-track Set-1 setlist polled 10 times, no encore.
    for _ in range(10):
        complete, state, reason = _evaluate(
            _setlist(tracks=8, encore=False), state, now=now
        )
        now += timedelta(minutes=5)
    assert complete is False
    assert reason == "waiting"
    assert state.encore_seen is False
    # Stability accrued, but the encore clause blocks scoring.
    assert state.stable_polls >= _REQUIRED


def test_encore_but_still_growing_is_not_complete() -> None:
    """Encore is in, but the track count is still increasing each poll."""
    state = PollState(show_date="2026-06-08")
    now = _LOCK + timedelta(minutes=30)
    complete = True
    for tracks in range(20, 26):  # grows every poll
        complete, state, reason = _evaluate(
            _setlist(tracks=tracks, encore=True), state, now=now
        )
        now += timedelta(minutes=5)
        assert complete is False
        assert reason == "waiting"
    # Encore was latched, but stability never accrued (resets each growth).
    assert state.encore_seen is True
    assert state.stable_polls == 1


def test_encore_plus_n_stable_polls_completes() -> None:
    """Encore + exactly N stable polls flips to complete, not before."""
    state = PollState(show_date="2026-06-08")
    now = _LOCK + timedelta(minutes=30)

    # First poll establishes the 24-track baseline (stable_polls == 1).
    complete, state, reason = _evaluate(
        _setlist(tracks=24, encore=True), state, now=now
    )
    assert complete is False
    assert state.stable_polls == 1
    assert state.encore_seen is True

    # Polls 2..(REQUIRED-1): still stable, still not complete.
    for expected in range(2, _REQUIRED):
        now += timedelta(minutes=5)
        complete, state, reason = _evaluate(
            _setlist(tracks=24, encore=True), state, now=now
        )
        assert state.stable_polls == expected
        assert complete is False, f"completed too early at {expected} stable polls"
        assert reason == "waiting"

    # The REQUIRED-th stable poll flips it.
    now += timedelta(minutes=5)
    complete, state, reason = _evaluate(
        _setlist(tracks=24, encore=True), state, now=now
    )
    assert state.stable_polls == _REQUIRED
    assert complete is True
    assert reason == "stable"


def test_growth_resets_stability_then_completes() -> None:
    """A late add resets the counter; stability must re-accrue from there."""
    state = PollState(show_date="2026-06-08")
    now = _LOCK + timedelta(minutes=30)

    # Accrue some stability at 22 tracks.
    for _ in range(4):
        _, state, _ = _evaluate(_setlist(tracks=22, encore=True), state, now=now)
        now += timedelta(minutes=5)
    assert state.stable_polls == 4

    # A 23rd song drops late: counter resets to 1.
    _, state, _ = _evaluate(_setlist(tracks=23, encore=True), state, now=now)
    now += timedelta(minutes=5)
    assert state.stable_polls == 1

    # Now hold steady at 23 until REQUIRED.
    complete = False
    reason = ""
    for _ in range(_REQUIRED - 1):
        complete, state, reason = _evaluate(
            _setlist(tracks=23, encore=True), state, now=now
        )
        now += timedelta(minutes=5)
    assert state.stable_polls == _REQUIRED
    assert complete is True
    assert reason == "stable"


def test_backstop_completes_without_stability() -> None:
    """Past the backstop, score even if stability never converged."""
    state = PollState(show_date="2026-06-08")
    # Well past lock + backstop. Partial setlist, no encore, unstable.
    now = _LOCK + _BACKSTOP + timedelta(minutes=1)
    complete, state, reason = _evaluate(
        _setlist(tracks=8, encore=False), state, now=now
    )
    assert complete is True
    assert reason == "backstop"


def test_backstop_boundary_is_inclusive() -> None:
    """now == lock + backstop fires; one second before does not."""
    state = PollState(show_date="2026-06-08")
    partial = _setlist(tracks=8, encore=False)

    before = _LOCK + _BACKSTOP - timedelta(seconds=1)
    complete_before, _, reason_before = _evaluate(partial, state, now=before)
    assert complete_before is False
    assert reason_before == "waiting"

    at = _LOCK + _BACKSTOP
    complete_at, _, reason_at = _evaluate(partial, state, now=at)
    assert complete_at is True
    assert reason_at == "backstop"


def test_stable_clause_wins_reason_over_backstop() -> None:
    """When both clauses are satisfied, the reason is 'stable' (happy path)."""
    # Build a state already at REQUIRED-1 stable polls with encore seen.
    state = PollState(
        show_date="2026-06-08",
        last_track_count=24,
        encore_seen=True,
        stable_polls=_REQUIRED - 1,
    )
    # now is also past the backstop, so both clauses are true.
    now = _LOCK + _BACKSTOP + timedelta(minutes=5)
    complete, state, reason = _evaluate(
        _setlist(tracks=24, encore=True), state, now=now
    )
    assert complete is True
    assert reason == "stable"
