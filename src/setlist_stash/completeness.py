"""Setlist-completeness gate for the resolver.

phish.net setlists are typed in live DURING a show and grow set by set, with
the encore entered last. The resolver must NOT score a show on the first
non-empty setlist it sees — that would score everyone's encore pick against
the end of Set 1 and stamp those wrong scores in permanently. Instead it
waits until the setlist looks final.

A setlist is COMPLETE when:

  (encore detected AND track count STABLE across N consecutive polls)
  OR
  (now >= effective_lock + backstop_hours)        [time backstop]

The first clause is the happy path on a normal show night: once the encore is
in and no new tracks have appeared for ~30 minutes (6 polls at the 5-minute
active cadence), the setlist is done. The backstop guarantees eventual scoring
even if the stability signal never converges (phish.net edits trickling for
days, an unusual encore label, etc.).

Per-show poll bookkeeping lives in the durable ``poll_state`` table (migration
005) so a resolver restart mid-show can't reset the stable-poll counter and
re-arm a premature score.

PLATFORM DEPENDENCY (satisfied): the stability clause is only meaningful if
each resolver poll can actually observe a NEW track count, and the resolver
never talks to phish.net directly — every consumer reads through mcp-phish, so
that upstream's cache policy decides whether polls see fresh data.

mcp-phish splits its cache TTL by recency. Live reads of a show inside the hot
window (``VAULT_HOT_WINDOW_HOURS``, 24h) use
``HOT_WINDOW_CACHE_TTL_SECONDS`` — 90 seconds by default. The 24h
``CACHE_TTL_SECONDS`` applies only to historical reads. Because the resolver's
show-night cadence (``RESOLVER_ACTIVE_INTERVAL_SECONDS``, 300s) is well longer
than that 90s TTL, every active-window poll gets a genuinely fresh phish.net
read and a real track count. A partial setlist therefore looks partial, not
falsely "stable".

This was NOT always true. An earlier mcp-phish applied the flat 24h TTL to
hot-window reads too, which froze the first partial snapshot for the whole
show and would have satisfied the stability clause instantly. Back then the
time backstop was the only real safety net. It no longer is: the stability
clause is now the working happy path, and the backstop is what it was always
meant to be — the fallback for a setlist whose track count never settles
(phish.net edits trickling for days, an unusual encore label, etc.).

If you point this resolver at an MCP without a short hot-window TTL, the
stability clause silently degrades back to "first partial snapshot wins" and
the backstop becomes load-bearing again. Verify with
``load_settings().hot_window_cache_ttl_seconds`` on the upstream before
tightening ``stable_polls`` or shortening the backstop.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import asyncpg

from setlist_stash.resolve_types import ParsedSetlist


@dataclass(frozen=True)
class PollState:
    """A row of the ``poll_state`` table (or its in-memory default)."""

    show_date: Any  # datetime.date
    last_track_count: int = 0
    encore_seen: bool = False
    stable_polls: int = 0
    complete: bool = False


@dataclass(frozen=True)
class CompletenessDecision:
    """Outcome of evaluating one poll against prior poll state.

    ``next_state`` is what should be persisted to ``poll_state``.
    ``complete`` is whether the show may be scored on THIS tick.
    ``reason`` is a short tag for logging / observability.
    """

    complete: bool
    next_state: PollState
    reason: str


def evaluate_completeness(
    *,
    parsed: ParsedSetlist,
    prior: PollState,
    now: datetime,
    effective_lock: datetime,
    stable_polls_required: int,
    backstop: timedelta,
) -> CompletenessDecision:
    """Pure completeness heuristic. No I/O.

    Folds this poll's observation into the prior poll state and decides whether
    the setlist may be scored now.

    Rules:
    - ``encore_seen`` latches True (a transient drop can't un-see it).
    - ``stable_polls`` increments when the track count is unchanged from the
      prior poll AND > 0; it resets to 0 the moment the count changes.
    - The first poll for a show (prior.last_track_count == 0, prior all
      defaults) establishes the baseline and counts as 1 stable poll only if
      it already carries tracks; otherwise stable stays 0.
    - Complete when (encore_seen AND stable_polls >= required) OR backstop.
    """
    track_count = parsed.song_count
    encore_now = bool(parsed.encore_slugs)

    encore_seen = prior.encore_seen or encore_now

    if track_count == 0:
        # No setlist this poll. Don't advance stability; leave the baseline.
        # (The resolver handles empty setlists separately via the cancel
        # window; this branch is defensive.)
        stable_polls = 0
    elif track_count == prior.last_track_count:
        # Count held steady. This is a stable poll.
        stable_polls = prior.stable_polls + 1
    else:
        # Count grew (or first time we have any tracks). New baseline; the
        # current observation itself is the first poll at this count.
        stable_polls = 1

    backstop_fired = now >= effective_lock + backstop
    stable_complete = encore_seen and stable_polls >= stable_polls_required
    complete = bool(stable_complete or backstop_fired)

    if backstop_fired and not stable_complete:
        reason = "backstop"
    elif stable_complete:
        reason = "stable"
    else:
        reason = "waiting"

    next_state = PollState(
        show_date=prior.show_date,
        last_track_count=track_count,
        encore_seen=encore_seen,
        stable_polls=stable_polls,
        complete=complete,
    )
    return CompletenessDecision(complete=complete, next_state=next_state, reason=reason)


# ----- poll_state persistence ----------------------------------------------


async def read_poll_state(pool: asyncpg.Pool[Any], show_date: Any) -> PollState:
    """Read a show's poll_state, or a fresh default if none exists yet."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT show_date, last_track_count, encore_seen, stable_polls, complete
              FROM poll_state
             WHERE show_date = $1
            """,
            show_date,
        )
    if row is None:
        return PollState(show_date=show_date)
    return PollState(
        show_date=row["show_date"],
        last_track_count=int(row["last_track_count"]),
        encore_seen=bool(row["encore_seen"]),
        stable_polls=int(row["stable_polls"]),
        complete=bool(row["complete"]),
    )


async def upsert_poll_state(pool: asyncpg.Pool[Any], state: PollState) -> None:
    """Persist poll state (insert or update), stamping last_polled_at."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO poll_state (
                show_date, last_track_count, encore_seen, stable_polls,
                complete, last_polled_at
            )
            VALUES ($1, $2, $3, $4, $5, now())
            ON CONFLICT (show_date) DO UPDATE
               SET last_track_count = EXCLUDED.last_track_count,
                   encore_seen      = EXCLUDED.encore_seen,
                   stable_polls     = EXCLUDED.stable_polls,
                   complete         = EXCLUDED.complete,
                   last_polled_at   = now()
            """,
            state.show_date,
            state.last_track_count,
            state.encore_seen,
            state.stable_polls,
            state.complete,
        )
