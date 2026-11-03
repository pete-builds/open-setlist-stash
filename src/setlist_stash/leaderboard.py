"""Leaderboard rebuilders + read helpers.

Materializes ``leaderboard_snapshots`` from ``predictions`` joined with
``users``. Three scopes:

- ``weekly``    — bucketed by ISO week of ``prediction_locks.show_date``
                  (e.g. ``2026-W18``). Easy: Postgres ``to_char(date,'IYYY-"W"IW')``.
- ``tour``      — DEFERRED. mcp-phish does not yet expose a clean tour mapping
                  per show. Until it does, we use a "season" fallback bucket
                  derived from year + meteorological season:
                  ``2026-spring`` (Mar/Apr/May), ``2026-summer`` (Jun/Jul/Aug),
                  ``2026-fall`` (Sep/Oct/Nov), ``2026-winter`` (Dec/Jan/Feb,
                  bucketed under the December year). Stored in the same
                  ``scope='tour'`` rows so the UI keeps a 3-scope shape;
                  upgrade to real tour slugs in a follow-on session when
                  mcp-phish exposes them.
- ``all_time``  — single bucket, ``scope_key='all'``.

Atomicity: each scope rebuild runs as one transaction
(DELETE-by-scope + INSERTs). Row counts are tiny (handles x scope-keys),
so cost is negligible. Caller should call ``rebuild_all`` after each
successful resolver tick.

Tie-break: ranks are dense by ``score_total DESC``, then deterministic by
``MIN(predictions.submitted_at) ASC`` (earlier-submitter wins), then
``handle ASC`` as a final tie-break so output is repeatable.

Schema notes (migration 001):
- Column names use ``total_score``, ``shows_played`` (not the spec-doc's
  ``score_total`` / ``predictions_resolved``). The session-3 prompt's
  shape was advisory; we honor migration 001 as-built. Reads adapt.
- The unique key is ``(scope, scope_key, user_id)``. We DELETE by
  ``(scope, scope_key)`` per rebuild, then INSERT the fresh ranks.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import asyncpg

# Split by concern: pure key logic in leaderboard_keys, every writer in
# leaderboard_rebuild, the read side here. Every public name that used to live
# in this module is re-imported under its old name, so callers keep importing
# it from setlist_stash.leaderboard.
from setlist_stash.leaderboard_keys import DEFAULT_TABS as DEFAULT_TABS
from setlist_stash.leaderboard_keys import VALID_SCOPES as VALID_SCOPES
from setlist_stash.leaderboard_keys import LeaderboardTab as LeaderboardTab
from setlist_stash.leaderboard_keys import derive_season_key as derive_season_key
from setlist_stash.leaderboard_keys import derive_tour_season_key as derive_tour_season_key
from setlist_stash.leaderboard_keys import normalize_scope as normalize_scope
from setlist_stash.leaderboard_keys import parse_runs as parse_runs
from setlist_stash.leaderboard_keys import parse_tabs as parse_tabs
from setlist_stash.leaderboard_keys import sort_scope_keys as sort_scope_keys
from setlist_stash.leaderboard_rebuild import rebuild_all as rebuild_all
from setlist_stash.leaderboard_rebuild import rebuild_all_time as rebuild_all_time
from setlist_stash.leaderboard_rebuild import rebuild_leagues as rebuild_leagues
from setlist_stash.leaderboard_rebuild import rebuild_runs as rebuild_runs
from setlist_stash.leaderboard_rebuild import rebuild_season as rebuild_season
from setlist_stash.leaderboard_rebuild import rebuild_weekly as rebuild_weekly

logger = logging.getLogger("setlist_stash.leaderboard")


# ----- types ----------------------------------------------------------------


@dataclass(frozen=True)
class LeaderboardRow:
    """One materialized leaderboard entry."""

    scope: str
    scope_key: str
    user_id: int
    handle: str
    total_score: int
    shows_played: int
    rank: int
    refreshed_at: datetime


# ----- read helpers ---------------------------------------------------------


async def list_scope_keys(
    pool: asyncpg.Pool[Any], scope: str
) -> list[str]:
    """Return the distinct scope_keys for a scope, newest first."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT scope_key
              FROM leaderboard_snapshots
             WHERE scope = $1
            """,
            scope,
        )
    return sort_scope_keys(scope, [str(r["scope_key"]) for r in rows])


async def latest_scope_key(pool: asyncpg.Pool[Any], scope: str) -> str | None:
    """Return the most recent scope_key for a scope, or None if empty.

    "Most recent" is the first key of :func:`sort_scope_keys`, which orders
    season buckets by calendar rather than by string. ``all`` is the only key
    for ``all_time``.
    """
    keys = await list_scope_keys(pool, scope)
    return keys[0] if keys else None


async def fetch_leaderboard(
    pool: asyncpg.Pool[Any],
    scope: str,
    scope_key: str,
    *,
    limit: int = 50,
) -> list[LeaderboardRow]:
    """Return the top-N leaderboard rows for a (scope, scope_key)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT scope, scope_key, user_id, handle,
                   total_score, shows_played, rank, refreshed_at
              FROM leaderboard_snapshots
             WHERE scope = $1 AND scope_key = $2
             ORDER BY rank ASC, handle ASC
             LIMIT $3
            """,
            scope,
            scope_key,
            limit,
        )
    return [
        LeaderboardRow(
            scope=str(r["scope"]),
            scope_key=str(r["scope_key"]),
            user_id=int(r["user_id"]),
            handle=str(r["handle"]),
            total_score=int(r["total_score"]),
            shows_played=int(r["shows_played"]),
            rank=int(r["rank"]),
            refreshed_at=r["refreshed_at"],
        )
        for r in rows
    ]


async def list_show_entrants(
    pool: asyncpg.Pool[Any],
    show_date: Any,
    *,
    limit: int = 50,
) -> list[LeaderboardRow]:
    """List the players who've entered a show, all at score 0.

    Used to populate the global leaderboard *before* any scores land: rather
    than a bare "No scores yet" panel, the page lists everyone who has a
    prediction for the target show at 0 (same spirit as the league
    scoreboard's pre-show zeros). Ordered by earliest submitter first so the
    list is stable across refreshes.

    Fair-play: this returns only handles at 0 — never anyone's picks. Safe to
    show pre-lock. The ``rank`` is the 1-based position in submit order;
    ``shows_played`` is 0 (this entry isn't a scored show yet) and
    ``refreshed_at`` is ``now()`` so the row shape matches ``LeaderboardRow``.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT u.handle, p.submitted_at, p.user_id
              FROM predictions p
              JOIN users u ON u.id = p.user_id
             WHERE p.show_date = $1
             ORDER BY p.submitted_at ASC, u.handle ASC
             LIMIT $2
            """,
            show_date,
            limit,
        )
    # Timezone-aware UTC, matching what asyncpg hands back for the real
    # ``refreshed_at`` TIMESTAMPTZ rows. The template's ``display_dt`` filter
    # converts to Eastern on the way out; a naive local ``datetime.now()`` here
    # would render as an unconverted server clock.
    now = datetime.now(UTC)
    return [
        LeaderboardRow(
            scope="entrants",
            scope_key=str(show_date),
            user_id=int(r["user_id"]),
            handle=str(r["handle"]),
            total_score=0,
            shows_played=0,
            rank=i,
            refreshed_at=now,
        )
        for i, r in enumerate(rows, start=1)
    ]


async def fetch_user_rank(
    pool: asyncpg.Pool[Any],
    scope: str,
    scope_key: str,
    user_id: int,
) -> LeaderboardRow | None:
    """Return the current user's leaderboard row for this (scope, scope_key).

    Used by the page header to surface the user's rank even when they're
    outside the top-N.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT scope, scope_key, user_id, handle,
                   total_score, shows_played, rank, refreshed_at
              FROM leaderboard_snapshots
             WHERE scope = $1 AND scope_key = $2 AND user_id = $3
            """,
            scope,
            scope_key,
            user_id,
        )
    if row is None:
        return None
    return LeaderboardRow(
        scope=str(row["scope"]),
        scope_key=str(row["scope_key"]),
        user_id=int(row["user_id"]),
        handle=str(row["handle"]),
        total_score=int(row["total_score"]),
        shows_played=int(row["shows_played"]),
        rank=int(row["rank"]),
        refreshed_at=row["refreshed_at"],
    )
