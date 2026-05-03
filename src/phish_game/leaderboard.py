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
from datetime import datetime
from typing import Any

import asyncpg

logger = logging.getLogger("phish_game.leaderboard")


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


# ----- scope-key derivation -------------------------------------------------


_SEASON_BY_MONTH = {
    1: "winter",  # Jan attaches to previous year (handled in SQL)
    2: "winter",
    3: "spring",
    4: "spring",
    5: "spring",
    6: "summer",
    7: "summer",
    8: "summer",
    9: "fall",
    10: "fall",
    11: "fall",
    12: "winter",  # Dec attaches to current year
}


def derive_season_key(show_date: Any) -> str:
    """Return the season scope key for a show date.

    Plan B for the deferred tour scope. Format: ``YYYY-<season>``.
    Winter spans Dec-Jan-Feb and is bucketed under the December year, so
    Jan 2026 lands in ``2025-winter`` alongside Dec 2025.
    """
    if hasattr(show_date, "month"):
        month = int(show_date.month)
        year = int(show_date.year)
    else:  # ISO string fallback
        parts = str(show_date).split("-")
        year = int(parts[0])
        month = int(parts[1])
    season = _SEASON_BY_MONTH[month]
    if month in (1, 2):
        year -= 1
    return f"{year}-{season}"


# ----- rebuilders -----------------------------------------------------------


async def rebuild_weekly(pool: asyncpg.Pool[Any]) -> int:
    """Rebuild the weekly leaderboard for every ISO week with resolved scores.

    Returns the count of rows written across all scope_keys.
    """
    # NOTE on the format string: PG's to_char treats double-quoted text as
    # literal. We embed a single-quoted SQL literal containing double quotes
    # via Python concatenation so neither layer's escape rules conflict.
    # Result: scope_key looks like '2026-W18'.
    weekly_fmt = "'IYYY-" + '"W"' + "IW'"
    return await _rebuild_bucketed(
        pool,
        scope="weekly",
        bucket_sql=f"to_char(p.show_date, {weekly_fmt})",
    )


async def rebuild_season(pool: asyncpg.Pool[Any]) -> int:
    """Rebuild the season ('tour') leaderboard via the meteorological-season fallback.

    Stored under ``scope='tour'``. When mcp-phish exposes real tour mappings,
    a follow-on session can swap this for `rebuild_tour` and migrate the
    scope_keys. The UI continues to render whichever rows exist.
    """
    # The bucket SQL mirrors derive_season_key:
    #   Mar-May -> spring, Jun-Aug -> summer, Sep-Nov -> fall,
    #   Dec or Jan/Feb -> winter (Jan/Feb roll back to prior year).
    bucket_sql = r"""
        CASE
            WHEN extract(month FROM p.show_date) BETWEEN 3 AND 5
                THEN to_char(p.show_date, 'YYYY') || '-spring'
            WHEN extract(month FROM p.show_date) BETWEEN 6 AND 8
                THEN to_char(p.show_date, 'YYYY') || '-summer'
            WHEN extract(month FROM p.show_date) BETWEEN 9 AND 11
                THEN to_char(p.show_date, 'YYYY') || '-fall'
            WHEN extract(month FROM p.show_date) = 12
                THEN to_char(p.show_date, 'YYYY') || '-winter'
            ELSE
                -- Jan or Feb: roll back to the previous year's winter.
                to_char(p.show_date - INTERVAL '2 months', 'YYYY') || '-winter'
        END
    """
    return await _rebuild_bucketed(pool, scope="tour", bucket_sql=bucket_sql)


async def rebuild_all_time(pool: asyncpg.Pool[Any]) -> int:
    """Rebuild the all-time leaderboard. Single bucket: ``scope_key='all'``."""
    return await _rebuild_bucketed(pool, scope="all_time", bucket_sql="'all'")


async def rebuild_all(pool: asyncpg.Pool[Any]) -> dict[str, int]:
    """Rebuild every scope. Returns ``{scope: rows_written}``.

    Each scope's rebuild is its own transaction. A failure in one scope does
    NOT roll back the others (caller logs and continues).
    """
    out: dict[str, int] = {}
    for name, fn in (
        ("weekly", rebuild_weekly),
        ("tour", rebuild_season),
        ("all_time", rebuild_all_time),
    ):
        try:
            out[name] = await fn(pool)
        except Exception:
            logger.exception("leaderboard rebuild failed", extra={"scope": name})
            out[name] = -1
    return out


# ----- league scope (Phase 4c) ----------------------------------------------


async def rebuild_leagues(pool: asyncpg.Pool[Any]) -> dict[str, int]:
    """Rebuild a per-league leaderboard for every active league.

    For each non-deleted league, recompute ``total_score`` and
    ``shows_played`` for every member, optionally filtered to
    ``[start_date, end_date]`` when the league has a tour window set.
    Inserts into ``leaderboard_snapshots`` with ``scope='league'`` and
    ``scope_key=league.slug``.

    Each league rebuild is its own transaction (DELETE-by-scope_key +
    INSERTs). A failure on one league does NOT block others.

    Returns ``{slug: rows_written}``. ``-1`` is the sentinel for "this
    league errored; check logs."
    """
    out: dict[str, int] = {}
    async with pool.acquire() as conn:
        leagues = await conn.fetch(
            """
            SELECT id, slug, start_date, end_date
              FROM leagues
             WHERE deleted_at IS NULL
            """
        )
    for league_row in leagues:
        slug = str(league_row["slug"])
        try:
            out[slug] = await _rebuild_one_league(
                pool,
                league_id=int(league_row["id"]),
                slug=slug,
                start_date=league_row["start_date"],
                end_date=league_row["end_date"],
            )
        except Exception:
            logger.exception(
                "league leaderboard rebuild failed",
                extra={"slug": slug},
            )
            out[slug] = -1
    return out


async def _rebuild_one_league(
    pool: asyncpg.Pool[Any],
    *,
    league_id: int,
    slug: str,
    start_date: Any,
    end_date: Any,
) -> int:
    """Atomically rebuild ``scope='league', scope_key=<slug>``.

    Tour window: when ``start_date`` is set, only predictions whose
    ``show_date >= start_date`` are counted; same for ``end_date`` /
    ``<= end_date``. Both nullable; both inclusive when set.

    Predictions filter:
      - ``score IS NOT NULL`` (resolved)
      - user is in ``league_members`` for this league
      - show_date within optional tour window
    """
    sql = """
        WITH agg AS (
            SELECT
                p.user_id,
                u.handle,
                SUM(p.score)::int           AS total_score,
                COUNT(*)::int               AS shows_played,
                MIN(p.submitted_at)         AS first_submitted_at
            FROM predictions p
            JOIN users u ON u.id = p.user_id
            JOIN league_members lm ON lm.user_id = p.user_id
            WHERE p.score IS NOT NULL
              AND lm.league_id = $1
              AND ($3::date IS NULL OR p.show_date >= $3)
              AND ($4::date IS NULL OR p.show_date <= $4)
            GROUP BY p.user_id, u.handle
        ),
        ranked AS (
            SELECT
                'league'::text AS scope,
                $2::text AS scope_key,
                user_id,
                handle,
                total_score,
                shows_played,
                RANK() OVER (
                    ORDER BY total_score DESC, first_submitted_at ASC, handle ASC
                ) AS rank
            FROM agg
        ),
        deleted AS (
            DELETE FROM leaderboard_snapshots
             WHERE scope = 'league' AND scope_key = $2
            RETURNING 1
        ),
        inserted AS (
            INSERT INTO leaderboard_snapshots
                (scope, scope_key, user_id, handle, total_score, shows_played, rank, refreshed_at)
            SELECT
                scope, scope_key, user_id, handle, total_score, shows_played, rank, now()
            FROM ranked
            RETURNING 1
        )
        SELECT
            (SELECT COUNT(*) FROM deleted)  AS deleted_count,
            (SELECT COUNT(*) FROM inserted) AS inserted_count
    """
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(sql, league_id, slug, start_date, end_date)
    inserted = int(row["inserted_count"]) if row else 0
    deleted = int(row["deleted_count"]) if row else 0
    logger.info(
        "league leaderboard rebuilt",
        extra={
            "scope_key": slug,
            "deleted": deleted,
            "inserted": inserted,
        },
    )
    return inserted


async def _rebuild_bucketed(
    pool: asyncpg.Pool[Any], *, scope: str, bucket_sql: str
) -> int:
    """Atomically rebuild one scope.

    Strategy:
        DELETE rows where scope = $1
        INSERT new ranked rows from a windowed SELECT over predictions+users
        joined to compute total_score + shows_played per (scope_key, user).

    The WHERE clause filters to predictions whose score is non-NULL — that is
    the resolver's "this prediction has been scored" signal. Cancelled-show
    predictions have score=0 (set by the resolver), so they DO count toward
    shows_played but contribute 0 to total_score, which feels right: the
    user showed up, they just earned no points for that night.
    """
    # bucket_sql is a module-private SQL fragment built from constants in
    # this file (never user input). Safe against injection. ruff S608 is
    # silenced explicitly so this stays auditable.
    sql_template = """
        WITH agg AS (
            SELECT
                __BUCKET__ AS scope_key,
                p.user_id,
                u.handle,
                SUM(p.score)::int           AS total_score,
                COUNT(*)::int               AS shows_played,
                MIN(p.submitted_at)         AS first_submitted_at
            FROM predictions p
            JOIN users u ON u.id = p.user_id
            WHERE p.score IS NOT NULL
            GROUP BY 1, p.user_id, u.handle
        ),
        ranked AS (
            SELECT
                $1::text AS scope,
                scope_key,
                user_id,
                handle,
                total_score,
                shows_played,
                RANK() OVER (
                    PARTITION BY scope_key
                    ORDER BY total_score DESC, first_submitted_at ASC, handle ASC
                ) AS rank
            FROM agg
        ),
        deleted AS (
            DELETE FROM leaderboard_snapshots WHERE scope = $1
            RETURNING 1
        ),
        inserted AS (
            INSERT INTO leaderboard_snapshots
                (scope, scope_key, user_id, handle, total_score, shows_played, rank, refreshed_at)
            SELECT
                scope, scope_key, user_id, handle, total_score, shows_played, rank, now()
            FROM ranked
            RETURNING 1
        )
        SELECT
            (SELECT COUNT(*) FROM deleted)  AS deleted_count,
            (SELECT COUNT(*) FROM inserted) AS inserted_count
    """
    sql = sql_template.replace("__BUCKET__", bucket_sql)
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(sql, scope)
    inserted = int(row["inserted_count"]) if row else 0
    deleted = int(row["deleted_count"]) if row else 0
    logger.info(
        "leaderboard rebuilt",
        extra={"scope": scope, "deleted": deleted, "inserted": inserted},
    )
    return inserted


# ----- read helpers ---------------------------------------------------------


async def list_scope_keys(
    pool: asyncpg.Pool[Any], scope: str
) -> list[str]:
    """Return the distinct scope_keys for a scope, ordered desc (newest first)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT scope_key
              FROM leaderboard_snapshots
             WHERE scope = $1
             ORDER BY scope_key DESC
            """,
            scope,
        )
    return [str(r["scope_key"]) for r in rows]


async def latest_scope_key(pool: asyncpg.Pool[Any], scope: str) -> str | None:
    """Return the most recent scope_key for a scope, or None if empty.

    "Most recent" is defined as max(scope_key) lexicographically — which is
    correct for our keys (``2026-W18`` > ``2026-W17``, ``2026-spring`` >
    ``2025-winter``, ``all`` is the only one for ``all_time``).
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT scope_key
              FROM leaderboard_snapshots
             WHERE scope = $1
             ORDER BY scope_key DESC
             LIMIT 1
            """,
            scope,
        )
    if row is None:
        return None
    return str(row["scope_key"])


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


# ----- scope helpers --------------------------------------------------------


VALID_SCOPES: tuple[str, ...] = ("weekly", "tour", "all_time")


def normalize_scope(raw: str) -> str:
    """Coerce 'all-time' / 'all_time' / 'season' / 'tour' / 'weekly' to canonical.

    The task's design called for a hyphenated ``all-time``; migration 001
    declared ``all_time``. We accept both at the URL boundary so links don't
    break when typed either way.
    """
    s = (raw or "").strip().lower()
    if s in ("weekly", "week"):
        return "weekly"
    if s in ("tour", "season"):
        return "tour"
    if s in ("all_time", "all-time", "alltime", "all"):
        return "all_time"
    return s
