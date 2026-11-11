"""Leaderboard rebuilders: materialize ``leaderboard_snapshots``.

Split out of ``leaderboard.py``, whose module docstring still describes the
scopes, atomicity and tie-break rules these implement.
``setlist_stash.leaderboard`` re-exports the public names.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

import asyncpg

from setlist_stash.leaderboard_keys import _RUN_KEY_RE

# Same logger name as before the split, so log lines are unchanged.
logger = logging.getLogger("setlist_stash.leaderboard")


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
    """Rebuild the season ('tour') leaderboard from the real tour when known.

    Stored under ``scope='tour'``. Each show's bucket is
    ``prediction_locks.tour_season_key`` — derived from the UPSTREAM tour name
    by :func:`derive_tour_season_key` when the lock row was written — and falls
    back to the calendar-month expression below whenever that column is NULL.

    The fallback is load-bearing in two directions, not just for history.
    Shows whose upstream tour field is a placeholder stay NULL forever, and on
    the Umphrey's tenant that is EVERY show, so this function must keep
    producing exactly its old output when no row has a tour key.

    Bucket keys keep the ``YYYY-<season>`` shape rather than becoming raw
    upstream tour slugs, so a deployment's configured ``LEADERBOARD_TABS``
    entries (which name scope_keys like ``2026-summer``) keep resolving.
    """
    # The fallback SQL mirrors derive_season_key:
    #   Mar-May -> spring, Jun-Aug -> summer, Sep-Nov -> fall,
    #   Dec or Jan/Feb -> winter (Jan/Feb roll back to prior year).
    month_sql = r"""
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
    return await _rebuild_bucketed(
        pool,
        scope="tour",
        bucket_sql=f"COALESCE(pl.tour_season_key, {month_sql})",
        extra_join=(
            "LEFT JOIN prediction_locks pl ON pl.show_date = p.show_date"
        ),
    )


async def rebuild_all_time(pool: asyncpg.Pool[Any]) -> int:
    """Rebuild the all-time leaderboard. Single bucket: ``scope_key='all'``."""
    return await _rebuild_bucketed(pool, scope="all_time", bucket_sql="'all'")


# ----- run scope (named show runs: residencies, festivals, venue stands) ----


async def rebuild_runs(
    pool: asyncpg.Pool[Any], runs: Mapping[str, Sequence[date]]
) -> int:
    """Rebuild every configured run board in one pass. ``scope='run'``.

    All runs rebuild together because ``_rebuild_bucketed`` deletes by scope;
    a per-run call would wipe the boards built by the previous call.
    """
    if not runs:
        return 0
    cases: list[str] = []
    every_date: set[date] = set()
    for key, dates in runs.items():
        if not _RUN_KEY_RE.match(key):
            # parse_runs already guarantees this; belt and braces, because a
            # bad key here would be inlined into SQL below.
            raise ValueError(f"unsafe run key: {key!r}")
        literals = ", ".join(f"DATE '{d.isoformat()}'" for d in dates)
        cases.append(f"WHEN p.show_date IN ({literals}) THEN '{key}'")
        every_date.update(dates)
    bucket_sql = "CASE " + " ".join(cases) + " END"
    all_literals = ", ".join(
        f"DATE '{d.isoformat()}'" for d in sorted(every_date)
    )
    # Every value inlined above is a `date` rendered as ISO or a slug matched
    # against _RUN_KEY_RE, so neither can carry SQL. Same auditability posture
    # as the other bucket_sql fragments in this module.
    return await _rebuild_bucketed(
        pool,
        scope="run",
        bucket_sql=bucket_sql,
        extra_where=f"AND p.show_date IN ({all_literals})",
    )


async def rebuild_all(
    pool: asyncpg.Pool[Any],
    runs: Mapping[str, Sequence[date]] | None = None,
) -> dict[str, int]:
    """Rebuild every scope. Returns ``{scope: rows_written}``.

    Each scope's rebuild is its own transaction. A failure in one scope does
    NOT roll back the others (caller logs and continues).

    ``runs`` is the parsed ``LEADERBOARD_RUNS`` config. Omitted or empty, no
    run boards are built and the behavior is exactly the pre-run-scope one —
    the weekly/tour/all_time scopes always rebuild regardless of which tabs a
    deployment chooses to render, so hiding a tab never destroys its data.
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
    if runs:
        try:
            out["run"] = await rebuild_runs(pool, runs)
        except Exception:
            logger.exception("leaderboard rebuild failed", extra={"scope": "run"})
            out["run"] = -1
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
    pool: asyncpg.Pool[Any],
    *,
    scope: str,
    bucket_sql: str,
    extra_where: str = "",
    extra_join: str = "",
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
            __EXTRA_JOIN__
            WHERE p.score IS NOT NULL __EXTRA_WHERE__
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
    sql = (
        sql_template.replace("__BUCKET__", bucket_sql)
        .replace("__EXTRA_WHERE__", extra_where)
        .replace("__EXTRA_JOIN__", extra_join)
    )
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(sql, scope)
    inserted = int(row["inserted_count"]) if row else 0
    deleted = int(row["deleted_count"]) if row else 0
    logger.info(
        "leaderboard rebuilt",
        extra={"scope": scope, "deleted": deleted, "inserted": inserted},
    )
    return inserted
