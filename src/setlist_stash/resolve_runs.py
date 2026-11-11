"""``scoring_runs`` bookkeeping for the resolver.

Split out of ``resolve.py``: the watchdog, the start/finish stamps for one
tick's row, and the latest-run summary /healthz reads.
``setlist_stash.resolve`` re-exports the public names.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

import asyncpg

# Same logger name as before the split, so log lines are unchanged.
logger = logging.getLogger("setlist_stash.resolve")


# ----- run lifecycle helpers ------------------------------------------------


async def watchdog_stale_running(
    pool: asyncpg.Pool[Any], stale_after_minutes: int = 15
) -> int:
    """Mark stale 'running' rows as 'error'. Returns count flipped.

    A previous resolver process that died mid-run leaves a row with
    status='running'. Subsequent ticks should not block on it. This stamps
    them and proceeds.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            UPDATE scoring_runs
               SET status = 'error',
                   finished_at = COALESCE(finished_at, now()),
                   error_message = 'watchdog: stale running row'
             WHERE status = 'running'
               AND started_at < now() - ($1 || ' minutes')::interval
            RETURNING id
            """,
            str(stale_after_minutes),
        )
    if rows:
        logger.warning(
            "watchdog flipped stale running rows",
            extra={"count": len(rows), "ids": [int(r["id"]) for r in rows]},
        )
    return len(rows)


async def _start_run(pool: asyncpg.Pool[Any]) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO scoring_runs (status, started_at)
            VALUES ('running', now())
            RETURNING id
            """
        )
    if row is None:
        raise RuntimeError("could not start scoring_runs row")
    return int(row["id"])


async def _finish_run(
    pool: asyncpg.Pool[Any],
    run_id: int,
    *,
    status: str,
    shows_scanned: int,
    shows_resolved: int,
    predictions_scored: int,
    summary: dict[str, Any],
    error_message: str | None = None,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE scoring_runs
               SET status = $2,
                   finished_at = now(),
                   shows_scanned = $3,
                   shows_resolved = $4,
                   predictions_scored = $5,
                   summary = $6::jsonb,
                   error_message = $7
             WHERE id = $1
            """,
            run_id,
            status,
            shows_scanned,
            shows_resolved,
            predictions_scored,
            json.dumps(summary),
            error_message,
        )


# ----- run summary ----------------------------------------------------------


async def latest_run_summary(
    pool: asyncpg.Pool[Any], *, display_tz: str = "America/New_York"
) -> dict[str, Any] | None:
    """Return ``{started_at, finished_at, status}`` for the most recent run.

    Timestamps come back as ISO-8601 in ``display_tz`` (Eastern), not UTC.
    /healthz is the operator-visible status surface, so the resolver heartbeat
    should read in the same clock as the rest of the site. The offset stays on
    the string, so it is still an unambiguous instant for machine consumers.
    """
    tz = ZoneInfo(display_tz)

    def _iso(value: datetime | None) -> str | None:
        if value is None:
            return None
        aware = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return aware.astimezone(tz).isoformat()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT started_at, finished_at, status
              FROM scoring_runs
             ORDER BY id DESC
             LIMIT 1
            """
        )
    if row is None:
        return None
    return {
        "started_at": _iso(row["started_at"]),
        "finished_at": _iso(row["finished_at"]),
        "status": row["status"],
    }
