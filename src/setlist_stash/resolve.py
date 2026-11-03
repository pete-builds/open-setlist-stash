"""Auto-resolver for predictions.

When phish.net publishes a setlist, this module fetches the setlist via
mcp-phish, scores every prediction for that show, writes the scores back,
and stamps `prediction_locks.resolved_at`.

docs/PHASE-4-PLAN.md §5 is the design source. Pete approval (build session 2):
- Inside-container loop (asyncio.sleep) over external cron, for parity with
  the rest of the homelab and easier log capture.
- Conservative cancelled-show window: don't auto-cancel within 72h of
  lock_at — phish.net's setlist publish can lag.
- Watchdog stamps stale 'running' rows as 'error' on startup so a crashed
  process doesn't poison subsequent ticks.

Entrypoints:
- ``python -m setlist_stash.resolve``         - single tick, exit 0
- ``python -m setlist_stash.resolve --loop``  - run forever, sleep between ticks

Both honor ``RESOLVER_INTERVAL_SECONDS`` (default 1800) and
``RESOLVER_CANCEL_AFTER_HOURS`` (default 72).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import asyncpg

from setlist_stash import __version__
from setlist_stash.config import Settings, get_settings
from setlist_stash.db import close_pool, get_pool, init_pool
from setlist_stash.leaderboard import (
    derive_tour_season_key,
    parse_runs,
    rebuild_all,
    rebuild_leagues,
)
from setlist_stash.logging_setup import configure_logging

# McpPhishClient is looked up in THIS module at call time by run_tick and
# backfill_tour_season_keys; tests swap it via monkeypatch.setattr(resolve, ...).
from setlist_stash.mcp_client import (
    McpPhishClient,
    McpPhishError,
    McpPhishUnavailable,
)
from setlist_stash.migrate import run_migrations

# The resolver is split by concern across resolve_setlist / resolve_runs /
# resolve_shows. Every public name that used to live here is re-imported under
# its old name, so callers and tests keep importing it from
# setlist_stash.resolve. The private helpers imported below are the ones
# run_tick calls, looked up in this module's namespace.
from setlist_stash.resolve_runs import _finish_run as _finish_run
from setlist_stash.resolve_runs import _start_run as _start_run
from setlist_stash.resolve_runs import latest_run_summary as latest_run_summary
from setlist_stash.resolve_runs import watchdog_stale_running as watchdog_stale_running
from setlist_stash.resolve_setlist import parse_setlist as parse_setlist
from setlist_stash.resolve_shows import ShowResolveOutcome as ShowResolveOutcome
from setlist_stash.resolve_shows import _open_locks as _open_locks
from setlist_stash.resolve_shows import _resolve_show as _resolve_show
from setlist_stash.resolve_types import ParsedSetlist

# Re-exported for backwards-compat: callers (and tests) import ParsedSetlist
# from setlist_stash.resolve.
__all__ = ["ParsedSetlist", "parse_setlist", "run_tick"]

logger = logging.getLogger("setlist_stash.resolve")


# ----- single tick ----------------------------------------------------------


@dataclass
class TickResult:
    status: str  # 'noop' | 'success' | 'partial' | 'error'
    shows_scanned: int
    shows_resolved: int
    predictions_scored: int
    summary: dict[str, Any]
    # Recommended seconds to sleep before the next tick. Fast cadence while an
    # open unresolved lock has an active show window; coarse otherwise. The
    # loop honors this; single-tick callers ignore it.
    next_interval_seconds: int = 0


def _has_active_window(
    open_locks: list[dict[str, Any]], settings: Settings, now: datetime
) -> bool:
    """True if any open lock's show window is currently active.

    A show window is active from the effective lock until
    ``resolver_active_window_hours`` after it — the span during which a live
    setlist is being typed in and the fast poll cadence is warranted.
    """
    window = timedelta(hours=settings.resolver_active_window_hours)
    for lock_row in open_locks:
        effective_lock = lock_row.get("lock_at_override") or lock_row["lock_at"]
        if effective_lock.tzinfo is None:
            effective_lock = effective_lock.replace(tzinfo=UTC)
        if effective_lock <= now < effective_lock + window:
            return True
    return False


def _next_interval(
    open_locks: list[dict[str, Any]], settings: Settings, now: datetime
) -> int:
    """Pick the next sleep interval: fast if a show window is active."""
    if _has_active_window(open_locks, settings, now):
        return settings.resolver_active_interval_seconds
    return settings.resolver_interval_seconds


async def run_tick(settings: Settings) -> TickResult:
    """Execute one resolver tick. Idempotent + safe to invoke any time."""
    pool = get_pool()
    await watchdog_stale_running(pool)
    run_id = await _start_run(pool)

    cancel_after = timedelta(hours=settings.resolver_cancel_after_hours)
    summary: dict[str, Any] = {"shows": [], "errors": []}
    shows_resolved = 0
    live_scored = 0
    predictions_scored = 0
    encountered_error = False
    now = datetime.now(UTC)

    try:
        open_locks = await _open_locks(pool)
        next_interval = _next_interval(open_locks, settings, now)
        if not open_locks:
            await _finish_run(
                pool, run_id,
                status="noop",
                shows_scanned=0,
                shows_resolved=0,
                predictions_scored=0,
                summary={"shows": [], "note": "no open locks"},
            )
            return TickResult(
                "noop", 0, 0, 0, {"shows": [], "note": "no open locks"},
                next_interval_seconds=next_interval,
            )

        async with McpPhishClient(
            settings.mcp_phish_url,
            timeout_seconds=settings.mcp_phish_timeout_seconds,
        ) as mcp:
            for lock_row in open_locks:
                show_date_str = (
                    lock_row["show_date"].isoformat()
                    if hasattr(lock_row["show_date"], "isoformat")
                    else str(lock_row["show_date"])
                )
                try:
                    outcome = await _resolve_show(
                        pool, mcp, lock_row,
                        cancel_after=cancel_after,
                        settings=settings,
                    )
                except McpPhishUnavailable as exc:
                    encountered_error = True
                    summary["errors"].append(
                        {"show_date": show_date_str, "error": f"mcp_unavailable: {exc!s}"[:200]}
                    )
                    logger.exception("mcp-phish unavailable resolving show",
                                     extra={"show_date": show_date_str})
                    continue
                except Exception as exc:
                    encountered_error = True
                    summary["errors"].append(
                        {"show_date": show_date_str, "error": str(exc)[:200]}
                    )
                    logger.exception("error resolving show",
                                     extra={"show_date": show_date_str})
                    continue

                summary["shows"].append(
                    {
                        "show_date": outcome.show_date,
                        "status": outcome.status,
                        "predictions_scored": outcome.predictions_scored,
                        "setlist_song_count": outcome.setlist_song_count,
                    }
                )
                if outcome.status in ("resolved", "cancelled"):
                    shows_resolved += 1
                    predictions_scored += outcome.predictions_scored
                elif outcome.status == "scored_live":
                    live_scored += 1
                    predictions_scored += outcome.predictions_scored
                if outcome.status == "error":
                    encountered_error = True
                    if outcome.error:
                        summary["errors"].append(
                            {"show_date": outcome.show_date, "error": outcome.error}
                        )

        worked = shows_resolved + live_scored
        if encountered_error:
            status = "partial" if worked > 0 else "error"
        else:
            status = "success" if worked > 0 else "noop"

        # Refresh leaderboard snapshots when any show was scored this tick —
        # including live partial scoring (scored_live), so the league board
        # updates throughout the show, not just at finalization. Errors here
        # log + continue so the resolver tick stays green; the next tick retries.
        if worked > 0:
            try:
                rebuild_counts = await rebuild_all(
                    pool, parse_runs(settings.leaderboard_runs)
                )
                summary["leaderboard"] = rebuild_counts
            except Exception as exc:  # pragma: no cover - belt-and-suspenders
                logger.exception("leaderboard rebuild failed; continuing tick")
                summary["leaderboard_error"] = str(exc)[:200]
            # Phase 4c: per-league leaderboards. Same log-and-continue stance —
            # a busted league rebuild can't fail the resolver tick.
            try:
                league_counts = await rebuild_leagues(pool)
                summary["league_leaderboards"] = league_counts
            except Exception as exc:  # pragma: no cover - belt-and-suspenders
                logger.exception(
                    "league leaderboard rebuild failed; continuing tick"
                )
                summary["league_leaderboards_error"] = str(exc)[:200]

        await _finish_run(
            pool, run_id,
            status=status,
            shows_scanned=len(open_locks),
            shows_resolved=shows_resolved,
            predictions_scored=predictions_scored,
            summary=summary,
            error_message=None if not encountered_error else "see summary.errors",
        )
        return TickResult(
            status=status,
            shows_scanned=len(open_locks),
            shows_resolved=shows_resolved,
            predictions_scored=predictions_scored,
            summary=summary,
            next_interval_seconds=next_interval,
        )
    except Exception as exc:
        logger.exception("resolver tick failed")
        await _finish_run(
            pool, run_id,
            status="error",
            shows_scanned=0,
            shows_resolved=shows_resolved,
            predictions_scored=predictions_scored,
            summary=summary,
            error_message=str(exc)[:500],
        )
        raise


# ----- entrypoint -----------------------------------------------------------


async def backfill_tour_season_keys(
    pool: asyncpg.Pool[Any], settings: Settings
) -> int:
    """Fill ``prediction_locks.tour_season_key`` for rows that predate it.

    Migration 014 adds the column NULL, and a NULL is indistinguishable from
    "this show genuinely has no tour" as far as rebuild_season is concerned.
    So without this, every show locked before the migration keeps its
    month-derived bucket forever and the fix ships doing nothing visible —
    which is precisely the failure it was written to correct.

    Returns the number of rows updated. Cheap and self-limiting: it queries the
    MCP once per distinct year that still has NULL rows, and once every row is
    filled it costs a single indexed COUNT and exits. Fail-soft by design; a
    backfill is never worth taking the resolver down for, and the next startup
    retries whatever it missed.

    Deliberately only writes rows that are NULL. A show whose upstream tour tag
    later changes is not re-keyed here, because silently rewriting settled
    leaderboard history on a container restart is worse than a stale bucket.
    """
    async with pool.acquire() as conn:
        pending = await conn.fetch(
            "SELECT show_date FROM prediction_locks "
            "WHERE tour_season_key IS NULL ORDER BY show_date"
        )
    if not pending:
        return 0
    years = sorted({r["show_date"].year for r in pending})
    tour_by_date: dict[str, str] = {}
    try:
        async with McpPhishClient(
            settings.mcp_phish_url,
            timeout_seconds=settings.mcp_phish_timeout_seconds,
        ) as mcp:
            for yr in years:
                for row in await mcp.search_shows(year=yr, limit=200):
                    d = str(row.get("date") or "")
                    tour = row.get("tour_name")
                    if d and tour:
                        tour_by_date[d] = str(tour)
    except McpPhishError:
        logger.warning(
            "tour backfill skipped: mcp unreachable",
            extra={"pending_rows": len(pending), "years": years},
        )
        return 0
    updates: list[tuple[str, date]] = []
    for r in pending:
        show_date = r["show_date"]
        key = derive_tour_season_key(
            tour_by_date.get(show_date.isoformat()), show_date
        )
        if key is not None:
            updates.append((key, show_date))
    if not updates:
        logger.info(
            "tour backfill: no upstream tour names for pending shows",
            extra={"pending_rows": len(pending)},
        )
        return 0
    async with pool.acquire() as conn:
        await conn.executemany(
            "UPDATE prediction_locks SET tour_season_key = $1 "
            "WHERE show_date = $2 AND tour_season_key IS NULL",
            updates,
        )
    logger.info(
        "tour backfill complete",
        extra={"rows_updated": len(updates), "pending_rows": len(pending)},
    )
    return len(updates)


async def _bootstrap(settings: Settings) -> None:
    """Initialize pool + apply migrations. Mirrors the FastAPI lifespan."""
    pool = await init_pool(settings)
    await run_migrations(pool)
    # After migrations, never before: 014 creates the column this reads.
    await backfill_tour_season_keys(pool, settings)


async def _amain(loop: bool, interval_override: int | None) -> int:
    settings = get_settings()
    configure_logging(settings.log_format)
    await _bootstrap(settings)
    try:
        if not loop:
            result = await run_tick(settings)
            logger.info(
                "resolver tick complete",
                extra={
                    "status": result.status,
                    "shows_scanned": result.shows_scanned,
                    "shows_resolved": result.shows_resolved,
                    "predictions_scored": result.predictions_scored,
                },
            )
            return 0

        logger.info(
            "resolver loop starting",
            extra={
                "interval_override": interval_override,
                "coarse_interval_seconds": settings.resolver_interval_seconds,
                "active_interval_seconds": settings.resolver_active_interval_seconds,
                # Derived, not configured: stable_polls * active_interval is the
                # real protection against finalizing a show mid-encore. Logged so
                # a cadence change that quietly shrank it is visible in the logs
                # instead of only showing up as a wrong score months later.
                "stable_polls_required": settings.resolver_stable_polls_required,
                "stable_quiet_window_seconds": (
                    settings.resolver_stable_polls_required
                    * settings.resolver_active_interval_seconds
                ),
                "live_refresh_seconds": settings.live_refresh_seconds,
                "version": __version__,
            },
        )
        while True:
            # Default to the coarse interval; each tick recommends a cadence
            # (fast while a show window is active). An explicit
            # --interval-seconds override pins the interval and disables the
            # show-day-aware cadence.
            sleep_for = settings.resolver_interval_seconds
            try:
                result = await run_tick(settings)
                logger.info(
                    "resolver tick",
                    extra={
                        "status": result.status,
                        "shows_scanned": result.shows_scanned,
                        "shows_resolved": result.shows_resolved,
                        "predictions_scored": result.predictions_scored,
                        "next_interval_seconds": result.next_interval_seconds,
                    },
                )
                if result.next_interval_seconds > 0:
                    sleep_for = result.next_interval_seconds
            except Exception:
                # Already logged by run_tick. Don't crash the loop on a
                # transient error — the next tick will retry.
                logger.exception("tick raised; continuing loop")
            if interval_override is not None:
                sleep_for = interval_override
            await asyncio.sleep(sleep_for)
    finally:
        await close_pool()


def main() -> None:
    """CLI: ``setlist-stash-resolve`` (one tick) or ``--loop`` (forever)."""
    parser = argparse.ArgumentParser(
        prog="setlist-stash-resolve",
        description="Resolve unresolved setlist-stash predictions against published setlists.",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run forever, sleeping RESOLVER_INTERVAL_SECONDS between ticks.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=None,
        help=(
            "Pin the loop interval (only used with --loop). When omitted, the "
            "resolver uses show-day-aware cadence: fast "
            "(RESOLVER_ACTIVE_INTERVAL_SECONDS) while a show window is active, "
            "coarse (RESOLVER_INTERVAL_SECONDS) otherwise."
        ),
    )
    args = parser.parse_args()

    rc = asyncio.run(_amain(loop=args.loop, interval_override=args.interval_seconds))
    sys.exit(rc)


if __name__ == "__main__":
    main()
