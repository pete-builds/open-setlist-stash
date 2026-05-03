"""Auto-resolver for predictions.

When phish.net publishes a setlist, this module fetches the setlist via
mcp-phish, scores every prediction for that show, writes the scores back,
and stamps `prediction_locks.resolved_at`.

PHASE-4-PLAN.md §5 is the design source. Pete approval (build session 2):
- Inside-container loop (asyncio.sleep) over external cron, for parity with
  the rest of the homelab and easier log capture.
- Conservative cancelled-show window: don't auto-cancel within 72h of
  lock_at — phish.net's setlist publish can lag.
- Watchdog stamps stale 'running' rows as 'error' on startup so a crashed
  process doesn't poison subsequent ticks.

Entrypoints:
- ``python -m phish_game.resolve``         - single tick, exit 0
- ``python -m phish_game.resolve --loop``  - run forever, sleep between ticks

Both honor ``RESOLVER_INTERVAL_SECONDS`` (default 1800) and
``RESOLVER_CANCEL_AFTER_HOURS`` (default 72).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from phish_game import __version__
from phish_game.config import Settings, get_settings
from phish_game.db import close_pool, get_pool, init_pool
from phish_game.leaderboard import rebuild_all, rebuild_leagues
from phish_game.logging_setup import configure_logging
from phish_game.mcp_client import (
    McpPhishClient,
    McpPhishError,
    McpPhishNotFound,
    McpPhishUnavailable,
)
from phish_game.migrate import run_migrations
from phish_game.scoring import score_prediction

logger = logging.getLogger("phish_game.resolve")


# ----- setlist parsing ------------------------------------------------------


@dataclass(frozen=True)
class ParsedSetlist:
    """Setlist parsed into the shape the scorer wants.

    Built from the ``setlist`` array returned by mcp-phish ``get_show``.
    Each element has ``{position, set_name, song_slug, song_title, ...}``.
    """

    opener_slug: str | None
    closer_slug: str | None
    encore_slugs: list[str]
    all_slugs: set[str]
    song_count: int


def _is_encore_set(set_name: str) -> bool:
    return set_name.strip().lower().startswith("encore")


def parse_setlist(setlist: list[dict[str, Any]]) -> ParsedSetlist:
    """Parse mcp-phish setlist rows into opener / closer / encore / all.

    Conventions:
    - Opener = first song (lowest ``position``) of the first non-encore set.
    - Closer = last song (highest ``position``) of the last non-encore set.
    - Encore songs = every slug whose ``set_name`` starts with ``Encore``.
    - all_slugs = every slug in the show, used to decide whether a bag pick
      was "played".

    A setlist with no non-encore songs (rare/impossible in real life) leaves
    opener and closer as ``None``. A setlist with no encore returns an empty
    encore_slugs list.
    """
    if not setlist:
        return ParsedSetlist(None, None, [], set(), 0)

    sorted_rows = sorted(setlist, key=lambda r: int(r.get("position", 0)))
    non_encore = [r for r in sorted_rows if not _is_encore_set(str(r.get("set_name", "")))]
    encore_rows = [r for r in sorted_rows if _is_encore_set(str(r.get("set_name", "")))]

    opener: str | None = None
    closer: str | None = None
    if non_encore:
        opener_raw = non_encore[0].get("song_slug")
        closer_raw = non_encore[-1].get("song_slug")
        opener = str(opener_raw) if opener_raw else None
        closer = str(closer_raw) if closer_raw else None

    encore_slugs = [
        str(r["song_slug"])
        for r in encore_rows
        if r.get("song_slug")
    ]
    all_slugs = {
        str(r["song_slug"])
        for r in sorted_rows
        if r.get("song_slug")
    }
    return ParsedSetlist(
        opener_slug=opener,
        closer_slug=closer,
        encore_slugs=encore_slugs,
        all_slugs=all_slugs,
        song_count=len(sorted_rows),
    )


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


# ----- show resolution ------------------------------------------------------


@dataclass
class ShowResolveOutcome:
    """Result of trying to resolve one show."""

    show_date: str
    status: str  # "resolved" | "skipped" | "cancelled" | "error"
    predictions_scored: int = 0
    setlist_song_count: int = 0
    error: str | None = None


async def _open_locks(pool: asyncpg.Pool[Any]) -> list[dict[str, Any]]:
    """Return rows for unresolved locks whose lock_at has passed."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT show_date, lock_at, lock_at_override
              FROM prediction_locks
             WHERE resolved_at IS NULL
               AND COALESCE(lock_at_override, lock_at) < now()
             ORDER BY show_date ASC
            """
        )
    return [dict(r) for r in rows]


async def _predictions_for_show(
    pool: asyncpg.Pool[Any], show_date: Any
) -> list[dict[str, Any]]:
    """Fetch every prediction for a show.

    ``show_date`` should be a ``datetime.date`` (asyncpg requires a date
    object to bind to a DATE column).
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, user_id, pick_song_slugs,
                   opener_slug, closer_slug, encore_slug
              FROM predictions
             WHERE show_date = $1
            """,
            show_date,
        )
    return [dict(r) for r in rows]


async def _fetch_song_meta(
    mcp: McpPhishClient, slugs: set[str]
) -> dict[str, dict[str, Any]]:
    """Fetch ``gap_current`` + ``times_played`` for each slug.

    Per-tick cache: each unique slug is fetched once. Slugs that 404 (unknown
    in the vault) get a zero-meta record so scoring still produces base=0
    cleanly without crashing the run.
    """
    out: dict[str, dict[str, Any]] = {}
    for slug in sorted(slugs):
        try:
            row = await mcp.get_song(slug)
        except McpPhishNotFound:
            logger.warning("get_song not found", extra={"slug": slug})
            out[slug] = {"gap_current": 0, "times_played": 0}
            continue
        out[slug] = {
            "gap_current": int(row.get("gap_current") or row.get("gap") or 0),
            "times_played": int(row.get("times_played") or 0),
        }
    return out


async def _resolve_show(
    pool: asyncpg.Pool[Any],
    mcp: McpPhishClient,
    lock_row: dict[str, Any],
    *,
    cancel_after: timedelta,
) -> ShowResolveOutcome:
    """Try to resolve a single open lock.

    - Fetch the show via mcp-phish.
    - If setlist is empty:
        - If lock_at is older than cancel_after, mark cancelled.
        - Otherwise leave for next tick.
    - If setlist is present, score every prediction and stamp resolved_at.
    """
    show_date_obj = lock_row["show_date"]
    show_date = (
        show_date_obj.isoformat() if hasattr(show_date_obj, "isoformat") else str(show_date_obj)
    )
    effective_lock = lock_row["lock_at_override"] or lock_row["lock_at"]
    if effective_lock.tzinfo is None:
        effective_lock = effective_lock.replace(tzinfo=UTC)
    now = datetime.now(UTC)

    try:
        show = await mcp.get_show(show_date)
        setlist = list(show.get("setlist") or [])
    except McpPhishNotFound:
        # Show not in the vault. If we're past the cancel window, treat as
        # cancelled. Otherwise leave it for next tick (vault may catch up).
        if now - effective_lock >= cancel_after:
            scored = await _mark_show_cancelled(pool, show_date_obj)
            return ShowResolveOutcome(
                show_date=show_date,
                status="cancelled",
                predictions_scored=scored,
            )
        return ShowResolveOutcome(show_date=show_date, status="skipped")
    except McpPhishUnavailable:
        # Network / 5xx: bubble up — caller marks the run partial/error.
        raise
    except McpPhishError as exc:
        return ShowResolveOutcome(
            show_date=show_date, status="error", error=str(exc)[:200]
        )

    if not setlist:
        # Vault knows the show but no setlist published yet.
        if now - effective_lock >= cancel_after:
            scored = await _mark_show_cancelled(pool, show_date_obj)
            return ShowResolveOutcome(
                show_date=show_date,
                status="cancelled",
                predictions_scored=scored,
            )
        return ShowResolveOutcome(show_date=show_date, status="skipped")

    parsed = parse_setlist(setlist)
    predictions = await _predictions_for_show(pool, show_date_obj)

    # Build the union of slugs we need song meta for: bag picks + slot picks
    # that ended up "played" (slot bonuses don't need song_meta, but if a
    # slot pick is also in the bag, we still need its meta).
    needed_slugs: set[str] = set()
    for pred in predictions:
        for slug in pred["pick_song_slugs"]:
            if slug in parsed.all_slugs:
                needed_slugs.add(str(slug))

    song_meta = await _fetch_song_meta(mcp, needed_slugs)

    scored_count = 0
    async with pool.acquire() as conn, conn.transaction():
        for pred in predictions:
            breakdown = score_prediction(
                pick_song_slugs=list(pred["pick_song_slugs"]),
                opener_slug=pred["opener_slug"],
                closer_slug=pred["closer_slug"],
                encore_slug=pred["encore_slug"],
                actual_opener=parsed.opener_slug,
                actual_closer=parsed.closer_slug,
                actual_encore_slugs=parsed.encore_slugs,
                setlist_slugs=parsed.all_slugs,
                song_meta=song_meta,
            )
            await conn.execute(
                """
                UPDATE predictions
                   SET score = $2,
                       score_breakdown = $3::jsonb
                 WHERE id = $1
                """,
                int(pred["id"]),
                int(breakdown["total"]),
                json.dumps(breakdown),
            )
            scored_count += 1

        await conn.execute(
            """
            UPDATE prediction_locks
               SET resolved_at = now(),
                   summary = $2::jsonb
             WHERE show_date = $1
            """,
            show_date_obj,
            json.dumps(
                {
                    "setlist_song_count": parsed.song_count,
                    "predictions_scored": scored_count,
                }
            ),
        )

    return ShowResolveOutcome(
        show_date=show_date,
        status="resolved",
        predictions_scored=scored_count,
        setlist_song_count=parsed.song_count,
    )


async def _mark_show_cancelled(pool: asyncpg.Pool[Any], show_date: Any) -> int:
    """Zero every prediction's score and stamp resolved_at with cancelled sentinel.

    ``show_date`` should be a ``datetime.date`` (asyncpg requires a date
    object to bind to a DATE column).
    """
    async with pool.acquire() as conn, conn.transaction():
        result = await conn.execute(
            """
            UPDATE predictions
               SET score = 0,
                   score_breakdown = $2::jsonb
             WHERE show_date = $1
               AND score IS NULL
            """,
            show_date,
            json.dumps({"cancelled": True, "total": 0}),
        )
        await conn.execute(
            """
            UPDATE prediction_locks
               SET resolved_at = now(),
                   summary = $2::jsonb
             WHERE show_date = $1
            """,
            show_date,
            json.dumps({"cancelled": True}),
        )
    # asyncpg execute returns the command tag like 'UPDATE 3'.
    try:
        return int(result.split()[-1])
    except (ValueError, AttributeError):
        return 0


# ----- single tick ----------------------------------------------------------


@dataclass
class TickResult:
    status: str  # 'noop' | 'success' | 'partial' | 'error'
    shows_scanned: int
    shows_resolved: int
    predictions_scored: int
    summary: dict[str, Any]


async def run_tick(settings: Settings) -> TickResult:
    """Execute one resolver tick. Idempotent + safe to invoke any time."""
    pool = get_pool()
    await watchdog_stale_running(pool)
    run_id = await _start_run(pool)

    cancel_after = timedelta(hours=settings.resolver_cancel_after_hours)
    summary: dict[str, Any] = {"shows": [], "errors": []}
    shows_resolved = 0
    predictions_scored = 0
    encountered_error = False

    try:
        open_locks = await _open_locks(pool)
        if not open_locks:
            await _finish_run(
                pool, run_id,
                status="noop",
                shows_scanned=0,
                shows_resolved=0,
                predictions_scored=0,
                summary={"shows": [], "note": "no open locks"},
            )
            return TickResult("noop", 0, 0, 0, {"shows": [], "note": "no open locks"})

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
                        pool, mcp, lock_row, cancel_after=cancel_after
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
                if outcome.status == "error":
                    encountered_error = True
                    if outcome.error:
                        summary["errors"].append(
                            {"show_date": outcome.show_date, "error": outcome.error}
                        )

        if encountered_error:
            status = "partial" if shows_resolved > 0 else "error"
        else:
            status = "success" if shows_resolved > 0 else "noop"

        # Refresh leaderboard snapshots when at least one show resolved this
        # tick. Errors here log + continue so the resolver tick stays green
        # even if a leaderboard rebuild has trouble — the next tick will
        # retry. We capture counts into the summary for observability.
        if shows_resolved > 0:
            try:
                rebuild_counts = await rebuild_all(pool)
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


async def latest_run_summary(pool: asyncpg.Pool[Any]) -> dict[str, Any] | None:
    """Return ``{started_at, finished_at, status}`` for the most recent run."""
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
        "started_at": row["started_at"].isoformat() if row["started_at"] else None,
        "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
        "status": row["status"],
    }


# ----- entrypoint -----------------------------------------------------------


async def _bootstrap(settings: Settings) -> None:
    """Initialize pool + apply migrations. Mirrors the FastAPI lifespan."""
    pool = await init_pool(settings)
    await run_migrations(pool)


async def _amain(loop: bool, interval_seconds: int) -> int:
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
            extra={"interval_seconds": interval_seconds, "version": __version__},
        )
        while True:
            try:
                result = await run_tick(settings)
                logger.info(
                    "resolver tick",
                    extra={
                        "status": result.status,
                        "shows_scanned": result.shows_scanned,
                        "shows_resolved": result.shows_resolved,
                        "predictions_scored": result.predictions_scored,
                    },
                )
            except Exception:
                # Already logged by run_tick. Don't crash the loop on a
                # transient error — the next tick will retry.
                logger.exception("tick raised; continuing loop")
            await asyncio.sleep(interval_seconds)
    finally:
        await close_pool()


def main() -> None:
    """CLI: ``phish-game-resolve`` (one tick) or ``--loop`` (forever)."""
    parser = argparse.ArgumentParser(
        prog="phish-game-resolve",
        description="Resolve unresolved phish-game predictions against published setlists.",
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
        help="Override RESOLVER_INTERVAL_SECONDS (only used with --loop).",
    )
    args = parser.parse_args()

    settings = get_settings()
    interval = args.interval_seconds or settings.resolver_interval_seconds
    rc = asyncio.run(_amain(loop=args.loop, interval_seconds=interval))
    sys.exit(rc)


if __name__ == "__main__":
    main()
