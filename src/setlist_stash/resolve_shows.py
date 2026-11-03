"""Per-show resolution: fetch the setlist, gate on completeness, score, finalize.

Split out of ``resolve.py``. ``run_tick`` in ``setlist_stash.resolve`` drives
these once per open lock.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from setlist_stash.completeness import (
    evaluate_completeness,
    read_poll_state,
    save_setlist_snapshot,
    upsert_poll_state,
)
from setlist_stash.config import Settings
from setlist_stash.mcp_client import (
    McpPhishClient,
    McpPhishError,
    McpPhishNotFound,
    McpPhishUnavailable,
)
from setlist_stash.resolve_setlist import parse_setlist
from setlist_stash.resolve_types import ParsedSetlist
from setlist_stash.scoring import score_prediction

# Same logger name as before the split, so log lines are unchanged.
logger = logging.getLogger("setlist_stash.resolve")


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


async def _resolve_show(
    pool: asyncpg.Pool[Any],
    mcp: McpPhishClient,
    lock_row: dict[str, Any],
    *,
    cancel_after: timedelta,
    settings: Settings,
) -> ShowResolveOutcome:
    """Try to resolve a single open lock.

    - Fetch the show via mcp-phish.
    - If setlist is empty:
        - If lock_at is older than cancel_after, mark cancelled.
        - Otherwise leave for next tick.
    - If setlist is present but NOT yet complete (no encore / track count still
      growing, and the time backstop hasn't fired), update poll_state and skip
      — leave the lock open for the next tick. This is the game-night gate:
      scoring a partial setlist would mark every encore pick a miss and lock
      that in forever.
    - If setlist is present AND complete, score every prediction and stamp
      resolved_at.
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

    # --- completeness gate -------------------------------------------------
    # Don't score until the setlist looks final. Fold this poll into the
    # durable poll_state and decide. If not complete, persist state and skip;
    # the lock stays open for the next (fast-cadence) tick.
    prior = await read_poll_state(pool, show_date_obj)
    decision = evaluate_completeness(
        parsed=parsed,
        prior=prior,
        now=now,
        effective_lock=effective_lock,
        stable_polls_required=settings.resolver_stable_polls_required,
        backstop=timedelta(hours=settings.resolver_backstop_hours),
    )
    await upsert_poll_state(pool, decision.next_state)

    # Live scoring: score every prediction against the CURRENT setlist on every
    # tick. The model is additive (a pick is +2 once it's played; the encore
    # call is +3 once the encore lands), so re-scoring a partial setlist shows
    # running totals and self-corrects if the setlist is edited — it never
    # freezes a wrong score. The completeness gate now only decides when to
    # FINALIZE (stamp resolved_at) so we stop re-scoring on later ticks.
    scored_count = await _score_predictions(pool, show_date_obj, parsed)

    # Publish the exact setlist those scores were computed from, so the live
    # show page renders setlist + standings from ONE tick and can never show a
    # song the scores don't yet count. Deliberately fail-soft: a snapshot is a
    # display convenience and must never be able to cost a real show its
    # scoring. If this write fails the page just falls back to a live read.
    try:
        await save_setlist_snapshot(pool, show_date_obj, setlist)
    except Exception:  # pragma: no cover - defensive, never fatal
        logger.exception(
            "setlist snapshot write failed; page will fall back to a live read",
            extra={"show_date": show_date},
        )

    if not decision.complete:
        logger.info(
            "live partial scoring",
            extra={
                "show_date": show_date,
                "track_count": parsed.song_count,
                "encore_seen": decision.next_state.encore_seen,
                "stable_polls": decision.next_state.stable_polls,
                "scored": scored_count,
            },
        )
        return ShowResolveOutcome(
            show_date=show_date,
            status="scored_live",
            predictions_scored=scored_count,
            setlist_song_count=parsed.song_count,
        )

    logger.info(
        "setlist complete; finalizing",
        extra={
            "show_date": show_date,
            "reason": decision.reason,
            "track_count": parsed.song_count,
            "stable_polls": decision.next_state.stable_polls,
            "scored": scored_count,
        },
    )
    async with pool.acquire() as conn:
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


async def _score_predictions(
    pool: asyncpg.Pool[Any], show_date_obj: Any, parsed: ParsedSetlist
) -> int:
    """Score every prediction for a show against ``parsed`` (possibly partial).

    Idempotent and safe to re-run each tick — the additive model recomputes
    each prediction's total from the current setlist every time.
    """
    predictions = await _predictions_for_show(pool, show_date_obj)
    scored_count = 0
    async with pool.acquire() as conn, conn.transaction():
        for pred in predictions:
            breakdown = score_prediction(
                pick_song_slugs=list(pred["pick_song_slugs"]),
                encore_slug=pred["encore_slug"],
                actual_encore_slugs=parsed.encore_slugs,
                setlist_slugs=parsed.all_slugs,
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
    return scored_count


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
