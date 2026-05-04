"""Show selection + lock-time computation.

Phase 4 source-of-truth: ``PHASE-4-PLAN.md`` §4.

Default cutoff is 22:00 in ``DEFAULT_LOCK_TZ`` on the show date. Operators
override per-show via ``prediction_locks.lock_at_override`` (when present,
that value wins).

Show selection for the form (session 1):
- ``ADMIN_SHOW_DATE`` env var (YYYY-MM-DD) if set: that show is the target.
- Otherwise: pick the first show from ``mcp__phish__recent_shows`` whose
  date is in the future. If none upcoming, return None; the form will
  surface a "no upcoming show" message.

Returning a show without a lock row in the DB is fine. We create the lock
row lazily on the first prediction submit.

Smart-pick assist gating (PHASE-4-PLAN.md §7):
- ``assist_allowed(pool, show_date, settings)`` returns True iff
  ``now() >= effective lock_at`` OR ``settings.assist_pre_lock`` is True
  (dev override only). This is the single helper every assist surface
  consults; never re-implement the rule per-route.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

import asyncpg

from setlist_stash.config import Settings
from setlist_stash.mcp_client import McpPhishClient

logger = logging.getLogger("setlist_stash.locks")


@dataclass(frozen=True)
class ShowTarget:
    show_date: date
    show_id: str | None
    venue_name: str | None
    location: str | None
    tour_name: str | None


@dataclass(frozen=True)
class LockState:
    show_date: date
    lock_at: datetime          # effective cutoff (override if present)
    is_locked: bool
    seconds_until_lock: int    # negative when locked


def compute_default_lock_at(
    show_date: date, settings: Settings, *, now: datetime | None = None
) -> datetime:
    """Return the default cutoff for a show as a UTC TIMESTAMPTZ.

    Default: ``DEFAULT_LOCK_TIME_LOCAL`` in ``DEFAULT_LOCK_TZ`` on
    ``show_date``. Always returned in UTC.
    """
    _ = now  # not needed for default calculation; reserved for future logic
    tz = ZoneInfo(settings.default_lock_tz)
    hh, mm = settings.default_lock_time_local.split(":", 1)
    cutoff_local = datetime.combine(show_date, time(int(hh), int(mm)), tzinfo=tz)
    return cutoff_local.astimezone(ZoneInfo("UTC"))


async def get_or_create_lock(
    pool: asyncpg.Pool[Any],
    show: ShowTarget,
    settings: Settings,
) -> LockState:
    """Read or lazily create the prediction_locks row for a show."""
    default_lock = compute_default_lock_at(show.show_date, settings)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO prediction_locks (show_date, show_id, lock_at, venue_tz)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (show_date) DO UPDATE
                SET show_id = COALESCE(prediction_locks.show_id, EXCLUDED.show_id)
            RETURNING show_date, lock_at, lock_at_override
            """,
            show.show_date,
            show.show_id,
            default_lock,
            settings.default_lock_tz,
        )
        if row is None:
            raise RuntimeError("prediction_locks upsert returned no row")
        effective = row["lock_at_override"] or row["lock_at"]
        now = await conn.fetchval("SELECT now() AT TIME ZONE 'UTC'")
    if not isinstance(now, datetime):
        raise RuntimeError("could not read DB now()")
    if now.tzinfo is None:
        now = now.replace(tzinfo=ZoneInfo("UTC"))
    if effective.tzinfo is None:
        effective = effective.replace(tzinfo=ZoneInfo("UTC"))
    return LockState(
        show_date=row["show_date"],
        lock_at=effective,
        is_locked=now > effective,
        seconds_until_lock=int((effective - now).total_seconds()),
    )


async def read_lock(
    pool: asyncpg.Pool[Any], show_date: date
) -> LockState | None:
    """Read an existing prediction_locks row without creating one.

    Returns None if no row exists. Used by post-lock views that should NOT
    lazily create a lock for a date that's never been predicted-on.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT show_date, lock_at, lock_at_override
              FROM prediction_locks
             WHERE show_date = $1
            """,
            show_date,
        )
        if row is None:
            return None
        now = await conn.fetchval("SELECT now() AT TIME ZONE 'UTC'")
    effective = row["lock_at_override"] or row["lock_at"]
    if not isinstance(now, datetime):
        raise RuntimeError("could not read DB now()")
    if now.tzinfo is None:
        now = now.replace(tzinfo=ZoneInfo("UTC"))
    if effective.tzinfo is None:
        effective = effective.replace(tzinfo=ZoneInfo("UTC"))
    return LockState(
        show_date=row["show_date"],
        lock_at=effective,
        is_locked=now > effective,
        seconds_until_lock=int((effective - now).total_seconds()),
    )


async def assist_allowed(
    pool: asyncpg.Pool[Any], show_date: date, settings: Settings
) -> bool:
    """Single source of truth for the assist gate (PHASE-4-PLAN.md §7).

    Returns True iff:
      - the show's lock_at is in the past (post-lock retro is fair game), OR
      - ``settings.assist_pre_lock`` is True (dev/operator override; MUST
        stay False in production).

    A missing prediction_locks row is treated as "not yet locked" (the show
    has never been predicted-on, so there's no public assist to surface).
    The dev override still applies in that case.
    """
    if settings.assist_pre_lock:
        return True
    lock = await read_lock(pool, show_date)
    if lock is None:
        return False
    return lock.is_locked


async def select_form_show(
    settings: Settings, mcp: McpPhishClient
) -> ShowTarget | None:
    """Pick the show that the predict form should target.

    See module docstring for the precedence rules.
    """
    if settings.admin_show_date:
        # Admin override: trust the operator. We don't validate against
        # mcp-phish here because future shows aren't in the corpus until
        # they're played. The form will still work — just no venue label
        # until phish.net publishes the show row.
        return ShowTarget(
            show_date=settings.admin_show_date,
            show_id=None,
            venue_name=settings.admin_show_venue,
            location=settings.admin_show_location,
            tour_name=None,
        )

    today = date.today()
    try:
        rows = await mcp.recent_shows(limit=20)
    except Exception:
        logger.exception("recent_shows lookup failed")
        return None
    for row in rows:
        try:
            d = date.fromisoformat(str(row["date"]))
        except (KeyError, ValueError):
            continue
        if d > today:
            return ShowTarget(
                show_date=d,
                show_id=str(row.get("show_id")) if row.get("show_id") else None,
                venue_name=row.get("venue_name"),
                location=row.get("location"),
                tour_name=row.get("tour_name"),
            )
    return None
