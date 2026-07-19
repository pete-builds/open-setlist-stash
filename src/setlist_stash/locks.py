"""Show selection + lock-time computation.

Phase 4 source-of-truth: ``PHASE-4-PLAN.md`` §4.

Default cutoff is ``DEFAULT_LOCK_TIME_LOCAL`` interpreted in the show's
**venue-local** timezone (resolved from the show location; falls back to
``DEFAULT_LOCK_TZ`` when the location can't be mapped), so ``19:25`` means
7:25 PM local to the venue regardless of where that is. Operators override
per-show via ``prediction_locks.lock_at_override`` (when present, that value
wins). The instant is stored as a UTC ``TIMESTAMPTZ``; it is rendered to
viewers in ``DISPLAY_TZ`` (Eastern) by the server, so a Central-time show
locking at 7:25 PM local displays as 8:25 PM EDT.

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


# US state (2-letter code or full name) -> predominant IANA timezone. mcp-phish
# exposes no venue timezone or showtime, so we resolve the anchor zone from the
# show location string. Covers all 50 states + DC; states that span zones use
# their predominant zone (good enough for a lock cutoff). Arizona uses
# America/Phoenix (no DST). Anything unmapped falls back to DEFAULT_LOCK_TZ.
_STATE_TZ: dict[str, str] = {
    # Eastern
    "CT": "America/New_York", "DE": "America/New_York", "DC": "America/New_York",
    "FL": "America/New_York", "GA": "America/New_York", "IN": "America/New_York",
    "KY": "America/New_York", "ME": "America/New_York", "MD": "America/New_York",
    "MA": "America/New_York", "MI": "America/New_York", "NH": "America/New_York",
    "NJ": "America/New_York", "NY": "America/New_York", "NC": "America/New_York",
    "OH": "America/New_York", "PA": "America/New_York", "RI": "America/New_York",
    "SC": "America/New_York", "VT": "America/New_York", "VA": "America/New_York",
    "WV": "America/New_York",
    # Central
    "AL": "America/Chicago", "AR": "America/Chicago", "IA": "America/Chicago",
    "IL": "America/Chicago", "KS": "America/Chicago", "LA": "America/Chicago",
    "MN": "America/Chicago", "MO": "America/Chicago", "MS": "America/Chicago",
    "ND": "America/Chicago", "NE": "America/Chicago", "OK": "America/Chicago",
    "SD": "America/Chicago", "TN": "America/Chicago", "TX": "America/Chicago",
    "WI": "America/Chicago",
    # Mountain (AZ = Phoenix, no DST)
    "AZ": "America/Phoenix", "CO": "America/Denver", "ID": "America/Denver",
    "MT": "America/Denver", "NM": "America/Denver", "UT": "America/Denver",
    "WY": "America/Denver",
    # Pacific
    "CA": "America/Los_Angeles", "NV": "America/Los_Angeles",
    "OR": "America/Los_Angeles", "WA": "America/Los_Angeles",
    # Non-contiguous
    "AK": "America/Anchorage", "HI": "Pacific/Honolulu",
}
_STATE_NAME_TO_CODE: dict[str, str] = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "district of columbia": "DC", "washington dc": "DC", "florida": "FL",
    "georgia": "GA", "hawaii": "HI", "idaho": "ID", "illinois": "IL",
    "indiana": "IN", "iowa": "IA", "kansas": "KS", "kentucky": "KY",
    "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
    "montana": "MT", "nebraska": "NE", "nevada": "NV", "new hampshire": "NH",
    "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD", "tennessee": "TN",
    "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA",
    "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}


def resolve_venue_tz(location: str | None, default_tz: str) -> str:
    """Resolve an IANA timezone for a show from its location string.

    Location looks like ``"Madison, WI"`` (2-letter code) or
    ``"Madison, Wisconsin"`` (full name), optionally with a trailing country.
    Returns ``default_tz`` for empty/unmappable locations (e.g. international
    shows or an empty string), which keeps behavior safe and Eastern-leaning.
    """
    if not location or not location.strip():
        return default_tz
    parts = [p.strip() for p in location.split(",") if p.strip()]
    if not parts:
        return default_tz
    tail = parts[-1]
    # Drop a trailing country token so "City, ST, USA" still finds the state.
    if tail.upper() in {"USA", "US", "U.S.A.", "UNITED STATES"} and len(parts) >= 2:
        tail = parts[-2]
    code: str | None = None
    if len(tail) == 2 and tail.isalpha():
        code = tail.upper()
    else:
        code = _STATE_NAME_TO_CODE.get(tail.lower())
    if code and code in _STATE_TZ:
        return _STATE_TZ[code]
    return default_tz


def compute_default_lock_at(
    show_date: date,
    settings: Settings,
    *,
    venue_tz: str | None = None,
    now: datetime | None = None,
) -> datetime:
    """Return the default cutoff for a show as a UTC TIMESTAMPTZ.

    ``DEFAULT_LOCK_TIME_LOCAL`` interpreted in ``venue_tz`` (the show's
    venue-local zone) when provided, else ``DEFAULT_LOCK_TZ``. Always returned
    in UTC.
    """
    _ = now  # not needed for default calculation; reserved for future logic
    tz = ZoneInfo(venue_tz or settings.default_lock_tz)
    hh, mm = settings.default_lock_time_local.split(":", 1)
    cutoff_local = datetime.combine(show_date, time(int(hh), int(mm)), tzinfo=tz)
    return cutoff_local.astimezone(ZoneInfo("UTC"))


async def get_or_create_lock(
    pool: asyncpg.Pool[Any],
    show: ShowTarget,
    settings: Settings,
) -> LockState:
    """Read or lazily create the prediction_locks row for a show.

    The lock instant is anchored to the show's venue-local timezone (resolved
    from ``show.location``) and persisted alongside the resolved zone in
    ``venue_tz``. On conflict we refresh ``lock_at`` from the freshly resolved
    zone *unless* an operator has set ``lock_at_override`` (their value always
    wins), so a location/zone correction propagates to an existing row.
    """
    venue_tz = resolve_venue_tz(show.location, settings.default_lock_tz)
    default_lock = compute_default_lock_at(
        show.show_date, settings, venue_tz=venue_tz
    )
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO prediction_locks (show_date, show_id, lock_at, venue_tz)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (show_date) DO UPDATE
                SET show_id = COALESCE(prediction_locks.show_id, EXCLUDED.show_id),
                    venue_tz = EXCLUDED.venue_tz,
                    lock_at = CASE
                        WHEN prediction_locks.lock_at_override IS NULL
                        THEN EXCLUDED.lock_at
                        ELSE prediction_locks.lock_at
                    END
            RETURNING show_date, lock_at, lock_at_override
            """,
            show.show_date,
            show.show_id,
            default_lock,
            venue_tz,
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

    Auto-advances through the tour: picks the NEAREST upcoming show (minimum
    date on/after today), so the "Make Your Picks" call-to-action moves to the
    next show on its own after each one plays. ``ADMIN_SHOW_DATE`` is honored
    only as an override while it is still in the future; once it passes we stop
    relying on it and resume auto-selection.

    ``recent_shows`` is date-DESC and windowed, so it can't be trusted to
    surface the *nearest* future date once far-future shows (e.g. next-year
    runs) exist upstream. We instead pull each candidate year via
    ``search_shows`` and take the minimum future date.
    """
    today = date.today()

    # Operator override: only while the pinned date is still upcoming.
    if settings.admin_show_date and settings.admin_show_date >= today:
        return ShowTarget(
            show_date=settings.admin_show_date,
            show_id=None,
            venue_name=settings.admin_show_venue,
            location=settings.admin_show_location,
            tour_name=None,
        )

    # Cover a tour that straddles a year boundary (summer run into next-year
    # Mexico, etc.) so we never miss the true nearest date.
    candidates: list[tuple[date, dict[str, Any]]] = []
    try:
        for yr in (today.year, today.year + 1):
            for row in await mcp.search_shows(year=yr, limit=200):
                try:
                    d = date.fromisoformat(str(row["date"]))
                except (KeyError, ValueError):
                    continue
                if d >= today:
                    candidates.append((d, row))
    except Exception:
        logger.exception("search_shows lookup failed")
        return None

    if not candidates:
        return None
    d, row = min(candidates, key=lambda pair: pair[0])
    return ShowTarget(
        show_date=d,
        show_id=str(row.get("show_id")) if row.get("show_id") else None,
        venue_name=row.get("venue_name"),
        location=row.get("location"),
        tour_name=row.get("tour_name"),
    )
