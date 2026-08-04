"""Pure/near-pure helpers shared across routers.

These lived in ``server.py`` until the routes-only split (2026-08-04). Behavior
is unchanged; they are only moved so the routers can import them without
pulling the entry-point module. The public names below are still re-exported
from ``setlist_stash.server`` for backwards compatibility with existing tests.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import asyncpg
from fastapi import Request

from setlist_stash.config import Settings
from setlist_stash.locks import LockState, select_form_show
from setlist_stash.mcp_client import McpPhishClient, McpPhishError

logger = logging.getLogger("setlist_stash.server")

_PACKAGE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = _PACKAGE_DIR / "templates"
STATIC_DIR = _PACKAGE_DIR / "static"


# How long past its lock instant a show still counts as "playing right now" on
# the home page. The resolver only stamps ``resolved_at`` once the setlist is
# published upstream, which routinely lags the encore by hours, so the live
# entry point needs its own time box or it would linger until the next morning.
# Lock lands at showtime, so this window covers doors-to-encore plus slop.
LIVE_SHOW_WINDOW_HOURS = 6


def _group_setlist(setlist: list[Any]) -> list[dict[str, Any]]:
    """Group a raw ``get_show`` setlist into ordered per-set blocks.

    Input items look like
    ``{position, set_name, song_title, song_slug, transition, footnote,
    provenance, advisory}``. ``provenance`` and ``advisory`` are NEW optional
    fields (the Phish MCP may not send them yet), so they default to
    ``"atu"`` / ``False`` — a song is only flagged unconfirmed when the MCP
    explicitly says ``advisory=true`` or ``provenance=="x"``.

    Returns ``[{set_name, songs: [...]}, ...]`` with sets in first-seen order
    and songs in the order they arrived (the MCP returns them in play order).
    Generic: no band-specific set labels are assumed.
    """
    groups: list[dict[str, Any]] = []
    index: dict[str, dict[str, Any]] = {}
    for item in setlist:
        if not isinstance(item, dict):
            continue
        set_name = str(item.get("set_name") or "Set")
        provenance = str(item.get("provenance") or "atu").lower()
        advisory = bool(item.get("advisory")) or provenance == "x"
        song = {
            "song_title": item.get("song_title") or item.get("song_slug") or "",
            "song_slug": item.get("song_slug") or "",
            "transition": item.get("transition") or "",
            "footnote": item.get("footnote") or "",
            "advisory": advisory,
        }
        bucket = index.get(set_name)
        if bucket is None:
            bucket = {"set_name": set_name, "songs": []}
            index[set_name] = bucket
            groups.append(bucket)
        bucket["songs"].append(song)
    return groups


def _compute_asset_version(theme_file: str = "") -> str:
    """Short content hash of the CSS, for cache-busting ``?v=`` query stamps.

    Hashes the bytes of ``style.css`` plus the active theme file (named by
    ``THEME_FILE``, under the static dir) when present. Missing files are
    skipped gracefully so this never crashes the app. The value only changes
    when a CSS file's content changes, so Cloudflare/browsers fetch a fresh
    object after a styling deploy while older URLs stay cached but unreferenced.
    """
    h = hashlib.sha256()
    paths = [STATIC_DIR / "style.css"]
    if theme_file:
        paths.append(STATIC_DIR / theme_file)
    for path in paths:
        try:
            h.update(path.read_bytes())
        except OSError:
            continue
    return h.hexdigest()[:8]


def _gap_label(gap: Any) -> str:
    """Human-readable "shows since last play" label for the picker.

    ``gap`` is the number of completed shows since the song last appeared:
    0 means it was played at the most recent completed show (last night),
    higher means longer since. Returns ``""`` when gap is unknown (None) so
    the caller can degrade to a plain song title — keeps the shared repo's
    Phish deployment working even if its upstream omits gap.
    """
    if gap is None:
        return ""
    try:
        n = int(gap)
    except (TypeError, ValueError):
        return ""
    if n < 0:
        return ""
    if n == 0:
        return "last show"
    if n == 1:
        return "1 show gap"
    return f"{n} show gap"


def _humanize_countdown(seconds: int) -> str:
    """Human-readable "time until lock" as hours and minutes.

    - under 1 hour  -> ``43m``
    - 1h to <24h    -> ``5h 12m`` (99595s -> ``27h 40m`` becomes ``1d 3h 40m``)
    - 24h or more   -> ``1d 3h 40m`` (days prepended, then hours + minutes)
    - zero/negative -> ``0m`` (callers should guard the locked state upstream)

    Seconds are intentionally dropped: this is a static, server-rendered label
    (the live ticking countdown on the predict page is a separate JS widget).
    """
    secs = max(int(seconds), 0)
    days, rem = divmod(secs, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    if days > 0:
        return f"{days}d {hours}h {minutes}m"
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def home_card_state(
    *,
    upcoming_date: date | None,
    live_date: date | None,
    is_locked: bool,
) -> str:
    """Which of four states the home page's show card is in.

    - ``"no_show"``  nothing announced on the board.
    - ``"live"``     the target show is the one being played right now.
    - ``"over"``     the target show is locked and NOT being played, i.e. it
      has finished. ``select_form_show`` is date-only, so it keeps returning
      tonight's show until midnight Eastern; without this state the card would
      spend the rest of the night calling a finished show "Next show" and
      offering a pick sheet that can no longer be submitted to.
    - ``"pre_lock"`` picks are open.

    Order matters: ``live`` is checked before ``over`` because a show in
    progress is also locked.
    """
    if upcoming_date is None:
        return "no_show"
    if live_date is not None and live_date == upcoming_date:
        return "live"
    if is_locked:
        return "over"
    return "pre_lock"


def display_now(settings: Settings) -> datetime:
    """Wallclock "now" in the viewer-facing display zone.

    Every user-facing date boundary ("is this show today?", "which show played
    last night?") must be evaluated here, not in UTC. Containers run with
    ``TZ=UTC``, so a bare ``date.today()`` rolls over to tomorrow at 8pm
    Eastern — mid-show, which is exactly when it matters most.
    """
    return datetime.now(tz=ZoneInfo(settings.display_tz))


def display_dt(
    value: Any,
    fmt: str = "%b %-d, %-I:%M %p %Z",
    *,
    tz: str = "America/New_York",
) -> str:
    """Jinja filter: render a timestamp in the display zone (Eastern).

    Registered as ``|display_dt`` (bound to ``DISPLAY_TZ``) so no template ever
    formats a raw stored instant. Timestamps are stored and compared in UTC;
    this is the single conversion point on the way out. A naive datetime is
    assumed to be UTC, which is what every naive value in this codebase means.
    ``%Z`` renders a DST-correct label — EDT in summer, EST in winter — so a
    viewer never sees a UTC clock time.
    """
    if not isinstance(value, datetime):
        return "" if value is None else str(value)
    aware = (
        value if value.tzinfo is not None else value.replace(tzinfo=ZoneInfo("UTC"))
    )
    return aware.astimezone(ZoneInfo(tz)).strftime(fmt)


async def home_show_pointers(
    pool: asyncpg.Pool[Any],
) -> tuple[date | None, date | None]:
    """Resolve the home page's two show pointers from ``prediction_locks``.

    Returns ``(live_show_date, recent_show_date)``:

    - **live**: the show playing right now — locked, not yet finalized, and
      still inside ``LIVE_SHOW_WINDOW_HOURS`` of its lock instant. Drives the
      showtime-only "watch it live" button.
    - **recent**: the most recently played show, excluding whichever one is
      live. Drives the "last show's setlist" entry point, which is what
      players reach for the morning after.

    DB-only (no upstream call) and safe on an empty table: every show the game
    targets gets a ``prediction_locks`` row the first time the home page or the
    predict form resolves it, so this table is the reliable show spine.

    Timezone: the window is pure instant arithmetic on two ``TIMESTAMPTZ``
    values, so it is correct regardless of the container's ``TZ``. It never
    computes a calendar "today", which is the thing that breaks under UTC.
    ``lock_at`` itself is anchored to the venue's local showtime (see
    ``locks.compute_default_lock_at``) and is rendered to viewers in
    ``DISPLAY_TZ`` (Eastern), so nothing here surfaces a UTC clock.
    """
    async with pool.acquire() as conn:
        live: date | None = await conn.fetchval(
            """
            SELECT show_date
              FROM prediction_locks
             WHERE resolved_at IS NULL
               AND COALESCE(lock_at_override, lock_at) <= now()
               AND COALESCE(lock_at_override, lock_at)
                     > now() - ($1 || ' hours')::interval
             ORDER BY show_date DESC
             LIMIT 1
            """,
            str(LIVE_SHOW_WINDOW_HOURS),
        )
        recent: date | None = await conn.fetchval(
            """
            SELECT show_date
              FROM prediction_locks
             WHERE COALESCE(lock_at_override, lock_at) <= now()
               AND ($1::date IS NULL OR show_date <> $1::date)
             ORDER BY show_date DESC
             LIMIT 1
            """,
            live,
        )
    return live, recent


def _format_lock(lock: LockState, settings: Settings) -> dict[str, Any]:
    # Render in the viewer-facing display tz (Eastern by default), not the
    # anchor tz the lock was computed in. strftime("%Z") on a ZoneInfo zone is
    # DST-aware (EDT in summer, EST in winter). lock_at_iso stays a UTC-anchored
    # ISO instant so the JS countdown is correct regardless of the display label.
    tz = ZoneInfo(settings.display_tz)
    local = lock.lock_at.astimezone(tz)
    return {
        "is_locked": lock.is_locked,
        "lock_at_display": local.strftime("%a %b %-d, %-I:%M %p %Z"),
        # ISO-8601 with timezone, parseable by JS ``new Date()``. Used by
        # the predict-page countdown and post-lock panels.
        "lock_at_iso": lock.lock_at.isoformat(),
        "seconds_until_lock": max(lock.seconds_until_lock, 0),
        # Human-readable "Xh Ym from now" for static server-rendered labels.
        "countdown_human": _humanize_countdown(lock.seconds_until_lock),
    }


async def _resolve_song_titles(
    slugs: list[str], settings: Settings
) -> dict[str, str]:
    """Best-effort slug -> display title via mcp-phish; falls back to the slug.

    Labels a returning user's pre-filled picks in the edit form. Never raises:
    if upstream is unavailable each slug maps to itself, so the form still
    works (the hidden slug, not the visible title, is what the submit ships).
    """
    result: dict[str, str] = {s: s for s in slugs}
    if not slugs:
        return result

    async def _one(mcp: McpPhishClient, slug: str) -> tuple[str, str]:
        try:
            song = await mcp.get_song(slug)
            return slug, str(song.get("title") or slug)
        except Exception:  # best-effort labeling only
            return slug, slug

    try:
        async with McpPhishClient(
            settings.mcp_phish_url,
            timeout_seconds=settings.mcp_phish_timeout_seconds,
        ) as mcp:
            pairs = await asyncio.gather(*(_one(mcp, s) for s in slugs))
        for slug, title in pairs:
            result[slug] = title
    except McpPhishError:
        pass
    return result


def invite_url(request: Request, slug: str) -> str:
    """Absolute URL for a game invite. Public because both leagues and
    predictions routers build them (leagues to render dashboards, predictions
    to show the post-submit game share prompt)."""
    base = str(request.base_url).rstrip("/")
    return f"{base}/game/{slug}"


async def upcoming_show_date(settings: Settings) -> date | None:
    """Best-effort target show date for scoreboard 0-pre-scoring.

    Returns the upcoming show's date so the scoreboard can show each
    member's score for that show (0 before it's scored). Degrades to None
    when mcp-phish is unreachable; callers treat None as "everyone at 0".
    """
    try:
        async with McpPhishClient(
            settings.mcp_phish_url,
            timeout_seconds=settings.mcp_phish_timeout_seconds,
        ) as mcp:
            upcoming = await select_form_show(settings, mcp)
    except McpPhishError:
        return None
    return upcoming.show_date if upcoming is not None else None


def safe_next(raw: str) -> str:
    """Whitelist a post-handle redirect target.

    Only same-origin absolute paths (``/league/...``, ``/game/...``,
    ``/predict/...``) are honored, so a crafted ``next`` can never bounce a
    new player to an external site. Anything else falls back to ``/``.
    """
    s = (raw or "").strip()
    if s.startswith("/") and not s.startswith("//"):
        return s
    return "/"


