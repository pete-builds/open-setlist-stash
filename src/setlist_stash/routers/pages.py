"""Site-level pages: home, shows list, stats, public leaderboards,
about/connect, and the /healthz probe. Nothing here mutates state.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from setlist_stash import __version__
from setlist_stash.config import Settings
from setlist_stash.db import get_pool
from setlist_stash.deps import get_cfg, get_current_user, get_templates, render
from setlist_stash.leaderboard import (
    VALID_SCOPES,
    fetch_leaderboard,
    fetch_user_rank,
    latest_scope_key,
    list_scope_keys,
    list_show_entrants,
    normalize_scope,
)
from setlist_stash.locks import (
    get_or_create_lock,
    select_form_show,
    select_next_show,
)
from setlist_stash.mcp_client import McpPhishClient, McpPhishError
from setlist_stash.predictions import count_entrants
from setlist_stash.web_helpers import (
    _format_lock,
    home_card_state,
    home_show_pointers,
    upcoming_show_date,
)

router = APIRouter()
logger = logging.getLogger("setlist_stash.server")


SCOPE_LABELS = {
    "weekly": "Weekly",
    "tour": "Season",
    "all_time": "All-time",
}


@router.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    from setlist_stash.auth import HANDLE_HELP

    # Two DB-only show pointers (see home_show_pointers): the show playing
    # right now, and the most recently played one. Cheap enough to run on
    # every landing-page hit and independent of upstream availability.
    live_show: dict[str, Any] | None = None
    recent_show: dict[str, Any] | None = None
    live_date: date | None = None
    recent_date: date | None = None
    try:
        live_date, recent_date = await home_show_pointers(get_pool())
        if live_date is not None:
            live_show = {"show_date": live_date, "venue": None}
        if recent_date is not None:
            recent_show = {"show_date": recent_date, "venue": None}
    except RuntimeError:
        pass
    # Lock display state + entrant count for a show. Lazily creates the
    # prediction_locks row (that is how a show first joins the board).
    # ``(None, 0)`` when the pool isn't up, so the page still renders.
    async def _lock_context(show: Any) -> tuple[dict[str, Any] | None, int]:
        try:
            pool = get_pool()
        except RuntimeError:
            return None, 0
        lock = await get_or_create_lock(pool, show, cfg)
        # Entrant count is a count only (no picks revealed), so it is fair
        # to show pre-lock.
        return _format_lock(lock, cfg), await count_entrants(pool, show.show_date)

    # Resolve the upcoming show for everyone (not just signed-in users) so
    # the home-page countdown widget renders for anonymous visitors too.
    upcoming = None
    upcoming_lock: dict[str, Any] | None = None
    entrant_count = 0
    card_state = "no_show"
    try:
        async with McpPhishClient(
            cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
        ) as mcp:
            upcoming = await select_form_show(cfg, mcp)
            # Best-effort venue labels for the live / most-recent buttons,
            # on the client we already have open. A failure here leaves the
            # bare date, which still links correctly.
            for card in (live_show, recent_show):
                if card is None:
                    continue
                try:
                    meta = await mcp.get_show(card["show_date"].isoformat())
                except McpPhishError:
                    continue
                venue = meta.get("venue") or {}
                if isinstance(venue, dict):
                    card["venue"] = venue.get("name") or venue.get("location")
            if upcoming is not None:
                upcoming_lock, entrant_count = await _lock_context(upcoming)
                card_state = home_card_state(
                    upcoming_date=upcoming.show_date,
                    live_date=live_date,
                    is_locked=bool(upcoming_lock and upcoming_lock["is_locked"]),
                )
                # The target show is locked and not being played, so it is
                # over. select_form_show is date-only and would keep
                # returning it until midnight Eastern; advance the board to
                # the next announced show instead. If nothing follows (end
                # of tour) we keep what we have and the template suppresses
                # the picks CTA rather than linking a locked sheet.
                if card_state == "over":
                    nxt = await select_next_show(
                        cfg, mcp, after=upcoming.show_date
                    )
                    if nxt is not None:
                        upcoming = nxt
                        upcoming_lock, entrant_count = await _lock_context(nxt)
                        card_state = home_card_state(
                            upcoming_date=nxt.show_date,
                            live_date=live_date,
                            is_locked=bool(
                                upcoming_lock and upcoming_lock["is_locked"]
                            ),
                        )
    except McpPhishError:
        logger.warning("mcp-phish unreachable on /; rendering without show")
        upcoming = None
    return render(
        templates,
        request,
        "index.html",
        current_user=user,
        handle_help=HANDLE_HELP,
        upcoming_show=upcoming,
        upcoming_lock=upcoming_lock,
        entrant_count=entrant_count,
        live_show=live_show,
        recent_show=recent_show,
        card_state=card_state,
    )


async def _render_leaderboard(
    request: Request,
    templates: Jinja2Templates,
    cfg: Settings,
    user: Any,
    *,
    scope: str,
    scope_key: str | None,
    partial: bool,
) -> HTMLResponse:
    """Shared rendering for full-page and HTMX-fragment leaderboard views."""
    pool = get_pool()
    # Resolve effective scope_key: explicit > latest > None (empty state).
    effective_key = scope_key or await latest_scope_key(pool, scope)
    rows: list[Any] = []
    user_row = None
    if effective_key:
        rows = await fetch_leaderboard(pool, scope, effective_key, limit=50)
        if user is not None:
            user_row = await fetch_user_rank(
                pool, scope, effective_key, user.id
            )
    # Target show for the "Make your picks" CTA. Resolved once here and
    # reused for the pre-score fallback below, so we never double-call
    # mcp-phish.
    target_date = await upcoming_show_date(cfg)
    # Pre-score fallback: no scored rows for any bucket yet. Rather than an
    # empty "No scores yet" panel, list the players who've entered the
    # upcoming show at 0 (handles only, no picks — fair-play safe).
    pre_score = False
    if not rows and target_date is not None:
        rows = await list_show_entrants(pool, target_date, limit=50)
        pre_score = bool(rows)
    scope_keys = await list_scope_keys(pool, scope)
    ctx: dict[str, Any] = {
        "current_user": user,
        "scope": scope,
        "scope_label": SCOPE_LABELS.get(scope, scope),
        "scope_key": effective_key,
        "scope_keys": scope_keys,
        "rows": rows,
        "pre_score": pre_score,
        "upcoming_date": target_date,
        "user_row": user_row,
        "scope_options": [
            ("weekly", "Weekly"),
            ("tour", "Season"),
            ("all_time", "All-time"),
        ],
    }
    template = "_leaderboard_table.html" if partial else "leaderboard.html"
    return render(templates, request, template, **ctx)


@router.get("/leaderboard", response_class=HTMLResponse)
async def leaderboard_index(
    request: Request,
    scope: str = Query("weekly"),
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    normalized = normalize_scope(scope)
    if normalized not in VALID_SCOPES:
        normalized = "weekly"
    partial = request.headers.get("HX-Request", "").lower() == "true"
    return await _render_leaderboard(
        request, templates, cfg, user,
        scope=normalized, scope_key=None, partial=partial,
    )


@router.get("/leaderboard/{scope}/{scope_key}", response_class=HTMLResponse)
async def leaderboard_at(
    request: Request,
    scope: str,
    scope_key: str,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    normalized = normalize_scope(scope)
    if normalized not in VALID_SCOPES:
        normalized = "weekly"
    partial = request.headers.get("HX-Request", "").lower() == "true"
    # scope_key is user-provided; whitelist to alphanumeric + dash + underscore
    # to keep it impossible to inject something weird into the page.
    safe_key = "".join(c for c in scope_key if c.isalnum() or c in "-_")
    return await _render_leaderboard(
        request, templates, cfg, user,
        scope=normalized, scope_key=safe_key or None, partial=partial,
    )


@router.get("/shows", response_class=HTMLResponse)
async def shows_index(
    request: Request,
    viewer: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    """Archive index of every show that's had a prediction lock.

    Read-only, no schema change. One row per ``prediction_locks`` show,
    newest first, with an entrant count (LEFT JOIN predictions) and a
    finalized flag (``resolved_at IS NOT NULL``). Each row links to that
    show's per-show leaderboard. Venue names are best-effort via mcp-phish
    and degrade to the bare date when upstream is down.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT pl.show_date,
                   pl.resolved_at,
                   pl.lock_at,
                   COUNT(p.id) AS entrants
              FROM prediction_locks pl
              LEFT JOIN predictions p ON p.show_date = pl.show_date
             GROUP BY pl.show_date, pl.resolved_at, pl.lock_at
             ORDER BY pl.show_date DESC
            """
        )
    # Best-effort venue lookup, keyed by ISO date. Query each year present
    # in the archive via search_shows (covers played + announced-future
    # shows for the whole tour), instead of a date-DESC recent_shows window
    # that misses the earliest tour dates once far-future shows exist.
    # Degrade to bare dates if upstream is down so the archive always renders.
    venue_by_date: dict[str, str] = {}
    years = sorted({r["show_date"].year for r in rows})
    try:
        async with McpPhishClient(
            cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
        ) as mcp:
            for yr in years:
                for row in await mcp.search_shows(year=yr, limit=120):
                    d = str(row.get("date") or "")
                    name = row.get("venue_name") or row.get("location") or ""
                    if d and name:
                        venue_by_date[d] = str(name)
    except McpPhishError:
        logger.warning("mcp-phish unreachable on /shows; bare dates only")
    # Split into upcoming vs past against "today" in the display timezone.
    # Rows arrive newest-first (query ORDER BY show_date DESC). Past keeps
    # that order (most-recent past at the top). Upcoming gets reversed to
    # ascending so the SOONEST future show sits at the top and further-out
    # shows descend down the list.
    today = datetime.now(tz=ZoneInfo(cfg.display_tz)).date()
    upcoming: list[dict[str, Any]] = []
    past: list[dict[str, Any]] = []
    for r in rows:
        iso = r["show_date"].isoformat()
        entry = {
            "show_date": r["show_date"],
            "venue": venue_by_date.get(iso),
            "entrants": int(r["entrants"]),
            "resolved": r["resolved_at"] is not None,
        }
        (past if r["show_date"] < today else upcoming).append(entry)
    # DESC append order gives newest-first; reverse upcoming to soonest-first.
    upcoming.reverse()
    return render(
        templates,
        request,
        "shows.html",
        current_user=viewer,
        upcoming=upcoming,
        past=past,
    )


@router.get("/stats", response_class=HTMLResponse)
async def stats_page(
    request: Request,
    viewer: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    """Public catalog-wide statistics page.

    Reads a single ``stats_overview`` roll-up from the MCP server and
    renders it as a set of cards + tables (headline numbers, most-played,
    biggest bust-outs, rarest songs, recent debuts, longest shows). The
    upstream tool is band-specific; a deployment whose MCP omits it (the
    Phish demo, a third-party self-host pointed at a different MCP) gets a
    graceful "stats unavailable" panel instead of a crash — same degrade
    pattern the rest of the app uses for an unreachable MCP.
    """
    stats: dict[str, Any] | None = None
    try:
        async with McpPhishClient(
            cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
        ) as mcp:
            stats = await mcp.stats_overview(top_n=10)
    except McpPhishError:
        logger.warning("stats_overview unavailable on /stats")
        stats = None
    return render(
        templates,
        request,
        "stats.html",
        current_user=viewer,
        stats=stats,
    )


@router.get("/about", response_class=HTMLResponse)
async def about_page(
    request: Request,
    viewer: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    """Static About page: what the game is, how it works, who built it.

    Generic by default (uses ``site_name``); operator credit rides the
    same env-driven ``footer_credit`` the footer uses, so a third-party
    self-host shows no operator branding.
    """
    upcoming = None
    try:
        async with McpPhishClient(
            cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
        ) as mcp:
            upcoming = await select_form_show(cfg, mcp)
    except McpPhishError:
        upcoming = None
    return render(
        templates,
        request,
        "about.html",
        current_user=viewer,
        upcoming_show=upcoming,
    )


@router.get("/connect", response_class=HTMLResponse)
async def connect_page(
    request: Request,
    viewer: Any = Depends(get_current_user),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    """Developer docs page for the deployment's public read-only MCP.

    Gives copy-paste setup for Claude Code, Claude Desktop, and any MCP
    client, plus the tool list. When no public MCP is configured
    (``MCP_PUBLIC_URL`` empty — the OSS image, the Phish demo) it renders a
    "no public MCP on this deployment" panel; the nav link is hidden in
    that case (``has_mcp`` global).
    """
    return render(
        templates,
        request,
        "connect.html",
        current_user=viewer,
    )


@router.get("/healthz", include_in_schema=False)
async def healthz(cfg: Settings = Depends(get_cfg)) -> JSONResponse:
    """Liveness probe + dependency reachability."""
    body: dict[str, Any] = {"status": "ok", "version": __version__}
    # DB ping
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            ok = await conn.fetchval("SELECT 1")
        body["db"] = {"reachable": ok == 1}
    except Exception as exc:
        body["db"] = {"reachable": False, "error": str(exc)[:120]}
        body["status"] = "degraded"
    # mcp-phish ping
    try:
        async with McpPhishClient(
            cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
        ) as mcp:
            h = await mcp.health()
        body["mcp_phish"] = {
            "reachable": True,
            "vault_stale": h.get("vault", {}).get("stale"),
        }
    except Exception as exc:
        body["mcp_phish"] = {"reachable": False, "error": str(exc)[:120]}
        body["status"] = "degraded"
    # Resolver heartbeat (most-recent scoring_runs row).
    try:
        from setlist_stash.resolve import latest_run_summary
        pool = get_pool()
        latest = await latest_run_summary(pool, display_tz=cfg.display_tz)
        if latest is None:
            body["resolver_last_run"] = None
            body["resolver_last_status"] = None
        else:
            body["resolver_last_run"] = latest["finished_at"] or latest["started_at"]
            body["resolver_last_status"] = latest["status"]
    except Exception as exc:
        body["resolver_last_run"] = None
        body["resolver_last_status"] = f"error: {str(exc)[:80]}"
    return JSONResponse(body, status_code=200)
