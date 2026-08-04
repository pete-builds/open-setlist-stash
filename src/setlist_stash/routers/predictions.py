"""Predict / post-lock / assist / autocomplete routes.

The show-picker form, the per-show predictions page, the post-lock assist
page, and the pre-lock song autocomplete all live here. Business logic (slug
normalization, scoring, lock enforcement) stays in the domain modules.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from html import escape
from typing import Any

from fastapi import APIRouter, Depends, Form, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from setlist_stash.completeness import read_setlist_snapshot
from setlist_stash.config import Settings
from setlist_stash.db import get_pool
from setlist_stash.deps import get_cfg, get_current_user, get_templates, render
from setlist_stash.leagues import (
    get_or_create_user_game,
    list_members_with_scores,
    list_user_leagues,
)
from setlist_stash.locks import (
    ShowTarget,
    assist_allowed,
    get_or_create_lock,
    live_board_active,
    read_lock,
)
from setlist_stash.mcp_client import McpPhishClient, McpPhishError
from setlist_stash.predictions import (
    PredictionDuplicate,
    PredictionError,
    PredictionLocked,
    count_entrants,
    get_user_prediction,
    insert_prediction,
    normalize_picks,
)
from setlist_stash.web_helpers import (
    _format_lock,
    _gap_label,
    _group_setlist,
    _resolve_song_titles,
    invite_url,
)

router = APIRouter()
logger = logging.getLogger("setlist_stash.server")


@router.get("/predict/{show_date}", response_class=HTMLResponse)
async def predict_form(
    request: Request,
    show_date: date,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    if user is None:
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    pool = get_pool()
    # Resolve the show metadata via mcp-phish; fall back to plain values
    # if upstream is down.
    show_id: str | None = None
    venue_name: str | None = None
    location: str | None = None
    tour_name: str | None = None
    try:
        async with McpPhishClient(
            cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
        ) as mcp:
            # Targeted per-date lookup so ANY tour date resolves its venue,
            # regardless of how far the vault reaches into the future. The
            # old recent_shows(limit=N) scan is date-DESC windowed and misses
            # the earliest tour dates once far-future shows exist upstream.
            row = await mcp.get_show(show_date.isoformat())
        raw_id = row.get("show_id")
        show_id = str(raw_id) if raw_id else None
        venue = row.get("venue") or {}
        venue_name = (venue.get("name") if isinstance(venue, dict) else None) or None
        location = (
            venue.get("location") if isinstance(venue, dict) else None
        ) or None
        tour_name = row.get("tour_name") or None
    except McpPhishError:
        # McpPhishNotFound (no show that date) and unavailability both land
        # here; venue/location stay None and the page degrades gracefully.
        logger.warning(
            "mcp-phish show lookup missed",
            extra={"show_date": str(show_date)},
        )

    # Operator-set target show: prefer its venue/location. Kept as a
    # belt-and-suspenders override for a manually pinned show.
    if cfg.admin_show_date and show_date == cfg.admin_show_date:
        venue_name = cfg.admin_show_venue or venue_name
        location = cfg.admin_show_location or location

    show: dict[str, Any] = {
        "show_date": show_date,
        "show_id": show_id,
        "venue_name": venue_name,
        "location": location,
        "tour_name": tour_name,
    }

    target = ShowTarget(
        show_date=show_date,
        show_id=show_id,
        venue_name=venue_name,
        location=location,
        tour_name=tour_name,
    )
    lock = await get_or_create_lock(pool, target, cfg)
    existing = await get_user_prediction(pool, user.id, show_date)
    # Pre-load a returning user's picks into the editable form (pre-lock
    # only; once locked the template shows a read-only view). form_values
    # seeds the pick slugs + encore slot; prefill carries the resolved song
    # titles so each slot's datalist has its option (the picker keeps a
    # pre-filled slug on blur only when it matches an option).
    form_values: dict[str, str] = {}
    prefill: dict[str, dict[str, str]] = {}
    if existing is not None and not lock.is_locked:
        titles = await _resolve_song_titles(existing.pick_song_slugs, cfg)
        for i, slug in enumerate(existing.pick_song_slugs, start=1):
            slot = f"pick_{i}"
            form_values[slot] = slug
            prefill[slot] = {"slug": slug, "title": titles.get(slug, slug)}
            if slug == existing.encore_slug:
                form_values["encore_pick"] = slot
    return render(
        templates,
        request,
        "predict.html",
        current_user=user,
        show=show,
        lock=_format_lock(lock, cfg),
        existing=existing,
        form_values=form_values,
        prefill=prefill,
        bad_slugs=[],
    )


async def _re_render_predict(
    templates: Jinja2Templates,
    request: Request,
    user: Any,
    show_date: date,
    cfg: Settings,
    *,
    error: str,
    status_code: int = status.HTTP_400_BAD_REQUEST,
    form_values: dict[str, str] | None = None,
    bad_slugs: list[str] | None = None,
) -> Response:
    pool = get_pool()
    target = ShowTarget(
        show_date=show_date, show_id=None, venue_name=None,
        location=None, tour_name=None,
    )
    lock = await get_or_create_lock(pool, target, cfg)
    existing = await get_user_prediction(pool, user.id, show_date)
    resp = render(
        templates,
        request,
        "predict.html",
        current_user=user,
        show={
            "show_date": show_date,
            "show_id": None,
            "venue_name": None,
            "location": None,
            "tour_name": None,
        },
        lock=_format_lock(lock, cfg),
        existing=existing,
        error=error,
        form_values=form_values or {},
        bad_slugs=bad_slugs or [],
    )
    resp.status_code = status_code
    return resp


@router.post("/predict/{show_date}")
async def predict_submit(
    request: Request,
    show_date: date,
    pick_1: str = Form(...),
    pick_2: str = Form(""),
    pick_3: str = Form(""),
    pick_4: str = Form(""),
    pick_5: str = Form(""),
    encore_pick: str = Form(""),
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    if user is None:
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    pool = get_pool()

    raw_picks = [pick_1, pick_2, pick_3, pick_4, pick_5]
    # Per-slot raw values, so we can resolve the encore call (which names
    # a slot like "pick_3") back to its submitted slug.
    slot_values: dict[str, str] = {
        "pick_1": pick_1.strip().lower(),
        "pick_2": pick_2.strip().lower(),
        "pick_3": pick_3.strip().lower(),
        "pick_4": pick_4.strip().lower(),
        "pick_5": pick_5.strip().lower(),
    }

    # Capture raw values up-front so any error path can re-render the
    # form with the user's existing picks intact (including invalid
    # ones, so they can see what to fix). ``encore_pick`` rides along so
    # the selected encore radio survives a validation error.
    raw_form: dict[str, str] = {**slot_values, "encore_pick": encore_pick.strip()}

    try:
        picks = normalize_picks(raw_picks)
    except PredictionError as exc:
        return await _re_render_predict(
            templates, request, user, show_date, cfg,
            error=str(exc), form_values=raw_form,
        )

    target = ShowTarget(
        show_date=show_date,
        show_id=None,
        venue_name=None,
        location=None,
        tour_name=None,
    )
    lock = await get_or_create_lock(pool, target, cfg)
    if lock.is_locked:
        return await _re_render_predict(
            templates, request, user, show_date, cfg,
            error="Predictions are locked for this show.",
            status_code=status.HTTP_409_CONFLICT,
            form_values=raw_form,
        )

    # Resolve the encore call. ``encore_pick`` names a pick slot (e.g.
    # "pick_3"); the encore slug is whatever that slot submitted. It must
    # be set and must reference a slot that holds a real (validated-later)
    # slug — i.e. one that survived normalize_picks.
    encore = slot_values.get(encore_pick.strip(), "") or None
    if encore is None or encore not in picks:
        return await _re_render_predict(
            templates, request, user, show_date, cfg,
            error="Tap one of your picks as the encore call.",
            form_values=raw_form,
        )

    # Slug validation gate (Layer 1): confirm every submitted slug
    # corresponds to a real song before we touch the DB. The picker UI
    # is a UX guardrail; this is the trust boundary. A user submitting
    # via curl, with JS off, or against a stale autocomplete list
    # cannot bypass this. The encore slug is already one of ``picks``, so
    # it needs no separate validation.
    slugs_to_check = list(picks)
    try:
        async with McpPhishClient(
            cfg.mcp_phish_url,
            timeout_seconds=cfg.mcp_phish_timeout_seconds,
        ) as mcp:
            valid_slugs = await mcp.validate_song_slugs(slugs_to_check)
    except McpPhishError:
        logger.warning(
            "mcp-phish unreachable for slug validation",
            extra={"show_date": str(show_date)},
        )
        return await _re_render_predict(
            templates, request, user, show_date, cfg,
            error=(
                "Could not validate song picks right now (upstream "
                "unavailable). Please try again in a moment."
            ),
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            form_values=raw_form,
        )
    bad_slugs = [s for s in slugs_to_check if s not in valid_slugs]
    if bad_slugs:
        # Highlight which slugs failed. Order-preserving + de-duped.
        seen: set[str] = set()
        unique_bad: list[str] = []
        for s in bad_slugs:
            if s not in seen:
                seen.add(s)
                unique_bad.append(s)
        error_msg = (
            "These picks aren't real songs in the database: "
            + ", ".join(unique_bad)
            + ". Pick from the autocomplete suggestions."
        )
        return await _re_render_predict(
            templates, request, user, show_date, cfg,
            error=error_msg,
            status_code=status.HTTP_400_BAD_REQUEST,
            form_values=raw_form,
            bad_slugs=unique_bad,
        )

    try:
        await insert_prediction(
            pool,
            user_id=user.id,
            show_date=show_date,
            pick_song_slugs=picks,
            encore_slug=encore,
        )
    except PredictionLocked as exc:
        # Trigger fired even though app check passed: race condition.
        return await _re_render_predict(
            templates, request, user, show_date, cfg,
            error=str(exc),
            status_code=status.HTTP_409_CONFLICT,
            form_values=raw_form,
        )
    except PredictionDuplicate as exc:
        return await _re_render_predict(
            templates, request, user, show_date, cfg,
            error=str(exc),
            status_code=status.HTTP_409_CONFLICT,
            form_values=raw_form,
        )
    except PredictionError as exc:
        return await _re_render_predict(
            templates, request, user, show_date, cfg,
            error=str(exc),
            status_code=status.HTTP_400_BAD_REQUEST,
            form_values=raw_form,
        )

    memberships = await list_user_leagues(pool, user.id)
    # "Share your game" payoff: make sure the player has a game to share, so
    # the confirmation page can hand out a real invite link + show who's in.
    game = await get_or_create_user_game(
        pool, user_id=user.id, handle=user.handle, settings=cfg
    )
    game_invite_url = invite_url(request, game.slug)
    game_members = await list_members_with_scores(
        pool, game.id, show_date, limit=200
    )
    return render(
        templates,
        request,
        "predicted.html",
        current_user=user,
        show_date=show_date.isoformat(),
        pick_song_slugs=picks,
        encore_slug=encore,
        leagues=memberships,
        game=game,
        game_invite_url=game_invite_url,
        game_members=game_members,
        # Lock state so the confirmation page can offer a "Modify your
        # picks" affordance while the lock is still open (gated on the same
        # lock check the upsert/DB-trigger enforce). lock was just read
        # above and passed the open-lock gate to reach this success path.
        lock=_format_lock(lock, cfg),
    )


@router.get("/songs/search", response_class=HTMLResponse)
async def songs_search(
    request: Request,
    q: str = Query("", min_length=0, max_length=64),
    cfg: Settings = Depends(get_cfg),
) -> HTMLResponse:
    """Pre-lock picker autocomplete.

    Returns ``<option value="slug" data-gap-label="...">title</option>``
    rows. Play counts stay stripped; only the song's current gap (shows
    since last play) is surfaced, as a fair-play help so a player doesn't
    waste a pick on a song played last night. The ``data-gap-label`` text
    is appended to the visible option label so it shows in the native
    datalist dropdown too. Empty when gap is unknown (e.g. the Phish
    deployment) — the UI degrades to plain title.
    """
    if not q.strip():
        return HTMLResponse("")
    try:
        async with McpPhishClient(
            cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
        ) as mcp:
            rows = await mcp.search_songs_for_picker(q.strip(), limit=10)
    except McpPhishError:
        logger.warning("songs_search: mcp-phish unreachable")
        return HTMLResponse("")
    # slug + title + gap label only. Play counts stay stripped.
    opts_parts: list[str] = []
    for r in rows:
        slug = escape(str(r["slug"]), quote=True)
        title = escape(str(r["title"]))
        label = _gap_label(r.get("gap_current"))
        if label:
            safe_label = escape(label, quote=True)
            # Append the gap to the visible option text so it shows in the
            # native dropdown; keep the raw label in data-gap-label so the
            # JS can show it as a muted hint on the picked chip.
            opts_parts.append(
                f'<option value="{slug}" data-gap-label="{safe_label}">'
                f"{title} ({escape(label)})</option>"
            )
        else:
            opts_parts.append(f'<option value="{slug}">{title}</option>')
    return HTMLResponse("".join(opts_parts))


@router.get("/show/{show_date}/predictions", response_class=HTMLResponse)
async def show_predictions(
    request: Request,
    show_date: date,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    """Read-only post-lock predictions list.

    Pre-lock returns a "predictions hidden until lock" panel. Once the
    show resolves, scores show up alongside the picks. Before that, only
    handles + slugs are visible (so a late peek can't leak strategy).
    """
    pool = get_pool()
    lock = await read_lock(pool, show_date)
    # Entrant count is fair to show pre-lock (a count reveals no picks).
    entrant_count = await count_entrants(pool, show_date)
    # Comment thread — independent of lock state, rendered in every branch.
    # Skip the query entirely when comments are disabled for the deployment.
    from setlist_stash.comments import list_comments
    comments = (
        await list_comments(pool, show_date) if cfg.enable_comments else []
    )
    if lock is None:
        # No prediction_locks row at all means the form was never opened;
        # treat as "no predictions yet" rather than 404.
        return render(
            templates,
            request,
            "show_predictions.html",
            current_user=user,
            show_date=show_date,
            lock=None,
            rows=[],
            resolved=False,
            pre_lock=True,
            entrant_count=entrant_count,
            comments=comments,
        )
    if not lock.is_locked:
        # Pre-lock: never list predictions. Renders the panel with a
        # "open after lock" message.
        return render(
            templates,
            request,
            "show_predictions.html",
            current_user=user,
            show_date=show_date,
            lock=_format_lock(lock, cfg),
            rows=[],
            resolved=False,
            pre_lock=True,
            entrant_count=entrant_count,
            comments=comments,
        )
    # Post-lock: this page IS the per-show leaderboard. Rank everyone by
    # current score (live scoring climbs this throughout the show; pre-score
    # everyone sits at 0), tie-broken by submit order so the list is stable.
    # COALESCE(score, 0) keeps unscored rows at 0 rather than NULL-sorting.
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT u.handle, p.pick_song_slugs, p.opener_slug,
                   p.closer_slug, p.encore_slug, p.submitted_at, p.score
              FROM predictions p
              JOIN users u ON u.id = p.user_id
             WHERE p.show_date = $1
             ORDER BY COALESCE(p.score, 0) DESC, p.submitted_at ASC
            """,
            show_date,
        )
        resolved_at = await conn.fetchval(
            "SELECT resolved_at FROM prediction_locks WHERE show_date = $1",
            show_date,
        )
    # Whether the show is finalized (`resolved_at` stamped). Scores show
    # live before that too (the resolver re-scores each tick), but the
    # "final" wording only lands once finalized.
    resolved = resolved_at is not None
    # Are any picks actually scored yet? Drives "everyone at 0 — scores
    # climb live" wording vs. live/final scores. A show can be post-lock
    # with no setlist published, so every score is NULL until the first
    # resolver tick.
    any_scored = any(r["score"] is not None for r in rows)
    # Live setlist. PREFERRED SOURCE is the resolver's snapshot: the exact
    # setlist the scores in `rows` were computed from (migration 009). That
    # is what makes this page internally consistent — a song can't appear
    # here before the standings next to it count it, no matter how often
    # the browser refreshes, because both halves come off one resolver tick.
    # It also means page views cost nothing upstream.
    #
    # FALLBACK is the original live get_show: shows resolved before 009 have
    # no snapshot, and neither does a show whose first tick hasn't run. Same
    # soft-failure stance as before (empty list -> "not posted yet"), so an
    # upstream outage still renders the standings.
    setlist_groups: list[dict[str, Any]] = []
    snapshot, _snapshot_at = await read_setlist_snapshot(pool, show_date)
    if snapshot is not None:
        setlist_groups = _group_setlist(snapshot)
    else:
        try:
            async with McpPhishClient(
                cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
            ) as mcp:
                show_meta = await mcp.get_show(show_date.isoformat())
            setlist_groups = _group_setlist(
                list(show_meta.get("setlist") or [])
            )
        except McpPhishError:
            logger.warning(
                "get_show failed in /show/predictions (setlist degraded)",
                extra={"show_date": str(show_date)},
            )
    # Auto-refresh only while the show is genuinely live: locked, not yet
    # finalized, and still inside the same active window the resolver uses
    # for its fast cadence. Past that the data stops moving, so polling
    # would just be load. Zero disables it entirely.
    live_now = live_board_active(
        lock_at=lock.lock_at,
        resolved=resolved,
        now=datetime.now(UTC),
        active_window_hours=cfg.resolver_active_window_hours,
    )
    refresh_seconds = cfg.live_refresh_seconds if live_now else 0
    ctx: dict[str, Any] = {
        "current_user": user,
        "show_date": show_date,
        "lock": _format_lock(lock, cfg),
        "rows": [dict(r) for r in rows],
        "resolved": resolved,
        "any_scored": any_scored,
        "pre_lock": False,
        "entrant_count": entrant_count,
        "setlist_groups": setlist_groups,
        "comments": comments,
        "live_refresh_seconds": refresh_seconds,
    }
    # htmx refresh: return ONLY the board fragment (setlist + standings).
    # Same route, same query, same snapshot — the fragment is by
    # construction the identical render the full page would have produced,
    # which is what keeps the two halves in lockstep. The comment thread
    # polls its own fragment and is deliberately outside this swap so a
    # half-typed comment survives a board refresh.
    if request.headers.get("HX-Request", "").lower() == "true":
        return render(templates, request, "_live_board.html", **ctx)
    return render(templates, request, "show_predictions.html", **ctx)


@router.get("/show/{show_date}/assist", response_class=HTMLResponse)
async def show_assist(
    request: Request,
    show_date: date,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    """Post-lock smart-pick assist: gap stats + venue history + recent setlists.

    Gated by ``assist_allowed``. Pre-lock with default config returns a
    "locked" message linking to the predict form; the assist data is
    never built or sent in that case.
    """
    pool = get_pool()
    allowed = await assist_allowed(pool, show_date, cfg)
    if not allowed:
        lock = await read_lock(pool, show_date)
        return render(
            templates,
            request,
            "show_assist.html",
            current_user=user,
            show_date=show_date,
            lock=_format_lock(lock, cfg) if lock else None,
            allowed=False,
            gap_chart=[],
            venue_rows=[],
            recent_shows=[],
            show_meta=None,
        )

    # Allowed. Pull the assist data via mcp-phish. Each block degrades
    # independently; a failed venue lookup doesn't poison gap stats.
    gap_chart: list[dict[str, Any]] = []
    venue_rows: list[dict[str, Any]] = []
    recent_show_rows: list[dict[str, Any]] = []
    show_meta: dict[str, Any] | None = None
    venue_slug: str | None = None
    try:
        async with McpPhishClient(
            cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
        ) as mcp:
            # 1. Gap chart: top 20 by gap.
            try:
                gap_chart = await mcp.songs_by_gap(limit=20)
            except McpPhishError:
                logger.warning("songs_by_gap failed in /assist")
            # 2. Show metadata (for venue slug + setlist context).
            try:
                show_meta = await mcp.get_show(show_date.isoformat())
            except McpPhishError:
                logger.warning(
                    "get_show failed in /assist",
                    extra={"show_date": str(show_date)},
                )
            if show_meta:
                venue = show_meta.get("venue") or {}
                venue_slug = venue.get("slug") or None
            # 3. Venue history (last 10 shows at the room).
            if venue_slug:
                try:
                    venue_rows = await mcp.venue_history(
                        venue_slug, limit=10
                    )
                except McpPhishError:
                    logger.warning(
                        "venue_history failed",
                        extra={"venue_slug": venue_slug},
                    )
            # 4. Recent setlists (last 3 shows).
            try:
                recent = await mcp.recent_shows(limit=3)
            except McpPhishError:
                logger.warning("recent_shows failed in /assist")
                recent = []
            for r in recent:
                show_id_or_date = str(r.get("date") or "")
                if not show_id_or_date:
                    continue
                try:
                    full = await mcp.get_show(show_id_or_date)
                except McpPhishError:
                    logger.warning(
                        "get_show failed in /assist recent",
                        extra={"date": show_id_or_date},
                    )
                    continue
                recent_show_rows.append(
                    {
                        "date": r.get("date"),
                        "venue_name": r.get("venue_name"),
                        "location": r.get("location"),
                        "setlist": full.get("setlist") or [],
                    }
                )
    except McpPhishError:
        logger.exception("mcp-phish unreachable in /assist")

    lock = await read_lock(pool, show_date)
    return render(
        templates,
        request,
        "show_assist.html",
        current_user=user,
        show_date=show_date,
        lock=_format_lock(lock, cfg) if lock else None,
        allowed=True,
        gap_chart=gap_chart,
        venue_rows=venue_rows,
        recent_shows=recent_show_rows,
        show_meta=show_meta,
    )
