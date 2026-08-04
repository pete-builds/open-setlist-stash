"""Private-league / shareable-game routes (Phase 4c).

Gated by ``settings.enable_games`` — a deployment that turns games off
returns 404 for every route in this module, matching the pre-split
``_games_gate`` behavior. The domain logic lives in ``setlist_stash.leagues``.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, Form, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from setlist_stash.config import Settings
from setlist_stash.db import get_pool
from setlist_stash.deps import get_cfg, get_current_user, get_templates, render
from setlist_stash.leaderboard import fetch_leaderboard, fetch_user_rank
from setlist_stash.leagues import (
    LeagueDateWindowError,
    LeagueForbidden,
    LeagueFull,
    LeagueHostCannotLeave,
    LeagueNameError,
    create_league,
    get_league_by_slug,
    get_or_create_user_game,
    is_member,
    join_league,
    leave_league,
    list_league_members,
    list_members_with_scores,
    list_user_leagues,
    member_count,
    rotate_slug,
    soft_delete_league,
    update_league,
)
from setlist_stash.locks import select_form_show
from setlist_stash.mcp_client import McpPhishClient, McpPhishError
from setlist_stash.web_helpers import invite_url, upcoming_show_date

router = APIRouter()
logger = logging.getLogger("setlist_stash.server")


def _games_gate(cfg: Settings) -> Response | None:
    """Block league/game routes when games are disabled.

    Returns a 404 Response when ``enable_games`` is False so a gated
    deployment (Wappy Picks) exposes no league/game surface, even by
    direct URL. Returns None when games are enabled, so the route runs
    normally. The league code and tables still exist — this only gates
    the HTTP surface (oss-platform-split; nothing is deleted).
    """
    if cfg.enable_games:
        return None
    resp: Response = HTMLResponse(
        "Not found", status_code=status.HTTP_404_NOT_FOUND
    )
    return resp


def _parse_optional_date(raw: str) -> date | None:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError as exc:
        raise LeagueDateWindowError(
            f"'{s}' is not a valid date (YYYY-MM-DD)."
        ) from exc


def _league_404(
    templates: Jinja2Templates,
    request: Request,
    cfg: Settings,
    signed_in: bool,
) -> HTMLResponse:
    resp = render(
        templates,
        request,
        "auth_verify_error.html",
        current_user=None,
        message="That league doesn't exist (or the slug rotated).",
        ttl_hours=cfg.magic_link_ttl_hours,
        signed_in=signed_in,
    )
    resp.status_code = status.HTTP_404_NOT_FOUND
    return resp


async def _require_host(
    request: Request,
    slug: str,
    user: Any,
    cfg: Settings,
    templates: Jinja2Templates,
) -> tuple[Any, Any] | Response:
    """Resolve user + league + enforce host-only. Returns the pair on success
    or a Response on failure (use ``isinstance(..., Response)``).
    """
    if (gate := _games_gate(cfg)) is not None:
        return gate
    if user is None:
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    pool = get_pool()
    league = await get_league_by_slug(pool, slug)
    if league is None:
        return _league_404(templates, request, cfg, signed_in=True)
    if league.host_user_id != user.id:
        resp = render(
            templates,
            request,
            "auth_verify_error.html",
            current_user=user,
            message="Only the league host can do that.",
            ttl_hours=cfg.magic_link_ttl_hours,
            signed_in=True,
        )
        resp.status_code = status.HTTP_403_FORBIDDEN
        return resp
    return (user, league)


@router.post("/game/start")
async def game_start(
    request: Request,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
) -> Response:
    """Auto-create (or find) the caller's game and return its invite URL.

    Powers the "share link IS a game" flow: the predict-page Share button
    POSTs here first, gets back a real game invite URL, then hands that to
    the native share / clipboard helper. Idempotent — a user who already
    has a game gets that same game back, never a duplicate.

    Returns JSON ``{"invite_url": ..., "slug": ..., "name": ...}``. A
    signed-out caller gets 401 so the client can fall back to sharing the
    current page.
    """
    if (gate := _games_gate(cfg)) is not None:
        return gate
    if user is None:
        return JSONResponse(
            {"error": "Pick a handle first."},
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
    pool = get_pool()
    league = await get_or_create_user_game(
        pool, user_id=user.id, handle=user.handle, settings=cfg
    )
    return JSONResponse(
        {
            "invite_url": invite_url(request, league.slug),
            "slug": league.slug,
            "name": league.name,
        }
    )


@router.get("/leagues", response_class=HTMLResponse)
async def leagues_index(
    request: Request,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    if (gate := _games_gate(cfg)) is not None:
        return gate
    if user is None:
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    pool = get_pool()
    memberships = await list_user_leagues(pool, user.id)
    return render(
        templates,
        request,
        "leagues_list.html",
        current_user=user,
        leagues=memberships,
    )


@router.get("/leagues/new", response_class=HTMLResponse)
async def league_new_form(
    request: Request,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    if (gate := _games_gate(cfg)) is not None:
        return gate
    if user is None:
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    return render(
        templates,
        request,
        "leagues_new.html",
        current_user=user,
        member_cap=cfg.league_member_cap,
    )


@router.post("/leagues/new")
async def league_new_submit(
    request: Request,
    name: str = Form(...),
    start_date: str = Form(""),
    end_date: str = Form(""),
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    if (gate := _games_gate(cfg)) is not None:
        return gate
    if user is None:
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    pool = get_pool()
    try:
        start = _parse_optional_date(start_date)
        end = _parse_optional_date(end_date)
        league = await create_league(
            pool,
            name=name,
            host_user_id=user.id,
            settings=cfg,
            start_date=start,
            end_date=end,
        )
    except (LeagueNameError, LeagueDateWindowError) as exc:
        resp = render(
            templates,
            request,
            "leagues_new.html",
            current_user=user,
            member_cap=cfg.league_member_cap,
            error=str(exc),
            form_name=name,
            form_start=start_date,
            form_end=end_date,
        )
        resp.status_code = status.HTTP_400_BAD_REQUEST
        return resp
    return RedirectResponse(
        f"/league/{league.slug}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.get("/league/{slug}", response_class=HTMLResponse)
async def league_detail(
    request: Request,
    slug: str,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    if (gate := _games_gate(cfg)) is not None:
        return gate
    pool = get_pool()
    league = await get_league_by_slug(pool, slug)
    if league is None:
        return _league_404(templates, request, cfg, signed_in=user is not None)
    count = await member_count(pool, league.id)
    if user is None or not await is_member(pool, league.id, user.id):
        return render(
            templates,
            request,
            "league_join.html",
            current_user=user,
            league=league,
            member_count=count,
            at_cap=count >= league.member_cap,
        )
    # Member: render the dashboard.
    members = await list_league_members(pool, league.id, limit=200)
    upcoming = None
    try:
        async with McpPhishClient(
            cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
        ) as mcp:
            upcoming = await select_form_show(cfg, mcp)
    except McpPhishError:
        logger.warning("mcp-phish unreachable on league dashboard")
    flash = request.cookies.get("phishgame_league_flash")
    resp = render(
        templates,
        request,
        "league_dashboard.html",
        current_user=user,
        league=league,
        member_count=count,
        members=members,
        is_host=(league.host_user_id == user.id),
        upcoming_show=upcoming,
        invite_url=invite_url(request, league.slug),
        flash=flash,
    )
    if flash:
        resp.delete_cookie("phishgame_league_flash")
    return resp


@router.get("/game/{slug}", response_class=HTMLResponse)
async def game_detail(
    request: Request,
    slug: str,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    """Friendlier alias for ``/league/{slug}``.

    Same behavior: non-members see the join page, members see the
    dashboard. Kept as a thin wrapper so existing /league/ links never
    break while shared "game" links read naturally.
    """
    return await league_detail(request, slug, user, cfg, templates)


@router.post("/league/{slug}/join")
async def league_join(
    request: Request,
    slug: str,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    if (gate := _games_gate(cfg)) is not None:
        return gate
    if user is None:
        return RedirectResponse(
            f"/league/{slug}", status_code=status.HTTP_303_SEE_OTHER
        )
    pool = get_pool()
    league = await get_league_by_slug(pool, slug)
    if league is None:
        return RedirectResponse(
            "/leagues", status_code=status.HTTP_303_SEE_OTHER
        )
    try:
        await join_league(pool, league, user.id)
    except LeagueFull as exc:
        count = await member_count(pool, league.id)
        full_resp = render(
            templates,
            request,
            "league_join.html",
            current_user=user,
            league=league,
            member_count=count,
            at_cap=True,
            error=str(exc),
        )
        full_resp.status_code = status.HTTP_409_CONFLICT
        return full_resp
    # Land joiners on the shared scoreboard, not the dashboard — that is
    # the "everyone sees one scoreboard" payoff the invite promised.
    redirect: Response = RedirectResponse(
        f"/league/{league.slug}/leaderboard",
        status_code=status.HTTP_303_SEE_OTHER,
    )
    redirect.set_cookie(
        "phishgame_league_flash",
        f"You're in {league.name}.",
        max_age=30,
        httponly=True,
        samesite="lax",
        secure=cfg.cookie_secure,
    )
    return redirect


@router.post("/game/{slug}/join")
async def game_join(
    request: Request,
    slug: str,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    """Alias for ``/league/{slug}/join``."""
    return await league_join(request, slug, user, cfg, templates)


@router.post("/league/{slug}/leave")
async def league_leave(
    request: Request,
    slug: str,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    if (gate := _games_gate(cfg)) is not None:
        return gate
    if user is None:
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    pool = get_pool()
    league = await get_league_by_slug(pool, slug)
    if league is None:
        return RedirectResponse(
            "/leagues", status_code=status.HTTP_303_SEE_OTHER
        )
    try:
        await leave_league(pool, league, user.id)
    except LeagueHostCannotLeave as exc:
        members = await list_league_members(pool, league.id, limit=200)
        count = await member_count(pool, league.id)
        resp = render(
            templates,
            request,
            "league_dashboard.html",
            current_user=user,
            league=league,
            member_count=count,
            members=members,
            is_host=True,
            upcoming_show=None,
            invite_url=invite_url(request, league.slug),
            flash=str(exc),
        )
        resp.status_code = status.HTTP_409_CONFLICT
        return resp
    return RedirectResponse(
        "/leagues", status_code=status.HTTP_303_SEE_OTHER
    )


@router.get("/league/{slug}/leaderboard", response_class=HTMLResponse)
async def league_leaderboard_view(
    request: Request,
    slug: str,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    if (gate := _games_gate(cfg)) is not None:
        return gate
    pool = get_pool()
    league = await get_league_by_slug(pool, slug)
    if league is None:
        return _league_404(templates, request, cfg, signed_in=user is not None)
    count = await member_count(pool, league.id)
    rows = await fetch_leaderboard(
        pool, scope="league", scope_key=league.slug, limit=50
    )
    user_row = None
    if user is not None:
        user_row = await fetch_user_rank(
            pool, scope="league", scope_key=league.slug, user_id=user.id
        )
    # Pre-show / pre-score, the snapshot table is empty so the cumulative
    # ``rows`` above is blank. Show every player at "0" instead of an empty
    # panel by listing members joined to their score for the upcoming show.
    # This is display-only; it never touches the snapshot rebuild.
    target_date = await upcoming_show_date(cfg)
    member_scores = await list_members_with_scores(
        pool, league.id, target_date, limit=200
    )
    league_invite_url = invite_url(request, league.slug)
    # Did the viewer arrive via this game link and already join? Members get
    # a direct "Make your picks" button; a brand-new visitor gets a
    # "Join & make your picks" CTA that routes through the game's join flow,
    # so picking + membership stay connected.
    viewer_is_member = user is not None and await is_member(
        pool, league.id, user.id
    )
    return render(
        templates,
        request,
        "league_leaderboard.html",
        current_user=user,
        league=league,
        member_count=count,
        rows=rows,
        user_row=user_row,
        member_scores=member_scores,
        invite_url=league_invite_url,
        upcoming_date=target_date,
        viewer_is_member=viewer_is_member,
    )


@router.get("/game/{slug}/leaderboard", response_class=HTMLResponse)
async def game_leaderboard_view(
    request: Request,
    slug: str,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    """Alias for ``/league/{slug}/leaderboard`` — the shared scoreboard."""
    return await league_leaderboard_view(request, slug, user, cfg, templates)


@router.get("/league/{slug}/settings", response_class=HTMLResponse)
async def league_settings_view(
    request: Request,
    slug: str,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    result = await _require_host(request, slug, user, cfg, templates)
    if isinstance(result, Response):
        return result
    user, league = result
    flash = request.cookies.get("phishgame_league_flash")
    resp = render(
        templates,
        request,
        "league_settings.html",
        current_user=user,
        league=league,
        flash=flash,
    )
    if flash:
        resp.delete_cookie("phishgame_league_flash")
    return resp


@router.post("/league/{slug}/settings")
async def league_settings_submit(
    request: Request,
    slug: str,
    name: str = Form(...),
    start_date: str = Form(""),
    end_date: str = Form(""),
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    result = await _require_host(request, slug, user, cfg, templates)
    if isinstance(result, Response):
        return result
    user, league = result
    pool = get_pool()
    try:
        start = _parse_optional_date(start_date)
        end = _parse_optional_date(end_date)
        await update_league(
            pool,
            league,
            host_user_id=user.id,
            name=name,
            start_date=start,
            end_date=end,
        )
    except (
        LeagueNameError,
        LeagueDateWindowError,
        LeagueForbidden,
    ) as exc:
        err_resp = render(
            templates,
            request,
            "league_settings.html",
            current_user=user,
            league=league,
            error=str(exc),
        )
        err_resp.status_code = status.HTTP_400_BAD_REQUEST
        return err_resp
    redirect: Response = RedirectResponse(
        f"/league/{league.slug}/settings",
        status_code=status.HTTP_303_SEE_OTHER,
    )
    redirect.set_cookie(
        "phishgame_league_flash",
        "League updated.",
        max_age=30,
        httponly=True,
        samesite="lax",
        secure=cfg.cookie_secure,
    )
    return redirect


@router.post("/league/{slug}/rotate")
async def league_rotate(
    request: Request,
    slug: str,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    result = await _require_host(request, slug, user, cfg, templates)
    if isinstance(result, Response):
        return result
    user, league = result
    pool = get_pool()
    try:
        new_slug = await rotate_slug(pool, league, host_user_id=user.id)
    except LeagueForbidden as exc:
        err_resp = render(
            templates,
            request,
            "league_settings.html",
            current_user=user,
            league=league,
            error=str(exc),
        )
        err_resp.status_code = status.HTTP_403_FORBIDDEN
        return err_resp
    # Migrate any existing leaderboard rows to the new scope_key so the
    # leaderboard survives a rotate without a resolver tick. Same scope,
    # new key.
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE leaderboard_snapshots
               SET scope_key = $2
             WHERE scope = 'league' AND scope_key = $1
            """,
            league.slug,
            new_slug,
        )
    redirect: Response = RedirectResponse(
        f"/league/{new_slug}/settings",
        status_code=status.HTTP_303_SEE_OTHER,
    )
    redirect.set_cookie(
        "phishgame_league_flash",
        f"Slug rotated. Old URL is dead. New URL: /league/{new_slug}",
        max_age=30,
        httponly=True,
        samesite="lax",
        secure=cfg.cookie_secure,
    )
    return redirect


@router.post("/league/{slug}/delete")
async def league_delete(
    request: Request,
    slug: str,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    result = await _require_host(request, slug, user, cfg, templates)
    if isinstance(result, Response):
        return result
    user, league = result
    pool = get_pool()
    try:
        await soft_delete_league(pool, league, host_user_id=user.id)
    except LeagueForbidden as exc:
        resp = render(
            templates,
            request,
            "league_settings.html",
            current_user=user,
            league=league,
            error=str(exc),
        )
        resp.status_code = status.HTTP_403_FORBIDDEN
        return resp
    # Wipe league leaderboard snapshots so the deleted slug doesn't
    # leak rows into a future rebuild.
    async with pool.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM leaderboard_snapshots
             WHERE scope = 'league' AND scope_key = $1
            """,
            league.slug,
        )
    return RedirectResponse(
        "/leagues", status_code=status.HTTP_303_SEE_OTHER
    )
