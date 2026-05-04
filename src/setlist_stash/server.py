"""FastAPI app entrypoint.

Build session 1 routes:
- GET  /                        index (handle form OR welcome)
- POST /handle                  create anonymous handle, set signed cookie
- GET  /predict/{show_date}     picks form for a show (or "locked" view)
- POST /predict/{show_date}     submit a prediction
- GET  /songs/search?q=...      pre-lock-safe autocomplete (slug+title only)
- GET  /healthz                 status + mcp-phish reachability + DB ping

Out of scope for this session: leaderboards, resolver wiring, magic-link
email, post-lock assist views. See PHASE-4-PLAN.md §9.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import uvicorn
from fastapi import FastAPI, Form, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from setlist_stash import __version__
from setlist_stash.auth import (
    COOKIE_MAX_AGE_SECONDS,
    COOKIE_NAME,
    HANDLE_HELP,
    HandleError,
    create_user,
    current_user,
    sign_user_id,
    validate_handle,
)
from setlist_stash.auth_email import (
    EmailFormatError,
    EmailTakenError,
    get_email_status,
    request_email_link,
    request_login_link,
    verify_token,
)
from setlist_stash.config import Settings, get_settings
from setlist_stash.db import close_pool, get_pool, init_pool
from setlist_stash.email import EmailProvider, EmailSendError, build_provider
from setlist_stash.leaderboard import (
    VALID_SCOPES,
    fetch_leaderboard,
    fetch_user_rank,
    latest_scope_key,
    list_scope_keys,
    normalize_scope,
)
from setlist_stash.leagues import (
    LeagueDateWindowError,
    LeagueForbidden,
    LeagueFull,
    LeagueHostCannotLeave,
    LeagueNameError,
    create_league,
    get_league_by_slug,
    is_member,
    join_league,
    leave_league,
    list_league_members,
    list_user_leagues,
    member_count,
    rotate_slug,
    soft_delete_league,
    update_league,
)
from setlist_stash.locks import (
    LockState,
    assist_allowed,
    get_or_create_lock,
    read_lock,
    select_form_show,
)
from setlist_stash.logging_setup import configure_logging
from setlist_stash.mcp_client import McpPhishClient, McpPhishError
from setlist_stash.migrate import run_migrations
from setlist_stash.predictions import (
    PredictionDuplicate,
    PredictionError,
    PredictionLocked,
    get_user_prediction,
    insert_prediction,
    normalize_picks,
    normalize_slot,
)

logger = logging.getLogger("setlist_stash.server")

_PACKAGE_DIR = Path(__file__).resolve().parent
_TEMPLATES_DIR = _PACKAGE_DIR / "templates"
_STATIC_DIR = _PACKAGE_DIR / "static"


def _format_lock(lock: LockState, settings: Settings) -> dict[str, Any]:
    tz = ZoneInfo(settings.default_lock_tz)
    local = lock.lock_at.astimezone(tz)
    return {
        "is_locked": lock.is_locked,
        "lock_at_display": local.strftime("%Y-%m-%d %H:%M %Z"),
        # ISO-8601 with timezone, parseable by JS ``new Date()``. Used by
        # the predict-page countdown and post-lock panels.
        "lock_at_iso": lock.lock_at.isoformat(),
        "seconds_until_lock": max(lock.seconds_until_lock, 0),
    }


def build_app(
    settings: Settings | None = None,
    *,
    email_provider: EmailProvider | None = None,
) -> FastAPI:
    """Construct the FastAPI app.

    Factory pattern so tests can inject a Settings without touching env.
    Tests can also inject a fake ``email_provider`` directly to avoid the
    factory + EMAIL_PROVIDER env dance.
    """
    cfg = settings or get_settings()
    configure_logging(cfg.log_format)
    provider: EmailProvider = email_provider or build_provider(cfg)

    templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
    templates.env.globals["site_name"] = cfg.site_name
    templates.env.globals["theme_file"] = cfg.theme_file

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        # Startup: pool + migrations.
        try:
            pool = await init_pool(cfg)
            await run_migrations(pool)
        except Exception:
            logger.exception("startup failed")
            raise
        yield
        await close_pool()

    app = FastAPI(
        title="setlist-stash",
        version=__version__,
        description=(
            "Setlist prediction game for Phish shows. "
            "Phase 4 of the Phish Data Platform."
        ),
        lifespan=lifespan,
    )

    if _STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    # ----- helpers ----------------------------------------------------------

    async def _resolve_user(request: Request) -> Any:
        try:
            pool = get_pool()
        except RuntimeError:
            return None
        return await current_user(request, pool, cfg)

    def _render(
        request: Request, name: str, **ctx: Any
    ) -> HTMLResponse:
        ctx.setdefault("version", __version__)
        return templates.TemplateResponse(
            request=request, name=name, context=ctx
        )

    def _set_session_cookie(resp: Response, user_id: int) -> None:
        resp.set_cookie(
            COOKIE_NAME,
            sign_user_id(cfg, user_id),
            max_age=COOKIE_MAX_AGE_SECONDS,
            httponly=True,
            samesite="lax",
            secure=False,  # LAN/Tailscale; Phase 6 enables Secure under HTTPS
        )

    # ----- routes -----------------------------------------------------------

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> HTMLResponse:
        user = await _resolve_user(request)
        upcoming = None
        if user is not None:
            try:
                async with McpPhishClient(
                    cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
                ) as mcp:
                    upcoming = await select_form_show(cfg, mcp)
            except McpPhishError:
                logger.warning("mcp-phish unreachable on /; rendering without show")
                upcoming = None
        return _render(
            request,
            "index.html",
            current_user=user,
            handle_help=HANDLE_HELP,
            upcoming_show=upcoming,
        )

    @app.post("/handle")
    async def post_handle(request: Request, handle: str = Form(...)) -> Response:
        try:
            canonical = validate_handle(handle)
        except HandleError as exc:
            return _render(
                request,
                "index.html",
                current_user=None,
                handle_help=HANDLE_HELP,
                error=str(exc),
            )
        pool = get_pool()
        try:
            user_id = await create_user(pool, canonical)
        except HandleError as exc:
            return _render(
                request,
                "index.html",
                current_user=None,
                handle_help=HANDLE_HELP,
                error=str(exc),
            )
        resp: Response = RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
        _set_session_cookie(resp, user_id)
        logger.info("created handle", extra={"user_id": user_id})
        return resp

    @app.get("/predict/{show_date}", response_class=HTMLResponse)
    async def predict_form(request: Request, show_date: date) -> Response:
        user = await _resolve_user(request)
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
                rows = await mcp.recent_shows(limit=20)
            for row in rows:
                if str(row.get("date")) == show_date.isoformat():
                    raw_id = row.get("show_id")
                    show_id = str(raw_id) if raw_id else None
                    venue_name = row.get("venue_name") or None
                    location = row.get("location") or None
                    tour_name = row.get("tour_name") or None
                    break
        except McpPhishError:
            logger.warning(
                "mcp-phish unreachable for show lookup",
                extra={"show_date": str(show_date)},
            )

        show: dict[str, Any] = {
            "show_date": show_date,
            "show_id": show_id,
            "venue_name": venue_name,
            "location": location,
            "tour_name": tour_name,
        }

        from setlist_stash.locks import ShowTarget  # local import to avoid cycle

        target = ShowTarget(
            show_date=show_date,
            show_id=show_id,
            venue_name=venue_name,
            location=location,
            tour_name=tour_name,
        )
        lock = await get_or_create_lock(pool, target, cfg)
        existing = await get_user_prediction(pool, user.id, show_date)
        return _render(
            request,
            "predict.html",
            current_user=user,
            show=show,
            lock=_format_lock(lock, cfg),
            existing=existing,
            form_values={},
            bad_slugs=[],
        )

    @app.post("/predict/{show_date}")
    async def predict_submit(
        request: Request,
        show_date: date,
        pick_1: str = Form(...),
        pick_2: str = Form(...),
        pick_3: str = Form(...),
        opener_slug: str = Form(""),
        closer_slug: str = Form(""),
        encore_slug: str = Form(""),
    ) -> Response:
        user = await _resolve_user(request)
        if user is None:
            return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
        pool = get_pool()

        # Capture raw values up-front so any error path can re-render the
        # form with the user's existing picks intact (including invalid
        # ones, so they can see what to fix).
        raw_form: dict[str, str] = {
            "pick_1": pick_1.strip().lower(),
            "pick_2": pick_2.strip().lower(),
            "pick_3": pick_3.strip().lower(),
            "opener_slug": opener_slug.strip().lower(),
            "closer_slug": closer_slug.strip().lower(),
            "encore_slug": encore_slug.strip().lower(),
        }

        try:
            picks = normalize_picks([pick_1, pick_2, pick_3])
            opener = normalize_slot(opener_slug)
            closer = normalize_slot(closer_slug)
            encore = normalize_slot(encore_slug)
        except PredictionError as exc:
            return await _re_render_predict(
                request, user, show_date, error=str(exc), form_values=raw_form
            )

        from setlist_stash.locks import ShowTarget

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
                request,
                user,
                show_date,
                error="Predictions are locked for this show.",
                status_code=status.HTTP_409_CONFLICT,
                form_values=raw_form,
            )

        # Slug validation gate (Layer 1): confirm every submitted slug
        # corresponds to a real song before we touch the DB. The picker UI
        # is a UX guardrail; this is the trust boundary. A user submitting
        # via curl, with JS off, or against a stale autocomplete list
        # cannot bypass this.
        slugs_to_check = [
            s for s in (*picks, opener, closer, encore) if s
        ]
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
                request,
                user,
                show_date,
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
                "These picks aren't real Phish songs in the database: "
                + ", ".join(unique_bad)
                + ". Pick from the autocomplete suggestions."
            )
            return await _re_render_predict(
                request,
                user,
                show_date,
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
                opener_slug=opener,
                closer_slug=closer,
                encore_slug=encore,
            )
        except PredictionLocked as exc:
            # Trigger fired even though app check passed: race condition.
            return await _re_render_predict(
                request, user, show_date, error=str(exc),
                status_code=status.HTTP_409_CONFLICT,
                form_values=raw_form,
            )
        except PredictionDuplicate as exc:
            return await _re_render_predict(
                request, user, show_date, error=str(exc),
                status_code=status.HTTP_409_CONFLICT,
                form_values=raw_form,
            )
        except PredictionError as exc:
            return await _re_render_predict(
                request, user, show_date, error=str(exc),
                status_code=status.HTTP_400_BAD_REQUEST,
                form_values=raw_form,
            )

        memberships = await list_user_leagues(pool, user.id)
        return _render(
            request,
            "predicted.html",
            current_user=user,
            show_date=show_date.isoformat(),
            pick_song_slugs=picks,
            opener_slug=opener,
            closer_slug=closer,
            encore_slug=encore,
            leagues=memberships,
        )

    async def _re_render_predict(
        request: Request,
        user: Any,
        show_date: date,
        *,
        error: str,
        status_code: int = status.HTTP_400_BAD_REQUEST,
        form_values: dict[str, str] | None = None,
        bad_slugs: list[str] | None = None,
    ) -> Response:
        pool = get_pool()
        from setlist_stash.locks import ShowTarget

        target = ShowTarget(
            show_date=show_date, show_id=None, venue_name=None,
            location=None, tour_name=None,
        )
        lock = await get_or_create_lock(pool, target, cfg)
        existing = await get_user_prediction(pool, user.id, show_date)
        resp = _render(
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

    @app.get("/songs/search", response_class=HTMLResponse)
    async def songs_search(
        request: Request, q: str = Query("", min_length=0, max_length=64)
    ) -> HTMLResponse:
        """Pre-lock-safe autocomplete.

        Returns ``<option value="slug">title</option>`` rows. ``times_played``
        and ``gap`` are stripped at the wrapper boundary.
        """
        if not q.strip():
            return HTMLResponse("")
        try:
            async with McpPhishClient(
                cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
            ) as mcp:
                rows = await mcp.search_songs_pre_lock(q.strip(), limit=10)
        except McpPhishError:
            logger.warning("songs_search: mcp-phish unreachable")
            return HTMLResponse("")
        # Important: only slug + title. Validated by tests.
        opts = "".join(
            f'<option value="{r["slug"]}">{r["title"]}</option>' for r in rows
        )
        return HTMLResponse(opts)

    # ----- leaderboard ------------------------------------------------------

    SCOPE_LABELS = {
        "weekly": "Weekly",
        "tour": "Season",
        "all_time": "All-time",
    }

    async def _render_leaderboard(
        request: Request,
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
        user = await _resolve_user(request)
        if effective_key:
            rows = await fetch_leaderboard(pool, scope, effective_key, limit=50)
            if user is not None:
                user_row = await fetch_user_rank(
                    pool, scope, effective_key, user.id
                )
        scope_keys = await list_scope_keys(pool, scope)
        ctx: dict[str, Any] = {
            "current_user": user,
            "scope": scope,
            "scope_label": SCOPE_LABELS.get(scope, scope),
            "scope_key": effective_key,
            "scope_keys": scope_keys,
            "rows": rows,
            "user_row": user_row,
            "scope_options": [
                ("weekly", "Weekly"),
                ("tour", "Season"),
                ("all_time", "All-time"),
            ],
        }
        template = "_leaderboard_table.html" if partial else "leaderboard.html"
        return _render(request, template, **ctx)

    @app.get("/leaderboard", response_class=HTMLResponse)
    async def leaderboard_index(
        request: Request,
        scope: str = Query("weekly"),
    ) -> HTMLResponse:
        normalized = normalize_scope(scope)
        if normalized not in VALID_SCOPES:
            normalized = "weekly"
        partial = request.headers.get("HX-Request", "").lower() == "true"
        return await _render_leaderboard(
            request, scope=normalized, scope_key=None, partial=partial
        )

    @app.get("/leaderboard/{scope}/{scope_key}", response_class=HTMLResponse)
    async def leaderboard_at(
        request: Request, scope: str, scope_key: str
    ) -> HTMLResponse:
        normalized = normalize_scope(scope)
        if normalized not in VALID_SCOPES:
            normalized = "weekly"
        partial = request.headers.get("HX-Request", "").lower() == "true"
        # scope_key is user-provided; whitelist to alphanumeric + dash + underscore
        # to keep it impossible to inject something weird into the page.
        safe_key = "".join(c for c in scope_key if c.isalnum() or c in "-_")
        return await _render_leaderboard(
            request, scope=normalized, scope_key=safe_key or None, partial=partial
        )

    # ----- post-lock views (assist + read-only predictions) -----------------

    @app.get("/show/{show_date}/predictions", response_class=HTMLResponse)
    async def show_predictions(
        request: Request, show_date: date
    ) -> HTMLResponse:
        """Read-only post-lock predictions list.

        Pre-lock returns a "predictions hidden until lock" panel. Once the
        show resolves, scores show up alongside the picks. Before that, only
        handles + slugs are visible (so a late peek can't leak strategy).
        """
        user = await _resolve_user(request)
        pool = get_pool()
        lock = await read_lock(pool, show_date)
        if lock is None:
            # No prediction_locks row at all means the form was never opened;
            # treat as "no predictions yet" rather than 404.
            return _render(
                request,
                "show_predictions.html",
                current_user=user,
                show_date=show_date,
                lock=None,
                rows=[],
                resolved=False,
                pre_lock=True,
            )
        if not lock.is_locked:
            # Pre-lock: never list predictions. Renders the panel with a
            # "open after lock" message.
            return _render(
                request,
                "show_predictions.html",
                current_user=user,
                show_date=show_date,
                lock=_format_lock(lock, cfg),
                rows=[],
                resolved=False,
                pre_lock=True,
            )
        # Post-lock: list everyone's picks.
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT u.handle, p.pick_song_slugs, p.opener_slug,
                       p.closer_slug, p.encore_slug, p.submitted_at, p.score
                  FROM predictions p
                  JOIN users u ON u.id = p.user_id
                 WHERE p.show_date = $1
                 ORDER BY p.submitted_at ASC
                """,
                show_date,
            )
            resolved_at = await conn.fetchval(
                "SELECT resolved_at FROM prediction_locks WHERE show_date = $1",
                show_date,
            )
        # Hide score column until the show is resolved (`resolved_at` set).
        resolved = resolved_at is not None
        return _render(
            request,
            "show_predictions.html",
            current_user=user,
            show_date=show_date,
            lock=_format_lock(lock, cfg),
            rows=[dict(r) for r in rows],
            resolved=resolved,
            pre_lock=False,
        )

    @app.get("/show/{show_date}/assist", response_class=HTMLResponse)
    async def show_assist(
        request: Request, show_date: date
    ) -> HTMLResponse:
        """Post-lock smart-pick assist: gap stats + venue history + recent setlists.

        Gated by ``assist_allowed``. Pre-lock with default config returns a
        "locked" message linking to the predict form; the assist data is
        never built or sent in that case.
        """
        user = await _resolve_user(request)
        pool = get_pool()
        allowed = await assist_allowed(pool, show_date, cfg)
        if not allowed:
            lock = await read_lock(pool, show_date)
            return _render(
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
        return _render(
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

    # ----- Phase 4c: private leagues ----------------------------------------

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

    def _invite_url(request: Request, slug: str) -> str:
        base = str(request.base_url).rstrip("/")
        return f"{base}/league/{slug}"

    @app.get("/leagues", response_class=HTMLResponse)
    async def leagues_index(request: Request) -> Response:
        user = await _resolve_user(request)
        if user is None:
            return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
        pool = get_pool()
        memberships = await list_user_leagues(pool, user.id)
        return _render(
            request,
            "leagues_list.html",
            current_user=user,
            leagues=memberships,
        )

    @app.get("/leagues/new", response_class=HTMLResponse)
    async def league_new_form(request: Request) -> Response:
        user = await _resolve_user(request)
        if user is None:
            return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
        return _render(
            request,
            "leagues_new.html",
            current_user=user,
            member_cap=cfg.league_member_cap,
        )

    @app.post("/leagues/new")
    async def league_new_submit(
        request: Request,
        name: str = Form(...),
        start_date: str = Form(""),
        end_date: str = Form(""),
    ) -> Response:
        user = await _resolve_user(request)
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
            resp = _render(
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

    def _league_404(request: Request, signed_in: bool) -> HTMLResponse:
        resp = _render(
            request,
            "auth_verify_error.html",
            current_user=None,
            message="That league doesn't exist (or the slug rotated).",
            ttl_hours=cfg.magic_link_ttl_hours,
            signed_in=signed_in,
        )
        resp.status_code = status.HTTP_404_NOT_FOUND
        return resp

    @app.get("/league/{slug}", response_class=HTMLResponse)
    async def league_detail(request: Request, slug: str) -> Response:
        pool = get_pool()
        league = await get_league_by_slug(pool, slug)
        if league is None:
            user = await _resolve_user(request)
            return _league_404(request, signed_in=user is not None)
        user = await _resolve_user(request)
        count = await member_count(pool, league.id)
        if user is None or not await is_member(pool, league.id, user.id):
            return _render(
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
        resp = _render(
            request,
            "league_dashboard.html",
            current_user=user,
            league=league,
            member_count=count,
            members=members,
            is_host=(league.host_user_id == user.id),
            upcoming_show=upcoming,
            invite_url=_invite_url(request, league.slug),
            flash=flash,
        )
        if flash:
            resp.delete_cookie("phishgame_league_flash")
        return resp

    @app.post("/league/{slug}/join")
    async def league_join(request: Request, slug: str) -> Response:
        user = await _resolve_user(request)
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
            full_resp = _render(
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
        redirect: Response = RedirectResponse(
            f"/league/{league.slug}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
        redirect.set_cookie(
            "phishgame_league_flash",
            f"You joined {league.name}.",
            max_age=30,
            httponly=True,
            samesite="lax",
            secure=False,
        )
        return redirect

    @app.post("/league/{slug}/leave")
    async def league_leave(request: Request, slug: str) -> Response:
        user = await _resolve_user(request)
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
            resp = _render(
                request,
                "league_dashboard.html",
                current_user=user,
                league=league,
                member_count=count,
                members=members,
                is_host=True,
                upcoming_show=None,
                invite_url=_invite_url(request, league.slug),
                flash=str(exc),
            )
            resp.status_code = status.HTTP_409_CONFLICT
            return resp
        return RedirectResponse(
            "/leagues", status_code=status.HTTP_303_SEE_OTHER
        )

    @app.get("/league/{slug}/leaderboard", response_class=HTMLResponse)
    async def league_leaderboard_view(
        request: Request, slug: str
    ) -> Response:
        pool = get_pool()
        league = await get_league_by_slug(pool, slug)
        if league is None:
            user = await _resolve_user(request)
            return _league_404(request, signed_in=user is not None)
        user = await _resolve_user(request)
        count = await member_count(pool, league.id)
        rows = await fetch_leaderboard(
            pool, scope="league", scope_key=league.slug, limit=50
        )
        user_row = None
        if user is not None:
            user_row = await fetch_user_rank(
                pool, scope="league", scope_key=league.slug, user_id=user.id
            )
        return _render(
            request,
            "league_leaderboard.html",
            current_user=user,
            league=league,
            member_count=count,
            rows=rows,
            user_row=user_row,
        )

    async def _require_host(
        request: Request, slug: str
    ) -> tuple[Any, Any] | Response:
        """Resolve user + league + enforce host-only. Returns the pair on success
        or a Response on failure (use ``isinstance(..., Response)``).
        """
        user = await _resolve_user(request)
        if user is None:
            return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
        pool = get_pool()
        league = await get_league_by_slug(pool, slug)
        if league is None:
            return _league_404(request, signed_in=True)
        if league.host_user_id != user.id:
            resp = _render(
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

    @app.get("/league/{slug}/settings", response_class=HTMLResponse)
    async def league_settings_view(
        request: Request, slug: str
    ) -> Response:
        result = await _require_host(request, slug)
        if isinstance(result, Response):
            return result
        user, league = result
        flash = request.cookies.get("phishgame_league_flash")
        resp = _render(
            request,
            "league_settings.html",
            current_user=user,
            league=league,
            flash=flash,
        )
        if flash:
            resp.delete_cookie("phishgame_league_flash")
        return resp

    @app.post("/league/{slug}/settings")
    async def league_settings_submit(
        request: Request,
        slug: str,
        name: str = Form(...),
        start_date: str = Form(""),
        end_date: str = Form(""),
    ) -> Response:
        result = await _require_host(request, slug)
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
            err_resp = _render(
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
            secure=False,
        )
        return redirect

    @app.post("/league/{slug}/rotate")
    async def league_rotate(request: Request, slug: str) -> Response:
        result = await _require_host(request, slug)
        if isinstance(result, Response):
            return result
        user, league = result
        pool = get_pool()
        try:
            new_slug = await rotate_slug(pool, league, host_user_id=user.id)
        except LeagueForbidden as exc:
            err_resp = _render(
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
            secure=False,
        )
        return redirect

    @app.post("/league/{slug}/delete")
    async def league_delete(request: Request, slug: str) -> Response:
        result = await _require_host(request, slug)
        if isinstance(result, Response):
            return result
        user, league = result
        pool = get_pool()
        try:
            await soft_delete_league(pool, league, host_user_id=user.id)
        except LeagueForbidden as exc:
            resp = _render(
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

    # ----- Phase 4b: magic-link email auth ----------------------------------

    def _provider_enabled() -> bool:
        return provider.name != "disabled"

    @app.get("/auth/email", response_class=HTMLResponse)
    async def auth_email_form(request: Request) -> Response:
        """Form to attach (or change) an email on a signed-in handle."""
        user = await _resolve_user(request)
        if user is None:
            return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
        pool = get_pool()
        status_data = await get_email_status(pool, user.id)
        return _render(
            request,
            "auth_email.html",
            current_user=user,
            status=status_data,
            provider_enabled=_provider_enabled(),
        )

    @app.post("/auth/email")
    async def auth_email_submit(
        request: Request, email: str = Form(...)
    ) -> Response:
        user = await _resolve_user(request)
        if user is None:
            return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
        if not _provider_enabled():
            # Don't even try to mint a token if email is off — the user
            # could never click the link.
            return JSONResponse(
                {"error": "Email is disabled on this server."},
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        pool = get_pool()
        try:
            masked = await request_email_link(
                pool,
                user=user,
                email=email,
                settings=cfg,
                provider=provider,
            )
        except EmailFormatError as exc:
            status_data = await get_email_status(pool, user.id)
            resp = _render(
                request,
                "auth_email.html",
                current_user=user,
                status=status_data,
                provider_enabled=True,
                error=str(exc),
            )
            resp.status_code = status.HTTP_400_BAD_REQUEST
            return resp
        except EmailTakenError as exc:
            status_data = await get_email_status(pool, user.id)
            resp = _render(
                request,
                "auth_email.html",
                current_user=user,
                status=status_data,
                provider_enabled=True,
                error=str(exc),
            )
            resp.status_code = status.HTTP_409_CONFLICT
            return resp
        except EmailSendError as exc:
            logger.warning("email send failed", extra={"err": str(exc)})
            status_data = await get_email_status(pool, user.id)
            resp = _render(
                request,
                "auth_email.html",
                current_user=user,
                status=status_data,
                provider_enabled=True,
                error="Could not send email right now. Try again shortly.",
            )
            resp.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return resp
        return _render(
            request,
            "auth_email_sent.html",
            current_user=user,
            masked_email=masked,
            ttl_hours=cfg.magic_link_ttl_hours,
            log_mode=(provider.name == "log"),
        )

    @app.get("/auth/login", response_class=HTMLResponse)
    async def auth_login_form(request: Request) -> Response:
        """Cross-browser sign-in: enter your verified email, get a link."""
        # If already signed in, send to /account.
        user = await _resolve_user(request)
        if user is not None:
            return RedirectResponse(
                "/account", status_code=status.HTTP_303_SEE_OTHER
            )
        return _render(
            request,
            "auth_login.html",
            current_user=None,
            provider_enabled=_provider_enabled(),
        )

    @app.post("/auth/login")
    async def auth_login_submit(
        request: Request, email: str = Form(...)
    ) -> Response:
        user = await _resolve_user(request)
        if user is not None:
            return RedirectResponse(
                "/account", status_code=status.HTTP_303_SEE_OTHER
            )
        if not _provider_enabled():
            return JSONResponse(
                {"error": "Email is disabled on this server."},
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        pool = get_pool()
        try:
            masked = await request_login_link(
                pool, email=email, settings=cfg, provider=provider
            )
        except EmailFormatError as exc:
            resp = _render(
                request,
                "auth_login.html",
                current_user=None,
                provider_enabled=True,
                error=str(exc),
            )
            resp.status_code = status.HTTP_400_BAD_REQUEST
            return resp
        except EmailSendError:
            resp = _render(
                request,
                "auth_login.html",
                current_user=None,
                provider_enabled=True,
                error="Could not send email right now. Try again shortly.",
            )
            resp.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return resp
        return _render(
            request,
            "auth_login_sent.html",
            current_user=None,
            masked_email=masked,
            ttl_hours=cfg.magic_link_ttl_hours,
            log_mode=(provider.name == "log"),
        )

    @app.get("/auth/verify", response_class=HTMLResponse)
    async def auth_verify(
        request: Request, token: str = Query("", min_length=0, max_length=512)
    ) -> Response:
        """Consume a magic-link token (either purpose).

        On success: set/refresh the session cookie to the verified user's
        id and redirect to /account with a flash message in the cookie.
        On failure: render auth_verify_error.html with a clean message.
        """
        # Capture caller IP for audit. Trust the immediate peer here; the
        # platform is LAN/Tailscale only through Phase 5 so X-Forwarded-For
        # would be moot.
        client_ip: str | None = None
        if request.client and request.client.host:
            client_ip = request.client.host

        already_signed_in = (await _resolve_user(request)) is not None
        pool = get_pool()
        try:
            result = await verify_token(pool, token=token, ip=client_ip)
        except LookupError as exc:
            err_resp = _render(
                request,
                "auth_verify_error.html",
                current_user=await _resolve_user(request),
                message=str(exc),
                ttl_hours=cfg.magic_link_ttl_hours,
                signed_in=already_signed_in,
            )
            err_resp.status_code = status.HTTP_400_BAD_REQUEST
            return err_resp
        # Success: set the session cookie to the verified user's id, then
        # redirect to /account. Even for email_verify (where the user was
        # likely already signed in as that user), refreshing the cookie is
        # idempotent and ensures cross-browser flows land on the right id.
        flash = (
            "Email verified. You can now sign in from another browser."
            if result.purpose == "email_verify"
            else f"Signed in as {result.handle}."
        )
        resp: Response = RedirectResponse(
            "/account", status_code=status.HTTP_303_SEE_OTHER
        )
        _set_session_cookie(resp, result.user_id)
        # Short-lived flash cookie: rendered once and cleared by /account.
        resp.set_cookie(
            "phishgame_flash",
            flash,
            max_age=30,
            httponly=True,
            samesite="lax",
            secure=False,
        )
        return resp

    @app.get("/account", response_class=HTMLResponse)
    async def account_page(request: Request) -> Response:
        """Show handle + email status. Sign-in required."""
        user = await _resolve_user(request)
        if user is None:
            return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
        pool = get_pool()
        status_data = await get_email_status(pool, user.id)
        memberships = await list_user_leagues(pool, user.id)
        flash = request.cookies.get("phishgame_flash")
        resp = _render(
            request,
            "account.html",
            current_user=user,
            status=status_data,
            flash=flash,
            leagues=memberships,
        )
        if flash:
            resp.delete_cookie("phishgame_flash")
        return resp

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> JSONResponse:
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
            body["mcp_phish"] = {"reachable": True, "vault_stale": h.get("vault", {}).get("stale")}
        except Exception as exc:
            body["mcp_phish"] = {"reachable": False, "error": str(exc)[:120]}
            body["status"] = "degraded"
        # Resolver heartbeat (most-recent scoring_runs row).
        try:
            from setlist_stash.resolve import latest_run_summary
            pool = get_pool()
            latest = await latest_run_summary(pool)
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

    logger.info(
        "setlist-stash booted",
        extra={"version": __version__, "port": cfg.app_port},
    )
    return app


# Module-level app for ``uvicorn setlist_stash.server:app`` usage.
app = build_app()


def main() -> None:
    """Run the app under uvicorn. Used by the Docker entrypoint."""
    cfg = get_settings()
    uvicorn.run(
        "setlist_stash.server:app",
        host=cfg.app_host,
        port=cfg.app_port,
        log_config=None,
        access_log=True,
    )


if __name__ == "__main__":
    main()
