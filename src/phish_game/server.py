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

from phish_game import __version__
from phish_game.auth import (
    COOKIE_MAX_AGE_SECONDS,
    COOKIE_NAME,
    HANDLE_HELP,
    HandleError,
    create_user,
    current_user,
    sign_user_id,
    validate_handle,
)
from phish_game.config import Settings, get_settings
from phish_game.db import close_pool, get_pool, init_pool
from phish_game.locks import LockState, get_or_create_lock, select_form_show
from phish_game.logging_setup import configure_logging
from phish_game.mcp_client import McpPhishClient, McpPhishError
from phish_game.migrate import run_migrations
from phish_game.predictions import (
    PredictionDuplicate,
    PredictionError,
    PredictionLocked,
    get_user_prediction,
    insert_prediction,
    normalize_picks,
    normalize_slot,
)

logger = logging.getLogger("phish_game.server")

_PACKAGE_DIR = Path(__file__).resolve().parent
_TEMPLATES_DIR = _PACKAGE_DIR / "templates"
_STATIC_DIR = _PACKAGE_DIR / "static"


def _format_lock(lock: LockState, settings: Settings) -> dict[str, Any]:
    tz = ZoneInfo(settings.default_lock_tz)
    local = lock.lock_at.astimezone(tz)
    return {
        "is_locked": lock.is_locked,
        "lock_at_display": local.strftime("%Y-%m-%d %H:%M %Z"),
        "seconds_until_lock": max(lock.seconds_until_lock, 0),
    }


def build_app(settings: Settings | None = None) -> FastAPI:
    """Construct the FastAPI app.

    Factory pattern so tests can inject a Settings without touching env.
    """
    cfg = settings or get_settings()
    configure_logging(cfg.log_format)

    templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

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
        title="phish-game",
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

        from phish_game.locks import ShowTarget  # local import to avoid cycle

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

        try:
            picks = normalize_picks([pick_1, pick_2, pick_3])
            opener = normalize_slot(opener_slug)
            closer = normalize_slot(closer_slug)
            encore = normalize_slot(encore_slug)
        except PredictionError as exc:
            return await _re_render_predict(
                request, user, show_date, error=str(exc)
            )

        from phish_game.locks import ShowTarget

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
            )
        except PredictionDuplicate as exc:
            return await _re_render_predict(
                request, user, show_date, error=str(exc),
                status_code=status.HTTP_409_CONFLICT,
            )
        except PredictionError as exc:
            return await _re_render_predict(
                request, user, show_date, error=str(exc),
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        return _render(
            request,
            "predicted.html",
            current_user=user,
            show_date=show_date.isoformat(),
            pick_song_slugs=picks,
            opener_slug=opener,
            closer_slug=closer,
            encore_slug=encore,
        )

    async def _re_render_predict(
        request: Request,
        user: Any,
        show_date: date,
        *,
        error: str,
        status_code: int = status.HTTP_400_BAD_REQUEST,
    ) -> Response:
        pool = get_pool()
        from phish_game.locks import ShowTarget

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
        return JSONResponse(body, status_code=200)

    logger.info(
        "phish-game booted",
        extra={"version": __version__, "port": cfg.app_port},
    )
    return app


# Module-level app for ``uvicorn phish_game.server:app`` usage.
app = build_app()


def main() -> None:
    """Run the app under uvicorn. Used by the Docker entrypoint."""
    cfg = get_settings()
    uvicorn.run(
        "phish_game.server:app",
        host=cfg.app_host,
        port=cfg.app_port,
        log_config=None,
        access_log=True,
    )


if __name__ == "__main__":
    main()
