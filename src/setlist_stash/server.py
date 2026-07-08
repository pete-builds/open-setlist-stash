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

import asyncio
import hashlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import date, datetime
from html import escape
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import uvicorn
from authlib.integrations.starlette_client import OAuth, OAuthError
from fastapi import FastAPI, Form, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

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
from setlist_stash.auth_google import (
    GoogleLinkConflict,
    resolve_google_identity,
)
from setlist_stash.blog import get_post, load_posts
from setlist_stash.config import Settings, get_settings
from setlist_stash.db import close_pool, get_pool, init_pool
from setlist_stash.email import EmailProvider, EmailSendError, build_provider
from setlist_stash.leaderboard import (
    VALID_SCOPES,
    fetch_leaderboard,
    fetch_user_rank,
    latest_scope_key,
    list_scope_keys,
    list_show_entrants,
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
from setlist_stash.locks import (
    LockState,
    assist_allowed,
    get_or_create_lock,
    read_lock,
    select_form_show,
)
from setlist_stash.logging_setup import configure_logging
from setlist_stash.mcp_client import McpPhishClient, McpPhishError
from setlist_stash.mcp_proxy import (
    FixedWindowRateLimiter,
    McpReverseProxy,
    client_ip,
)
from setlist_stash.migrate import run_migrations
from setlist_stash.predictions import (
    PredictionDuplicate,
    PredictionError,
    PredictionLocked,
    count_entrants,
    get_user_prediction,
    insert_prediction,
    normalize_picks,
)

logger = logging.getLogger("setlist_stash.server")

_PACKAGE_DIR = Path(__file__).resolve().parent
_TEMPLATES_DIR = _PACKAGE_DIR / "templates"
_STATIC_DIR = _PACKAGE_DIR / "static"


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
    paths = [_STATIC_DIR / "style.css"]
    if theme_file:
        paths.append(_STATIC_DIR / theme_file)
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
        except Exception:  # noqa: BLE001 - best-effort labeling only
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
    # Content hash of the CSS, appended as ``?v=`` to the static stylesheet
    # links so a styling change is a fresh URL at the Cloudflare edge.
    templates.env.globals["asset_version"] = _compute_asset_version(cfg.theme_file)
    templates.env.globals["footer_credit"] = cfg.footer_credit
    templates.env.globals["footer_credit_url"] = cfg.footer_credit_url
    # GA4 measurement ID. Empty (default) renders no analytics tag at all, so
    # the OSS image / third-party self-host stay clean. Set per deployment via
    # the ANALYTICS_ID env var; base.html guards the gtag snippet on it.
    templates.env.globals["analytics_id"] = cfg.analytics_id
    # Optional beta notice. Empty (default) renders no banner at all, so the
    # OSS image / Phish demo / third-party self-host stay clean. Set per
    # deployment via the BETA_NOTICE env var; index.html guards it on truthiness.
    templates.env.globals["beta_notice"] = cfg.beta_notice
    # Whether the email/magic-link signup UI should render at all. Off when the
    # provider is disabled (default), so the email entry points disappear for
    # any deployment without email configured.
    templates.env.globals["email_enabled"] = provider.name != "disabled"
    # Whether the "Sign in with Google" entry points render at all. True only
    # when a Google OAuth client is fully configured for this deployment; empty
    # (the default) leaves every Google button off and the /auth/google/* routes
    # redirect home — so the OSS image, the Wappy sibling, and any third-party
    # self-host stay unaffected until they opt in (Phase 1 Google SSO).
    templates.env.globals["google_oauth_enabled"] = cfg.google_oauth_enabled
    # Whether to render the nav "Blog" link. True only when the bind-mounted
    # BLOG_DIR holds at least one parseable post. Empty/missing dir (the Phish
    # demo, third-party self-host) leaves the link off entirely. Evaluated at
    # build time: content is mounted before the container starts, so a fresh
    # post needs a container recreate to appear (cheap, and matches the theme
    # mount lifecycle).
    templates.env.globals["has_blog"] = len(load_posts(cfg.blog_dir)) > 0
    # Whether the private-leagues / shareable-game UI renders at all. True (the
    # default) keeps the full games experience; ENABLE_GAMES=false hides every
    # league/game link in the templates and makes the league/game routes
    # 404/redirect (see ``_games_gate``). The Phish demo and OSS image leave
    # this True; only the Wappy Picks deployment sets it false.
    templates.env.globals["enable_games"] = cfg.enable_games
    # Whether to render the "Connect" (public MCP docs) nav link. True only
    # when a public MCP endpoint is configured for this deployment. Empty/unset
    # (the OSS image, the Phish demo) leaves the link off and the route serves a
    # graceful "no public MCP" panel (oss-platform-split).
    templates.env.globals["has_mcp"] = bool(cfg.mcp_public_url)
    templates.env.globals["mcp_public_url"] = cfg.mcp_public_url
    templates.env.globals["mcp_subject"] = cfg.mcp_subject

    # Public MCP reverse proxy (oss-platform-split): only active when an
    # upstream is configured for this deployment. When unset, /mcp is not
    # mounted, so the OSS image and the Phish demo expose nothing.
    mcp_proxy: McpReverseProxy | None = None
    if cfg.mcp_upstream_url:
        mcp_proxy = McpReverseProxy(
            cfg.mcp_upstream_url,
            timeout_seconds=cfg.mcp_proxy_timeout_seconds,
        )
    mcp_rate_limiter = FixedWindowRateLimiter(cfg.mcp_rate_limit_per_minute)

    # Google SSO (Phase 1): register the OIDC client only when configured.
    # Authlib pulls Google's discovery document + JWKS lazily on first use and
    # verifies the id_token signature/claims for us. When disabled (default),
    # ``oauth`` stays None and the /auth/google/* routes redirect home.
    oauth: OAuth | None = None
    if cfg.google_oauth_enabled:
        oauth = OAuth()
        oauth.register(
            name="google",
            client_id=cfg.google_client_id,
            client_secret=cfg.google_client_secret.get_secret_value(),
            server_metadata_url=(
                "https://accounts.google.com/.well-known/openid-configuration"
            ),
            client_kwargs={"scope": "openid email profile"},
        )

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
        if mcp_proxy is not None:
            await mcp_proxy.aclose()
        await close_pool()

    app = FastAPI(
        title="setlist-stash",
        version=__version__,
        description="Open-source setlist prediction game.",
        lifespan=lifespan,
    )

    # Starlette session cookie used ONLY to carry the OAuth ``state``/``nonce``
    # across the Google redirect (Phase 1 Google SSO). It is short-lived and
    # completely separate from the primary ``phishgame_session`` signed-cookie
    # identity, which is untouched. Keyed with the same session_secret so no new
    # secret is needed; ``https_only`` follows COOKIE_SECURE.
    app.add_middleware(
        SessionMiddleware,
        secret_key=cfg.session_secret.get_secret_value(),
        session_cookie="phishgame_oauth",
        max_age=600,  # 10 min: only needs to survive the round-trip to Google
        same_site="lax",
        https_only=cfg.cookie_secure,
    )

    # Per-IP rate limit, scoped to the public /mcp proxy ONLY. The game UI,
    # static assets, and every other route are never touched by this middleware.
    @app.middleware("http")
    async def _mcp_rate_limit(request: Request, call_next: Any) -> Response:
        path = request.url.path
        if (
            mcp_rate_limiter.enabled
            and (path == "/mcp" or path.startswith("/mcp/"))
            and not mcp_rate_limiter.allow(client_ip(request))
        ):
            return JSONResponse(
                {"error": "rate limit exceeded"},
                status_code=429,
                headers={"Retry-After": "60"},
            )
        resp: Response = await call_next(request)
        return resp

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
            secure=cfg.cookie_secure,  # True on HTTPS deployments (COOKIE_SECURE)
        )

    # ----- routes -----------------------------------------------------------

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> HTMLResponse:
        user = await _resolve_user(request)
        # Resolve the upcoming show for everyone (not just signed-in users) so
        # the home-page countdown widget renders for anonymous visitors too.
        upcoming = None
        try:
            async with McpPhishClient(
                cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
            ) as mcp:
                upcoming = await select_form_show(cfg, mcp)
        except McpPhishError:
            logger.warning("mcp-phish unreachable on /; rendering without show")
            upcoming = None
        # Lock for the upcoming show, so the hero countdown has a target. Same
        # _format_lock shape the predict page consumes (lock_at_iso + display +
        # is_locked). Needs the DB pool; skip gracefully if it isn't up.
        upcoming_lock = None
        entrant_count = 0
        if upcoming is not None:
            try:
                pool = get_pool()
                lock = await get_or_create_lock(pool, upcoming, cfg)
                upcoming_lock = _format_lock(lock, cfg)
                # How many players are in for the upcoming show. Count only
                # (no picks revealed), so it's fair to show pre-lock.
                entrant_count = await count_entrants(pool, upcoming.show_date)
            except RuntimeError:
                upcoming_lock = None
        return _render(
            request,
            "index.html",
            current_user=user,
            handle_help=HANDLE_HELP,
            upcoming_show=upcoming,
            upcoming_lock=upcoming_lock,
            entrant_count=entrant_count,
        )

    def _safe_next(raw: str) -> str:
        """Whitelist a post-handle redirect target.

        Only same-origin absolute paths (``/league/...``, ``/game/...``,
        ``/predict/...``) are honored, so a crafted ``next`` can never bounce a
        new player to an external site. Anything else falls back to ``/``.
        """
        s = (raw or "").strip()
        if s.startswith("/") and not s.startswith("//"):
            return s
        return "/"

    @app.post("/handle")
    async def post_handle(
        request: Request,
        handle: str = Form(...),
        next: str = Form(""),
    ) -> Response:
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
        # Honor a safe same-origin ``next`` (e.g. a game invite the player
        # arrived from) so a brand-new handle lands back on the invite, one
        # step from joining. Defaults to home.
        resp: Response = RedirectResponse(
            url=_safe_next(next), status_code=status.HTTP_303_SEE_OTHER
        )
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

        # Operator-set target show: prefer its venue/location. An upcoming show
        # can sit outside the recent_shows window, so the scan above may miss it.
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
        return _render(
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

    @app.post("/predict/{show_date}")
    async def predict_submit(
        request: Request,
        show_date: date,
        pick_1: str = Form(...),
        pick_2: str = Form(""),
        pick_3: str = Form(""),
        pick_4: str = Form(""),
        pick_5: str = Form(""),
        encore_pick: str = Form(""),
    ) -> Response:
        user = await _resolve_user(request)
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

        # Resolve the encore call. ``encore_pick`` names a pick slot (e.g.
        # "pick_3"); the encore slug is whatever that slot submitted. It must
        # be set and must reference a slot that holds a real (validated-later)
        # slug — i.e. one that survived normalize_picks.
        encore = slot_values.get(encore_pick.strip(), "") or None
        if encore is None or encore not in picks:
            return await _re_render_predict(
                request,
                user,
                show_date,
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
                "These picks aren't real songs in the database: "
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
        # "Share your game" payoff: make sure the player has a game to share, so
        # the confirmation page can hand out a real invite link + show who's in.
        game = await get_or_create_user_game(
            pool, user_id=user.id, handle=user.handle, settings=cfg
        )
        game_invite_url = _invite_url(request, game.slug)
        game_members = await list_members_with_scores(
            pool, game.id, show_date, limit=200
        )
        return _render(
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
        # Target show for the "Make your picks" CTA. Resolved once here and
        # reused for the pre-score fallback below, so we never double-call
        # mcp-phish.
        target_date = await _upcoming_show_date()
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
        # Entrant count is fair to show pre-lock (a count reveals no picks).
        entrant_count = await count_entrants(pool, show_date)
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
                entrant_count=entrant_count,
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
                entrant_count=entrant_count,
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
        # Live setlist: fold a single soft get_show into this existing post-lock
        # data path so players watch the setlist fill in beside the standings.
        # Reuses the same MCP client/error pattern as /assist (no second client).
        # A failure/timeout degrades to an empty list — the standings still
        # render and the template shows a "not posted yet" placeholder.
        setlist_groups: list[dict[str, Any]] = []
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
        return _render(
            request,
            "show_predictions.html",
            current_user=user,
            show_date=show_date,
            lock=_format_lock(lock, cfg),
            rows=[dict(r) for r in rows],
            resolved=resolved,
            any_scored=any_scored,
            pre_lock=False,
            entrant_count=entrant_count,
            setlist_groups=setlist_groups,
        )

    @app.get("/u/{handle}", response_class=HTMLResponse)
    async def user_profile(request: Request, handle: str) -> HTMLResponse:
        """Public player profile: handle + their pick history across shows.

        Every name link on a leaderboard points here, so this must resolve
        (a missing route is the 404 source the leaderboards hit). Read-only
        and fair-play safe: picks for a show are only listed once that show is
        post-lock (the same rule the per-show predictions page enforces), so a
        profile can't leak a live entrant's strategy before lock.
        """
        viewer = await _resolve_user(request)
        pool = get_pool()
        async with pool.acquire() as conn:
            target = await conn.fetchrow(
                "SELECT id, handle FROM users WHERE lower(handle) = lower($1)",
                handle,
            )
            if target is None:
                resp = _render(
                    request,
                    "u_profile.html",
                    current_user=viewer,
                    profile_handle=handle,
                    found=False,
                    history=[],
                )
                resp.status_code = status.HTTP_404_NOT_FOUND
                return resp
            # Pick history, newest show first. Join the lock so we know whether
            # each show is post-lock (picks revealable) and resolved (score
            # final). Pre-lock shows list the date + "locked until showtime"
            # rather than the picks, to stay fair-play safe.
            rows = await conn.fetch(
                """
                SELECT p.show_date,
                       p.pick_song_slugs,
                       p.encore_slug,
                       p.score,
                       pl.lock_at,
                       pl.resolved_at
                  FROM predictions p
                  LEFT JOIN prediction_locks pl ON pl.show_date = p.show_date
                 WHERE p.user_id = $1
                 ORDER BY p.show_date DESC
                """,
                target["id"],
            )
        now = datetime.now(tz=ZoneInfo("UTC"))
        history: list[dict[str, Any]] = []
        for r in rows:
            lock_at = r["lock_at"]
            is_locked = lock_at is not None and now >= lock_at
            history.append(
                {
                    "show_date": r["show_date"],
                    "pick_song_slugs": list(r["pick_song_slugs"] or []),
                    "encore_slug": r["encore_slug"],
                    "score": r["score"],
                    "is_locked": is_locked,
                    "resolved": r["resolved_at"] is not None,
                }
            )
        return _render(
            request,
            "u_profile.html",
            current_user=viewer,
            profile_handle=target["handle"],
            found=True,
            history=history,
        )

    @app.get("/shows", response_class=HTMLResponse)
    async def shows_index(request: Request) -> HTMLResponse:
        """Archive index of every show that's had a prediction lock.

        Read-only, no schema change. One row per ``prediction_locks`` show,
        newest first, with an entrant count (LEFT JOIN predictions) and a
        finalized flag (``resolved_at IS NOT NULL``). Each row links to that
        show's per-show leaderboard. Venue names are best-effort via mcp-phish
        and degrade to the bare date when upstream is down.
        """
        viewer = await _resolve_user(request)
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
        # Best-effort venue lookup, keyed by ISO date. One mcp call; degrade to
        # bare dates if it's unreachable so the archive always renders.
        venue_by_date: dict[str, str] = {}
        try:
            async with McpPhishClient(
                cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
            ) as mcp:
                recent = await mcp.recent_shows(limit=50)
            for row in recent:
                d = str(row.get("date") or "")
                name = row.get("venue_name") or row.get("location") or ""
                if d and name:
                    venue_by_date[d] = str(name)
        except McpPhishError:
            logger.warning("mcp-phish unreachable on /shows; bare dates only")
        shows: list[dict[str, Any]] = []
        for r in rows:
            iso = r["show_date"].isoformat()
            shows.append(
                {
                    "show_date": r["show_date"],
                    "venue": venue_by_date.get(iso),
                    "entrants": int(r["entrants"]),
                    "resolved": r["resolved_at"] is not None,
                }
            )
        return _render(
            request,
            "shows.html",
            current_user=viewer,
            shows=shows,
        )

    @app.get("/stats", response_class=HTMLResponse)
    async def stats_page(request: Request) -> HTMLResponse:
        """Public catalog-wide statistics page.

        Reads a single ``stats_overview`` roll-up from the MCP server and
        renders it as a set of cards + tables (headline numbers, most-played,
        biggest bust-outs, rarest songs, recent debuts, longest shows). The
        upstream tool is band-specific; a deployment whose MCP omits it (the
        Phish demo, a third-party self-host pointed at a different MCP) gets a
        graceful "stats unavailable" panel instead of a crash — same degrade
        pattern the rest of the app uses for an unreachable MCP.
        """
        viewer = await _resolve_user(request)
        stats: dict[str, Any] | None = None
        try:
            async with McpPhishClient(
                cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
            ) as mcp:
                stats = await mcp.stats_overview(top_n=10)
        except McpPhishError:
            logger.warning("stats_overview unavailable on /stats")
            stats = None
        return _render(
            request,
            "stats.html",
            current_user=viewer,
            stats=stats,
        )

    @app.get("/about", response_class=HTMLResponse)
    async def about_page(request: Request) -> HTMLResponse:
        """Static About page: what the game is, how it works, who built it.

        Generic by default (uses ``site_name``); operator credit rides the
        same env-driven ``footer_credit`` the footer uses, so a third-party
        self-host shows no operator branding.
        """
        viewer = await _resolve_user(request)
        upcoming = None
        try:
            async with McpPhishClient(
                cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
            ) as mcp:
                upcoming = await select_form_show(cfg, mcp)
        except McpPhishError:
            upcoming = None
        return _render(
            request,
            "about.html",
            current_user=viewer,
            upcoming_show=upcoming,
        )

    @app.get("/connect", response_class=HTMLResponse)
    async def connect_page(request: Request) -> HTMLResponse:
        """Developer docs page for the deployment's public read-only MCP.

        Gives copy-paste setup for Claude Code, Claude Desktop, and any MCP
        client, plus the tool list. When no public MCP is configured
        (``MCP_PUBLIC_URL`` empty — the OSS image, the Phish demo) it renders a
        "no public MCP on this deployment" panel; the nav link is hidden in
        that case (``has_mcp`` global).
        """
        viewer = await _resolve_user(request)
        return _render(
            request,
            "connect.html",
            current_user=viewer,
        )

    # ----- public MCP reverse proxy ----------------------------------------
    # Streaming passthrough to the deployment's internal Streamable-HTTP MCP.
    # Only registered when MCP_UPSTREAM_URL is set (oss-platform-split): the
    # OSS image / Phish demo never expose these routes. Rate-limited per IP by
    # the _mcp_rate_limit middleware above.
    if mcp_proxy is not None:

        @app.api_route(
            "/mcp",
            methods=["GET", "POST", "DELETE"],
            include_in_schema=False,
        )
        async def mcp_root(request: Request) -> Response:
            return await mcp_proxy.handle(request)

        @app.api_route(
            "/mcp/{path:path}",
            methods=["GET", "POST", "DELETE"],
            include_in_schema=False,
        )
        async def mcp_subpath(request: Request, path: str) -> Response:
            return await mcp_proxy.handle(request, path)

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

    def _games_gate() -> Response | None:
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

    def _invite_url(request: Request, slug: str) -> str:
        base = str(request.base_url).rstrip("/")
        return f"{base}/game/{slug}"

    async def _upcoming_show_date() -> date | None:
        """Best-effort target show date for scoreboard 0-pre-scoring.

        Returns the upcoming show's date so the scoreboard can show each
        member's score for that show (0 before it's scored). Degrades to None
        when mcp-phish is unreachable; callers treat None as "everyone at 0".
        """
        try:
            async with McpPhishClient(
                cfg.mcp_phish_url, timeout_seconds=cfg.mcp_phish_timeout_seconds
            ) as mcp:
                upcoming = await select_form_show(cfg, mcp)
        except McpPhishError:
            return None
        return upcoming.show_date if upcoming is not None else None

    @app.post("/game/start")
    async def game_start(request: Request) -> Response:
        """Auto-create (or find) the caller's game and return its invite URL.

        Powers the "share link IS a game" flow: the predict-page Share button
        POSTs here first, gets back a real game invite URL, then hands that to
        the native share / clipboard helper. Idempotent — a user who already
        has a game gets that same game back, never a duplicate.

        Returns JSON ``{"invite_url": ..., "slug": ..., "name": ...}``. A
        signed-out caller gets 401 so the client can fall back to sharing the
        current page.
        """
        if (gate := _games_gate()) is not None:
            return gate
        user = await _resolve_user(request)
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
                "invite_url": _invite_url(request, league.slug),
                "slug": league.slug,
                "name": league.name,
            }
        )

    @app.get("/leagues", response_class=HTMLResponse)
    async def leagues_index(request: Request) -> Response:
        if (gate := _games_gate()) is not None:
            return gate
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
        if (gate := _games_gate()) is not None:
            return gate
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
        if (gate := _games_gate()) is not None:
            return gate
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
        if (gate := _games_gate()) is not None:
            return gate
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

    @app.get("/game/{slug}", response_class=HTMLResponse)
    async def game_detail(request: Request, slug: str) -> Response:
        """Friendlier alias for ``/league/{slug}``.

        Same behavior: non-members see the join page, members see the
        dashboard. Kept as a thin wrapper so existing /league/ links never
        break while shared "game" links read naturally.
        """
        return await league_detail(request, slug)

    @app.post("/league/{slug}/join")
    async def league_join(request: Request, slug: str) -> Response:
        if (gate := _games_gate()) is not None:
            return gate
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

    @app.post("/game/{slug}/join")
    async def game_join(request: Request, slug: str) -> Response:
        """Alias for ``/league/{slug}/join``."""
        return await league_join(request, slug)

    @app.post("/league/{slug}/leave")
    async def league_leave(request: Request, slug: str) -> Response:
        if (gate := _games_gate()) is not None:
            return gate
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
        if (gate := _games_gate()) is not None:
            return gate
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
        # Pre-show / pre-score, the snapshot table is empty so the cumulative
        # ``rows`` above is blank. Show every player at "0" instead of an empty
        # panel by listing members joined to their score for the upcoming show.
        # This is display-only; it never touches the snapshot rebuild.
        target_date = await _upcoming_show_date()
        member_scores = await list_members_with_scores(
            pool, league.id, target_date, limit=200
        )
        invite_url = _invite_url(request, league.slug)
        # Did the viewer arrive via this game link and already join? Members get
        # a direct "Make your picks" button; a brand-new visitor gets a
        # "Join & make your picks" CTA that routes through the game's join flow,
        # so picking + membership stay connected.
        viewer_is_member = user is not None and await is_member(
            pool, league.id, user.id
        )
        return _render(
            request,
            "league_leaderboard.html",
            current_user=user,
            league=league,
            member_count=count,
            rows=rows,
            user_row=user_row,
            member_scores=member_scores,
            invite_url=invite_url,
            upcoming_date=target_date,
            viewer_is_member=viewer_is_member,
        )

    @app.get("/game/{slug}/leaderboard", response_class=HTMLResponse)
    async def game_leaderboard_view(request: Request, slug: str) -> Response:
        """Alias for ``/league/{slug}/leaderboard`` — the shared scoreboard."""
        return await league_leaderboard_view(request, slug)

    async def _require_host(
        request: Request, slug: str
    ) -> tuple[Any, Any] | Response:
        """Resolve user + league + enforce host-only. Returns the pair on success
        or a Response on failure (use ``isinstance(..., Response)``).
        """
        if (gate := _games_gate()) is not None:
            return gate
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
            secure=cfg.cookie_secure,
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
            secure=cfg.cookie_secure,
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

    # ----- Phase 1: Google SSO + logout -------------------------------------

    def _oauth_error_page(
        request: Request,
        *,
        message: str,
        current: Any,
        code: int,
    ) -> Response:
        """Render the shared auth-error template for a failed Google sign-in."""
        resp = _render(
            request,
            "auth_verify_error.html",
            current_user=current,
            message=message,
            ttl_hours=cfg.magic_link_ttl_hours,
            signed_in=current is not None,
        )
        resp.status_code = code
        return resp

    @app.get("/auth/google/start")
    async def google_start(request: Request) -> Response:
        """Kick off the Google OAuth redirect (scope: openid email profile).

        Redirects home when Google SSO is not configured for this deployment.
        """
        if oauth is None:
            return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
        # Authlib stashes state + nonce in the (session-middleware) cookie and
        # returns the redirect to Google's consent screen.
        return await oauth.google.authorize_redirect(  # type: ignore[no-any-return]
            request, cfg.google_redirect_uri
        )

    @app.get("/auth/google/callback")
    async def google_callback(request: Request) -> Response:
        """Handle Google's redirect back: verify the id_token, resolve the
        account, set the session cookie, and land on /account.
        """
        if oauth is None:
            return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
        current = await _resolve_user(request)
        try:
            # Authlib verifies the id_token signature + claims (aud/iss/exp/
            # nonce) against Google's JWKS and returns the parsed OIDC claims
            # under token["userinfo"].
            token = await oauth.google.authorize_access_token(request)
        except OAuthError as exc:
            logger.warning("google oauth error", extra={"err": str(exc)})
            return _oauth_error_page(
                request,
                message="Google sign-in failed or was cancelled. Please try again.",
                current=current,
                code=status.HTTP_400_BAD_REQUEST,
            )
        userinfo = token.get("userinfo") or {}
        google_sub = str(userinfo.get("sub") or "")
        if not google_sub:
            return _oauth_error_page(
                request,
                message="Google did not return an account id. Please try again.",
                current=current,
                code=status.HTTP_400_BAD_REQUEST,
            )
        email = userinfo.get("email")
        email_verified = bool(userinfo.get("email_verified"))
        pool = get_pool()
        try:
            user_id = await resolve_google_identity(
                pool,
                google_sub=google_sub,
                email=email,
                email_verified=email_verified,
                current=current,
            )
        except GoogleLinkConflict as exc:
            return _oauth_error_page(
                request,
                message=str(exc),
                current=current,
                code=status.HTTP_409_CONFLICT,
            )
        resp: Response = RedirectResponse(
            "/account", status_code=status.HTTP_303_SEE_OTHER
        )
        _set_session_cookie(resp, user_id)
        resp.set_cookie(
            "phishgame_flash",
            "Signed in with Google.",
            max_age=30,
            httponly=True,
            samesite="lax",
            secure=cfg.cookie_secure,
        )
        return resp

    @app.post("/logout")
    async def logout(request: Request) -> Response:
        """Clear the session identity cookie and return home. Idempotent."""
        resp: Response = RedirectResponse(
            "/", status_code=status.HTTP_303_SEE_OTHER
        )
        resp.delete_cookie(
            COOKIE_NAME, samesite="lax", secure=cfg.cookie_secure
        )
        return resp

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
            secure=cfg.cookie_secure,
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
        async with pool.acquire() as conn:
            google_sub = await conn.fetchval(
                "SELECT google_sub FROM users WHERE id = $1", user.id
            )
        flash = request.cookies.get("phishgame_flash")
        resp = _render(
            request,
            "account.html",
            current_user=user,
            status=status_data,
            google_linked=google_sub is not None,
            flash=flash,
            leagues=memberships,
        )
        if flash:
            resp.delete_cookie("phishgame_flash")
        return resp

    # ----- blog (deployment-specific content, mounted at BLOG_DIR) ----------

    @app.get("/blog", response_class=HTMLResponse)
    async def blog_index(request: Request) -> HTMLResponse:
        """List published posts, newest first.

        Reads from the bind-mounted ``BLOG_DIR``. Empty/missing dir renders an
        empty list (no crash), and the nav link is already hidden in that case.
        """
        user = await _resolve_user(request)
        posts = load_posts(cfg.blog_dir)
        return _render(
            request,
            "blog_index.html",
            current_user=user,
            posts=posts,
        )

    @app.get("/blog/{slug}", response_class=HTMLResponse)
    async def blog_post(request: Request, slug: str) -> Response:
        """Render one post. The slug is validated against the known post files
        (``get_post`` returns None for anything not in BLOG_DIR), so there is
        no path traversal and unknown slugs 404.
        """
        user = await _resolve_user(request)
        post = get_post(cfg.blog_dir, slug)
        if post is None:
            resp = _render(
                request,
                "auth_verify_error.html",
                current_user=user,
                message="That post doesn't exist.",
                ttl_hours=cfg.magic_link_ttl_hours,
                signed_in=user is not None,
            )
            resp.status_code = status.HTTP_404_NOT_FOUND
            return resp
        return _render(
            request,
            "blog_post.html",
            current_user=user,
            post=post,
        )

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
