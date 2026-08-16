"""FastAPI app entrypoint.

Every route lives in a domain-scoped ``APIRouter`` under
``setlist_stash.routers.*``; this module only wires them together:
settings + templates + oauth setup, middleware install, mcp reverse-proxy
mount, ``include_router`` for each domain, lifespan.

Pre-2026-08 this file was 2,878 lines of closure-scoped handlers. The split
moved every handler into ``routers/{auth,leagues,predictions,comments,blog,
pages}.py`` and the shared state (settings, templates, oauth, email provider)
onto ``app.state`` where ``deps.py`` injects it. Behavior is byte-identical.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from authlib.integrations.starlette_client import OAuth
from fastapi import FastAPI
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from setlist_stash import __version__
from setlist_stash.config import Settings, get_settings
from setlist_stash.db import close_pool, init_pool
from setlist_stash.deps import build_templates
from setlist_stash.email import EmailProvider, build_provider
from setlist_stash.logging_setup import configure_logging
from setlist_stash.mcp_proxy import FixedWindowRateLimiter, McpReverseProxy
from setlist_stash.middleware import (
    install_mcp_rate_limit,
    install_mcp_reverse_proxy,
    install_security_headers,
)
from setlist_stash.migrate import run_migrations
from setlist_stash.routers import (
    auth as auth_router,
)
from setlist_stash.routers import (
    blog as blog_router,
)
from setlist_stash.routers import (
    comments as comments_router,
)
from setlist_stash.routers import (
    leagues as leagues_router,
)
from setlist_stash.routers import (
    pages as pages_router,
)
from setlist_stash.routers import (
    predictions as predictions_router,
)
from setlist_stash.security_headers import build_security_headers
from setlist_stash.web_helpers import STATIC_DIR as _STATIC_DIR

__all__ = ["app", "build_app", "main"]

logger = logging.getLogger("setlist_stash.server")


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
    templates = build_templates(cfg, provider)

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

    # Shared state the routers reach via ``deps.get_cfg`` / ``get_templates``
    # / ``get_oauth`` / ``get_email_provider``. Pre-split these were closed
    # over inside ``build_app``.
    app.state.cfg = cfg
    app.state.templates = templates
    app.state.oauth = oauth
    app.state.email_provider = provider
    app.state.mcp_proxy = mcp_proxy

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

    # Order matters: install_security_headers registers LAST so it runs
    # OUTERMOST (Starlette applies http middleware in reverse registration
    # order). Do not reorder without re-reading middleware.py.
    install_mcp_rate_limit(app, mcp_rate_limiter)
    install_security_headers(
        app, build_security_headers(cfg) if cfg.security_headers else {}
    )

    if _STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    # Root /favicon.ico. The <link> tags in base.html cover modern browsers,
    # but the bare /favicon.ico path is requested regardless of markup by
    # crawlers, link unfurlers, RSS readers and browser chrome outside the
    # tab (bookmarks, history, the new-tab grid). Serving 404 there is what
    # makes a correctly-branded page still fall back to the generic globe in
    # those surfaces. Resolves per deployment through the same FAVICON_*
    # config as the link tags, so no tenant artwork is named here.
    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon() -> Response:
        for name in (cfg.favicon_ico, cfg.favicon_png, cfg.favicon_svg):
            if not name:
                continue
            # Guard against a config value escaping the static dir.
            candidate = (_STATIC_DIR / name).resolve()
            if not candidate.is_relative_to(_STATIC_DIR.resolve()):
                continue
            if candidate.is_file():
                return FileResponse(candidate)
        return Response(status_code=404)

    if mcp_proxy is not None:
        install_mcp_reverse_proxy(app, mcp_proxy)

    # Route domains, in the order the pre-split file defined them. Include
    # order does not affect matching (FastAPI dispatches by path + verb), but
    # keeping it stable keeps the OpenAPI schema and startup logs unchanged.
    app.include_router(pages_router.router)         # / and public read pages
    app.include_router(auth_router.router)          # /handle, /auth/*, /account/*, /u/*
    app.include_router(predictions_router.router)   # /predict, /songs/search, /show/*
    app.include_router(comments_router.router)      # /show/*/comments, /comment/*
    app.include_router(leagues_router.router)       # /leagues, /league/*, /game/*
    app.include_router(blog_router.router)          # /blog, /blog/*

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
