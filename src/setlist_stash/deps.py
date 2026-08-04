"""FastAPI dependency providers and shared render helpers.

The pre-2026-08 ``build_app()`` closed over ``cfg``, ``templates``,
``provider`` and ``oauth`` and every route was defined inside the closure.
The routes-only split moved every handler into a router module, so the shared
state now lives on ``app.state`` and is fetched here via ``Depends(...)``.

Behavior is identical: the same objects that used to be closed over are
attached to ``app.state`` in ``build_app`` and returned by these providers.
"""

from __future__ import annotations

from functools import partial
from typing import Any

from authlib.integrations.starlette_client import OAuth
from fastapi import Depends, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates

from setlist_stash import __version__
from setlist_stash.auth import (
    COOKIE_MAX_AGE_SECONDS,
    COOKIE_NAME,
    current_user,
    sign_user_id,
)
from setlist_stash.blog import load_posts
from setlist_stash.config import Settings
from setlist_stash.db import get_pool
from setlist_stash.email import EmailProvider
from setlist_stash.web_helpers import (
    TEMPLATES_DIR,
    _compute_asset_version,
    display_dt,
)


def build_templates(cfg: Settings, provider: EmailProvider) -> Jinja2Templates:
    """Configure the shared Jinja2 environment.

    Every jinja global here matches the pre-split ``build_app`` setup byte for
    byte — same names, same values, same derivation. Extracted only so
    ``server.py`` stays under the file-size ceiling. Provider is needed for
    the ``email_enabled`` flag templates guard the email UI on.
    """
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    # Single display-layer timezone conversion for every timestamp the site
    # renders. Storage and comparison stay UTC; templates call
    # ``{{ ts|display_dt }}`` and always get DISPLAY_TZ (Eastern by default,
    # DST-aware via ZoneInfo). No template formats a stored instant directly.
    templates.env.filters["display_dt"] = partial(display_dt, tz=cfg.display_tz)
    templates.env.globals["site_name"] = cfg.site_name
    # Social-preview blurb. Derived per deployment (see
    # Settings.site_description_effective) so the platform never hardcodes one
    # band's name into another deployment's og:description.
    templates.env.globals["site_description"] = cfg.site_description_effective
    templates.env.globals["theme_file"] = cfg.theme_file
    # Content hash of the CSS, appended as ``?v=`` to the static stylesheet
    # links so a styling change is a fresh URL at the Cloudflare edge.
    templates.env.globals["asset_version"] = _compute_asset_version(cfg.theme_file)
    templates.env.globals["footer_credit"] = cfg.footer_credit
    templates.env.globals["footer_credit_url"] = cfg.footer_credit_url
    templates.env.globals["data_source_name"] = cfg.data_source_name
    templates.env.globals["data_source_url"] = cfg.data_source_url
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
    # Whether the per-show comment thread renders + its routes are live. True
    # (the default) shows the read-open, handle-gated thread under each show's
    # predictions page. ENABLE_COMMENTS=false hides the section and 404s the
    # comment routes for a deployment that doesn't want threads.
    templates.env.globals["enable_comments"] = cfg.enable_comments
    # Whether to render the "Connect" (public MCP docs) nav link. True only
    # when a public MCP endpoint is configured for this deployment. Empty/unset
    # (the OSS image, the Phish demo) leaves the link off and the route serves a
    # graceful "no public MCP" panel (oss-platform-split).
    templates.env.globals["has_mcp"] = bool(cfg.mcp_public_url)
    templates.env.globals["mcp_public_url"] = cfg.mcp_public_url
    templates.env.globals["mcp_subject"] = cfg.mcp_subject
    # Suggested local MCP-client alias on /connect (the name in `claude mcp add`
    # and the Claude Desktop JSON key). Derived per tenant from mcp_subject when
    # MCP_ALIAS is unset, so tweezerpicks renders "phish" and wappypicks renders
    # "umphreys" with no per-deployment config.
    templates.env.globals["mcp_alias"] = cfg.mcp_alias_effective
    # Canonical base URL for absolute-URL meta tags (OG image, canonical link).
    # Stripped of trailing slash so templates write ``{{ canonical_base }}/.../x``.
    templates.env.globals["canonical_base"] = cfg.base_url.rstrip("/")
    return templates


def get_cfg(request: Request) -> Settings:
    """Return the app's Settings, attached to app.state at build time."""
    return request.app.state.cfg  # type: ignore[no-any-return]


def get_templates(request: Request) -> Jinja2Templates:
    """Return the shared Jinja2Templates instance."""
    return request.app.state.templates  # type: ignore[no-any-return]


def get_oauth(request: Request) -> OAuth | None:
    """Return the OAuth client if Google SSO is configured, else None."""
    oauth: OAuth | None = request.app.state.oauth
    return oauth


def get_email_provider(request: Request) -> EmailProvider:
    """Return the configured EmailProvider (log/smtp/disabled)."""
    return request.app.state.email_provider  # type: ignore[no-any-return]


async def get_current_user(
    request: Request,
    cfg: Settings = Depends(get_cfg),
) -> Any:
    """Return the signed-in user (or None) for the current request.

    Mirrors the pre-split ``_resolve_user`` helper: returns None when the
    pool isn't up so pages can still render without a session.
    """
    try:
        pool = get_pool()
    except RuntimeError:
        return None
    return await current_user(request, pool, cfg)


def render(
    templates: Jinja2Templates, request: Request, name: str, **ctx: Any
) -> HTMLResponse:
    """Render a template with the ``version`` default stamped in.

    The pre-split code called this ``_render`` inside ``build_app``; it is now
    a free function so any router can call it with the templates dep.
    """
    ctx.setdefault("version", __version__)
    return templates.TemplateResponse(request=request, name=name, context=ctx)


def set_session_cookie(resp: Response, user_id: int, cfg: Settings) -> None:
    """Sign a user id into the ``phishgame_session`` cookie."""
    resp.set_cookie(
        COOKIE_NAME,
        sign_user_id(cfg, user_id),
        max_age=COOKIE_MAX_AGE_SECONDS,
        httponly=True,
        samesite="lax",
        secure=cfg.cookie_secure,  # True on HTTPS deployments (COOKIE_SECURE)
    )
