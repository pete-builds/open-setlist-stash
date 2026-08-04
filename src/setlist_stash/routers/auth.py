"""Identity routes: handle creation, Google SSO, magic-link email, account,
user profile.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from authlib.integrations.starlette_client import OAuth, OAuthError
from fastapi import APIRouter, Depends, Form, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from setlist_stash.auth import (
    COOKIE_NAME,
    HANDLE_HELP,
    HandleError,
    create_user,
    update_handle,
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
from setlist_stash.config import Settings
from setlist_stash.db import get_pool
from setlist_stash.deps import (
    get_cfg,
    get_current_user,
    get_email_provider,
    get_oauth,
    get_templates,
    render,
    set_session_cookie,
)
from setlist_stash.email import EmailProvider, EmailSendError
from setlist_stash.leagues import list_user_leagues
from setlist_stash.web_helpers import safe_next

router = APIRouter()
logger = logging.getLogger("setlist_stash.server")


def _oauth_error_page(
    templates: Jinja2Templates,
    request: Request,
    cfg: Settings,
    *,
    message: str,
    current: Any,
    code: int,
) -> Response:
    """Render the shared auth-error template for a failed Google sign-in."""
    resp = render(
        templates,
        request,
        "auth_verify_error.html",
        current_user=current,
        message=message,
        ttl_hours=cfg.magic_link_ttl_hours,
        signed_in=current is not None,
    )
    resp.status_code = code
    return resp


@router.post("/handle")
async def post_handle(
    request: Request,
    handle: str = Form(...),
    next: str = Form(""),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    try:
        canonical = validate_handle(handle)
    except HandleError as exc:
        return render(
            templates,
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
        return render(
            templates,
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
        url=safe_next(next), status_code=status.HTTP_303_SEE_OTHER
    )
    set_session_cookie(resp, user_id, cfg)
    logger.info("created handle", extra={"user_id": user_id})
    return resp


@router.get("/auth/google/start")
async def google_start(
    request: Request,
    cfg: Settings = Depends(get_cfg),
    oauth: OAuth | None = Depends(get_oauth),
) -> Response:
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


@router.get("/auth/google/callback")
async def google_callback(
    request: Request,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    oauth: OAuth | None = Depends(get_oauth),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    """Handle Google's redirect back: verify the id_token, resolve the
    account, set the session cookie, and land on /account.
    """
    if oauth is None:
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    current = user
    try:
        # Authlib verifies the id_token signature + claims (aud/iss/exp/
        # nonce) against Google's JWKS and returns the parsed OIDC claims
        # under token["userinfo"].
        token = await oauth.google.authorize_access_token(request)
    except OAuthError as exc:
        logger.warning("google oauth error", extra={"err": str(exc)})
        return _oauth_error_page(
            templates,
            request,
            cfg,
            message="Google sign-in failed or was cancelled. Please try again.",
            current=current,
            code=status.HTTP_400_BAD_REQUEST,
        )
    userinfo = token.get("userinfo") or {}
    google_sub = str(userinfo.get("sub") or "")
    if not google_sub:
        return _oauth_error_page(
            templates,
            request,
            cfg,
            message="Google did not return an account id. Please try again.",
            current=current,
            code=status.HTTP_400_BAD_REQUEST,
        )
    email = userinfo.get("email")
    email_verified = bool(userinfo.get("email_verified"))
    pool = get_pool()
    try:
        resolution = await resolve_google_identity(
            pool,
            google_sub=google_sub,
            email=email,
            email_verified=email_verified,
            current=current,
        )
    except GoogleLinkConflict as exc:
        return _oauth_error_page(
            templates,
            request,
            cfg,
            message=str(exc),
            current=current,
            code=status.HTTP_409_CONFLICT,
        )
    # A brand-new Google account gets an auto-generated PROVISIONAL handle.
    # Sign them in, then send them to the "choose your handle" step with the
    # suggestion pre-filled and editable — nobody is forced to keep the
    # placeholder. Existing accounts (link / returning / email-match) keep
    # their handle and go straight to /account.
    if resolution.is_new:
        resp = RedirectResponse(
            "/account/handle?new=1", status_code=status.HTTP_303_SEE_OTHER
        )
        set_session_cookie(resp, resolution.user_id, cfg)
        return resp
    resp = RedirectResponse(
        "/account", status_code=status.HTTP_303_SEE_OTHER
    )
    set_session_cookie(resp, resolution.user_id, cfg)
    resp.set_cookie(
        "phishgame_flash",
        "Signed in with Google.",
        max_age=30,
        httponly=True,
        samesite="lax",
        secure=cfg.cookie_secure,
    )
    return resp


@router.post("/logout")
async def logout(
    request: Request,
    cfg: Settings = Depends(get_cfg),
) -> Response:
    """Clear the session identity cookie and return home. Idempotent."""
    resp: Response = RedirectResponse(
        "/", status_code=status.HTTP_303_SEE_OTHER
    )
    resp.delete_cookie(
        COOKIE_NAME, samesite="lax", secure=cfg.cookie_secure
    )
    return resp


def _provider_enabled(provider: EmailProvider) -> bool:
    return provider.name != "disabled"


@router.get("/auth/email", response_class=HTMLResponse)
async def auth_email_form(
    request: Request,
    user: Any = Depends(get_current_user),
    provider: EmailProvider = Depends(get_email_provider),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    """Form to attach (or change) an email on a signed-in handle."""
    if user is None:
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    pool = get_pool()
    status_data = await get_email_status(pool, user.id)
    return render(
        templates,
        request,
        "auth_email.html",
        current_user=user,
        status=status_data,
        provider_enabled=_provider_enabled(provider),
    )


@router.post("/auth/email")
async def auth_email_submit(
    request: Request,
    email: str = Form(...),
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    provider: EmailProvider = Depends(get_email_provider),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    if user is None:
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    if not _provider_enabled(provider):
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
        resp = render(
            templates,
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
        resp = render(
            templates,
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
        resp = render(
            templates,
            request,
            "auth_email.html",
            current_user=user,
            status=status_data,
            provider_enabled=True,
            error="Could not send email right now. Try again shortly.",
        )
        resp.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return resp
    return render(
        templates,
        request,
        "auth_email_sent.html",
        current_user=user,
        masked_email=masked,
        ttl_hours=cfg.magic_link_ttl_hours,
        log_mode=(provider.name == "log"),
    )


@router.get("/auth/login", response_class=HTMLResponse)
async def auth_login_form(
    request: Request,
    user: Any = Depends(get_current_user),
    provider: EmailProvider = Depends(get_email_provider),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    """Cross-browser sign-in: enter your verified email, get a link."""
    # If already signed in, send to /account.
    if user is not None:
        return RedirectResponse(
            "/account", status_code=status.HTTP_303_SEE_OTHER
        )
    return render(
        templates,
        request,
        "auth_login.html",
        current_user=None,
        provider_enabled=_provider_enabled(provider),
    )


@router.post("/auth/login")
async def auth_login_submit(
    request: Request,
    email: str = Form(...),
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    provider: EmailProvider = Depends(get_email_provider),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    if user is not None:
        return RedirectResponse(
            "/account", status_code=status.HTTP_303_SEE_OTHER
        )
    if not _provider_enabled(provider):
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
        resp = render(
            templates,
            request,
            "auth_login.html",
            current_user=None,
            provider_enabled=True,
            error=str(exc),
        )
        resp.status_code = status.HTTP_400_BAD_REQUEST
        return resp
    except EmailSendError:
        resp = render(
            templates,
            request,
            "auth_login.html",
            current_user=None,
            provider_enabled=True,
            error="Could not send email right now. Try again shortly.",
        )
        resp.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return resp
    return render(
        templates,
        request,
        "auth_login_sent.html",
        current_user=None,
        masked_email=masked,
        ttl_hours=cfg.magic_link_ttl_hours,
        log_mode=(provider.name == "log"),
    )


@router.get("/auth/verify", response_class=HTMLResponse)
async def auth_verify(
    request: Request,
    token: str = Query("", min_length=0, max_length=512),
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
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

    already_signed_in = user is not None
    pool = get_pool()
    try:
        result = await verify_token(pool, token=token, ip=client_ip)
    except LookupError as exc:
        err_resp = render(
            templates,
            request,
            "auth_verify_error.html",
            current_user=user,
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
    set_session_cookie(resp, result.user_id, cfg)
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


@router.get("/account", response_class=HTMLResponse)
async def account_page(
    request: Request,
    user: Any = Depends(get_current_user),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    """Show handle + email status. Sign-in required."""
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
    resp = render(
        templates,
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


@router.get("/account/handle", response_class=HTMLResponse)
async def account_handle_form(
    request: Request,
    new: int = Query(0),
    user: Any = Depends(get_current_user),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    """Form to choose / change your handle. Sign-in required.

    ``?new=1`` (used right after a first-time Google sign-in) shows a
    welcome prompt with the auto-suggested handle pre-filled and editable,
    so nobody is stuck with the placeholder.
    """
    if user is None:
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    return render(
        templates,
        request,
        "account_handle.html",
        current_user=user,
        is_new=bool(new),
        handle_help=HANDLE_HELP,
    )


@router.post("/account/handle")
async def account_handle_submit(
    request: Request,
    handle: str = Form(...),
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    if user is None:
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    pool = get_pool()
    try:
        new_handle = await update_handle(pool, user.id, handle)
    except HandleError as exc:
        resp = render(
            templates,
            request,
            "account_handle.html",
            current_user=user,
            is_new=False,
            handle_help=HANDLE_HELP,
            error=str(exc),
            attempted=handle,
        )
        resp.status_code = status.HTTP_400_BAD_REQUEST
        return resp
    redirect: Response = RedirectResponse(
        "/account", status_code=status.HTTP_303_SEE_OTHER
    )
    redirect.set_cookie(
        "phishgame_flash",
        f"Handle updated to {new_handle}.",
        max_age=30,
        httponly=True,
        samesite="lax",
        secure=cfg.cookie_secure,
    )
    return redirect


@router.get("/u/{handle}", response_class=HTMLResponse)
async def user_profile(
    request: Request,
    handle: str,
    viewer: Any = Depends(get_current_user),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    """Public player profile: handle + their pick history across shows.

    Every name link on a leaderboard points here, so this must resolve
    (a missing route is the 404 source the leaderboards hit). Read-only
    and fair-play safe: picks for a show are only listed once that show is
    post-lock (the same rule the per-show predictions page enforces), so a
    profile can't leak a live entrant's strategy before lock.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        target = await conn.fetchrow(
            "SELECT id, handle FROM users WHERE lower(handle) = lower($1)",
            handle,
        )
        if target is None:
            resp = render(
                templates,
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
    return render(
        templates,
        request,
        "u_profile.html",
        current_user=viewer,
        profile_handle=target["handle"],
        found=True,
        history=history,
    )
