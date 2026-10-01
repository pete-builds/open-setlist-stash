"""Unsubscribe + email preference routes.

Three entry points, and the split between them is the whole design:

``GET /email/unsubscribe``
    Renders a confirmation page. It does NOT unsubscribe anyone. Corporate
    mail gateways, link-safety scanners and some inbox previews fetch every
    URL in a message the moment it arrives; a GET that acted would silently
    unsubscribe people who never touched the link, and the first symptom
    would be "the reminders just stopped working for some players".

``POST /email/unsubscribe``
    Actually unsubscribes. Reached two ways: the button on that confirmation
    page, and the RFC 8058 one-click POST that Gmail and Apple Mail send from
    their own native unsubscribe control. Both carry the signed token, so
    neither needs a session. No CSRF token is required and none is checked:
    the mail client posts cross-origin by definition, and the worst a forged
    request can achieve is unsubscribing somebody from mail they can turn
    back on in one click while signed in.

``POST /account/email-prefs``
    The signed-in toggle on the account page, which is also the recovery path
    when a token no longer verifies (rotated ``SESSION_SECRET``).

Every route here is deliberately reachable with the email UI disabled. A
deployment can turn ``EMAIL_PROVIDER`` off while messages it already sent are
still sitting in inboxes, and those links must keep working.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Form, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from setlist_stash.config import Settings
from setlist_stash.db import get_pool
from setlist_stash.deps import get_cfg, get_current_user, get_templates, render
from setlist_stash.unsubscribe import read_unsubscribe_token, set_opt_out

logger = logging.getLogger("setlist_stash.routers.email_prefs")

router = APIRouter()


async def _handle_lookup(user_id: int) -> str | None:
    """The handle behind a token, for the "is this you?" line on the page."""
    try:
        pool = get_pool()
    except RuntimeError:
        return None
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            "SELECT handle FROM users WHERE id = $1", user_id
        )
    return str(value) if value is not None else None


@router.get("/email/unsubscribe", response_class=HTMLResponse)
async def unsubscribe_confirm(
    request: Request,
    t: str = Query("", description="Signed unsubscribe token from the email"),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    """Confirmation page. Reading this page unsubscribes nobody."""
    user_id = read_unsubscribe_token(cfg, t) if t else None
    handle = await _handle_lookup(user_id) if user_id is not None else None
    # A token that verifies but names a deleted user is treated as unreadable:
    # there is nothing to unsubscribe and the page should not claim otherwise.
    valid = user_id is not None and handle is not None
    return render(
        templates,
        request,
        "email_unsubscribe.html",
        token=t,
        handle=handle,
        token_valid=valid,
    )


@router.post("/email/unsubscribe")
async def unsubscribe_submit(
    request: Request,
    token: str = Form(""),
    t: str = Query(""),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    """Unsubscribe. Accepts the token from the form body or the query string.

    The form posts it in the body; an RFC 8058 one-click POST from a mail
    client reuses the URL as-is and so carries it in the query string. Reading
    both is what makes the same route serve the page's button and the mail
    client's native control.
    """
    raw = token or t

    def _bad_token() -> Response:
        resp = render(
            templates,
            request,
            "email_unsubscribe.html",
            token=raw,
            handle=None,
            token_valid=False,
            error="That unsubscribe link could not be read.",
        )
        resp.status_code = status.HTTP_400_BAD_REQUEST
        return resp

    user_id = read_unsubscribe_token(cfg, raw) if raw else None
    if user_id is None:
        return _bad_token()
    try:
        pool = get_pool()
    except RuntimeError:
        logger.error("unsubscribe attempted with no DB pool")
        return HTMLResponse(
            "Unsubscribe is temporarily unavailable. Please try again shortly.",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    known = await set_opt_out(pool, user_id, opted_out=True)
    if not known:
        return _bad_token()
    logger.info("player unsubscribed from reminders", extra={"user_id": user_id})
    return render(templates, request, "email_unsubscribed.html")


@router.post("/account/email-prefs")
async def account_email_prefs(
    request: Request,
    reminders: str = Form("off"),
    user: Any = Depends(get_current_user),
) -> Response:
    """Signed-in toggle for show-day reminders.

    Checkbox semantics: a checked box posts ``reminders=on`` and an unchecked
    one posts nothing at all, so the default here has to be "off" and the
    absence of the field is a real answer rather than a missing one.
    """
    _ = request
    if user is None:
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    pool = get_pool()
    wants_reminders = reminders.strip().lower() in {"on", "true", "1", "yes"}
    await set_opt_out(pool, user.id, opted_out=not wants_reminders)
    resp = RedirectResponse("/account", status_code=status.HTTP_303_SEE_OTHER)
    resp.set_cookie(
        "phishgame_flash",
        (
            "Show-day reminders are on."
            if wants_reminders
            else "Show-day reminders are off."
        ),
        max_age=30,
        httponly=True,
        samesite="lax",
    )
    return resp
