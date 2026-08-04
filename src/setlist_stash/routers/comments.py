"""Per-show comment thread routes.

Gated by ``settings.enable_comments`` — a deployment that turns comments off
returns 404 for every route in this module (see ``_comments_gate``).
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, Form, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates

from setlist_stash.comments import (
    CommentError,
    add_comment,
    list_comments,
    soft_delete_comment,
)
from setlist_stash.config import Settings
from setlist_stash.db import get_pool
from setlist_stash.deps import get_cfg, get_current_user, get_templates, render

router = APIRouter()


def _comments_gate(cfg: Settings) -> Response | None:
    """Return a 404 when comments are disabled for this deployment.

    Mirrors ``_games_gate``: an off deployment exposes no comment surface,
    even by direct URL. None when enabled so the route runs normally.
    """
    if cfg.enable_comments:
        return None
    return HTMLResponse("Not found", status_code=status.HTTP_404_NOT_FOUND)


def _comments_fragment(
    templates: Jinja2Templates,
    request: Request,
    user: Any,
    comments: Any,
    *,
    error: str | None = None,
) -> HTMLResponse:
    """Render the inner comment-list fragment htmx swaps into #comments-list."""
    return render(
        templates,
        request,
        "_comments_list.html",
        current_user=user,
        comments=comments,
        comment_error=error,
    )


@router.get("/show/{show_date}/comments", response_class=HTMLResponse)
async def get_comments(
    request: Request,
    show_date: date,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    """The fragment htmx polls (hx-get, every 12s). Read-open to anyone."""
    if (gate := _comments_gate(cfg)) is not None:
        return gate
    pool = get_pool()
    comments = await list_comments(pool, show_date)
    return _comments_fragment(templates, request, user, comments)


@router.post("/show/{show_date}/comments")
async def post_comment(
    request: Request,
    show_date: date,
    body: str = Form(...),
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    """Post a comment. Gated on having a handle (same gate as picks).

    On success returns the refreshed inner list fragment (200) so the form's
    hx-swap replaces #comments-list with the thread including the new post.
    A validation failure re-renders the SAME fragment with an inline error
    at 200 (htmx swaps only on 2xx, so the message lands in the thread). An
    anonymous caller gets 401 — the post form is hidden for them anyway.
    """
    if (gate := _comments_gate(cfg)) is not None:
        return gate
    pool = get_pool()
    if user is None:
        return JSONResponse(
            {"error": "Pick a handle first."},
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
    try:
        await add_comment(
            pool, show_date=show_date, user_id=user.id, body=body
        )
    except CommentError as exc:
        comments = await list_comments(pool, show_date)
        return _comments_fragment(
            templates, request, user, comments, error=str(exc)
        )
    comments = await list_comments(pool, show_date)
    return _comments_fragment(templates, request, user, comments)


@router.post("/comment/{comment_id}/delete")
async def delete_comment(
    request: Request,
    comment_id: int,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    """Soft-delete a comment, author-only.

    Returns the refreshed thread fragment on success. A non-author (or a
    missing/already-deleted id) gets 403 without revealing which — the
    author check lives in ``soft_delete_comment`` and returns the show_date
    only when the requester actually owned the row.
    """
    if (gate := _comments_gate(cfg)) is not None:
        return gate
    pool = get_pool()
    if user is None:
        return JSONResponse(
            {"error": "Pick a handle first."},
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
    deleted_show_date = await soft_delete_comment(pool, comment_id, user.id)
    if deleted_show_date is None:
        return HTMLResponse(
            "Forbidden", status_code=status.HTTP_403_FORBIDDEN
        )
    comments = await list_comments(pool, deleted_show_date)
    return _comments_fragment(templates, request, user, comments)
