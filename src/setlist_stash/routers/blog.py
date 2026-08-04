"""Blog routes. Content is bind-mounted at ``BLOG_DIR``."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates

from setlist_stash.blog import get_post, load_posts
from setlist_stash.config import Settings
from setlist_stash.deps import get_cfg, get_current_user, get_templates, render

router = APIRouter()


@router.get("/blog", response_class=HTMLResponse)
async def blog_index(
    request: Request,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    """List published posts, newest first.

    Reads from the bind-mounted ``BLOG_DIR``. Empty/missing dir renders an
    empty list (no crash), and the nav link is already hidden in that case.
    """
    posts = load_posts(cfg.blog_dir)
    return render(
        templates,
        request,
        "blog_index.html",
        current_user=user,
        posts=posts,
    )


@router.get("/blog/{slug}", response_class=HTMLResponse)
async def blog_post(
    request: Request,
    slug: str,
    user: Any = Depends(get_current_user),
    cfg: Settings = Depends(get_cfg),
    templates: Jinja2Templates = Depends(get_templates),
) -> Response:
    """Render one post. The slug is validated against the known post files
    (``get_post`` returns None for anything not in BLOG_DIR), so there is
    no path traversal and unknown slugs 404.
    """
    post = get_post(cfg.blog_dir, slug)
    if post is None:
        resp = render(
            templates,
            request,
            "auth_verify_error.html",
            current_user=user,
            message="That post doesn't exist.",
            ttl_hours=cfg.magic_link_ttl_hours,
            signed_in=user is not None,
        )
        resp.status_code = status.HTTP_404_NOT_FOUND
        return resp
    return render(
        templates,
        request,
        "blog_post.html",
        current_user=user,
        post=post,
    )
