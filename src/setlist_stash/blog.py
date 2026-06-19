"""Deployment-specific blog.

The blog *engine* (loading, parsing, rendering) lives in the shared OSS repo;
the *content* does not. Posts are markdown files read at runtime from a
configurable directory (``BLOG_DIR``) that each deployment bind-mounts in.
With nothing mounted (the Phish demo, any third-party self-host) the directory
is missing or empty, the loader returns no posts, and the nav "Blog" link
never renders. This mirrors the private-theme mount: branded/owned content
stays out of the public image.

Frontmatter is a small, dependency-free subset of YAML: a leading ``---``
fenced block of flat ``key: value`` lines. Supported keys: ``title``,
``date`` (ISO ``YYYY-MM-DD``), ``summary``, ``slug``. Anything missing falls
back to: title = first ``# H1`` in the body, slug = filename stem, date =
frontmatter date or the file's mtime.

Markdown is rendered with python-markdown (``tables`` + ``fenced_code``) so the
posts' tables and code fences render as real HTML. Content is operator-owned
and trusted; we still never read outside ``BLOG_DIR`` (slug is validated
against the known files, no path joining of user input).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import markdown

logger = logging.getLogger("setlist_stash.blog")

# Slugs are derived from filenames (or a frontmatter override) and used in the
# URL. Keep them to a safe, traversal-proof charset.
_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_H1_RE = re.compile(r"^#\s+(.+?)\s*$", re.MULTILINE)


@dataclass(frozen=True)
class BlogPost:
    """A single rendered blog post."""

    slug: str
    title: str
    date: date
    summary: str
    body_html: str


def _slugify(value: str) -> str:
    """Lowercase, dash-separated, traversal-proof slug from a string."""
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def _parse_frontmatter(text: str) -> tuple[dict[str, str], str]:
    """Split optional ``---`` frontmatter from the markdown body.

    Returns ``(meta, body)``. ``meta`` is a flat str->str map of the simple
    ``key: value`` lines; ``body`` is everything after the closing fence (or
    the whole text when no frontmatter is present).
    """
    if not text.startswith("---"):
        return {}, text
    # Match a leading fenced block: --- ... --- (newline-terminated fences).
    m = re.match(r"^---\s*\n(.*?)\n---\s*\n?(.*)$", text, re.DOTALL)
    if not m:
        return {}, text
    raw_meta, body = m.group(1), m.group(2)
    meta: dict[str, str] = {}
    for line in raw_meta.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, _, val = line.partition(":")
        meta[key.strip().lower()] = val.strip().strip("'\"")
    return meta, body


def _parse_date(raw: str | None, fallback_mtime: float) -> date:
    if raw:
        try:
            return date.fromisoformat(raw.strip())
        except ValueError:
            logger.warning("blog: bad frontmatter date %r; using mtime", raw)
    return datetime.fromtimestamp(fallback_mtime, tz=UTC).date()


def _render(md_body: str) -> str:
    return markdown.markdown(
        md_body,
        extensions=["tables", "fenced_code"],
        output_format="html",
    )


def _load_post_file(path: Path) -> BlogPost | None:
    """Parse + render one ``*.md`` file into a BlogPost, or None on error."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        logger.warning("blog: cannot read %s: %s", path.name, exc)
        return None

    meta, body = _parse_frontmatter(text)

    title = meta.get("title") or ""
    if not title:
        h1 = _H1_RE.search(body)
        title = h1.group(1).strip() if h1 else path.stem.replace("-", " ").title()

    slug = meta.get("slug") or path.stem
    slug = _slugify(slug)
    if not _SLUG_RE.match(slug):
        logger.warning("blog: skipping %s (unusable slug %r)", path.name, slug)
        return None

    try:
        mtime = path.stat().st_mtime
    except OSError:
        mtime = 0.0
    post_date = _parse_date(meta.get("date"), mtime)
    summary = meta.get("summary") or ""

    return BlogPost(
        slug=slug,
        title=title,
        date=post_date,
        summary=summary,
        body_html=_render(body),
    )


def load_posts(blog_dir: str | Path) -> list[BlogPost]:
    """Load all posts from ``blog_dir``, newest first.

    Robust to a missing/empty directory (returns ``[]``). Files that fail to
    parse are skipped with a warning rather than crashing the page. Duplicate
    slugs keep the first encountered (sorted by name for determinism) and log
    the collision.
    """
    base = Path(blog_dir)
    if not base.is_dir():
        return []
    posts: list[BlogPost] = []
    seen: set[str] = set()
    for path in sorted(base.glob("*.md")):
        post = _load_post_file(path)
        if post is None:
            continue
        if post.slug in seen:
            logger.warning("blog: duplicate slug %r in %s; skipping", post.slug, path.name)
            continue
        seen.add(post.slug)
        posts.append(post)
    posts.sort(key=lambda p: p.date, reverse=True)
    return posts


def get_post(blog_dir: str | Path, slug: str) -> BlogPost | None:
    """Return the single post matching ``slug``, or None if unknown.

    The slug is validated against the loaded set of posts; there is no path
    joining of caller input, so this cannot be coerced into reading a file
    outside ``blog_dir``.
    """
    if not _SLUG_RE.match(slug or ""):
        return None
    for post in load_posts(blog_dir):
        if post.slug == slug:
            return post
    return None
