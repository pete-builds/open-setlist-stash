"""Unit tests for the blog loader/renderer (no DB needed).

Covers frontmatter parsing, fallbacks (H1 title, filename slug, mtime date),
table rendering, slug validation/traversal safety, and graceful handling of a
missing directory — the last is what keeps the Phish demo's empty blog from
crashing or showing a nav link.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from setlist_stash.blog import get_post, load_posts


def _write(tmp_path: Path, name: str, text: str, mtime: float | None = None) -> Path:
    p = tmp_path / name
    p.write_text(text, encoding="utf-8")
    if mtime is not None:
        os.utime(p, (mtime, mtime))
    return p


def test_missing_dir_returns_empty(tmp_path: Path) -> None:
    assert load_posts(tmp_path / "does-not-exist") == []
    assert get_post(tmp_path / "does-not-exist", "anything") is None


def test_empty_dir_returns_empty(tmp_path: Path) -> None:
    assert load_posts(tmp_path) == []


def test_frontmatter_and_table(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "post.md",
        "---\n"
        "title: My Title\n"
        "date: 2026-06-19\n"
        "summary: A short summary.\n"
        "slug: custom-slug\n"
        "---\n"
        "# Heading\n\n"
        "| Song | Gap |\n|---|---|\n| Mantis | 5 |\n\n"
        "See [the repo](https://example.com).\n",
    )
    posts = load_posts(tmp_path)
    assert len(posts) == 1
    post = posts[0]
    assert post.title == "My Title"
    assert post.slug == "custom-slug"
    assert post.date == date(2026, 6, 19)
    assert post.summary == "A short summary."
    # Markdown table renders as real HTML, not raw pipes.
    assert "<table>" in post.body_html
    assert "<td>Mantis</td>" in post.body_html
    assert "|---|" not in post.body_html
    # Links render as anchors.
    assert '<a href="https://example.com">the repo</a>' in post.body_html


def test_fallbacks_no_frontmatter(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "my-cool-post.md",
        "# Title From H1\n\nBody text here.\n",
        mtime=1_700_000_000.0,  # 2023-11-14 UTC
    )
    posts = load_posts(tmp_path)
    assert len(posts) == 1
    post = posts[0]
    assert post.title == "Title From H1"
    assert post.slug == "my-cool-post"  # filename stem
    assert post.date == date(2023, 11, 14)  # from mtime
    assert post.summary == ""


def test_newest_first_ordering(tmp_path: Path) -> None:
    _write(tmp_path, "a.md", "---\ndate: 2026-01-01\n---\n# A\n")
    _write(tmp_path, "b.md", "---\ndate: 2026-06-01\n---\n# B\n")
    posts = load_posts(tmp_path)
    assert [p.slug for p in posts] == ["b", "a"]


def test_get_post_known_and_unknown(tmp_path: Path) -> None:
    _write(tmp_path, "real.md", "---\ndate: 2026-06-01\n---\n# Real\n")
    assert get_post(tmp_path, "real") is not None
    assert get_post(tmp_path, "missing") is None


def test_get_post_rejects_traversal(tmp_path: Path) -> None:
    _write(tmp_path, "real.md", "---\ndate: 2026-06-01\n---\n# Real\n")
    # Path-traversal-ish slugs never match the validator and never touch disk.
    assert get_post(tmp_path, "../real") is None
    assert get_post(tmp_path, "../../etc/passwd") is None
    assert get_post(tmp_path, "real/..") is None


def test_bad_date_falls_back_to_mtime(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "post.md",
        "---\ndate: not-a-date\n---\n# Title\n",
        mtime=1_700_000_000.0,
    )
    posts = load_posts(tmp_path)
    assert posts[0].date == date(2023, 11, 14)
