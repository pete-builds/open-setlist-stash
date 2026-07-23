"""Per-show comment threads: module + route/fragment contract tests.

Covers the approved design:
- add + list round-trips and joins the author handle at read time.
- validate_body enforces the 1..1000 length (matching the DB CHECK).
- soft-delete is author-only: the author hides their post, a non-author
  cannot touch it.
- Route surface: signed-in post works and returns the documented HTML
  fragment; an anonymous post is rejected (401); a read returns the joined
  handle; the fragment is a partial (no full-page chrome); the show page
  renders the comments section.

DB-backed; skipped unless ``TEST_PG_DSN`` is set (see conftest). Route tests
use ``httpx.AsyncClient`` + ``ASGITransport`` per conftest.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import asyncpg
import pytest
from httpx import AsyncClient

from setlist_stash.auth import sign_user_id
from setlist_stash.comments import (
    CommentError,
    add_comment,
    list_comments,
    soft_delete_comment,
    validate_body,
)
from setlist_stash.config import get_settings
from tests.conftest import requires_pg


def _cookie_for(client: AsyncClient, user_id: int) -> None:
    client.cookies.set("phishgame_session", sign_user_id(get_settings(), user_id))


async def _make_user(pool: asyncpg.Pool[Any], handle: str) -> int:
    async with pool.acquire() as conn:
        return int(
            await conn.fetchval(
                "INSERT INTO users (handle, handle_lower) VALUES ($1, $2) RETURNING id",
                handle,
                handle.lower(),
            )
        )


# ----- module unit tests ----------------------------------------------------


def test_validate_body_strips_and_bounds() -> None:
    assert validate_body("  hey now  ") == "hey now"
    with pytest.raises(CommentError):
        validate_body("   ")
    with pytest.raises(CommentError):
        validate_body("")
    with pytest.raises(CommentError):
        validate_body("x" * 1001)
    # Exactly 1000 is allowed.
    assert validate_body("y" * 1000) == "y" * 1000


@requires_pg
async def test_add_and_list_joins_handle(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    show_date = date(2030, 7, 1)
    uid = await _make_user(pg_pool, "commenter")
    cid = await add_comment(
        pg_pool, show_date=show_date, user_id=uid, body="great show"
    )
    assert cid > 0

    rows = await list_comments(pg_pool, show_date)
    assert len(rows) == 1
    assert rows[0].handle == "commenter"  # joined at read time, not denormalized
    assert rows[0].body == "great show"
    assert rows[0].user_id == uid


@requires_pg
async def test_list_is_oldest_first_and_scoped(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    show_a = date(2030, 7, 2)
    show_b = date(2030, 7, 3)
    uid = await _make_user(pg_pool, "poster")
    await add_comment(pg_pool, show_date=show_a, user_id=uid, body="first")
    await add_comment(pg_pool, show_date=show_a, user_id=uid, body="second")
    await add_comment(pg_pool, show_date=show_b, user_id=uid, body="other show")

    rows_a = await list_comments(pg_pool, show_a)
    assert [r.body for r in rows_a] == ["first", "second"]  # oldest first
    rows_b = await list_comments(pg_pool, show_b)
    assert [r.body for r in rows_b] == ["other show"]  # scoped by show_date


@requires_pg
async def test_soft_delete_author_hides_nonauthor_refused(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    show_date = date(2030, 7, 4)
    author = await _make_user(pg_pool, "author")
    other = await _make_user(pg_pool, "stranger")
    cid = await add_comment(
        pg_pool, show_date=show_date, user_id=author, body="mine to delete"
    )

    # Non-author cannot delete: returns None and the comment survives.
    assert await soft_delete_comment(pg_pool, cid, other) is None
    assert len(await list_comments(pg_pool, show_date)) == 1

    # Author deletes: returns the show_date and the comment disappears.
    assert await soft_delete_comment(pg_pool, cid, author) == show_date
    assert await list_comments(pg_pool, show_date) == []

    # Double-delete is a no-op (already soft-deleted).
    assert await soft_delete_comment(pg_pool, cid, author) is None


@requires_pg
async def test_add_comment_rejects_bad_body(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    show_date = date(2030, 7, 5)
    uid = await _make_user(pg_pool, "validator")
    with pytest.raises(CommentError):
        await add_comment(pg_pool, show_date=show_date, user_id=uid, body="   ")
    with pytest.raises(CommentError):
        await add_comment(
            pg_pool, show_date=show_date, user_id=uid, body="z" * 1001
        )
    assert await list_comments(pg_pool, show_date) == []


# ----- route / fragment contract tests --------------------------------------


@requires_pg
async def test_post_comment_signed_in_returns_fragment(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    """Documented contract: a signed-in POST returns 200 with the inner thread
    fragment — an HTML partial (no page chrome) containing the new comment in a
    ``.comment`` list item."""
    assert pg_pool is not None
    uid = await _make_user(pg_pool, "poster1")
    _cookie_for(async_client, uid)
    resp = await async_client.post(
        "/show/2030-08-01/comments", data={"body": "first post"}
    )
    assert resp.status_code == 200
    text = resp.text
    # Fragment shape: the comment body + a .comment item, no full-page chrome.
    assert "first post" in text
    assert 'class="comment"' in text
    assert "<html" not in text.lower()
    assert "<body" not in text.lower()
    # Persisted.
    rows = await list_comments(pg_pool, date(2030, 8, 1))
    assert [r.body for r in rows] == ["first post"]


@requires_pg
async def test_anonymous_post_rejected(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    assert pg_pool is not None
    async_client.cookies.clear()
    resp = await async_client.post(
        "/show/2030-08-02/comments", data={"body": "sneaky"}
    )
    assert resp.status_code == 401
    assert await list_comments(pg_pool, date(2030, 8, 2)) == []


@requires_pg
async def test_get_comments_fragment_has_handle(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    assert pg_pool is not None
    uid = await _make_user(pg_pool, "reader_target")
    await add_comment(
        pg_pool, show_date=date(2030, 8, 3), user_id=uid, body="hello thread"
    )
    resp = await async_client.get("/show/2030-08-03/comments")
    assert resp.status_code == 200
    text = resp.text
    assert "hello thread" in text
    assert "reader_target" in text  # joined handle rendered
    assert "<html" not in text.lower()


@requires_pg
async def test_post_body_length_validation(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    assert pg_pool is not None
    uid = await _make_user(pg_pool, "longwind")
    _cookie_for(async_client, uid)
    resp = await async_client.post(
        "/show/2030-08-04/comments", data={"body": "x" * 1001}
    )
    # Validation error re-renders the fragment inline (200, htmx swaps on 2xx).
    assert resp.status_code == 200
    assert "too long" in resp.text.lower()
    assert await list_comments(pg_pool, date(2030, 8, 4)) == []


@requires_pg
async def test_delete_route_author_and_non_author(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    assert pg_pool is not None
    alice = await _make_user(pg_pool, "alice_c")
    bob = await _make_user(pg_pool, "bob_c")
    cid = await add_comment(
        pg_pool, show_date=date(2030, 8, 5), user_id=alice, body="alice says hi"
    )

    # Bob (non-author) is refused.
    async_client.cookies.clear()
    _cookie_for(async_client, bob)
    refused = await async_client.post(f"/comment/{cid}/delete")
    assert refused.status_code == 403
    assert len(await list_comments(pg_pool, date(2030, 8, 5))) == 1

    # Alice (author) deletes; fragment comes back without her comment.
    async_client.cookies.clear()
    _cookie_for(async_client, alice)
    ok = await async_client.post(f"/comment/{cid}/delete")
    assert ok.status_code == 200
    assert "alice says hi" not in ok.text
    assert await list_comments(pg_pool, date(2030, 8, 5)) == []


@requires_pg
async def test_show_page_renders_comments_section(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    """The comments section is wired into the show predictions page."""
    assert pg_pool is not None
    # A future date with no lock row hits the fast no-lock branch (no mcp call).
    resp = await async_client.get("/show/2031-01-01/predictions")
    assert resp.status_code == 200
    text = resp.text
    assert 'id="comments-list"' in text  # the htmx poll target
    assert "hx-get=\"/show/2031-01-01/comments\"" in text
