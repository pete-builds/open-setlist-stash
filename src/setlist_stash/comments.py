"""Per-show comment threads (read/write helpers).

A comment thread is scoped to a ``show_date`` and is GLOBAL to the deployment:
one thread per show, shared by everyone (not per-league, not per-user). This is
a persisted, poll-refreshed thread, NOT a chatroom, so there are no websockets
and no presence.

Access model:
    - Reads are open to anyone (signed in or not).
    - Posting is gated on having a handle (``users`` row), the same identity
      gate the predict form uses. The route layer owns that gate; this module
      just writes what it's handed.

Soft delete:
    - ``soft_delete_comment`` only stamps ``deleted_at`` when the requesting
      user is the author, so a caller can never delete another player's post.
    - ``list_comments`` filters out soft-deleted rows, so a deleted comment
      simply disappears from the thread.

The author handle is joined from ``users`` at read time (never denormalized
onto the comment row), so a handle rename is reflected everywhere immediately.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import asyncpg

logger = logging.getLogger("setlist_stash.comments")

# Matches the DB CHECK (char_length(body) BETWEEN 1 AND 1000).
BODY_MAX_LEN = 1000


class CommentError(ValueError):
    """User-fixable validation failure (empty / too-long body)."""


@dataclass(frozen=True)
class CommentRow:
    id: int
    show_date: date
    user_id: int
    handle: str
    body: str
    created_at: datetime


def validate_body(raw: str) -> str:
    """Return the trimmed comment body or raise ``CommentError``.

    Mirrors ``auth.validate_handle``: strip, then enforce the 1..1000 length
    the DB CHECK also guards. The app-level check gives a clean error before
    the DB rejects it.
    """
    body = (raw or "").strip()
    if not body:
        raise CommentError("Comment cannot be empty.")
    if len(body) > BODY_MAX_LEN:
        raise CommentError(
            f"Comment is too long (max {BODY_MAX_LEN} characters)."
        )
    return body


async def add_comment(
    pool: asyncpg.Pool[Any],
    *,
    show_date: date,
    user_id: int,
    body: str,
) -> int:
    """Insert a comment. Caller must have validated + resolved the user.

    ``body`` is validated here too (belt-and-suspenders); a ``CommentError``
    surfaces a clean message and the DB CHECK is the final backstop.
    """
    clean = validate_body(body)
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO comments (show_date, user_id, body)
                VALUES ($1, $2, $3)
                RETURNING id
                """,
                show_date,
                user_id,
                clean,
            )
        except asyncpg.CheckViolationError as exc:
            # The comments_body_length CHECK is the DB backstop.
            raise CommentError("Comment failed length validation.") from exc
    if row is None:
        raise CommentError("Insert returned no row.")
    logger.info(
        "comment added",
        extra={"show_date": str(show_date), "user_id": user_id},
    )
    return int(row["id"])


async def list_comments(
    pool: asyncpg.Pool[Any], show_date: date, *, limit: int = 100
) -> list[CommentRow]:
    """Return a show's live comments, oldest first (thread order).

    Joins ``users.handle`` at read time (never denormalized). Soft-deleted
    rows are excluded. We fetch the newest ``limit`` rows (so a long thread
    keeps the most recent activity) and return them ascending so the thread
    reads top-to-bottom.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT c.id, c.show_date, c.user_id, u.handle, c.body, c.created_at
              FROM comments c
              JOIN users u ON u.id = c.user_id
             WHERE c.show_date = $1
               AND c.deleted_at IS NULL
             ORDER BY c.created_at DESC, c.id DESC
             LIMIT $2
            """,
            show_date,
            limit,
        )
    # Fetched newest-first for the LIMIT window; flip to oldest-first for display.
    return [
        CommentRow(
            id=int(r["id"]),
            show_date=r["show_date"],
            user_id=int(r["user_id"]),
            handle=str(r["handle"]),
            body=str(r["body"]),
            created_at=r["created_at"],
        )
        for r in reversed(rows)
    ]


async def soft_delete_comment(
    pool: asyncpg.Pool[Any], comment_id: int, requesting_user_id: int
) -> date | None:
    """Soft-delete a comment, author-only.

    Stamps ``deleted_at`` only when the row exists, is still live, and belongs
    to ``requesting_user_id``. Returns the comment's ``show_date`` on success
    (so the route can re-render that thread), or ``None`` when nothing was
    deleted (missing, already deleted, or not the author). A non-author can
    never delete another player's post.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE comments
               SET deleted_at = now()
             WHERE id = $1
               AND user_id = $2
               AND deleted_at IS NULL
            RETURNING show_date
            """,
            comment_id,
            requesting_user_id,
        )
    if row is None:
        return None
    logger.info(
        "comment soft-deleted",
        extra={"comment_id": comment_id, "user_id": requesting_user_id},
    )
    show_date: date = row["show_date"]
    return show_date
