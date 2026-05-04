"""Predictions read/write helpers.

Submissions:
    - Validate slugs (3 picks; opener/closer/encore optional).
    - Refuse if ``prediction_locks.lock_at`` has passed (DB trigger is the
      backstop, but we surface a clean error first).
    - Insert. The (user_id, show_date) UNIQUE constraint prevents
      double-submits.

Reads:
    - ``get_user_prediction(show_date, user_id)`` for the "you already
      submitted" view.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import asyncpg

logger = logging.getLogger("tweezer_picks.predictions")


class PredictionError(ValueError):
    """User-fixable validation failures (bad slugs, dup submit, etc.)."""


class PredictionLocked(PredictionError):
    """The show's lock_at has passed."""


class PredictionDuplicate(PredictionError):
    """A prediction already exists for (user, show)."""


@dataclass(frozen=True)
class PredictionRow:
    id: int
    show_date: date
    pick_song_slugs: list[str]
    opener_slug: str | None
    closer_slug: str | None
    encore_slug: str | None
    submitted_at: datetime
    score: int | None


def normalize_picks(raw_picks: list[str]) -> list[str]:
    """Strip + dedupe + sort the bag-pick slugs, enforce cardinality 3."""
    cleaned = [p.strip().lower() for p in raw_picks if p and p.strip()]
    if len(cleaned) != 3:
        raise PredictionError("Pick exactly three songs.")
    if len(set(cleaned)) != 3:
        raise PredictionError("Your three picks must be different songs.")
    return sorted(cleaned)


def normalize_slot(slug: str | None) -> str | None:
    if slug is None:
        return None
    s = slug.strip().lower()
    return s or None


async def insert_prediction(
    pool: asyncpg.Pool[Any],
    *,
    user_id: int,
    show_date: date,
    pick_song_slugs: list[str],
    opener_slug: str | None,
    closer_slug: str | None,
    encore_slug: str | None,
) -> int:
    """Insert a prediction. Raises PredictionLocked / PredictionDuplicate."""
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO predictions (
                    user_id, show_date, pick_song_slugs,
                    opener_slug, closer_slug, encore_slug
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id
                """,
                user_id,
                show_date,
                pick_song_slugs,
                opener_slug,
                closer_slug,
                encore_slug,
            )
        except asyncpg.UniqueViolationError as exc:
            raise PredictionDuplicate(
                "You already submitted a prediction for this show."
            ) from exc
        except asyncpg.CheckViolationError as exc:
            # The lock-guard trigger raises with ERRCODE 'check_violation'.
            msg = str(exc).lower()
            if "is locked" in msg or "lock" in msg:
                raise PredictionLocked(
                    "Predictions are locked for this show."
                ) from exc
            raise PredictionError(f"Validation failed: {exc}") from exc
    if row is None:
        raise PredictionError("Insert returned no row.")
    return int(row["id"])


async def get_user_prediction(
    pool: asyncpg.Pool[Any], user_id: int, show_date: date
) -> PredictionRow | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, show_date, pick_song_slugs, opener_slug, closer_slug,
                   encore_slug, submitted_at, score
            FROM predictions
            WHERE user_id = $1 AND show_date = $2
            """,
            user_id,
            show_date,
        )
    if row is None:
        return None
    return PredictionRow(
        id=int(row["id"]),
        show_date=row["show_date"],
        pick_song_slugs=list(row["pick_song_slugs"]),
        opener_slug=row["opener_slug"],
        closer_slug=row["closer_slug"],
        encore_slug=row["encore_slug"],
        submitted_at=row["submitted_at"],
        score=row["score"],
    )
