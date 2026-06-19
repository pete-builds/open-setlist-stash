"""Scoring formula for predictions.

Pure functions. No I/O. Inputs are plain values (slugs already parsed from
the published setlist) so this module is trivially unit-testable.

Fixed-points model:
- Each of the user's song picks (up to 5) that appears anywhere in the
  setlist earns ``SONG_POINTS``.
- The user tags one of their picks as the encore call. If that song appears
  anywhere in the encore, it earns ``ENCORE_BONUS``.

Pete approval (2026-05-03, updated 2026-06-18):
- Double-dip allowed: the encore call is scored independently of the pick
  bucket. A song that is one of your picks AND actually lands in the encore
  earns its pick points (played anywhere, 2) AND the encore bonus (5) —
  they stack. An encore call that's played but lands outside the encore
  still earns its 2 as one of your picks.
- Cancelled shows: scoring not invoked; the resolver writes ``resolved_at``
  with ``summary={"cancelled": true}`` and zeros every score. Handled at
  the resolver layer, not here.
"""

from __future__ import annotations

from typing import TypedDict


class PickBreakdown(TypedDict):
    slug: str
    played: bool
    points: int


class SlotBreakdown(TypedDict):
    pick: str | None
    actual: str | None
    bonus: int


class ScoreBreakdown(TypedDict):
    picks: list[PickBreakdown]
    encore: SlotBreakdown
    total: int


# Fixed point values.
SONG_POINTS = 2
ENCORE_BONUS = 5


def score_prediction(
    *,
    pick_song_slugs: list[str],
    encore_slug: str | None,
    actual_encore_slugs: list[str],
    setlist_slugs: set[str],
) -> ScoreBreakdown:
    """Score a single prediction.

    Args:
        pick_song_slugs: the user's any-set picks (up to five).
        encore_slug: the pick the user tagged as their encore call.
        actual_encore_slugs: every slug in any encore set.
        setlist_slugs: union of every slug in the show (used to decide
            whether a song pick was "played").

    Returns:
        ``ScoreBreakdown`` with each pick worth ``SONG_POINTS`` if played,
        plus the encore bonus.
    """
    picks: list[PickBreakdown] = []
    for slug in pick_song_slugs:
        played = slug in setlist_slugs
        points = SONG_POINTS if played else 0
        picks.append({"slug": slug, "played": played, "points": points})

    encore = _slot_breakdown_multi(encore_slug, actual_encore_slugs, ENCORE_BONUS)

    total = sum(p["points"] for p in picks) + encore["bonus"]
    return {
        "picks": picks,
        "encore": encore,
        "total": total,
    }


def _slot_breakdown_multi(
    pick: str | None,
    actuals: list[str],
    bonus_amount: int,
) -> SlotBreakdown:
    """Encore bonus: any slug in any encore set wins."""
    if pick is None:
        actual_repr = ",".join(actuals) if actuals else None
        return {"pick": None, "actual": actual_repr, "bonus": 0}
    bonus = bonus_amount if pick in actuals else 0
    actual_repr = ",".join(actuals) if actuals else None
    return {"pick": pick, "actual": actual_repr, "bonus": bonus}
