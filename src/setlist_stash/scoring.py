"""Scoring formula for predictions.

Pure functions. No I/O. Inputs are plain values (slugs already parsed from
the published setlist) so this module is trivially unit-testable.

Fixed-points model:
- Each of the user's song picks (up to 5) that appears anywhere in the
  setlist earns ``SONG_POINTS``.
- A correct opener call earns ``OPENER_BONUS``; a correct closer call earns
  ``CLOSER_BONUS``; an encore call that appears anywhere in the encore earns
  ``ENCORE_BONUS``.

Pete approval (2026-05-03):
- Double-dip allowed: a slug used both as a song pick and a slot pick is
  scored independently in both buckets. A song that is one of your picks AND
  actually opens the show earns its pick points (played anywhere) AND the
  opener bonus — they stack. A bonus call that's the right song in the wrong
  spot still earns its pick points as one of your picks.
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
    opener: SlotBreakdown
    closer: SlotBreakdown
    encore: SlotBreakdown
    total: int


# Fixed point values.
SONG_POINTS = 2
OPENER_BONUS = 5
CLOSER_BONUS = 5
ENCORE_BONUS = 3


def score_prediction(
    *,
    pick_song_slugs: list[str],
    opener_slug: str | None,
    closer_slug: str | None,
    encore_slug: str | None,
    actual_opener: str | None,
    actual_closer: str | None,
    actual_encore_slugs: list[str],
    setlist_slugs: set[str],
) -> ScoreBreakdown:
    """Score a single prediction.

    Args:
        pick_song_slugs: the user's any-set picks (up to five).
        opener_slug / closer_slug / encore_slug: optional slot picks.
        actual_opener: slug of the song that actually opened set 1.
        actual_closer: slug of the last song before any encore in the
            final non-encore set.
        actual_encore_slugs: every slug in any encore set.
        setlist_slugs: union of every slug in the show (used to decide
            whether a song pick was "played").

    Returns:
        ``ScoreBreakdown`` with each pick worth ``SONG_POINTS`` if played,
        plus the opener/closer/encore bonuses.
    """
    picks: list[PickBreakdown] = []
    for slug in pick_song_slugs:
        played = slug in setlist_slugs
        points = SONG_POINTS if played else 0
        picks.append({"slug": slug, "played": played, "points": points})

    opener = _slot_breakdown(opener_slug, actual_opener, OPENER_BONUS)
    closer = _slot_breakdown(closer_slug, actual_closer, CLOSER_BONUS)
    encore = _slot_breakdown_multi(encore_slug, actual_encore_slugs, ENCORE_BONUS)

    total = (
        sum(p["points"] for p in picks)
        + opener["bonus"]
        + closer["bonus"]
        + encore["bonus"]
    )
    return {
        "picks": picks,
        "opener": opener,
        "closer": closer,
        "encore": encore,
        "total": total,
    }


def _slot_breakdown(
    pick: str | None,
    actual: str | None,
    bonus_amount: int,
) -> SlotBreakdown:
    """Slot bonus when the actual is a single slug."""
    if pick is None:
        return {"pick": None, "actual": actual, "bonus": 0}
    bonus = bonus_amount if pick == actual else 0
    return {"pick": pick, "actual": actual, "bonus": bonus}


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
