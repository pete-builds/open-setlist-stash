"""Scoring formula for predictions.

Pure functions. No I/O. Inputs are plain dicts (already fetched from
mcp-phish) so this module is trivially unit-testable.

See ``PHASE-4-PLAN.md`` §3 for the canonical formula.

Pete approval (2026-05-03):
- Double-dip allowed: a slug used both as a bag pick and a slot pick is
  scored independently in both buckets. Bonuses stack.
- Cancelled shows: scoring not invoked; the resolver writes ``resolved_at``
  with ``summary={"cancelled": true}`` and zeros every score. Handled at
  the resolver layer, not here.
"""

from __future__ import annotations

import math
from typing import Any, TypedDict


class PickBreakdown(TypedDict):
    slug: str
    played: bool
    base: int


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


# Slot bonuses (PHASE-4-PLAN.md §3).
OPENER_BONUS = 25
CLOSER_BONUS = 25
ENCORE_BONUS = 30


def base_score(gap_current: int, times_played: int) -> int:
    """Per-pick base score (rarity points).

    ``base = round(10 * log2(1 + gap_current) * (200 / max(20, times_played)))``

    A pick that wasn't played in the show should be passed gap=0 and the
    caller treats it as zero — but easier: callers just don't invoke this
    and store base=0 directly. We still defend with a non-negative guard.
    """
    if gap_current < 0 or times_played < 0:
        raise ValueError("gap_current and times_played must be non-negative")
    rarity = math.log2(1 + gap_current)
    multiplier = 200 / max(20, times_played)
    return round(10 * rarity * multiplier)


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
    song_meta: dict[str, dict[str, Any]],
) -> ScoreBreakdown:
    """Score a single prediction.

    Args:
        pick_song_slugs: the user's three any-set picks.
        opener_slug / closer_slug / encore_slug: optional slot picks.
        actual_opener: slug of the song that actually opened set 1.
        actual_closer: slug of the last song before any encore in the
            final non-encore set.
        actual_encore_slugs: every slug in any encore set.
        setlist_slugs: union of every slug in the show (used to decide
            whether a bag pick was "played").
        song_meta: slug -> {"gap_current": int, "times_played": int}.
            Required for every slug that was played; ignored for slugs
            that weren't (their base is 0).

    Returns:
        ``ScoreBreakdown`` matching ``PHASE-4-PLAN.md`` §3.
    """
    picks: list[PickBreakdown] = []
    for slug in pick_song_slugs:
        played = slug in setlist_slugs
        if played:
            meta = song_meta[slug]
            base = base_score(
                gap_current=int(meta["gap_current"]),
                times_played=int(meta["times_played"]),
            )
        else:
            base = 0
        picks.append({"slug": slug, "played": played, "base": base})

    opener = _slot_breakdown(opener_slug, actual_opener, OPENER_BONUS, single=True)
    closer = _slot_breakdown(closer_slug, actual_closer, CLOSER_BONUS, single=True)
    encore = _slot_breakdown_multi(encore_slug, actual_encore_slugs, ENCORE_BONUS)

    total = (
        sum(p["base"] for p in picks)
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
    *,
    single: bool,
) -> SlotBreakdown:
    """Slot bonus when the actual is a single slug."""
    _ = single  # kept for clarity at call sites
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
