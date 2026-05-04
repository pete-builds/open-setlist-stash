"""Scoring formula unit tests.

Predict it, score it, assert. Pure functions only — no fixtures, no DB.
"""

from __future__ import annotations

import math

import pytest

from setlist_stash.scoring import (
    CLOSER_BONUS,
    ENCORE_BONUS,
    OPENER_BONUS,
    base_score,
    score_prediction,
)


def test_base_score_zero_when_gap_zero() -> None:
    # log2(1+0) = 0; nothing is rare if it just played.
    assert base_score(gap_current=0, times_played=100) == 0


def test_base_score_uses_floor_of_20_for_played_count() -> None:
    # times_played < 20 caps the multiplier at 200/20 = 10.
    rare_song = base_score(gap_current=10, times_played=1)
    floor_song = base_score(gap_current=10, times_played=20)
    assert rare_song == floor_song
    assert rare_song == round(10 * math.log2(11) * 10)


def test_base_score_typical_song_lands_in_documented_range() -> None:
    # gap=20, times_played=200 -> 10 * log2(21) * 1.0 ≈ 43.9 -> round 44.
    s = base_score(gap_current=20, times_played=200)
    assert 5 <= s <= 80
    assert s == round(10 * math.log2(21) * 1.0)


def test_base_score_rejects_negatives() -> None:
    with pytest.raises(ValueError):
        base_score(gap_current=-1, times_played=10)
    with pytest.raises(ValueError):
        base_score(gap_current=10, times_played=-1)


def _meta(gap: int, played: int) -> dict[str, int]:
    return {"gap_current": gap, "times_played": played}


def test_score_prediction_three_picks_one_played() -> None:
    breakdown = score_prediction(
        pick_song_slugs=["fluffhead", "tweezer", "harry-hood"],
        opener_slug=None,
        closer_slug=None,
        encore_slug=None,
        actual_opener=None,
        actual_closer=None,
        actual_encore_slugs=[],
        setlist_slugs={"tweezer"},
        song_meta={"tweezer": _meta(gap=10, played=400)},
    )
    assert [p["played"] for p in breakdown["picks"]] == [False, True, False]
    assert breakdown["picks"][1]["base"] == base_score(gap_current=10, times_played=400)
    assert breakdown["opener"]["bonus"] == 0
    assert breakdown["total"] == breakdown["picks"][1]["base"]


def test_score_prediction_double_dip_bonus_stacks() -> None:
    """Pete approved (2026-05-03): double-dip allowed, bonuses stack.

    Tweezer in the bag AND as the opener: scores base for the bag pick
    AND base + opener bonus for the slot.
    """
    base = base_score(gap_current=10, times_played=400)
    breakdown = score_prediction(
        pick_song_slugs=["tweezer", "fluffhead", "harry-hood"],
        opener_slug="tweezer",
        closer_slug=None,
        encore_slug=None,
        actual_opener="tweezer",
        actual_closer=None,
        actual_encore_slugs=[],
        setlist_slugs={"tweezer"},
        song_meta={"tweezer": _meta(gap=10, played=400)},
    )
    bag_total = sum(p["base"] for p in breakdown["picks"])
    assert bag_total == base
    assert breakdown["opener"]["bonus"] == OPENER_BONUS
    assert breakdown["total"] == bag_total + OPENER_BONUS


def test_score_prediction_encore_matches_any_encore_song() -> None:
    breakdown = score_prediction(
        pick_song_slugs=["a", "b", "c"],
        opener_slug=None,
        closer_slug=None,
        encore_slug="loving-cup",
        actual_opener=None,
        actual_closer=None,
        actual_encore_slugs=["tweezer-reprise", "loving-cup"],
        setlist_slugs={"tweezer-reprise", "loving-cup"},
        song_meta={},
    )
    assert breakdown["encore"]["bonus"] == ENCORE_BONUS


def test_score_prediction_closer_wrong_no_bonus() -> None:
    breakdown = score_prediction(
        pick_song_slugs=["a", "b", "c"],
        opener_slug=None,
        closer_slug="harry-hood",
        encore_slug=None,
        actual_opener=None,
        actual_closer="slave",
        actual_encore_slugs=[],
        setlist_slugs={"slave"},
        song_meta={},
    )
    assert breakdown["closer"]["bonus"] == 0
    assert breakdown["closer"]["pick"] == "harry-hood"
    assert breakdown["closer"]["actual"] == "slave"


def test_score_prediction_full_breakdown_shape_matches_plan() -> None:
    """The plan promises a stable JSON shape for the scoring page."""
    breakdown = score_prediction(
        pick_song_slugs=["tweezer", "fluffhead", "harry-hood"],
        opener_slug="tweezer",
        closer_slug="harry-hood",
        encore_slug="loving-cup",
        actual_opener="tweezer",
        actual_closer="slave",
        actual_encore_slugs=["loving-cup"],
        setlist_slugs={"tweezer", "harry-hood", "loving-cup"},
        song_meta={
            "tweezer": _meta(10, 400),
            "harry-hood": _meta(5, 300),
            "loving-cup": _meta(40, 80),
        },
    )
    assert set(breakdown.keys()) == {"picks", "opener", "closer", "encore", "total"}
    assert len(breakdown["picks"]) == 3
    for pick in breakdown["picks"]:
        assert set(pick.keys()) == {"slug", "played", "base"}
    for slot in (breakdown["opener"], breakdown["closer"], breakdown["encore"]):
        assert set(slot.keys()) == {"pick", "actual", "bonus"}
    assert breakdown["total"] == (
        sum(p["base"] for p in breakdown["picks"])
        + breakdown["opener"]["bonus"]
        + breakdown["closer"]["bonus"]
        + breakdown["encore"]["bonus"]
    )


def test_score_prediction_no_setlist_for_picks_means_zero_base() -> None:
    breakdown = score_prediction(
        pick_song_slugs=["a", "b", "c"],
        opener_slug=None,
        closer_slug=None,
        encore_slug=None,
        actual_opener=None,
        actual_closer=None,
        actual_encore_slugs=[],
        setlist_slugs=set(),
        song_meta={},
    )
    assert all(p["base"] == 0 for p in breakdown["picks"])
    assert breakdown["total"] == 0


def test_slot_bonuses_independent_when_picks_correct() -> None:
    """All three slots correct, no bag-pick overlap: total = sum of bonuses."""
    breakdown = score_prediction(
        pick_song_slugs=["a", "b", "c"],
        opener_slug="o",
        closer_slug="cl",
        encore_slug="e",
        actual_opener="o",
        actual_closer="cl",
        actual_encore_slugs=["e"],
        setlist_slugs={"o", "cl", "e"},
        song_meta={},
    )
    assert breakdown["total"] == OPENER_BONUS + CLOSER_BONUS + ENCORE_BONUS
