"""Scoring formula unit tests.

Predict it, score it, assert. Pure functions only — no fixtures, no DB.

Fixed-points model: each played pick is worth ``SONG_POINTS`` (2); a correct
opener/closer call adds ``OPENER_BONUS``/``CLOSER_BONUS`` (5 each); a correct
encore call adds ``ENCORE_BONUS`` (3).
"""

from __future__ import annotations

from setlist_stash.scoring import (
    CLOSER_BONUS,
    ENCORE_BONUS,
    OPENER_BONUS,
    SONG_POINTS,
    score_prediction,
)


def test_constants_are_the_fixed_model() -> None:
    assert SONG_POINTS == 2
    assert OPENER_BONUS == 5
    assert CLOSER_BONUS == 5
    assert ENCORE_BONUS == 3


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
    )
    assert [p["played"] for p in breakdown["picks"]] == [False, True, False]
    assert breakdown["picks"][1]["points"] == SONG_POINTS
    assert breakdown["picks"][0]["points"] == 0
    assert breakdown["opener"]["bonus"] == 0
    assert breakdown["total"] == SONG_POINTS


def test_score_prediction_three_played_picks_is_six() -> None:
    breakdown = score_prediction(
        pick_song_slugs=["a", "b", "c"],
        opener_slug=None,
        closer_slug=None,
        encore_slug=None,
        actual_opener=None,
        actual_closer=None,
        actual_encore_slugs=[],
        setlist_slugs={"a", "b", "c"},
    )
    assert all(p["points"] == SONG_POINTS for p in breakdown["picks"])
    assert breakdown["total"] == 6


def test_score_prediction_five_played_picks_is_ten() -> None:
    breakdown = score_prediction(
        pick_song_slugs=["a", "b", "c", "d", "e"],
        opener_slug=None,
        closer_slug=None,
        encore_slug=None,
        actual_opener=None,
        actual_closer=None,
        actual_encore_slugs=[],
        setlist_slugs={"a", "b", "c", "d", "e"},
    )
    assert breakdown["total"] == 10


def test_score_prediction_correct_opener_adds_five() -> None:
    breakdown = score_prediction(
        pick_song_slugs=["a"],
        opener_slug="o",
        closer_slug=None,
        encore_slug=None,
        actual_opener="o",
        actual_closer=None,
        actual_encore_slugs=[],
        setlist_slugs={"o"},  # note: pick "a" not played -> 0 pick points
    )
    assert breakdown["opener"]["bonus"] == OPENER_BONUS
    assert breakdown["total"] == OPENER_BONUS


def test_score_prediction_double_dip_bonus_stacks() -> None:
    """Pete approved (2026-05-03): double-dip allowed, bonuses stack.

    A song in the bag AND as the opener scores its pick points (played
    anywhere) AND the opener bonus.
    """
    breakdown = score_prediction(
        pick_song_slugs=["tweezer", "fluffhead", "harry-hood"],
        opener_slug="tweezer",
        closer_slug=None,
        encore_slug=None,
        actual_opener="tweezer",
        actual_closer=None,
        actual_encore_slugs=[],
        setlist_slugs={"tweezer"},
    )
    bag_total = sum(p["points"] for p in breakdown["picks"])
    assert bag_total == SONG_POINTS  # only tweezer played
    assert breakdown["opener"]["bonus"] == OPENER_BONUS
    assert breakdown["total"] == SONG_POINTS + OPENER_BONUS


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
    )
    assert breakdown["encore"]["bonus"] == ENCORE_BONUS


def test_score_prediction_wrong_position_tag_still_earns_pick_points() -> None:
    """A bonus call that's the right song in the wrong spot still earns its
    2 points as one of your picks (when it's also one of your picks)."""
    breakdown = score_prediction(
        pick_song_slugs=["harry-hood", "a", "b"],
        opener_slug=None,
        closer_slug="harry-hood",  # wrong: actual closer is slave
        encore_slug=None,
        actual_opener=None,
        actual_closer="slave",
        actual_encore_slugs=[],
        setlist_slugs={"harry-hood"},
    )
    assert breakdown["closer"]["bonus"] == 0
    assert breakdown["closer"]["pick"] == "harry-hood"
    assert breakdown["closer"]["actual"] == "slave"
    # harry-hood is played -> earns its pick points even though the closer
    # call missed.
    assert breakdown["picks"][0]["points"] == SONG_POINTS
    assert breakdown["total"] == SONG_POINTS


def test_score_prediction_full_breakdown_shape() -> None:
    """Stable JSON shape for the scoring page."""
    breakdown = score_prediction(
        pick_song_slugs=["tweezer", "fluffhead", "harry-hood"],
        opener_slug="tweezer",
        closer_slug="harry-hood",
        encore_slug="loving-cup",
        actual_opener="tweezer",
        actual_closer="slave",
        actual_encore_slugs=["loving-cup"],
        setlist_slugs={"tweezer", "harry-hood", "loving-cup"},
    )
    assert set(breakdown.keys()) == {"picks", "opener", "closer", "encore", "total"}
    assert len(breakdown["picks"]) == 3
    for pick in breakdown["picks"]:
        assert set(pick.keys()) == {"slug", "played", "points"}
    for slot in (breakdown["opener"], breakdown["closer"], breakdown["encore"]):
        assert set(slot.keys()) == {"pick", "actual", "bonus"}
    # tweezer + harry-hood played (4) + opener hit (5) + encore hit (3) = 12.
    # closer missed (actual is slave).
    assert breakdown["total"] == 4 + OPENER_BONUS + ENCORE_BONUS
    assert breakdown["total"] == (
        sum(p["points"] for p in breakdown["picks"])
        + breakdown["opener"]["bonus"]
        + breakdown["closer"]["bonus"]
        + breakdown["encore"]["bonus"]
    )


def test_score_prediction_no_setlist_for_picks_means_zero() -> None:
    breakdown = score_prediction(
        pick_song_slugs=["a", "b", "c"],
        opener_slug=None,
        closer_slug=None,
        encore_slug=None,
        actual_opener=None,
        actual_closer=None,
        actual_encore_slugs=[],
        setlist_slugs=set(),
    )
    assert all(p["points"] == 0 for p in breakdown["picks"])
    assert breakdown["total"] == 0


def test_slot_bonuses_independent_when_picks_correct() -> None:
    """All three slots correct, no bag-pick overlap: total = picks + bonuses."""
    breakdown = score_prediction(
        pick_song_slugs=["a", "b", "c"],
        opener_slug="o",
        closer_slug="cl",
        encore_slug="e",
        actual_opener="o",
        actual_closer="cl",
        actual_encore_slugs=["e"],
        setlist_slugs={"o", "cl", "e"},  # picks a/b/c not played -> 0
    )
    assert breakdown["total"] == OPENER_BONUS + CLOSER_BONUS + ENCORE_BONUS
