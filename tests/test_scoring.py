"""Scoring formula unit tests.

Predict it, score it, assert. Pure functions only — no fixtures, no DB.

Fixed-points model: each played pick is worth ``SONG_POINTS`` (2); the user
tags one pick as the encore call, which adds ``ENCORE_BONUS`` (5) if that song
appears anywhere in the encore.
"""

from __future__ import annotations

from setlist_stash.scoring import (
    ENCORE_BONUS,
    SONG_POINTS,
    score_prediction,
)


def test_constants_are_the_fixed_model() -> None:
    assert SONG_POINTS == 2
    assert ENCORE_BONUS == 5


def test_score_prediction_three_picks_one_played() -> None:
    breakdown = score_prediction(
        pick_song_slugs=["fluffhead", "tweezer", "harry-hood"],
        encore_slug=None,
        actual_encore_slugs=[],
        setlist_slugs={"tweezer"},
    )
    assert [p["played"] for p in breakdown["picks"]] == [False, True, False]
    assert breakdown["picks"][1]["points"] == SONG_POINTS
    assert breakdown["picks"][0]["points"] == 0
    assert breakdown["encore"]["bonus"] == 0
    assert breakdown["total"] == SONG_POINTS


def test_score_prediction_three_played_picks_is_six() -> None:
    breakdown = score_prediction(
        pick_song_slugs=["a", "b", "c"],
        encore_slug=None,
        actual_encore_slugs=[],
        setlist_slugs={"a", "b", "c"},
    )
    assert all(p["points"] == SONG_POINTS for p in breakdown["picks"])
    assert breakdown["total"] == 6


def test_score_prediction_five_played_picks_is_ten() -> None:
    breakdown = score_prediction(
        pick_song_slugs=["a", "b", "c", "d", "e"],
        encore_slug=None,
        actual_encore_slugs=[],
        setlist_slugs={"a", "b", "c", "d", "e"},
    )
    assert breakdown["total"] == 10


def test_score_prediction_correct_encore_adds_five() -> None:
    """An encore call that lands in the encore adds ENCORE_BONUS."""
    breakdown = score_prediction(
        pick_song_slugs=["a"],
        encore_slug="loving-cup",
        actual_encore_slugs=["loving-cup"],
        setlist_slugs={"loving-cup"},  # note: pick "a" not played -> 0
    )
    assert breakdown["encore"]["bonus"] == ENCORE_BONUS
    assert breakdown["total"] == ENCORE_BONUS


def test_score_prediction_double_dip_encore_stacks() -> None:
    """Pete approved: double-dip allowed, encore stacks on the pick points.

    A song in the bag AND landing in the encore scores its pick points
    (played anywhere) AND the encore bonus.
    """
    breakdown = score_prediction(
        pick_song_slugs=["loving-cup", "fluffhead", "harry-hood"],
        encore_slug="loving-cup",
        actual_encore_slugs=["loving-cup"],
        setlist_slugs={"loving-cup"},
    )
    bag_total = sum(p["points"] for p in breakdown["picks"])
    assert bag_total == SONG_POINTS  # only loving-cup played
    assert breakdown["encore"]["bonus"] == ENCORE_BONUS
    assert breakdown["total"] == SONG_POINTS + ENCORE_BONUS


def test_score_prediction_encore_matches_any_encore_song() -> None:
    breakdown = score_prediction(
        pick_song_slugs=["a", "b", "loving-cup"],
        encore_slug="loving-cup",
        actual_encore_slugs=["tweezer-reprise", "loving-cup"],
        setlist_slugs={"tweezer-reprise", "loving-cup"},
    )
    assert breakdown["encore"]["bonus"] == ENCORE_BONUS


def test_score_prediction_encore_call_outside_encore_earns_just_its_pick() -> None:
    """An encore call that plays but lands outside the encore earns only its
    2 points as one of your picks — no encore bonus."""
    breakdown = score_prediction(
        pick_song_slugs=["harry-hood", "a", "b"],
        encore_slug="harry-hood",  # played in the main set, not the encore
        actual_encore_slugs=["slave"],
        setlist_slugs={"harry-hood"},
    )
    assert breakdown["encore"]["bonus"] == 0
    assert breakdown["encore"]["pick"] == "harry-hood"
    assert breakdown["encore"]["actual"] == "slave"
    # harry-hood is played -> earns its pick points even though the encore
    # call missed.
    assert breakdown["picks"][0]["points"] == SONG_POINTS
    assert breakdown["total"] == SONG_POINTS


def test_score_prediction_full_breakdown_shape() -> None:
    """Stable JSON shape for the scoring page."""
    breakdown = score_prediction(
        pick_song_slugs=["tweezer", "fluffhead", "loving-cup"],
        encore_slug="loving-cup",
        actual_encore_slugs=["loving-cup"],
        setlist_slugs={"tweezer", "fluffhead", "loving-cup"},
    )
    assert set(breakdown.keys()) == {"picks", "encore", "total"}
    assert len(breakdown["picks"]) == 3
    for pick in breakdown["picks"]:
        assert set(pick.keys()) == {"slug", "played", "points"}
    assert set(breakdown["encore"].keys()) == {"pick", "actual", "bonus"}
    # 3 played picks (2 each = 6) + encore hit (5) = 11.
    assert breakdown["total"] == 6 + ENCORE_BONUS
    assert breakdown["total"] == (
        sum(p["points"] for p in breakdown["picks"])
        + breakdown["encore"]["bonus"]
    )


def test_score_prediction_no_setlist_for_picks_means_zero() -> None:
    breakdown = score_prediction(
        pick_song_slugs=["a", "b", "c"],
        encore_slug=None,
        actual_encore_slugs=[],
        setlist_slugs=set(),
    )
    assert all(p["points"] == 0 for p in breakdown["picks"])
    assert breakdown["total"] == 0


def test_score_prediction_matches_across_slug_punctuation() -> None:
    """Regression (2026-07-07 Kohl Center): the pick menu draws slugs from the
    phish.in-derived vault ("mike-s-song") but the setlist is scored against
    phish.net ("mikes-song"). Exact matching scored a played song as a miss.
    Normalization must let the two forms match."""
    breakdown = score_prediction(
        pick_song_slugs=["character-zero", "mike-s-song", "suzy-greenberg"],
        encore_slug=None,
        actual_encore_slugs=[],
        setlist_slugs={"character-zero", "mikes-song", "weekapaug-groove"},
    )
    played = {p["slug"]: p["played"] for p in breakdown["picks"]}
    assert played["mike-s-song"] is True
    assert played["character-zero"] is True
    assert played["suzy-greenberg"] is False
    # Two played picks, no encore call.
    assert breakdown["total"] == 2 * SONG_POINTS


def test_score_prediction_preserves_original_pick_slug_for_display() -> None:
    """Normalization is only for the match; the breakdown must echo the user's
    original slug so the UI still renders the right title."""
    breakdown = score_prediction(
        pick_song_slugs=["mike-s-song"],
        encore_slug=None,
        actual_encore_slugs=[],
        setlist_slugs={"mikes-song"},
    )
    assert breakdown["picks"][0]["slug"] == "mike-s-song"
    assert breakdown["picks"][0]["played"] is True


def test_score_prediction_encore_matches_across_slug_punctuation() -> None:
    """The encore bonus must survive the same cross-source slug divergence."""
    breakdown = score_prediction(
        pick_song_slugs=["mike-s-song"],
        encore_slug="mike-s-song",
        actual_encore_slugs=["mikes-song"],
        setlist_slugs={"mikes-song"},
    )
    assert breakdown["encore"]["bonus"] == ENCORE_BONUS
    # played pick (2) + encore bonus (5).
    assert breakdown["total"] == SONG_POINTS + ENCORE_BONUS
