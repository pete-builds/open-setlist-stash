"""Unit tests for ``_group_setlist`` — the live-setlist grouping helper.

Pure function, no DB or MCP. Covers ordered set grouping, transitions, the
NEW optional provenance/advisory fields (and their defaults so the Phish MCP,
which may omit them, still renders), and defensive handling of junk items.
"""

from __future__ import annotations

from setlist_stash.server import _group_setlist


def test_groups_by_set_in_order() -> None:
    setlist = [
        {"set_name": "Set 1", "song_title": "A", "song_slug": "a"},
        {"set_name": "Set 1", "song_title": "B", "song_slug": "b"},
        {"set_name": "Set 2", "song_title": "C", "song_slug": "c"},
        {"set_name": "Encore", "song_title": "D", "song_slug": "d"},
    ]
    groups = _group_setlist(setlist)
    assert [g["set_name"] for g in groups] == ["Set 1", "Set 2", "Encore"]
    assert [s["song_title"] for s in groups[0]["songs"]] == ["A", "B"]
    assert groups[2]["songs"][0]["song_title"] == "D"


def test_transition_preserved() -> None:
    setlist = [
        {"set_name": "Set 1", "song_title": "A", "song_slug": "a", "transition": ">"},
    ]
    groups = _group_setlist(setlist)
    assert groups[0]["songs"][0]["transition"] == ">"


def test_optional_fields_absent_default_atu_not_advisory() -> None:
    # The Phish MCP may not send provenance/advisory at all.
    setlist = [{"set_name": "Set 1", "song_title": "A", "song_slug": "a"}]
    song = _group_setlist(setlist)[0]["songs"][0]
    assert song["advisory"] is False


def test_advisory_true_flags_song() -> None:
    setlist = [
        {"set_name": "Set 1", "song_title": "A", "song_slug": "a", "advisory": True},
    ]
    assert _group_setlist(setlist)[0]["songs"][0]["advisory"] is True


def test_provenance_x_flags_song_as_advisory() -> None:
    setlist = [
        {"set_name": "Set 1", "song_title": "A", "song_slug": "a", "provenance": "x"},
    ]
    assert _group_setlist(setlist)[0]["songs"][0]["advisory"] is True


def test_provenance_atu_not_advisory() -> None:
    setlist = [
        {"set_name": "Set 1", "song_title": "A", "song_slug": "a", "provenance": "atu"},
    ]
    assert _group_setlist(setlist)[0]["songs"][0]["advisory"] is False


def test_empty_setlist_returns_empty() -> None:
    assert _group_setlist([]) == []


def test_missing_set_name_falls_back() -> None:
    groups = _group_setlist([{"song_title": "A", "song_slug": "a"}])
    assert groups[0]["set_name"] == "Set"


def test_title_falls_back_to_slug() -> None:
    groups = _group_setlist([{"set_name": "Set 1", "song_slug": "a"}])
    assert groups[0]["songs"][0]["song_title"] == "a"


def test_skips_non_dict_items() -> None:
    groups = _group_setlist(
        ["junk", None, {"set_name": "Set 1", "song_title": "A", "song_slug": "a"}]
    )
    assert len(groups) == 1
    assert groups[0]["songs"][0]["song_title"] == "A"
