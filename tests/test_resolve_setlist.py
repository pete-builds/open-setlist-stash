"""Setlist parsing tests.

Pure function -> opener / closer / encore extraction from the mcp-phish
``setlist`` array shape. No I/O.
"""

from __future__ import annotations

from tweezer_picks.resolve import parse_setlist


def _row(position: int, set_name: str, slug: str) -> dict:
    return {
        "position": position,
        "set_name": set_name,
        "song_slug": slug,
        "song_title": slug.replace("-", " ").title(),
        "transition": "",
        "footnote": "",
    }


def test_parse_setlist_canonical_two_set_with_encore() -> None:
    setlist = [
        _row(1, "Set 1", "sample-in-a-jar"),
        _row(2, "Set 1", "boogie-on-reggae-woman"),
        _row(3, "Set 1", "david-bowie"),
        _row(4, "Set 2", "wilson"),
        _row(5, "Set 2", "sand"),
        _row(6, "Encore", "loving-cup"),
    ]
    parsed = parse_setlist(setlist)
    assert parsed.opener_slug == "sample-in-a-jar"
    assert parsed.closer_slug == "sand"
    assert parsed.encore_slugs == ["loving-cup"]
    assert parsed.all_slugs == {
        "sample-in-a-jar", "boogie-on-reggae-woman", "david-bowie",
        "wilson", "sand", "loving-cup",
    }
    assert parsed.song_count == 6


def test_parse_setlist_no_encore_means_empty_encore_list() -> None:
    setlist = [
        _row(1, "Set 1", "tweezer"),
        _row(2, "Set 2", "harry-hood"),
    ]
    parsed = parse_setlist(setlist)
    assert parsed.encore_slugs == []
    assert parsed.opener_slug == "tweezer"
    assert parsed.closer_slug == "harry-hood"


def test_parse_setlist_multiple_encore_sets_collected() -> None:
    """Some shows have ``Encore`` and ``Encore 2``; both contribute songs."""
    setlist = [
        _row(1, "Set 1", "wilson"),
        _row(2, "Encore", "rock-and-roll"),
        _row(3, "Encore 2", "tweezer-reprise"),
    ]
    parsed = parse_setlist(setlist)
    assert parsed.opener_slug == "wilson"
    assert parsed.closer_slug == "wilson"  # last non-encore song
    assert set(parsed.encore_slugs) == {"rock-and-roll", "tweezer-reprise"}


def test_parse_setlist_empty_returns_zero_state() -> None:
    parsed = parse_setlist([])
    assert parsed.opener_slug is None
    assert parsed.closer_slug is None
    assert parsed.encore_slugs == []
    assert parsed.all_slugs == set()
    assert parsed.song_count == 0


def test_parse_setlist_orders_by_position_not_input_order() -> None:
    """The mcp-phish payload is usually pre-sorted, but we should not rely on it."""
    setlist = [
        _row(3, "Set 1", "third"),
        _row(1, "Set 1", "first"),
        _row(2, "Set 1", "second"),
    ]
    parsed = parse_setlist(setlist)
    assert parsed.opener_slug == "first"
    assert parsed.closer_slug == "third"
