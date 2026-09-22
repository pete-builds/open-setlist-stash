"""``/shows`` index assembly: the announced tour, not just the opened show.

The bug these pin: the page listed one row per ``prediction_locks`` record,
and the game opens ONE show at a time, so seven of the eight announced
2026 fall-tour dates were absent from the site entirely while the data sat
in the vault the whole time.
"""

from __future__ import annotations

from datetime import date

from setlist_stash.web_helpers import build_show_index

TODAY = date(2026, 9, 14)

# The announced 2026 fall tour as phish.net carried it on 2026-09-14
# (tour id 223), of which only the first had been opened for picks.
FALL_TOUR = [
    date(2026, 10, 2),
    date(2026, 10, 3),
    date(2026, 10, 4),
    date(2026, 10, 6),
    date(2026, 10, 7),
    date(2026, 10, 9),
    date(2026, 10, 10),
    date(2026, 10, 11),
]

VENUES = {
    "2026-10-02": "Jim Whelan Boardwalk Hall",
    "2026-10-11": "Orion Amphitheater",
    "2026-09-06": "Dick's Sporting Goods Park",
}


def _lock(show_date: date, *, entrants: int = 0, resolved: bool = False):
    return {
        "show_date": show_date,
        "entrants": entrants,
        "resolved_at": object() if resolved else None,
    }


def test_announced_tour_dates_appear_even_with_no_lock_row() -> None:
    """The regression. One opened show must not hide the other seven."""
    locks = [_lock(date(2026, 10, 2), entrants=2)]
    upcoming, past = build_show_index(locks, FALL_TOUR, VENUES, TODAY)
    assert [e["show_date"] for e in upcoming] == FALL_TOUR
    assert past == []


def test_scheduled_flag_separates_opened_from_announced() -> None:
    """Only the opened show is pickable; the rest must say so."""
    locks = [_lock(date(2026, 10, 2), entrants=2)]
    upcoming, _ = build_show_index(locks, FALL_TOUR, VENUES, TODAY)
    opened = upcoming[0]
    assert opened["scheduled"] is False
    assert opened["entrants"] == 2
    assert all(e["scheduled"] is True for e in upcoming[1:])
    assert all(e["entrants"] == 0 for e in upcoming[1:])


def test_announced_never_overwrites_a_real_lock_row() -> None:
    """A date in both lists keeps its entrant count and Final status."""
    locks = [_lock(date(2026, 10, 2), entrants=2, resolved=True)]
    upcoming, _ = build_show_index(locks, [date(2026, 10, 2)], VENUES, TODAY)
    assert len(upcoming) == 1
    assert upcoming[0]["entrants"] == 2
    assert upcoming[0]["resolved"] is True
    assert upcoming[0]["scheduled"] is False


def test_past_shows_stay_newest_first_and_keep_their_rows() -> None:
    locks = [
        _lock(date(2026, 9, 4), entrants=10, resolved=True),
        _lock(date(2026, 9, 6), entrants=3, resolved=True),
        _lock(date(2026, 10, 2), entrants=2),
    ]
    upcoming, past = build_show_index(locks, FALL_TOUR, VENUES, TODAY)
    assert [e["show_date"] for e in past] == [date(2026, 9, 6), date(2026, 9, 4)]
    assert past[0]["venue"] == "Dick's Sporting Goods Park"
    assert [e["show_date"] for e in upcoming] == FALL_TOUR


def test_announced_date_in_the_past_is_not_invented_as_a_past_show() -> None:
    """A never-opened past date has no picks; it must not appear at all."""
    upcoming, past = build_show_index([], [date(2026, 7, 4)], VENUES, TODAY)
    assert upcoming == []
    assert past == []


def test_unreachable_mcp_degrades_to_the_locked_only_list() -> None:
    """Empty announced list (MCP down) reproduces the old behavior exactly."""
    locks = [
        _lock(date(2026, 9, 6), entrants=3, resolved=True),
        _lock(date(2026, 10, 2), entrants=2),
    ]
    upcoming, past = build_show_index(locks, [], {}, TODAY)
    assert [e["show_date"] for e in upcoming] == [date(2026, 10, 2)]
    assert [e["show_date"] for e in past] == [date(2026, 9, 6)]
    assert upcoming[0]["venue"] is None


def test_venue_labels_are_attached_to_scheduled_rows_too() -> None:
    upcoming, _ = build_show_index([], FALL_TOUR, VENUES, TODAY)
    by_date = {e["show_date"]: e["venue"] for e in upcoming}
    assert by_date[date(2026, 10, 11)] == "Orion Amphitheater"
    assert by_date[date(2026, 10, 6)] is None


def test_todays_show_counts_as_upcoming_not_past() -> None:
    """Boundary: a show playing tonight must stay in the upcoming table."""
    upcoming, past = build_show_index([_lock(TODAY)], [TODAY], VENUES, TODAY)
    assert [e["show_date"] for e in upcoming] == [TODAY]
    assert past == []
