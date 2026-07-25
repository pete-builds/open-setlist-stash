"""The home card's four-state machine, plus the ``select_next_show`` lookup.

The bug this pins: ``select_form_show`` is date-only, so between a show
finishing and midnight Eastern it kept returning that finished show. The board
called it "Next show" and offered a pick sheet that could no longer be
submitted to. ``home_card_state`` names that state ``"over"`` so the index
route can advance to the next announced show.

Timeline these states walk through, using a real case: Phish at MSG on
2026-07-24 (lock 7:00 PM ET), with the next show the following night.

    6:00 PM Jul 24   pre_lock   picks open for Jul 24
    9:00 PM Jul 24   live       Jul 24 locked and being played
   11:50 PM Jul 24   over       resolver stamped it; still "today" in ET
   12:01 AM Jul 25   pre_lock   date rolled; board is on Jul 25
"""

from __future__ import annotations

from datetime import date
from typing import Any, cast

import pytest

from setlist_stash.config import get_settings
from setlist_stash.locks import select_next_show
from setlist_stash.mcp_client import McpPhishClient
from setlist_stash.server import home_card_state

MSG_JUL_24 = date(2026, 7, 24)
MSG_JUL_25 = date(2026, 7, 25)


# ----- the four states ------------------------------------------------------


def test_pre_lock_picks_are_open() -> None:
    """6:00 PM: Jul 24 is the target, lock hasn't passed."""
    assert (
        home_card_state(
            upcoming_date=MSG_JUL_24, live_date=None, is_locked=False
        )
        == "pre_lock"
    )


def test_live_when_the_target_show_is_the_one_playing() -> None:
    """9:00 PM: locked AND being played. Live must win over over."""
    assert (
        home_card_state(
            upcoming_date=MSG_JUL_24, live_date=MSG_JUL_24, is_locked=True
        )
        == "live"
    )


def test_over_when_locked_and_finished_but_still_the_same_day() -> None:
    """11:50 PM: the regression. Resolver stamped it, so it is no longer live,
    but ET is still Jul 24 so select_form_show still returns Jul 24."""
    assert (
        home_card_state(
            upcoming_date=MSG_JUL_24, live_date=None, is_locked=True
        )
        == "over"
    )


def test_next_day_rolls_forward_to_an_open_show() -> None:
    """12:01 AM Jul 25: the board is on Jul 25 and picks are open again."""
    assert (
        home_card_state(
            upcoming_date=MSG_JUL_25, live_date=None, is_locked=False
        )
        == "pre_lock"
    )


def test_after_midnight_with_last_nights_show_still_unresolved() -> None:
    """Same instant, but the resolver hasn't caught up: Jul 24 is still 'live'
    while the board has moved to Jul 25. That is NOT the live state for the
    card (different show); the standalone fallback button covers Jul 24."""
    assert (
        home_card_state(
            upcoming_date=MSG_JUL_25, live_date=MSG_JUL_24, is_locked=False
        )
        == "pre_lock"
    )


def test_no_show_when_nothing_is_announced() -> None:
    assert (
        home_card_state(upcoming_date=None, live_date=None, is_locked=False)
        == "no_show"
    )


def test_no_show_wins_even_if_something_is_playing() -> None:
    """Defensive: an upstream outage can blank ``upcoming`` while a live
    pointer survives in the DB. The card has nothing to describe."""
    assert (
        home_card_state(
            upcoming_date=None, live_date=MSG_JUL_24, is_locked=True
        )
        == "no_show"
    )


# ----- select_next_show -----------------------------------------------------


class _StubMcp:
    """Minimal stand-in for McpPhishClient: only ``search_shows`` is used."""

    def __init__(self, by_year: dict[int, list[dict[str, Any]]]) -> None:
        self._by_year = by_year
        self.calls: list[int] = []

    async def search_shows(
        self, *, year: int, limit: int = 60
    ) -> list[dict[str, Any]]:
        self.calls.append(year)
        return self._by_year.get(year, [])


def _mcp(by_year: dict[int, list[dict[str, Any]]]) -> McpPhishClient:
    return cast(McpPhishClient, _StubMcp(by_year))


RUN_2026 = [
    {"date": "2026-07-22", "show_id": "1", "venue_name": "MSG"},
    {"date": "2026-07-24", "show_id": "2", "venue_name": "MSG"},
    {"date": "2026-07-25", "show_id": "3", "venue_name": "MSG"},
    {"date": "2026-07-27", "show_id": "4", "venue_name": "Alpine Valley"},
]


@pytest.mark.asyncio
async def test_next_show_is_strictly_after_the_given_date() -> None:
    got = await select_next_show(get_settings(), _mcp({2026: RUN_2026}), after=MSG_JUL_24)
    assert got is not None
    assert got.show_date == MSG_JUL_25
    assert got.venue_name == "MSG"


@pytest.mark.asyncio
async def test_next_show_skips_gaps_in_the_run() -> None:
    got = await select_next_show(get_settings(), _mcp({2026: RUN_2026}), after=MSG_JUL_25)
    assert got is not None
    assert got.show_date == date(2026, 7, 27)
    assert got.venue_name == "Alpine Valley"


@pytest.mark.asyncio
async def test_next_show_returns_none_at_the_end_of_the_run() -> None:
    """End of tour: the caller keeps its current show and the template
    suppresses the picks CTA rather than linking a locked sheet."""
    got = await select_next_show(
        get_settings(), _mcp({2026: RUN_2026}), after=date(2026, 7, 27)
    )
    assert got is None


@pytest.mark.asyncio
async def test_next_show_crosses_a_year_boundary() -> None:
    """A New Year's run into next year still resolves."""
    by_year = {
        2026: [{"date": "2026-12-31", "show_id": "9", "venue_name": "MSG"}],
        2027: [{"date": "2027-01-01", "show_id": "10", "venue_name": "MSG"}],
    }
    got = await select_next_show(
        get_settings(), _mcp(by_year), after=date(2026, 12, 31)
    )
    assert got is not None
    assert got.show_date == date(2027, 1, 1)


@pytest.mark.asyncio
async def test_next_show_skips_unparseable_rows() -> None:
    """One malformed upstream row must not blank the board."""
    by_year = {
        2026: [
            {"show_id": "x"},  # no date at all
            {"date": "not-a-date", "show_id": "y"},
            {"date": "2026-07-25", "show_id": "3", "venue_name": "MSG"},
        ]
    }
    got = await select_next_show(get_settings(), _mcp(by_year), after=MSG_JUL_24)
    assert got is not None
    assert got.show_date == MSG_JUL_25


@pytest.mark.asyncio
async def test_next_show_degrades_to_none_when_upstream_raises() -> None:
    class _Boom:
        async def search_shows(
            self, *, year: int, limit: int = 60
        ) -> list[dict[str, Any]]:
            raise RuntimeError("upstream down")

    got = await select_next_show(
        get_settings(), cast(McpPhishClient, _Boom()), after=MSG_JUL_24
    )
    assert got is None
