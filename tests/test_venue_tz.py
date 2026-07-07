"""Unit tests for venue-timezone resolution and venue-anchored lock times.

mcp-phish exposes no venue timezone, so the lock instant is anchored to the
venue-local zone resolved from the show location string. Times are stored in
UTC and displayed in DISPLAY_TZ (Eastern) elsewhere. These tests pin the
location->tz mapping and the resulting UTC instant so a Central-time show
locks at the configured wall-clock time *local to the venue*.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from setlist_stash.locks import compute_default_lock_at, resolve_venue_tz


@pytest.mark.parametrize(
    ("location", "expected"),
    [
        ("Madison, WI", "America/Chicago"),
        ("Madison, Wisconsin", "America/Chicago"),
        ("Virginia Beach, VA", "America/New_York"),
        ("New York, NY", "America/New_York"),
        ("Denver, CO", "America/Denver"),
        ("Phoenix, AZ", "America/Phoenix"),  # no DST
        ("Los Angeles, CA", "America/Los_Angeles"),
        ("George, WA", "America/Los_Angeles"),
        ("Commerce City, CO, USA", "America/Denver"),  # trailing country
        # Unmappable / international / empty -> safe Eastern fallback.
        ("Cancun, Quintana Roo", "America/New_York"),
        ("", "America/New_York"),
        (None, "America/New_York"),
    ],
)
def test_resolve_venue_tz(location: str | None, expected: str) -> None:
    assert resolve_venue_tz(location, "America/New_York") == expected


def _settings() -> SimpleNamespace:
    # Only the two attributes compute_default_lock_at reads.
    return SimpleNamespace(
        default_lock_time_local="19:25",
        default_lock_tz="America/New_York",
    )


def test_lock_anchored_to_central_venue() -> None:
    """19:25 in Central (Madison) -> 00:25 UTC next day -> 8:25 PM EDT.

    2026-07-07 is CDT (UTC-5): 19:25 CDT == 00:25 UTC on 2026-07-08. Rendered
    in US Eastern (EDT, UTC-4) that is 8:25 PM on 2026-07-07.
    """
    tz = resolve_venue_tz("Madison, WI", "America/New_York")
    lock = compute_default_lock_at(date(2026, 7, 7), _settings(), venue_tz=tz)
    assert lock == compute_default_lock_at(
        date(2026, 7, 7), _settings(), venue_tz="America/Chicago"
    )
    eastern = lock.astimezone(ZoneInfo("America/New_York"))
    assert eastern.strftime("%-I:%M %p %Z") == "8:25 PM EDT"


def test_lock_anchored_to_eastern_venue_unchanged() -> None:
    """A Virginia (Eastern) show stays at the configured wall-clock time."""
    tz = resolve_venue_tz("Virginia Beach, VA", "America/New_York")
    lock = compute_default_lock_at(date(2026, 7, 8), _settings(), venue_tz=tz)
    eastern = lock.astimezone(ZoneInfo("America/New_York"))
    assert eastern.strftime("%-I:%M %p %Z") == "7:25 PM EDT"
