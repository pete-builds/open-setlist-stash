"""Timezone tests: nothing user-facing may render as UTC.

Storage and comparison stay UTC; ``display_dt`` is the single conversion point
on the way out, and it must be DST-aware (EDT in summer, EST in winter) rather
than a hardcoded offset. ``display_now`` is the matching rule for date
boundaries: containers run ``TZ=UTC``, where a bare ``date.today()`` rolls over
to tomorrow at 8pm Eastern, mid-show.
"""

from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from setlist_stash.config import get_settings
from setlist_stash.server import display_dt, display_now


def test_summer_instant_renders_as_edt() -> None:
    # 2026-07-24 23:30 UTC == 7:30 PM EDT the same evening.
    ts = datetime(2026, 7, 24, 23, 30, tzinfo=UTC)
    assert display_dt(ts) == "Jul 24, 7:30 PM EDT"


def test_winter_instant_renders_as_est() -> None:
    # DST-aware, not a fixed -4: January is EST.
    ts = datetime(2026, 1, 15, 23, 30, tzinfo=UTC)
    assert display_dt(ts) == "Jan 15, 6:30 PM EST"


def test_utc_midnight_renders_as_the_previous_evening() -> None:
    """The rollover case: 00:30 UTC is still last night in Eastern."""
    ts = datetime(2026, 7, 25, 0, 30, tzinfo=UTC)
    assert display_dt(ts) == "Jul 24, 8:30 PM EDT"


def test_naive_timestamps_are_treated_as_utc() -> None:
    naive = datetime(2026, 7, 24, 23, 30)
    aware = datetime(2026, 7, 24, 23, 30, tzinfo=UTC)
    assert display_dt(naive) == display_dt(aware)


def test_never_emits_a_utc_label() -> None:
    for month in range(1, 13):
        rendered = display_dt(datetime(2026, month, 15, 12, 0, tzinfo=UTC))
        assert "UTC" not in rendered
        assert rendered.endswith(("EST", "EDT"))


def test_non_datetime_values_degrade_quietly() -> None:
    assert display_dt(None) == ""
    assert display_dt("already a string") == "already a string"


def test_custom_format_still_converts() -> None:
    ts = datetime(2026, 7, 25, 0, 30, tzinfo=UTC)
    assert display_dt(ts, "%Y-%m-%d") == "2026-07-24"


def test_display_now_is_eastern_not_utc() -> None:
    now = display_now(get_settings())
    assert now.tzinfo is not None
    assert str(now.tzinfo) == "America/New_York"
    # Same instant as UTC "now", just a different wallclock.
    assert abs((now - datetime.now(UTC)).total_seconds()) < 5


def test_display_tz_default_is_eastern() -> None:
    """A deployment that never sets DISPLAY_TZ must not fall back to UTC."""
    assert get_settings().display_tz == "America/New_York"
    assert ZoneInfo(get_settings().display_tz) is not None
