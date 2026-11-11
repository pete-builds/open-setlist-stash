"""Pure leaderboard key logic: no database.

Split out of ``leaderboard.py``: deriving season / tour / run bucket keys,
ordering keys newest first, and parsing the tab and scope configuration.
``setlist_stash.leaderboard`` re-exports the public names.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

# Same logger name as before the split, so log lines are unchanged.
logger = logging.getLogger("setlist_stash.leaderboard")


# ----- scope-key derivation -------------------------------------------------


_SEASON_BY_MONTH = {
    1: "winter",  # Jan attaches to previous year (handled in SQL)
    2: "winter",
    3: "spring",
    4: "spring",
    5: "spring",
    6: "summer",
    7: "summer",
    8: "summer",
    9: "fall",
    10: "fall",
    11: "fall",
    12: "winter",  # Dec attaches to current year
}


def derive_season_key(show_date: Any) -> str:
    """Return the season scope key for a show date.

    Plan B for the deferred tour scope. Format: ``YYYY-<season>``.
    Winter spans Dec-Jan-Feb and is bucketed under the December year, so
    Jan 2026 lands in ``2025-winter`` alongside Dec 2025.
    """
    if hasattr(show_date, "month"):
        month = int(show_date.month)
        year = int(show_date.year)
    else:  # ISO string fallback
        parts = str(show_date).split("-")
        year = int(parts[0])
        month = int(parts[1])
    season = _SEASON_BY_MONTH[month]
    if month in (1, 2):
        year -= 1
    return f"{year}-{season}"


# Upstream strings that occupy the tour field without naming a tour. The
# Umphrey's MCP returns "No Tour Name" for every single show, and phish.net
# tags festivals and one-offs "Not Part of a Tour" (70 of 2155 Phish shows as
# of 2026-09-14). Treating either as a real tour would collapse a whole
# deployment into one bucket, so they are read as "no information" and fall
# back to the month.
_TOUR_PLACEHOLDERS = frozenset(
    {
        "",
        "no tour name",
        "not part of a tour",
        "none",
        "n/a",
        "unknown",
    }
)

_SEASON_WORDS = {
    "winter": "winter",
    "spring": "spring",
    "summer": "summer",
    "fall": "fall",
    "autumn": "fall",
}


def derive_tour_season_key(tour_name: Any, show_date: Any) -> str | None:
    """Season bucket taken from the upstream TOUR NAME, or None.

    Returns a ``YYYY-<season>`` key when ``tour_name`` names both a season and
    a four-digit year ("2026 Fall Tour", "Winter/Spring Tour 1993"), otherwise
    None to mean "no usable tour information" — the caller then falls back to
    :func:`derive_season_key`, which buckets on the calendar month.

    Both parts are REQUIRED on purpose. A name like "1990 Tour" carries a year
    but no season, and guessing the season from the date would silently label a
    date-derived bucket as tour-derived; a name with a season but no year would
    need the show's year, which re-opens the Dec/Jan boundary question that
    derive_season_key already answers. Demanding both keeps this function
    total, cheap to reason about, and wrong in only one direction: it declines
    rather than inventing.

    A multi-season name takes its FIRST season word, so every show on
    "Winter/Spring Tour 1993" shares one bucket. Keeping a tour together is the
    entire point of bucketing by tour; splitting it by month is the behavior
    being replaced.

    The key shape matches :func:`derive_season_key` exactly so the two can
    COALESCE into one column, and so existing ``LEADERBOARD_TABS`` entries that
    name a scope_key like ``2026-summer`` keep resolving.
    """
    if tour_name is None:
        return None
    name = str(tour_name).strip()
    if name.lower() in _TOUR_PLACEHOLDERS:
        return None
    lowered = name.lower()
    season: str | None = None
    season_at = len(lowered)
    for word, canonical in _SEASON_WORDS.items():
        idx = lowered.find(word)
        if idx != -1 and idx < season_at:
            season, season_at = canonical, idx
    if season is None:
        return None
    year_match = re.search(r"(?<!\d)(1[89]\d{2}|20\d{2})(?!\d)", name)
    if year_match is None:
        return None
    _ = show_date  # signature symmetry with derive_season_key; not needed here
    return f"{year_match.group(1)}-{season}"


# ----- run scope (named show runs: residencies, festivals, venue stands) ----

_RUN_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")


def parse_runs(spec: str) -> dict[str, list[date]]:
    """Parse ``Settings.leaderboard_runs`` into ``{key: [date, ...]}``.

    Format: ``key=YYYY-MM-DD,YYYY-MM-DD;key2=YYYY-MM-DD``.

    Fail-soft by design, matching the rest of the deployment-config surface:
    a malformed run is logged and skipped rather than crashing boot, because
    a typo in one env var must not take the whole game down. A key that
    survives is guaranteed slug-shaped and its dates guaranteed real, which
    is what lets ``rebuild_runs`` inline them into SQL safely.
    """
    out: dict[str, list[date]] = {}
    for chunk in (spec or "").split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        key, sep, raw_dates = chunk.partition("=")
        key = key.strip().lower()
        if not sep or not _RUN_KEY_RE.match(key):
            logger.warning("leaderboard run skipped: bad key", extra={"run": chunk})
            continue
        dates: list[date] = []
        bad = False
        for token in raw_dates.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                dates.append(date.fromisoformat(token))
            except ValueError:
                logger.warning(
                    "leaderboard run skipped: bad date",
                    extra={"run": key, "value": token},
                )
                bad = True
                break
        if bad or not dates:
            continue
        out[key] = sorted(set(dates))
    return out


# ----- scope-key ordering ---------------------------------------------------


_SEASON_ORDER = {"spring": 1, "summer": 2, "fall": 3, "winter": 4}


def _season_sort_key(key: str) -> tuple[int, int, int, str]:
    """Sort key for a ``YYYY-<season>`` bucket: newest first when reversed.

    Season names do not sort by calendar as text (``summer`` > ``fall`` >
    ``spring``), so the year and the season's position in it are compared as
    numbers. A key that is not ``YYYY-<season>`` sorts below every well-formed
    one so a stray bucket can never be picked as "newest".
    """
    year_s, _, season = key.partition("-")
    rank = _SEASON_ORDER.get(season)
    if rank is not None and year_s.isdigit():
        return (1, int(year_s), rank, key)
    return (0, 0, 0, key)


def sort_scope_keys(scope: str, keys: list[str]) -> list[str]:
    """Order bucket keys newest first for ``scope``.

    Weekly keys (``2026-W18``) are zero-padded and sort correctly as text.
    Season keys (``2026-fall``) do not: ``2026-summer`` > ``2026-fall`` as
    text, which opened the Season tab on the summer board all autumn. Run and
    league keys are operator-chosen slugs with no natural order, so text
    order is kept for them.
    """
    if scope == "tour":
        return sorted(keys, key=_season_sort_key, reverse=True)
    return sorted(keys, reverse=True)


# ----- scope helpers --------------------------------------------------------


VALID_SCOPES: tuple[str, ...] = ("weekly", "tour", "all_time", "run")


@dataclass(frozen=True)
class LeaderboardTab:
    """One entry in the leaderboard tab bar.

    ``scope_key`` pinned means this tab always shows that bucket; None means
    it tracks the newest bucket for its scope (the platform's original
    behavior). Pinning is what lets two tabs share one scope — e.g. a Summer
    and a Fall board both living on ``tour``.
    """

    label: str
    scope: str
    scope_key: str | None


DEFAULT_TABS: tuple[LeaderboardTab, ...] = (
    LeaderboardTab("Weekly", "weekly", None),
    LeaderboardTab("Season", "tour", None),
    LeaderboardTab("All-time", "all_time", None),
)


def parse_tabs(spec: str) -> tuple[LeaderboardTab, ...]:
    """Parse ``Settings.leaderboard_tabs`` into an ordered tab list.

    Format: ``Label|scope|scope_key`` per tab, comma-separated; ``scope_key``
    optional. Empty or fully-malformed input returns ``DEFAULT_TABS`` so a bad
    env var degrades to the platform's stock tab bar rather than rendering a
    leaderboard with no way to navigate it.
    """
    tabs: list[LeaderboardTab] = []
    for chunk in (spec or "").split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts = [p.strip() for p in chunk.split("|")]
        if len(parts) < 2 or not parts[0] or not parts[1]:
            logger.warning("leaderboard tab skipped: malformed", extra={"tab": chunk})
            continue
        label, raw_scope = parts[0], parts[1]
        scope = normalize_scope(raw_scope)
        if scope not in VALID_SCOPES:
            logger.warning(
                "leaderboard tab skipped: unknown scope",
                extra={"tab": chunk, "scope": raw_scope},
            )
            continue
        key = parts[2] if len(parts) > 2 and parts[2] else None
        tabs.append(LeaderboardTab(label, scope, key))
    return tuple(tabs) if tabs else DEFAULT_TABS


def normalize_scope(raw: str) -> str:
    """Coerce 'all-time' / 'all_time' / 'season' / 'tour' / 'weekly' to canonical.

    The task's design called for a hyphenated ``all-time``; migration 001
    declared ``all_time``. We accept both at the URL boundary so links don't
    break when typed either way.
    """
    s = (raw or "").strip().lower()
    if s in ("weekly", "week"):
        return "weekly"
    if s in ("tour", "season"):
        return "tour"
    if s in ("all_time", "all-time", "alltime", "all"):
        return "all_time"
    if s in ("run", "runs"):
        return "run"
    return s
