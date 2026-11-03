"""Setlist parsing for the resolver: opener / closer / encore / all slugs.

Split out of ``resolve.py``. ``setlist_stash.resolve`` re-exports
``parse_setlist``, so existing imports keep working.
"""

from __future__ import annotations

from typing import Any

from setlist_stash.resolve_types import ParsedSetlist

# ----- setlist parsing ------------------------------------------------------


def _is_encore_set(set_name: str) -> bool:
    return set_name.strip().lower().startswith("encore")


def parse_setlist(setlist: list[dict[str, Any]]) -> ParsedSetlist:
    """Parse mcp-phish setlist rows into opener / closer / encore / all.

    Conventions:
    - Opener = first song (lowest ``position``) of the first non-encore set.
    - Closer = last song (highest ``position``) of the last non-encore set.
    - Encore songs = every slug whose ``set_name`` starts with ``Encore``.
    - all_slugs = every slug in the show, used to decide whether a bag pick
      was "played".

    A setlist with no non-encore songs (rare/impossible in real life) leaves
    opener and closer as ``None``. A setlist with no encore returns an empty
    encore_slugs list.
    """
    if not setlist:
        return ParsedSetlist(None, None, [], set(), 0)

    sorted_rows = sorted(setlist, key=lambda r: int(r.get("position", 0)))
    non_encore = [r for r in sorted_rows if not _is_encore_set(str(r.get("set_name", "")))]
    encore_rows = [r for r in sorted_rows if _is_encore_set(str(r.get("set_name", "")))]

    opener: str | None = None
    closer: str | None = None
    if non_encore:
        opener_raw = non_encore[0].get("song_slug")
        closer_raw = non_encore[-1].get("song_slug")
        opener = str(opener_raw) if opener_raw else None
        closer = str(closer_raw) if closer_raw else None

    encore_slugs = [
        str(r["song_slug"])
        for r in encore_rows
        if r.get("song_slug")
    ]
    all_slugs = {
        str(r["song_slug"])
        for r in sorted_rows
        if r.get("song_slug")
    }
    return ParsedSetlist(
        opener_slug=opener,
        closer_slug=closer,
        encore_slugs=encore_slugs,
        all_slugs=all_slugs,
        song_count=len(sorted_rows),
    )
