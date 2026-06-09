"""Shared resolver dataclasses.

Lives in its own module so both ``resolve`` (which builds the parsed setlist)
and ``completeness`` (which evaluates it) can import ``ParsedSetlist`` without
a circular import.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ParsedSetlist:
    """Setlist parsed into the shape the scorer + completeness gate want.

    Built from the ``setlist`` array returned by mcp-phish ``get_show``.
    Each element has ``{position, set_name, song_slug, song_title, ...}``.
    """

    opener_slug: str | None
    closer_slug: str | None
    encore_slugs: list[str]
    all_slugs: set[str]
    song_count: int
