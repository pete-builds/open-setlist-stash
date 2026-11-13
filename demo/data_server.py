"""Bundled demo data server for the one-command demo (docker-compose.demo.yml).

DEMO ONLY. Not a production data source and not a reference implementation of
any real band's server. It exists so a stranger can run the game on a clean
machine without an account, an API key, or anybody else's MCP server.

What it serves: a made-up band ("The Placeholder Comets"), made-up songs, and
made-up venues in made-up towns. No real band, song, venue or person appears
here. Show dates are computed when the server starts, relative to that day, so
there are always a few past shows with setlists and a few upcoming shows to
pick, however long after this file was written the demo is run.

What it speaks: the slice of MCP (JSON-RPC 2.0 over Streamable HTTP, plain JSON
responses) that ``setlist_stash.mcp_client.McpPhishClient`` uses:

* ``initialize`` answers with an ``mcp-session-id`` header
* ``notifications/initialized`` answers 202
* ``tools/call`` answers ``{"content": [{"type": "text", "text": "<json>"}]}``
  where the JSON is ``{"data": ...}`` on success, or the reference servers'
  error envelope ``{"error": ..., "code": "NOT_FOUND"}`` for an unknown show or
  song (the same shape mcp-phish returns, so the game sees what it sees in
  production)
* ``DELETE`` ends a session

Tools, with the shapes the game reads: ``health``, ``recent_shows``,
``search_shows``, ``get_show``, ``search_songs``, ``get_song``,
``validate_song_slugs``, ``songs_by_gap``, ``stats_overview``,
``venue_history``.

Run: ``python demo/data_server.py`` (listens on ``DEMO_DATA_PORT``, default
3790). Inside the demo compose file it is reachable only on the compose
network; no host port is published.
"""

from __future__ import annotations

import json
import os
import secrets
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

BAND_NAME = "The Placeholder Comets"
TOUR_NAME = "Imaginary Tour"
PROTOCOL_VERSION = "2025-06-18"

# (slug, title). Invented titles for an invented band.
SONGS: tuple[tuple[str, str], ...] = (
    ("cardboard-comet", "Cardboard Comet"),
    ("tuesday-never-lands", "Tuesday Never Lands"),
    ("the-lighthouse-is-a-rumor", "The Lighthouse Is a Rumor"),
    ("placeholder-blues", "Placeholder Blues"),
    ("static-garden", "Static Garden"),
    ("moth-radio", "Moth Radio"),
    ("forty-stairs-down", "Forty Stairs Down"),
    ("velvet-weather", "Velvet Weather"),
    ("kite-engine", "Kite Engine"),
    ("lemon-telegram", "Lemon Telegram"),
    ("paper-satellites", "Paper Satellites"),
    ("borrowed-thunder", "Borrowed Thunder"),
    ("the-long-hallway", "The Long Hallway"),
    ("marigold-machine", "Marigold Machine"),
    ("slow-parade", "Slow Parade"),
    ("echo-pocket", "Echo Pocket"),
    ("tin-can-telescope", "Tin Can Telescope"),
    ("midnight-inventory", "Midnight Inventory"),
    ("gravel-and-honey", "Gravel and Honey"),
    ("lorem-ipsum-shuffle", "Lorem Ipsum Shuffle"),
    ("sample-text-stomp", "Sample Text Stomp"),
    ("example-dot-com", "Example Dot Com"),
    ("foo-bar-reprise", "Foo Bar Reprise"),
    ("hello-world-jam", "Hello World Jam"),
)

# venue slug -> (name, location). Locations end in a US state code because the
# game derives the pick cutoff's time zone from the state.
VENUES: dict[str, tuple[str, str]] = {
    "make-believe-ballroom": ("Make-Believe Ballroom", "Anytown, NY"),
    "placeholder-pavilion": ("Placeholder Pavilion", "Sampleville, CO"),
    "fictional-fieldhouse": ("Fictional Fieldhouse", "Exampleton, IL"),
    "demo-dome": ("The Demo Dome", "Testburg, CA"),
}

SetList = tuple[tuple[str, tuple[str, ...]], ...]

# Past shows: (days before today, venue slug, setlist). Every setlist has a
# set named "Encore", which is how the game recognizes encore songs.
PAST_SHOWS: tuple[tuple[int, str, SetList], ...] = (
    (
        11,
        "make-believe-ballroom",
        (
            ("Set 1", ("cardboard-comet", "moth-radio", "velvet-weather", "kite-engine")),
            ("Set 2", ("static-garden", "the-long-hallway", "echo-pocket", "slow-parade")),
            ("Encore", ("placeholder-blues",)),
        ),
    ),
    (
        9,
        "make-believe-ballroom",
        (
            ("Set 1", ("lemon-telegram", "forty-stairs-down", "gravel-and-honey")),
            ("Set 2", ("paper-satellites", "marigold-machine", "tin-can-telescope")),
            ("Encore", ("tuesday-never-lands", "cardboard-comet")),
        ),
    ),
    (
        4,
        "placeholder-pavilion",
        (
            ("Set 1", ("borrowed-thunder", "moth-radio", "lorem-ipsum-shuffle", "kite-engine")),
            ("Set 2", ("midnight-inventory", "static-garden", "velvet-weather")),
            ("Encore", ("the-lighthouse-is-a-rumor",)),
        ),
    ),
    (
        2,
        "placeholder-pavilion",
        (
            ("Set 1", ("sample-text-stomp", "echo-pocket", "cardboard-comet", "slow-parade")),
            ("Set 2", ("example-dot-com", "the-long-hallway", "gravel-and-honey")),
            ("Encore", ("hello-world-jam", "placeholder-blues")),
        ),
    ),
)

# Upcoming shows: (days after today, venue slug). Nothing is ever dated today or
# tomorrow, so the first upcoming show is always open for picks no matter what
# time of day, or which time zone, the demo is started in.
FUTURE_SHOWS: tuple[tuple[int, str], ...] = (
    (3, "fictional-fieldhouse"),
    (5, "fictional-fieldhouse"),
    (10, "demo-dome"),
)


@dataclass
class Show:
    show_id: str
    show_date: date
    venue_slug: str
    setlist: list[dict[str, Any]] = field(default_factory=list)

    @property
    def venue_name(self) -> str:
        return VENUES[self.venue_slug][0]

    @property
    def location(self) -> str:
        return VENUES[self.venue_slug][1]

    def summary(self) -> dict[str, Any]:
        return {
            "show_id": self.show_id,
            "date": self.show_date.isoformat(),
            "venue_name": self.venue_name,
            "location": self.location,
            "tour_name": TOUR_NAME,
        }

    def full(self) -> dict[str, Any]:
        return {
            "show_id": self.show_id,
            "date": self.show_date.isoformat(),
            "artist": BAND_NAME,
            "tour_name": TOUR_NAME,
            "venue": {
                "slug": self.venue_slug,
                "name": self.venue_name,
                "location": self.location,
            },
            "setlist": list(self.setlist),
        }


@dataclass
class Catalog:
    today: date
    shows: list[Show]  # ascending by date
    songs: dict[str, dict[str, Any]]  # slug -> song record

    def past(self) -> list[Show]:
        return [s for s in self.shows if s.show_date < self.today]

    def find_show(self, date_or_id: str) -> Show | None:
        key = date_or_id.strip()
        for show in self.shows:
            if key in (show.show_date.isoformat(), show.show_id):
                return show
        return None


def _setlist_rows(sets: SetList) -> list[dict[str, Any]]:
    titles = dict(SONGS)
    rows: list[dict[str, Any]] = []
    position = 0
    for set_name, slugs in sets:
        for i, slug in enumerate(slugs):
            position += 1
            rows.append(
                {
                    "position": position,
                    "set_name": set_name,
                    "song_slug": slug,
                    "song_title": titles[slug],
                    # Segue marker between songs in a set, as setlist sites
                    # print it; empty on the last song of each set.
                    "transition": ">" if i < len(slugs) - 1 and i % 2 == 0 else "",
                    "footnote": "",
                }
            )
    return rows


def build_catalog(today: date) -> Catalog:
    """Lay the fixed demo tour out around ``today``."""
    shows: list[Show] = []
    for days_ago, venue, sets in PAST_SHOWS:
        d = today - timedelta(days=days_ago)
        shows.append(Show(f"demo-{d.isoformat()}", d, venue, _setlist_rows(sets)))
    for days_ahead, venue in FUTURE_SHOWS:
        d = today + timedelta(days=days_ahead)
        shows.append(Show(f"demo-{d.isoformat()}", d, venue))
    shows.sort(key=lambda s: s.show_date)

    played = [s for s in shows if s.show_date < today]
    songs: dict[str, dict[str, Any]] = {}
    for slug, title in SONGS:
        dates = [s.show_date for s in played if any(r["song_slug"] == slug for r in s.setlist)]
        gap: int | None = None
        if dates:
            # Completed shows since the song was last played: 0 means it was
            # played at the most recent show.
            gap = sum(1 for s in played if s.show_date > dates[-1])
        songs[slug] = {
            "slug": slug,
            "title": title,
            "times_played": len(dates),
            "gap": gap,
            "last_played_date": dates[-1].isoformat() if dates else None,
            "debut_date": dates[0].isoformat() if dates else None,
        }
    return Catalog(today=today, shows=shows, songs=songs)


# ----- tools -----------------------------------------------------------------


class ToolNotFound(Exception):
    """The requested show or song does not exist (NOT_FOUND envelope)."""


def _song_row(song: dict[str, Any]) -> dict[str, Any]:
    return dict(song)


def tool_health(cat: Catalog, _args: dict[str, Any]) -> Any:
    return {"status": "ok", "demo": True, "vault": {"stale": False}}


def tool_recent_shows(cat: Catalog, args: dict[str, Any]) -> Any:
    limit = max(1, min(int(args.get("limit", 10)), 100))
    return [s.summary() for s in reversed(cat.past())][:limit]


def tool_search_shows(cat: Catalog, args: dict[str, Any]) -> Any:
    limit = max(1, min(int(args.get("limit", 60)), 500))
    year = args.get("year")
    rows = [s for s in cat.shows if year is None or s.show_date.year == int(year)]
    return [s.summary() for s in reversed(rows)][:limit]


def tool_get_show(cat: Catalog, args: dict[str, Any]) -> Any:
    show = cat.find_show(str(args.get("date_or_id", "")))
    if show is None:
        raise ToolNotFound(f"show not found: {args.get('date_or_id')}")
    return show.full()


def tool_search_songs(cat: Catalog, args: dict[str, Any]) -> Any:
    query = str(args.get("query", "")).strip().lower()
    limit = max(1, min(int(args.get("limit", 10)), 100))
    hits = [
        _song_row(s)
        for s in cat.songs.values()
        if query and (query in s["title"].lower() or query in s["slug"])
    ]
    return hits[:limit]


def tool_get_song(cat: Catalog, args: dict[str, Any]) -> Any:
    song = cat.songs.get(str(args.get("slug", "")).strip().lower())
    if song is None:
        raise ToolNotFound(f"song not found: {args.get('slug')}")
    return _song_row(song)


def tool_validate_song_slugs(cat: Catalog, args: dict[str, Any]) -> Any:
    slugs = [str(s).strip().lower() for s in (args.get("slugs") or [])][:50]
    return {
        "valid": [s for s in slugs if s in cat.songs],
        "unknown": [s for s in slugs if s not in cat.songs],
    }


def tool_songs_by_gap(cat: Catalog, args: dict[str, Any]) -> Any:
    limit = max(1, min(int(args.get("limit", 25)), 100))
    played = [s for s in cat.songs.values() if s["gap"] is not None]
    played.sort(key=lambda s: (-s["gap"], s["title"]))
    return [_song_row(s) for s in played[:limit]]


def tool_stats_overview(cat: Catalog, args: dict[str, Any]) -> Any:
    top_n = max(1, min(int(args.get("top_n", 10)), 50))
    past = cat.past()
    played = [s for s in cat.songs.values() if s["times_played"] > 0]
    performances = sum(len(s.setlist) for s in past)
    return {
        "total_shows": len(past),
        "avg_songs_per_show": round(performances / len(past), 1) if past else 0,
        "distinct_songs_played": len(played),
        "total_performances": performances,
        "total_songs_tracked": len(cat.songs),
        "first_show_date": past[0].show_date.isoformat() if past else None,
        "last_show_date": past[-1].show_date.isoformat() if past else None,
        "most_played": sorted(played, key=lambda s: (-s["times_played"], s["title"]))[:top_n],
        "biggest_gaps": [
            {**s, "gap_current": s["gap"]}
            for s in sorted(played, key=lambda s: (-s["gap"], s["title"]))[:top_n]
        ],
        "rarest_songs": sorted(played, key=lambda s: (s["times_played"], s["title"]))[:top_n],
        "recent_debuts": sorted(played, key=lambda s: s["debut_date"], reverse=True)[:top_n],
        "longest_shows": [
            {**s.summary(), "song_count": len(s.setlist)}
            for s in sorted(past, key=lambda s: -len(s.setlist))[:top_n]
        ],
    }


def tool_venue_history(cat: Catalog, args: dict[str, Any]) -> Any:
    slug = str(args.get("venue_slug", ""))
    limit = max(1, min(int(args.get("limit", 25)), 100))
    rows = [s.summary() for s in reversed(cat.past()) if s.venue_slug == slug]
    return rows[:limit]


TOOLS: dict[str, Any] = {
    "health": tool_health,
    "recent_shows": tool_recent_shows,
    "search_shows": tool_search_shows,
    "get_show": tool_get_show,
    "search_songs": tool_search_songs,
    "get_song": tool_get_song,
    "validate_song_slugs": tool_validate_song_slugs,
    "songs_by_gap": tool_songs_by_gap,
    "stats_overview": tool_stats_overview,
    "venue_history": tool_venue_history,
}


# ----- JSON-RPC over HTTP ----------------------------------------------------


def _text_result(payload: Any) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": json.dumps(payload)}], "isError": False}


def handle_rpc(cat: Catalog, body: dict[str, Any]) -> tuple[int, dict[str, Any] | None]:
    """Answer one JSON-RPC message. Returns (http status, body or None)."""
    method = body.get("method")
    req_id = body.get("id")
    if req_id is None:
        # A notification (e.g. notifications/initialized): no response body.
        return 202, None
    if method == "initialize":
        result: dict[str, Any] = {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "setlist-stash-demo-data", "version": "1"},
        }
    elif method == "ping":
        result = {}
    elif method == "tools/list":
        result = {
            "tools": [
                {"name": name, "inputSchema": {"type": "object"}} for name in sorted(TOOLS)
            ]
        }
    elif method == "tools/call":
        params = body.get("params") or {}
        tool = TOOLS.get(str(params.get("name")))
        if tool is None:
            return 200, {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32602, "message": f"unknown tool {params.get('name')}"},
            }
        try:
            result = _text_result({"data": tool(cat, params.get("arguments") or {})})
        except ToolNotFound as exc:
            result = _text_result({"error": str(exc), "code": "NOT_FOUND"})
    else:
        return 200, {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"method not found: {method}"},
        }
    return 200, {"jsonrpc": "2.0", "id": req_id, "result": result}


def _today() -> date:
    # The game decides "today" in DISPLAY_TZ, so the demo tour is laid out on
    # the same calendar.
    return datetime.now(tz=ZoneInfo(os.environ.get("DISPLAY_TZ", "America/New_York"))).date()


def build_app(catalog: Catalog | None = None) -> Starlette:
    cat = catalog or build_catalog(_today())

    async def mcp_post(request: Request) -> Response:
        try:
            body = await request.json()
        except ValueError:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}},
                status_code=400,
            )
        if not isinstance(body, dict):
            return JSONResponse(
                {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "batch"}},
                status_code=400,
            )
        status, payload = handle_rpc(cat, body)
        if payload is None:
            return Response(status_code=status)
        headers = {}
        if body.get("method") == "initialize":
            headers["mcp-session-id"] = secrets.token_hex(16)
        return JSONResponse(payload, status_code=status, headers=headers)

    async def mcp_delete(_request: Request) -> Response:
        return Response(status_code=200)

    async def healthz(_request: Request) -> Response:
        return JSONResponse(
            {
                "status": "ok",
                "band": BAND_NAME,
                "today": cat.today.isoformat(),
                "upcoming": [
                    s.show_date.isoformat() for s in cat.shows if s.show_date >= cat.today
                ],
            }
        )

    return Starlette(
        routes=[
            Route("/mcp", mcp_post, methods=["POST"]),
            Route("/mcp", mcp_delete, methods=["DELETE"]),
            Route("/healthz", healthz, methods=["GET"]),
        ]
    )


def main() -> None:
    import uvicorn

    uvicorn.run(
        build_app(),
        host=os.environ.get("DEMO_DATA_HOST", "0.0.0.0"),
        port=int(os.environ.get("DEMO_DATA_PORT", "3790")),
        log_level="warning",
    )


if __name__ == "__main__":
    main()
