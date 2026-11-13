"""The bundled demo data server speaks what the game's MCP client expects.

``demo/data_server.py`` backs ``docker-compose.demo.yml``. These tests drive it
through the game's real ``McpPhishClient`` (over an in-process ASGI transport),
so a change to either side that breaks the demo fails here, without Docker.
The demo-smoke CI job covers the same path over real containers.
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import AsyncIterator
from datetime import date, timedelta
from pathlib import Path
from types import ModuleType

import httpx
import pytest
import pytest_asyncio

from setlist_stash.config import Settings
from setlist_stash.locks import compute_default_lock_at, resolve_venue_tz, select_form_show
from setlist_stash.mcp_client import McpPhishClient
from setlist_stash.resolve_setlist import parse_setlist
from setlist_stash.scoring import score_prediction

_SERVER_PATH = Path(__file__).resolve().parent.parent / "demo" / "data_server.py"


def _load_server() -> ModuleType:
    spec = importlib.util.spec_from_file_location("demo_data_server", _SERVER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Registered before exec so dataclasses can resolve the module's annotations.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


server = _load_server()

# A fixed "today" keeps the assertions exact; the year-boundary test below
# covers other start days.
TODAY = date(2027, 3, 10)


def _client(today: date) -> McpPhishClient:
    app = server.build_app(server.build_catalog(today))
    transport = httpx.ASGITransport(app=app)
    http = httpx.AsyncClient(transport=transport, base_url="http://demo-data")
    return McpPhishClient("http://demo-data/mcp", client=http)


@pytest_asyncio.fixture
async def mcp() -> AsyncIterator[McpPhishClient]:
    async with _client(TODAY) as client:
        yield client


async def test_health_and_show_listings(mcp: McpPhishClient) -> None:
    health = await mcp.health()
    assert health["vault"]["stale"] is False

    recent = await mcp.recent_shows(limit=10)
    assert [r["date"] for r in recent] == ["2027-03-08", "2027-03-06", "2027-03-01", "2027-02-27"]
    assert all(date.fromisoformat(r["date"]) < TODAY for r in recent)

    year = await mcp.search_shows(year=2027, limit=200)
    dates = [r["date"] for r in year]
    assert dates == sorted(dates, reverse=True)  # newest first, like the real servers
    assert "2027-03-13" in dates  # first upcoming show is listed with the played ones
    assert {"show_id", "date", "venue_name", "location", "tour_name"} <= set(year[0])


async def test_get_show_shapes(mcp: McpPhishClient) -> None:
    past = await mcp.get_show("2027-03-08")
    assert past["venue"]["name"] == "Placeholder Pavilion"
    assert past["venue"]["slug"] == "placeholder-pavilion"
    assert past["setlist"], "a past show has a setlist"
    row = past["setlist"][0]
    assert {"position", "set_name", "song_slug", "song_title"} <= set(row)

    upcoming = await mcp.get_show("2027-03-13")
    assert upcoming["venue"]["name"] == "Fictional Fieldhouse"
    assert upcoming["setlist"] == []

    by_id = await mcp.get_show(past["show_id"])
    assert by_id["date"] == "2027-03-08"


async def test_unknown_show_and_song_use_the_reference_error_envelope(
    mcp: McpPhishClient,
) -> None:
    # Same shape mcp-phish returns: a successful tool call whose JSON is an
    # error envelope rather than {"data": ...}. The client hands it back as
    # a plain dict, which is what the game sees in production too.
    missing = await mcp.get_show("1999-01-01")
    assert missing["code"] == "NOT_FOUND"
    assert "setlist" not in missing
    song = await mcp.get_song("not-a-song")
    assert song["code"] == "NOT_FOUND"


async def test_song_calls(mcp: McpPhishClient) -> None:
    picker = await mcp.search_songs_for_picker("comet", limit=10)
    assert picker == [{"slug": "cardboard-comet", "title": "Cardboard Comet", "gap_current": 0}]
    assert await mcp.search_songs_pre_lock("comet") == [
        {"slug": "cardboard-comet", "title": "Cardboard Comet"}
    ]
    full = await mcp.search_songs_full("moth")
    assert full[0]["times_played"] == 2

    song = await mcp.get_song("kite-engine")
    assert song["title"] == "Kite Engine"
    assert song["gap_current"] == 1  # played two shows ago, so one show since

    valid = await mcp.validate_song_slugs(["Cardboard-Comet", "no-such-song", "moth-radio"])
    assert valid == {"cardboard-comet", "moth-radio"}

    by_gap = await mcp.songs_by_gap(limit=5)
    gaps = [r["gap"] for r in by_gap]
    assert gaps == sorted(gaps, reverse=True)


async def test_stats_and_venue_history(mcp: McpPhishClient) -> None:
    stats = await mcp.stats_overview(top_n=5)
    assert stats["total_shows"] == 4
    assert stats["total_songs_tracked"] == len(server.SONGS)
    for key in ("most_played", "biggest_gaps", "rarest_songs", "recent_debuts", "longest_shows"):
        assert stats[key], key
    assert "gap_current" in stats["biggest_gaps"][0]

    history = await mcp.venue_history("make-believe-ballroom")
    assert [r["date"] for r in history] == ["2027-03-01", "2027-02-27"]
    assert await mcp.venue_history("nowhere") == []


async def test_past_setlists_score_with_the_real_rules(mcp: McpPhishClient) -> None:
    show = await mcp.get_show("2027-03-08")
    parsed = parse_setlist(show["setlist"])
    assert parsed.encore_slugs == ["hello-world-jam", "placeholder-blues"]
    result = score_prediction(
        pick_song_slugs=["slow-parade", "hello-world-jam", "kite-engine"],
        encore_slug="hello-world-jam",
        actual_encore_slugs=parsed.encore_slugs,
        setlist_slugs=parsed.all_slugs,
    )
    assert result["total"] == 2 + 2 + 5


@pytest.mark.parametrize(
    "today",
    [date(2026, 12, 30), date(2026, 12, 31), date(2027, 1, 1), date(2028, 2, 29), TODAY],
)
async def test_there_is_always_an_open_upcoming_show(today: date) -> None:
    settings = Settings()
    async with _client(today) as mcp:
        target = await select_form_show(settings, mcp, today=today)
    assert target is not None
    assert target.show_date >= today + timedelta(days=2)
    assert target.venue_name == "Fictional Fieldhouse"
    tz = resolve_venue_tz(target.location, "UTC")
    assert tz == "America/Chicago"  # location maps to a real zone, not the fallback
    lock = compute_default_lock_at(target.show_date, settings, venue_tz=tz)
    assert lock.date() >= today + timedelta(days=2)


def test_catalog_names_are_invented() -> None:
    # Guard against someone "improving" the demo with a real band's data.
    cat = server.build_catalog(TODAY)
    assert server.BAND_NAME == "The Placeholder Comets"
    assert len(cat.songs) == len(server.SONGS) == len({s for s, _ in server.SONGS})
    for show in cat.shows:
        assert show.location.split(", ")[0] in {"Anytown", "Sampleville", "Exampleton", "Testburg"}
