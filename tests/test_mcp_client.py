"""mcp-phish client wrapper tests.

The `respx` library mocks httpx at the transport layer. We assert wire
contract (POST + JSON-RPC body), the FastMCP `content[0].text` unwrap, the
gap -> gap_current normalization, and the pre-lock-safe shape stripping.

The Streamable HTTP handshake (initialize + notifications/initialized) is
exercised in ``test_initialize_handshake``; in the other tests we
short-circuit it via ``_force_initialized()`` so each test mocks only the
single ``tools/call`` POST it cares about.
"""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from tweezer_picks.mcp_client import McpPhishClient, McpPhishNotFound, McpPhishUnavailable

URL = "http://mcp-phish:3705/mcp"


def _mcp_response(payload: dict, request_id: str = "abc") -> dict:
    """Build a FastMCP-shaped JSON-RPC response."""
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "content": [
                {"type": "text", "text": json.dumps({"data": payload})}
            ],
            "isError": False,
        },
    }


def _skip_handshake(client: McpPhishClient) -> None:
    """Stub out the FastMCP handshake for tool-only tests."""
    client._initialized = True
    client._session_id = "test-session"


@pytest.mark.asyncio
@respx.mock
async def test_recent_shows_returns_list() -> None:
    expected = [{"show_id": "1", "date": "2026-05-01", "venue_name": "Sphere"}]
    respx.post(URL).respond(json=_mcp_response(expected))
    async with McpPhishClient(URL) as c:
        _skip_handshake(c)
        rows = await c.recent_shows(limit=5)
    assert rows == expected


@pytest.mark.asyncio
@respx.mock
async def test_get_song_normalizes_gap_to_gap_current() -> None:
    upstream = {"slug": "tweezer", "title": "Tweezer", "gap": 6, "times_played": 456}
    respx.post(URL).respond(json=_mcp_response(upstream))
    async with McpPhishClient(URL) as c:
        _skip_handshake(c)
        row = await c.get_song("tweezer")
    assert row["gap_current"] == 6
    # Original gap stays available for any post-lock UI that wants it.
    assert row["gap"] == 6


@pytest.mark.asyncio
@respx.mock
async def test_search_songs_pre_lock_strips_to_slug_and_title() -> None:
    """PHASE-4-PLAN §7: pre-lock autocomplete is fair-play safe."""
    upstream = [
        {"slug": "tweezer", "title": "Tweezer", "times_played": 456, "original": True},
        {"slug": "tweezer-reprise", "title": "Tweezer Reprise", "times_played": 340},
    ]
    respx.post(URL).respond(json=_mcp_response(upstream))
    async with McpPhishClient(URL) as c:
        _skip_handshake(c)
        rows = await c.search_songs_pre_lock("tweez")
    assert rows == [
        {"slug": "tweezer", "title": "Tweezer"},
        {"slug": "tweezer-reprise", "title": "Tweezer Reprise"},
    ]
    # Hard-lock against assist-data leakage.
    for r in rows:
        assert "times_played" not in r
        assert "gap" not in r
        assert "gap_current" not in r
        assert "original" not in r


@pytest.mark.asyncio
@respx.mock
async def test_search_songs_full_keeps_assist_fields() -> None:
    upstream = [{"slug": "tweezer", "title": "Tweezer", "times_played": 456}]
    respx.post(URL).respond(json=_mcp_response(upstream))
    async with McpPhishClient(URL) as c:
        _skip_handshake(c)
        rows = await c.search_songs_full("tweez")
    assert rows[0]["times_played"] == 456


@pytest.mark.asyncio
@respx.mock
async def test_5xx_raises_unavailable() -> None:
    respx.post(URL).respond(status_code=503, text="upstream down")
    with pytest.raises(McpPhishUnavailable):
        async with McpPhishClient(URL) as c:
            _skip_handshake(c)
            await c.health()


@pytest.mark.asyncio
@respx.mock
async def test_4xx_raises_notfound() -> None:
    respx.post(URL).respond(status_code=404, text="unknown method")
    with pytest.raises(McpPhishNotFound):
        async with McpPhishClient(URL) as c:
            _skip_handshake(c)
            await c.get_song("nothing")


@pytest.mark.asyncio
@respx.mock
async def test_network_error_raises_unavailable() -> None:
    respx.post(URL).mock(side_effect=httpx.ConnectError("dns fail"))
    with pytest.raises(McpPhishUnavailable):
        async with McpPhishClient(URL) as c:
            _skip_handshake(c)
            await c.health()


@pytest.mark.asyncio
@respx.mock
async def test_request_body_is_jsonrpc_tools_call() -> None:
    route = respx.post(URL).respond(json=_mcp_response({"status": "ok"}))
    async with McpPhishClient(URL) as c:
        _skip_handshake(c)
        await c.health()
    assert route.called
    sent = json.loads(route.calls[0].request.content.decode())
    assert sent["jsonrpc"] == "2.0"
    assert sent["method"] == "tools/call"
    assert sent["params"]["name"] == "health"
    assert sent["params"]["arguments"] == {}


@pytest.mark.asyncio
@respx.mock
async def test_validate_song_slugs_empty_input_no_round_trip() -> None:
    """Empty list short-circuits to empty set with zero round-trips."""
    route = respx.post(URL).respond(
        json=_mcp_response({"valid": [], "unknown": []})
    )
    async with McpPhishClient(URL) as c:
        _skip_handshake(c)
        valid = await c.validate_song_slugs([])
    assert valid == set()
    assert not route.called


@pytest.mark.asyncio
@respx.mock
async def test_validate_song_slugs_all_valid_returns_full_set() -> None:
    """All slugs valid -> single round-trip, full set returned."""
    route = respx.post(URL).respond(
        json=_mcp_response(
            {"valid": ["fluffhead", "tweezer"], "unknown": []}
        )
    )
    async with McpPhishClient(URL) as c:
        _skip_handshake(c)
        valid = await c.validate_song_slugs(["tweezer", "fluffhead"])
    assert valid == {"tweezer", "fluffhead"}
    assert route.call_count == 1
    sent = json.loads(route.calls[0].request.content.decode())
    assert sent["params"]["name"] == "validate_song_slugs"
    # Slugs are normalized (lower + strip + dedupe) before being sent.
    assert set(sent["params"]["arguments"]["slugs"]) == {"tweezer", "fluffhead"}


@pytest.mark.asyncio
@respx.mock
async def test_validate_song_slugs_mixed_returns_only_valid_subset() -> None:
    """Mixed valid/unknown -> only the valid subset comes back."""
    route = respx.post(URL).respond(
        json=_mcp_response(
            {"valid": ["fluffhead", "tweezer"], "unknown": ["blarghhh"]}
        )
    )
    async with McpPhishClient(URL) as c:
        _skip_handshake(c)
        valid = await c.validate_song_slugs(
            ["tweezer", "blarghhh", "fluffhead"]
        )
    assert valid == {"tweezer", "fluffhead"}
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_validate_song_slugs_upstream_error_raises() -> None:
    """Upstream 5xx bubbles up as McpPhishError (subclass)."""
    respx.post(URL).respond(status_code=503, text="upstream down")
    with pytest.raises(McpPhishUnavailable):
        async with McpPhishClient(URL) as c:
            _skip_handshake(c)
            await c.validate_song_slugs(["tweezer"])


@pytest.mark.asyncio
@respx.mock
async def test_initialize_handshake_sends_two_posts_and_session_header() -> None:
    """The first call triggers initialize + initialized notification, then
    tools/call carries the mcp-session-id header.
    """
    init_response = {
        "jsonrpc": "2.0",
        "id": "1",
        "result": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "serverInfo": {"name": "phish-mcp", "version": "0.1"},
        },
    }
    init_route = respx.post(URL).mock(
        return_value=httpx.Response(
            200,
            headers={
                "content-type": "application/json",
                "mcp-session-id": "deadbeef",
            },
            json=init_response,
        )
    )

    async with McpPhishClient(URL) as c:
        # First call -> handshake + tools/call. Switch route after first
        # two responses to return the tools/call payload.
        # respx routes are deterministic per URL; use side_effect for ordering.
        responses = [
            httpx.Response(
                200,
                headers={
                    "content-type": "application/json",
                    "mcp-session-id": "deadbeef",
                },
                json=init_response,
            ),
            httpx.Response(202, json={"jsonrpc": "2.0", "result": {}}),
            httpx.Response(200, json=_mcp_response({"status": "ok"})),
        ]
        init_route.side_effect = responses
        await c.health()

    assert init_route.call_count == 3
    # Second call (notifications/initialized) carries the session header.
    second = init_route.calls[1].request
    assert second.headers.get("mcp-session-id") == "deadbeef"
    second_body = json.loads(second.content.decode())
    assert second_body["method"] == "notifications/initialized"
    # Third call (tools/call) carries the session header.
    third = init_route.calls[2].request
    assert third.headers.get("mcp-session-id") == "deadbeef"
    third_body = json.loads(third.content.decode())
    assert third_body["method"] == "tools/call"
    assert third_body["params"]["name"] == "health"
