"""Tests for the public MCP reverse proxy + per-IP rate limiter.

No DB required: these exercise the rate-limit util in isolation and the route
wiring / rate-limit middleware against the real app with a stubbed upstream
client. The upstream is stubbed via httpx.MockTransport so no network is hit.
"""

from __future__ import annotations

import httpx
from httpx import ASGITransport, AsyncClient, MockTransport

from setlist_stash.config import Settings
from setlist_stash.mcp_proxy import (
    FixedWindowRateLimiter,
    McpReverseProxy,
    client_ip,
)
from setlist_stash.server import build_app

# --- rate limiter util --------------------------------------------------------


def test_rate_limiter_allows_under_cap() -> None:
    rl = FixedWindowRateLimiter(per_minute=3)
    assert rl.enabled
    now = 1000.0
    assert rl.allow("a", now=now)
    assert rl.allow("a", now=now)
    assert rl.allow("a", now=now)


def test_rate_limiter_blocks_over_cap() -> None:
    rl = FixedWindowRateLimiter(per_minute=2)
    now = 1000.0
    assert rl.allow("a", now=now)
    assert rl.allow("a", now=now)
    assert not rl.allow("a", now=now)  # third in-window request denied


def test_rate_limiter_window_resets() -> None:
    rl = FixedWindowRateLimiter(per_minute=1, window_seconds=60.0)
    assert rl.allow("a", now=1000.0)
    assert not rl.allow("a", now=1001.0)
    # 61s later the first hit has aged out of the window.
    assert rl.allow("a", now=1062.0)


def test_rate_limiter_per_key() -> None:
    rl = FixedWindowRateLimiter(per_minute=1)
    now = 1000.0
    assert rl.allow("a", now=now)
    assert rl.allow("b", now=now)  # different key, independent bucket
    assert not rl.allow("a", now=now)


def test_rate_limiter_disabled_when_zero() -> None:
    rl = FixedWindowRateLimiter(per_minute=0)
    assert not rl.enabled
    for _ in range(100):
        assert rl.allow("a")


# --- client IP extraction -----------------------------------------------------


def test_client_ip_prefers_first_xff() -> None:
    from starlette.requests import Request

    scope = {
        "type": "http",
        "headers": [(b"x-forwarded-for", b"203.0.113.7, 70.0.0.1")],
        "client": ("10.0.0.5", 1234),
    }
    assert client_ip(Request(scope)) == "203.0.113.7"


def test_client_ip_falls_back_to_peer() -> None:
    from starlette.requests import Request

    scope = {"type": "http", "headers": [], "client": ("10.0.0.5", 1234)}
    assert client_ip(Request(scope)) == "10.0.0.5"


# --- proxy route wiring (stubbed upstream) -----------------------------------


def _stub_upstream(handler) -> MockTransport:  # type: ignore[no-untyped-def]
    return MockTransport(handler)


async def test_no_mcp_routes_when_upstream_unset() -> None:
    """OSS / Phish default: /mcp not mounted, returns 404."""
    app = build_app(Settings(mcp_upstream_url=""))
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/mcp", json={"jsonrpc": "2.0"})
    assert resp.status_code == 404


async def test_proxy_forwards_and_passes_session_id() -> None:
    """Proxy forwards method/body and copies back mcp-session-id."""
    captured: dict[str, object] = {}

    def handler(req: httpx.Request) -> httpx.Response:
        captured["method"] = req.method
        captured["accept"] = req.headers.get("accept")
        captured["session_in"] = req.headers.get("mcp-session-id")
        captured["body"] = req.content
        return httpx.Response(
            200,
            headers={
                "content-type": "application/json",
                "mcp-session-id": "sess-xyz",
            },
            json={"jsonrpc": "2.0", "result": {"serverInfo": {"name": "Stub"}}},
        )

    app = build_app(Settings(mcp_upstream_url="http://upstream/mcp"))
    # Swap the proxy's client for one backed by the stub transport.
    proxy = _find_proxy(app)
    await proxy._client.aclose()
    proxy._client = AsyncClient(transport=_stub_upstream(handler))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/mcp",
            headers={
                "accept": "application/json, text/event-stream",
                "content-type": "application/json",
                "mcp-session-id": "client-sess",
            },
            content=b'{"jsonrpc":"2.0","method":"initialize","id":1}',
        )

    assert resp.status_code == 200
    assert resp.headers.get("mcp-session-id") == "sess-xyz"
    assert "Stub" in resp.text
    assert captured["method"] == "POST"
    assert captured["accept"] == "application/json, text/event-stream"
    assert captured["session_in"] == "client-sess"
    assert captured["body"] == b'{"jsonrpc":"2.0","method":"initialize","id":1}'


async def test_proxy_rate_limit_returns_429() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    app = build_app(
        Settings(mcp_upstream_url="http://upstream/mcp", mcp_rate_limit_per_minute=2)
    )
    proxy = _find_proxy(app)
    await proxy._client.aclose()
    proxy._client = AsyncClient(transport=_stub_upstream(handler))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"x-forwarded-for": "198.51.100.9"}
        r1 = await client.post("/mcp", headers=headers, json={})
        r2 = await client.post("/mcp", headers=headers, json={})
        r3 = await client.post("/mcp", headers=headers, json={})
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r3.status_code == 429


def _find_proxy(app: object) -> McpReverseProxy:
    """Pull the live McpReverseProxy out of the app's route closures."""
    # The proxy is captured in the route endpoint closures; reach it via the
    # closure cells of the registered /mcp handler.
    for route in app.routes:  # type: ignore[attr-defined]
        if getattr(route, "path", None) == "/mcp":
            endpoint = route.endpoint
            for cell in endpoint.__closure__ or ():
                val = cell.cell_contents
                if isinstance(val, McpReverseProxy):
                    return val
    raise AssertionError("proxy not found on app")
