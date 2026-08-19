"""Tests for the public MCP reverse proxy + per-IP rate limiter.

No DB required: these exercise the rate-limit util in isolation and the route
wiring / rate-limit middleware against the real app with a stubbed upstream
client. The upstream is stubbed via httpx.MockTransport so no network is hit.
"""

from __future__ import annotations

import httpx
from httpx import ASGITransport, AsyncClient, MockTransport

from setlist_stash.client_addr import resolve_client_ip
from setlist_stash.config import Settings
from setlist_stash.mcp_proxy import (
    FixedWindowRateLimiter,
    McpReverseProxy,
    _TraversalRejected,
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


def _req(  # type: ignore[no-untyped-def]
    headers: list[tuple[bytes, bytes]], peer: str = "10.0.0.5"
):
    from starlette.requests import Request

    return Request({"type": "http", "headers": headers, "client": (peer, 1234)})


def test_client_ip_ignores_xff_when_no_header_declared() -> None:
    """The security property: an undeclared deployment trusts NO header.

    Regression guard. This used to read the first X-Forwarded-For hop, which a
    caller sets themselves (Cloudflare appends rather than replaces), so any
    rate limit keyed on it was bypassable by rotating one header per request.
    """
    cfg = Settings(trusted_client_ip_header="")
    req = _req([(b"x-forwarded-for", b"203.0.113.7, 70.0.0.1")])
    assert resolve_client_ip(req, cfg) == "10.0.0.5"


def test_client_ip_uses_declared_header() -> None:
    cfg = Settings(trusted_client_ip_header="CF-Connecting-IP")
    req = _req([(b"cf-connecting-ip", b"203.0.113.9")])
    assert resolve_client_ip(req, cfg) == "203.0.113.9"


def test_client_ip_declared_header_takes_last_hop() -> None:
    """An edge appends what it observed, so the last hop is the vouched one."""
    cfg = Settings(trusted_client_ip_header="X-Forwarded-For")
    req = _req([(b"x-forwarded-for", b"1.2.3.4, 203.0.113.9")])
    assert resolve_client_ip(req, cfg) == "203.0.113.9"


def test_client_ip_falls_back_to_peer_when_declared_header_absent() -> None:
    cfg = Settings(trusted_client_ip_header="CF-Connecting-IP")
    req = _req([])
    assert resolve_client_ip(req, cfg) == "10.0.0.5"


def test_client_ip_unknown_without_peer() -> None:
    from starlette.requests import Request

    cfg = Settings()
    req = Request({"type": "http", "headers": [], "client": None})
    assert resolve_client_ip(req, cfg) == "unknown"


# --- upstream path joining ----------------------------------------------------


def _proxy() -> McpReverseProxy:
    return McpReverseProxy("http://mcp-upstream:3717/mcp", timeout_seconds=5.0)


def test_target_url_plain_subpath() -> None:
    assert (
        _proxy()._target_url("messages")
        == "http://mcp-upstream:3717/mcp/messages"
    )


def test_target_url_empty_subpath_is_endpoint() -> None:
    assert _proxy()._target_url("") == "http://mcp-upstream:3717/mcp"


def test_target_url_preserves_trailing_slash() -> None:
    assert (
        _proxy()._target_url("messages/")
        == "http://mcp-upstream:3717/mcp/messages/"
    )


def test_target_url_rejects_traversal_escape() -> None:
    """``/mcp/../admin`` must not resolve to ``/admin`` on the upstream host."""
    import pytest

    with pytest.raises(_TraversalRejected):
        _proxy()._target_url("../admin")


def test_target_url_rejects_deep_traversal() -> None:
    import pytest

    with pytest.raises(_TraversalRejected):
        _proxy()._target_url("a/b/../../../../etc/passwd")


def test_target_url_rejects_double_encoded_traversal() -> None:
    """%252e%252e survives Starlette's one decode and would reach the upstream.

    Whether ``/mcp/%2e%2e/admin`` then resolves upward depends on how the
    upstream decodes, which is not this proxy's assumption to make.
    """
    import pytest

    with pytest.raises(_TraversalRejected):
        _proxy()._target_url("%252e%252e/admin")


def test_target_url_rejects_single_encoded_traversal() -> None:
    import pytest

    with pytest.raises(_TraversalRejected):
        _proxy()._target_url("%2e%2e/admin")


def test_target_url_rejects_encoded_backslash_traversal() -> None:
    import pytest

    with pytest.raises(_TraversalRejected):
        _proxy()._target_url("%252e%252e%255cadmin")


def test_target_url_allows_encoded_content_that_is_not_traversal() -> None:
    """A legitimately encoded segment must still pass."""
    assert (
        _proxy()._target_url("tools/get%20show")
        == "http://mcp-upstream:3717/mcp/tools/get%20show"
    )


def test_target_url_allows_traversal_that_stays_inside() -> None:
    assert (
        _proxy()._target_url("a/../messages")
        == "http://mcp-upstream:3717/mcp/messages"
    )


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
