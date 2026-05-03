"""Smoke test: /healthz returns 200 and includes version + dep health.

The DB ping and mcp-phish ping report ``reachable: false`` here (no DB,
no MCP) but the endpoint still returns 200 and the body has the expected
keys. Live deploy verification covers the reachable=true path.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from phish_game import __version__
from phish_game.server import build_app


def test_healthz_returns_ok_with_dep_summary() -> None:
    app = build_app()
    # Avoid running lifespan (no DB available in unit test env).
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["version"] == __version__
    assert "status" in body
    assert "db" in body
    assert "mcp_phish" in body
