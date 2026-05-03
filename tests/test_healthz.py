"""Smoke test: /healthz returns 200 and the right shape."""

from __future__ import annotations

from fastapi.testclient import TestClient

from phish_game import __version__
from phish_game.server import build_app


def test_healthz_returns_ok() -> None:
    app = build_app()
    with TestClient(app) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"status": "ok", "version": __version__}
