"""Analytics (GA4) gating tests — no DB required.

The GA4 measurement ID is a deployment-level override injected as a Jinja
global (same mechanism as theme_file / footer_credit / has_blog). When
``ANALYTICS_ID`` is set, every page renders the gtag.js snippet; when unset
(the OSS default), NO analytics tag renders at all so the Phish demo and any
third-party self-host stay clean.

These tests render the real app's ``/`` route (which extends base.html and
needs no DB or cookie — ``_resolve_user`` returns None with no pool). That
exercises the full chain: config setting -> server Jinja global -> base.html
conditional.
"""

from __future__ import annotations

from httpx import ASGITransport, AsyncClient

from setlist_stash.config import Settings
from setlist_stash.server import build_app

MEASUREMENT_ID = "G-TEST00000"


async def _render_index(settings: Settings) -> str:
    app = build_app(settings)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/")
    assert resp.status_code == 200
    return resp.text


async def test_analytics_renders_when_set() -> None:
    html = await _render_index(Settings(analytics_id=MEASUREMENT_ID))
    # The gtag.js loader with the id in the src.
    assert (
        f"https://www.googletagmanager.com/gtag/js?id={MEASUREMENT_ID}" in html
    )
    # The inline config call with the id.
    assert f"gtag('config', '{MEASUREMENT_ID}')" in html


async def test_analytics_absent_when_unset() -> None:
    html = await _render_index(Settings(analytics_id=""))
    # Default (empty) => no analytics tag anywhere on the page.
    assert "googletagmanager" not in html
    assert "gtag(" not in html
