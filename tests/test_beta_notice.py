"""Beta-notice gating tests — no DB required.

The beta notice is a deployment-level override injected as a Jinja global
(same mechanism as analytics_id / theme_file / footer_credit). When
``BETA_NOTICE`` is set, the home page renders a subtle ``.beta-notice`` banner;
when unset (the OSS default), NO banner renders at all so the Phish demo and
any third-party self-host stay clean.

These render the real app's ``/`` route (extends base.html / index.html, needs
no DB or cookie — ``_resolve_user`` returns None with no pool, and the upcoming
lock lookup degrades gracefully when the pool isn't up).
"""

from __future__ import annotations

from httpx import ASGITransport, AsyncClient

from setlist_stash.config import Settings
from setlist_stash.server import build_app

NOTICE = "Beta — first live run is tonight in Fairport, NY."


async def _render_index(settings: Settings) -> str:
    app = build_app(settings)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/")
    assert resp.status_code == 200
    return resp.text


async def test_beta_notice_renders_when_set() -> None:
    html = await _render_index(Settings(beta_notice=NOTICE))
    assert NOTICE in html
    assert 'class="beta-notice"' in html


async def test_beta_notice_absent_when_unset() -> None:
    html = await _render_index(Settings(beta_notice=""))
    assert "beta-notice" not in html
