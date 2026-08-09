"""Icon + social-card gating tests — no DB required.

The favicon/touch-icon/og:image filenames are deployment-level overrides
injected as Jinja globals (same mechanism as theme_file / analytics_id). They
used to be hardcoded in base.html, which meant every tenant of the shared
image served the SAME mark: wappypicks.com shipped the Phish demo's icon and
social card under its own domain.

The regression these guard is subtle, because it is invisible from the tenant
that was changed: adding brand artwork is only correct if the deployment that
did NOT ask for it renders exactly what it rendered before.

Same approach as test_analytics: render the real ``/`` route (extends
base.html, needs no DB) so the whole chain is exercised — config setting ->
Jinja global -> base.html.
"""

from __future__ import annotations

from httpx import ASGITransport, AsyncClient

from setlist_stash.config import Settings
from setlist_stash.server import build_app

# Stand-ins for a deployment's own artwork; nothing reads these files here.
CUSTOM = {
    "favicon_svg": "logo-brand.svg",
    "favicon_ico": "favicon-brand.ico",
    "favicon_png": "logo-brand-192.png",
    "apple_touch_icon": "apple-touch-icon-brand.png",
    "og_image": "og-brand.png",
}


async def _render_index(settings: Settings) -> str:
    app = build_app(settings)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/")
    assert resp.status_code == 200
    return resp.text


async def test_icons_default_to_platform_placeholders() -> None:
    """A deployment that sets nothing keeps the neutral platform mark."""
    html = await _render_index(Settings())
    assert '<link rel="icon" type="image/svg+xml" href="/static/logo-t.svg">' in html
    assert (
        '<link rel="alternate icon" type="image/png" href="/static/logo-t-192.png">'
        in html
    )
    assert '<link rel="apple-touch-icon" href="/static/logo-t-192.png">' in html
    assert "/static/og-image.png" in html


async def test_favicon_ico_absent_when_unset() -> None:
    """No .ico link tag at all by default, rather than a dangling reference."""
    html = await _render_index(Settings())
    assert 'rel="icon" sizes=' not in html
    assert ".ico" not in html


async def test_icons_follow_deployment_overrides() -> None:
    html = await _render_index(Settings(**CUSTOM))
    assert '<link rel="icon" type="image/svg+xml" href="/static/logo-brand.svg">' in html
    assert (
        '<link rel="icon" sizes="16x16 32x32 48x48" href="/static/favicon-brand.ico">'
        in html
    )
    assert (
        '<link rel="alternate icon" type="image/png" href="/static/logo-brand-192.png">'
        in html
    )
    assert (
        '<link rel="apple-touch-icon" href="/static/apple-touch-icon-brand.png">' in html
    )


async def test_overriding_one_deployment_leaves_the_placeholders_alone() -> None:
    """The isolation property: custom artwork must not become the default.

    A shared image serves several deployments, so brand artwork has to be
    opted into by name. If this fails, one tenant's mark is leaking onto every
    other tenant.
    """
    html = await _render_index(Settings(**CUSTOM))
    for placeholder in ("logo-t.svg", "logo-t-192.png", "og-image.png"):
        assert placeholder not in html

    default_html = await _render_index(Settings())
    for custom in CUSTOM.values():
        assert custom not in default_html


async def test_og_image_is_an_absolute_url_on_the_deployment_origin() -> None:
    """Crawlers reject a relative og:image, so it must carry BASE_URL.

    An https base_url needs a real session_secret: the config refuses to boot
    a production-looking deployment on the shipped development key.
    """
    html = await _render_index(
        Settings(
            base_url="https://example.test/",
            og_image="og-brand.png",
            session_secret="t" * 48,
        )
    )
    assert (
        '<meta property="og:image" content="https://example.test/static/og-brand.png">'
        in html
    )
    assert (
        '<meta name="twitter:image" content="https://example.test/static/og-brand.png">'
        in html
    )
    assert '<meta name="twitter:card" content="summary_large_image">' in html
