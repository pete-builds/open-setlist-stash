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

import xml.etree.ElementTree as ET

import pytest
from httpx import ASGITransport, AsyncClient

from setlist_stash.config import Settings
from setlist_stash.server import build_app
from setlist_stash.web_helpers import STATIC_DIR

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


@pytest.mark.parametrize(
    "svg_path", sorted(STATIC_DIR.glob("*.svg")), ids=lambda p: p.name
)
def test_shipped_svgs_are_well_formed_xml(svg_path) -> None:
    """Every shipped .svg must parse as strict XML.

    A standalone .svg is served as image/svg+xml and parsed in XML mode, not
    HTML mode, so any well-formedness error makes the whole file fail to
    render and the browser falls back to its generic globe. The trap is that
    nothing else complains: the file still serves 200 with the right
    content-type, and the markup referencing it is correct, so the site looks
    fully wired while showing no icon.

    This shipped for real on wappypicks.com, whose mark carried a doubled
    hyphen inside an XML comment (writing CSS custom properties as ``--name``
    in a note about the palette). A doubled hyphen may not appear inside an
    XML comment.
    """
    # S314: the input is a first-party asset checked into this repo, not
    # untrusted data, and strict-XML parity with the browser is the point.
    ET.parse(svg_path)  # noqa: S314


async def test_favicon_ico_route_resolves_through_deployment_config() -> None:
    """/favicon.ico must serve bytes, not 404.

    Browser chrome outside the tab (bookmarks, history, the new-tab grid),
    crawlers and link unfurlers request the bare path regardless of the
    <link> tags, so a 404 here shows the generic globe on a page whose markup
    is perfectly correct.
    """
    app = build_app(Settings(favicon_ico="favicon-tweezer.ico"))
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/favicon.ico")
    assert resp.status_code == 200
    # Real .ico magic number, so a missing/renamed asset cannot pass as bytes.
    assert resp.content[:4] == b"\x00\x00\x01\x00"


async def test_favicon_ico_route_falls_back_when_no_ico_is_configured() -> None:
    """Deployments that ship no .ico still answer the path with their PNG."""
    app = build_app(Settings())  # favicon_ico defaults to empty
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/favicon.ico")
    assert resp.status_code == 200
    assert resp.content[:8] == b"\x89PNG\r\n\x1a\n"


async def test_favicon_ico_route_refuses_a_path_escaping_static() -> None:
    """A config value must not be able to read outside the static dir.

    Every fallback is blanked so the traversal attempt is the only candidate;
    otherwise a healthy fallback would serve 200 and mask the escape.
    """
    app = build_app(
        Settings(
            favicon_ico="../../../etc/passwd",
            favicon_png="",
            favicon_svg="",
        )
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/favicon.ico")
    assert resp.status_code == 404


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
