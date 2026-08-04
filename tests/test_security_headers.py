"""Response security headers: policy construction and live wiring.

Two layers are covered. The pure builders in ``security_headers`` are asserted
directly, and the middleware wiring is asserted against a real rendered
response so a policy that is correct but never attached still fails.

The CSP assertions deliberately check *what the frontend needs*, not just that
a header exists. A CSP that ships and quietly breaks Google Fonts or gtag.js is
worse than no CSP, because the breakage shows up as "the site looks wrong" long
after the deploy.
"""

from __future__ import annotations

from httpx import ASGITransport, AsyncClient

from setlist_stash.config import Settings
from setlist_stash.security_headers import build_csp, build_security_headers
from setlist_stash.server import build_app


def _directives(csp: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in csp.split(";"):
        part = part.strip()
        if not part:
            continue
        name, _, value = part.partition(" ")
        out[name] = value.strip()
    return out


class TestCsp:
    def test_locks_down_the_directives_that_matter_for_xss(self) -> None:
        d = _directives(build_csp(Settings()))
        assert d["default-src"] == "'self'"
        assert d["base-uri"] == "'self'"
        assert d["object-src"] == "'none'"
        assert d["frame-ancestors"] == "'none'"
        assert d["form-action"] == "'self'"

    def test_never_allows_eval(self) -> None:
        # HTMX hx-on attributes would have needed this. The one use was
        # replaced with a delegated listener precisely so it stays out.
        assert "'unsafe-eval'" not in build_csp(Settings())

    def test_allows_the_vendored_htmx_and_google_fonts_the_templates_load(
        self,
    ) -> None:
        d = _directives(build_csp(Settings()))
        # htmx is vendored under /static, so same-origin scripts must be OK.
        assert "'self'" in d["script-src"]
        assert "https://fonts.googleapis.com" in d["style-src"]
        assert "https://fonts.gstatic.com" in d["font-src"]

    def test_analytics_origins_only_appear_when_analytics_is_configured(
        self,
    ) -> None:
        off = build_csp(Settings(analytics_id=""))
        assert "googletagmanager" not in off
        assert "google-analytics" not in off

        on = _directives(build_csp(Settings(analytics_id="G-TEST00000")))
        assert "https://www.googletagmanager.com" in on["script-src"]
        assert "https://www.google-analytics.com" in on["connect-src"]


class TestHsts:
    def test_absent_on_a_plain_http_deployment(self) -> None:
        headers = build_security_headers(Settings(cookie_secure=False))
        assert "Strict-Transport-Security" not in headers

    def test_present_when_the_operator_declares_https(self) -> None:
        headers = build_security_headers(
            Settings(session_secret="x" * 40, cookie_secure=True)  # type: ignore[arg-type]
        )
        assert headers["Strict-Transport-Security"] == "max-age=31536000"

    def test_can_be_disabled_without_disabling_the_rest(self) -> None:
        headers = build_security_headers(
            Settings(
                session_secret="x" * 40,  # type: ignore[arg-type]
                cookie_secure=True,
                hsts_max_age_seconds=0,
            )
        )
        assert "Strict-Transport-Security" not in headers
        assert "Content-Security-Policy" in headers


async def _get_index(settings: Settings) -> dict[str, str]:
    app = build_app(settings)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/")
    assert resp.status_code == 200
    return dict(resp.headers)


class TestMiddlewareWiring:
    async def test_headers_reach_a_real_response(self) -> None:
        headers = await _get_index(Settings())
        assert headers["x-frame-options"] == "DENY"
        assert headers["x-content-type-options"] == "nosniff"
        assert headers["referrer-policy"] == "strict-origin-when-cross-origin"
        assert "default-src 'self'" in headers["content-security-policy"]

    async def test_master_switch_removes_every_header(self) -> None:
        headers = await _get_index(Settings(security_headers=False))
        assert "content-security-policy" not in headers
        assert "x-frame-options" not in headers
        assert "permissions-policy" not in headers
