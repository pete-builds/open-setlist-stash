"""Sign-in copy: an unconfigured feature is silent, not broken-looking.

No DB required. ``GET /auth/login`` renders for a signed-out visitor without
touching the pool (same as ``/`` in ``test_beta_notice.py``); ``auth_email.html``
is rendered straight through the real Jinja environment because its route
redirects anyone who isn't signed in.

Both hosted tenants run ``EMAIL_PROVIDER=disabled`` with Google OAuth on. Before
this, that combination printed a red "Email is currently disabled on this
server" card under a "Sign in by email" heading, and ``/auth/email`` told the
player to set ``EMAIL_PROVIDER=log``. A player reads that as a broken site, not
as a feature the operator never turned on. The positive control below keeps the
fix from degrading into "the email UI is gone everywhere".
"""

from __future__ import annotations

from httpx import ASGITransport, AsyncClient
from starlette.requests import Request

from setlist_stash.config import Settings
from setlist_stash.deps import build_templates
from setlist_stash.email import DisabledProvider, LogProvider
from setlist_stash.server import build_app

# Anything that names the operator's mail configuration to a player.
CONFIG_LEAKS = (
    "EMAIL_PROVIDER",
    "currently disabled",
    "Email is currently disabled",
)

_GOOGLE_ON = {
    "google_client_id": "test-client-id.apps.googleusercontent.com",
    "google_client_secret": "test-secret",
}


async def _get_login(**overrides: object) -> str:
    settings = Settings(**overrides)  # type: ignore[arg-type]
    transport = ASGITransport(app=build_app(settings))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/auth/login")
    assert resp.status_code == 200
    return resp.text


def _render_auth_email(settings: Settings, provider: object, **ctx: object) -> str:
    """Render auth_email.html outside its route (which requires a session)."""
    templates = build_templates(settings, provider)  # type: ignore[arg-type]
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/auth/email",
            "root_path": "",
            "scheme": "http",
            "query_string": b"",
            "headers": [],
            "server": ("test", 80),
        }
    )
    return templates.get_template("auth_email.html").render(
        request=request, version="test", **ctx
    )


# ---------- /auth/login ----------


async def test_login_page_offers_only_google_when_email_disabled() -> None:
    html = await _get_login(email_provider="disabled", **_GOOGLE_ON)
    assert "/auth/google/start" in html
    assert "Sign in with Google" in html
    assert "Sign in by email" not in html
    assert 'action="/auth/login"' not in html
    for leak in CONFIG_LEAKS:
        assert leak not in html, f"config detail {leak!r} leaked to the player"


async def test_login_page_still_offers_email_when_a_provider_is_set() -> None:
    """Positive control: the fix must not delete the email UI outright."""
    html = await _get_login(email_provider="log", **_GOOGLE_ON)
    assert "Sign in by email" in html
    assert 'action="/auth/login"' in html
    assert "Sign in with Google" in html


async def test_login_page_without_either_method_explains_itself() -> None:
    html = await _get_login(email_provider="disabled")
    assert "Sign in with Google" not in html
    assert "Sign in by email" not in html
    # Not a dead end: the visitor still gets somewhere to go.
    assert "pick a fresh handle" in html
    for leak in CONFIG_LEAKS:
        assert leak not in html


# ---------- /auth/email ----------


def test_auth_email_page_points_at_google_when_email_disabled() -> None:
    settings = Settings(**_GOOGLE_ON)  # type: ignore[arg-type]
    html = _render_auth_email(
        settings, DisabledProvider(), provider_enabled=False, current_user=None
    )
    assert "/auth/google/start" in html
    assert 'action="/auth/email"' not in html
    for leak in CONFIG_LEAKS:
        assert leak not in html, f"config detail {leak!r} leaked to the player"


def test_auth_email_page_still_renders_the_form_when_enabled() -> None:
    """Positive control for the other half of the branch."""
    settings = Settings(email_provider="log", **_GOOGLE_ON)  # type: ignore[arg-type]
    html = _render_auth_email(
        settings, LogProvider(), provider_enabled=True, current_user=None
    )
    assert 'action="/auth/email"' in html
    assert "Send me a link" in html
