"""The "Connect with Google" entry points on the home page.

Both live deployments (tweezerpicks.com, wappypicks.com) run with
``EMAIL_PROVIDER=disabled``, which makes Google the ONLY route back to a
handle from a second device or after a cookie clear. Before this, the offer
was a muted one-line link under the handle form and nothing at all once you
were signed in, so a new player had no visible way to keep the handle they
had just claimed.

These pin the two places it has to be obvious:

* the anonymous home page, beside the handle form, and
* the page a player lands on the instant they claim a handle, which is the
  home page again with a session cookie.

Plus the two ways it must stay quiet: once the account IS linked the nudge is
gone for good, and a deployment with no Google client configured (the OSS
image, any third-party self-host) renders nothing Google-shaped at all.
"""

from __future__ import annotations

from typing import Any

import asyncpg
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from setlist_stash.config import Settings
from setlist_stash.server import build_app
from tests.conftest import requires_pg

GOOGLE_SETTINGS = Settings(
    google_client_id="test-client-id.apps.googleusercontent.com",
    google_client_secret="test-client-secret",
)
NO_GOOGLE_SETTINGS = Settings(google_client_id="", google_client_secret="")


def _app_with_pool(pool: asyncpg.Pool[Any], settings: Settings) -> FastAPI:
    """``conftest.build_app_with_pool``, but with settings we choose.

    The shared fixture reads settings from the environment, which leaves
    Google disabled. These tests are specifically about the Google-enabled
    deployment, so they inject their own.
    """
    from setlist_stash import db as db_module

    db_module._pool = pool  # type: ignore[attr-defined]
    return build_app(settings)


async def _get(app: FastAPI, path: str, **kwargs: Any) -> str:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(path, **kwargs)
    assert resp.status_code == 200, resp.status_code
    return resp.text


# ----- anonymous home page --------------------------------------------------


async def test_anonymous_home_offers_google_as_a_real_button() -> None:
    """A visitor who has never played sees a Google button, not a text link."""
    html = await _get(build_app(GOOGLE_SETTINGS), "/")
    assert "btn-google" in html
    assert "Continue with Google" in html
    assert 'href="/auth/google/start"' in html
    # The four-colour G mark rides along, so the button is recognizable
    # rather than just another themed rectangle.
    assert "google-g" in html
    # ...and the handle form is still there: Google is the alternative, not a
    # gate in front of anonymous play.
    assert 'action="/handle"' in html


async def test_anonymous_home_stays_clean_without_a_google_client() -> None:
    html = await _get(build_app(NO_GOOGLE_SETTINGS), "/")
    assert "btn-google" not in html
    assert "/auth/google/start" not in html
    assert 'action="/handle"' in html  # anonymous play unaffected


# ----- the page you land on right after claiming a handle -------------------


@requires_pg
async def test_new_handle_lands_on_a_page_offering_to_save_it(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """POST /handle -> follow the redirect -> the offer is on that page.

    This is the whole point: the moment the handle exists is the moment it is
    most at risk, because it lives in exactly one cookie in one browser.
    """
    assert pg_pool is not None
    app = _app_with_pool(pg_pool, GOOGLE_SETTINGS)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/handle", data={"handle": "tweezer_fan"})
        assert resp.status_code == 303
        assert resp.headers["location"] == "/"
        # Same client, so the session cookie POST /handle just set is sent.
        landed = await client.get("/")

    assert landed.status_code == 200
    html = landed.text
    assert "save-account" in html
    assert "Connect with Google" in html
    assert 'href="/auth/google/start"' in html
    # Addressed to the handle they just picked, not a generic pitch.
    assert "tweezer_fan" in html


@requires_pg
async def test_linked_account_is_never_nudged_again(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """Once google_sub is set the card is gone, so it can't nag forever."""
    assert pg_pool is not None
    app = _app_with_pool(pg_pool, GOOGLE_SETTINGS)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/handle", data={"handle": "linked_fan"})
        assert resp.status_code == 303
        async with pg_pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET google_sub = $1 WHERE handle_lower = $2",
                "google-sub-12345",
                "linked_fan",
            )
        landed = await client.get("/")

    assert landed.status_code == 200
    assert "save-account" not in landed.text


@requires_pg
async def test_signed_in_player_sees_no_google_card_without_a_client(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """The OSS image has no Google client, so the card must not appear."""
    assert pg_pool is not None
    app = _app_with_pool(pg_pool, NO_GOOGLE_SETTINGS)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/handle", data={"handle": "oss_fan"})
        assert resp.status_code == 303
        landed = await client.get("/")

    assert landed.status_code == 200
    assert "save-account" not in landed.text
    assert "/auth/google/start" not in landed.text
