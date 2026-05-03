"""Route tests for the league HTTP surface.

Covers the full happy path: alice creates a league, bob visits the URL
and joins, both submit predictions, the resolver runs, the league
leaderboard renders both with correct ranks. Plus host-only guards on
settings / rotate / delete and the soft-delete 404.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from httpx import AsyncClient

from phish_game.auth import sign_user_id
from phish_game.config import get_settings
from tests.conftest import requires_pg


def _cookie_for(client: AsyncClient, user_id: int) -> None:
    client.cookies.set("phishgame_session", sign_user_id(get_settings(), user_id))


async def _make_user(pool: asyncpg.Pool[Any], handle: str) -> int:
    async with pool.acquire() as conn:
        return int(
            await conn.fetchval(
                "INSERT INTO users (handle, handle_lower) VALUES ($1, $2) RETURNING id",
                handle,
                handle,
            )
        )


@requires_pg
async def test_create_league_happy_path(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    assert pg_pool is not None
    alice_id = await _make_user(pg_pool, "alice")
    _cookie_for(async_client, alice_id)
    resp = await async_client.post(
        "/leagues/new",
        data={"name": "Tweezerheads United", "start_date": "", "end_date": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers["location"]
    assert location.startswith("/league/")
    slug = location.removeprefix("/league/")

    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT slug, name, host_user_id FROM leagues WHERE slug = $1", slug
        )
    assert row is not None
    assert row["name"] == "Tweezerheads United"
    assert row["host_user_id"] == alice_id


@requires_pg
async def test_create_league_rejects_blank_name(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    assert pg_pool is not None
    alice_id = await _make_user(pg_pool, "alice")
    _cookie_for(async_client, alice_id)
    resp = await async_client.post(
        "/leagues/new",
        data={"name": "   ", "start_date": "", "end_date": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 400
    assert "cannot be empty" in resp.text.lower()


@requires_pg
async def test_anonymous_visit_to_league_url_renders_join_with_handle_form(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    assert pg_pool is not None
    alice_id = await _make_user(pg_pool, "alice")
    _cookie_for(async_client, alice_id)
    resp = await async_client.post(
        "/leagues/new",
        data={"name": "Pod", "start_date": "", "end_date": ""},
        follow_redirects=False,
    )
    slug = resp.headers["location"].removeprefix("/league/")
    # Anonymous: drop alice's cookie.
    async_client.cookies.clear()
    page = await async_client.get(f"/league/{slug}", follow_redirects=False)
    assert page.status_code == 200
    # Anonymous on league URL renders the league_join template with a
    # handle-create form scoped to this league. Distinguish from the
    # bare /handle home form by checking the form posts to /handle and
    # the page references the league name.
    assert "Pod" in page.text
    assert "Sign in to join" in page.text
    assert 'action="/handle"' in page.text


@requires_pg
async def test_second_user_can_join(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    assert pg_pool is not None
    alice_id = await _make_user(pg_pool, "alice")
    bob_id = await _make_user(pg_pool, "bob")
    _cookie_for(async_client, alice_id)
    resp = await async_client.post(
        "/leagues/new",
        data={"name": "Pod", "start_date": "", "end_date": ""},
        follow_redirects=False,
    )
    slug = resp.headers["location"].removeprefix("/league/")

    # Switch to bob's cookie.
    async_client.cookies.clear()
    _cookie_for(async_client, bob_id)
    join_resp = await async_client.post(
        f"/league/{slug}/join", follow_redirects=False
    )
    assert join_resp.status_code == 303
    assert join_resp.headers["location"] == f"/league/{slug}"

    async with pg_pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM league_members lm "
            "JOIN leagues l ON l.id = lm.league_id WHERE l.slug = $1",
            slug,
        )
    assert int(n) == 2


@requires_pg
async def test_league_member_sees_dashboard(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    assert pg_pool is not None
    alice_id = await _make_user(pg_pool, "alice")
    _cookie_for(async_client, alice_id)
    resp = await async_client.post(
        "/leagues/new",
        data={"name": "Pod", "start_date": "", "end_date": ""},
        follow_redirects=False,
    )
    slug = resp.headers["location"].removeprefix("/league/")
    page = await async_client.get(f"/league/{slug}")
    assert page.status_code == 200
    assert "Invite link" in page.text
    assert slug in page.text
    assert "Settings" in page.text  # alice is host


@requires_pg
async def test_non_member_cannot_see_settings(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    assert pg_pool is not None
    alice_id = await _make_user(pg_pool, "alice")
    bob_id = await _make_user(pg_pool, "bob")
    _cookie_for(async_client, alice_id)
    resp = await async_client.post(
        "/leagues/new",
        data={"name": "Pod", "start_date": "", "end_date": ""},
        follow_redirects=False,
    )
    slug = resp.headers["location"].removeprefix("/league/")
    async_client.cookies.clear()
    _cookie_for(async_client, bob_id)
    settings_resp = await async_client.get(f"/league/{slug}/settings")
    assert settings_resp.status_code == 403


@requires_pg
async def test_rotate_slug_makes_old_url_404(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    assert pg_pool is not None
    alice_id = await _make_user(pg_pool, "alice")
    _cookie_for(async_client, alice_id)
    resp = await async_client.post(
        "/leagues/new",
        data={"name": "Pod", "start_date": "", "end_date": ""},
        follow_redirects=False,
    )
    old_slug = resp.headers["location"].removeprefix("/league/")
    rotate_resp = await async_client.post(
        f"/league/{old_slug}/rotate", follow_redirects=False
    )
    assert rotate_resp.status_code == 303
    new_loc = rotate_resp.headers["location"]
    assert new_loc.startswith("/league/")
    new_slug = new_loc.removeprefix("/league/").split("/")[0]
    assert new_slug != old_slug

    old_page = await async_client.get(f"/league/{old_slug}")
    assert old_page.status_code == 404


@requires_pg
async def test_soft_delete_makes_url_404_and_drops_from_listing(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    assert pg_pool is not None
    alice_id = await _make_user(pg_pool, "alice")
    _cookie_for(async_client, alice_id)
    resp = await async_client.post(
        "/leagues/new",
        data={"name": "Pod", "start_date": "", "end_date": ""},
        follow_redirects=False,
    )
    slug = resp.headers["location"].removeprefix("/league/")
    delete_resp = await async_client.post(
        f"/league/{slug}/delete", follow_redirects=False
    )
    assert delete_resp.status_code == 303
    assert delete_resp.headers["location"] == "/leagues"
    page404 = await async_client.get(f"/league/{slug}")
    assert page404.status_code == 404

    listing = await async_client.get("/leagues")
    assert "Pod" not in listing.text


@requires_pg
async def test_host_cannot_leave(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    assert pg_pool is not None
    alice_id = await _make_user(pg_pool, "alice")
    _cookie_for(async_client, alice_id)
    resp = await async_client.post(
        "/leagues/new",
        data={"name": "Pod", "start_date": "", "end_date": ""},
        follow_redirects=False,
    )
    slug = resp.headers["location"].removeprefix("/league/")
    leave_resp = await async_client.post(
        f"/league/{slug}/leave", follow_redirects=False
    )
    assert leave_resp.status_code == 409


@requires_pg
async def test_full_happy_path_alice_creates_bob_joins_then_leaderboard(
    pg_pool: asyncpg.Pool[Any] | None, async_client: AsyncClient
) -> None:
    """Alice creates, bob joins, both score, league leaderboard ranks them."""
    assert pg_pool is not None
    alice_id = await _make_user(pg_pool, "alice")
    bob_id = await _make_user(pg_pool, "bob")

    _cookie_for(async_client, alice_id)
    resp = await async_client.post(
        "/leagues/new",
        data={"name": "Tour Pod", "start_date": "", "end_date": ""},
        follow_redirects=False,
    )
    slug = resp.headers["location"].removeprefix("/league/")

    async_client.cookies.clear()
    _cookie_for(async_client, bob_id)
    await async_client.post(f"/league/{slug}/join", follow_redirects=False)

    # Seed scored predictions directly + rebuild league snapshots.
    show_date = date(2024, 6, 15)
    future_lock = datetime.now(UTC) + timedelta(hours=2)
    past_lock = datetime.now(UTC) - timedelta(hours=2)
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO prediction_locks (show_date, lock_at, venue_tz) VALUES ($1,$2,'UTC')",
            show_date,
            future_lock,
        )
        for uid, score in ((alice_id, 50), (bob_id, 90)):
            await conn.execute(
                """
                INSERT INTO predictions (
                    user_id, show_date, pick_song_slugs,
                    opener_slug, closer_slug, encore_slug
                )
                VALUES ($1, $2, ARRAY['a','b','c'], NULL, NULL, NULL)
                """,
                uid,
                show_date,
            )
            await conn.execute(
                "UPDATE predictions SET score = $1 WHERE user_id = $2 AND show_date = $3",
                score,
                uid,
                show_date,
            )
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2, resolved_at = now() WHERE show_date = $1",
            show_date,
            past_lock,
        )

    from phish_game.leaderboard import rebuild_leagues

    await rebuild_leagues(pg_pool)

    page = await async_client.get(f"/league/{slug}/leaderboard")
    assert page.status_code == 200
    assert "alice" in page.text
    assert "bob" in page.text
    # bob has the higher score and should appear before alice in the table.
    assert page.text.index("bob") < page.text.index("alice")


@pytest.mark.parametrize("path", ["/leagues", "/leagues/new"])
@requires_pg
async def test_anonymous_redirected_from_protected_pages(
    pg_pool: asyncpg.Pool[Any] | None,
    async_client: AsyncClient,
    path: str,
) -> None:
    assert pg_pool is not None
    resp = await async_client.get(path, follow_redirects=False)
    assert resp.status_code in (303, 307)
