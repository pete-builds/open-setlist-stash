"""League module tests.

Mix of pure unit tests (slug shape, name validation, tour-window bounds)
and DB-backed integration tests (create / join / leave / rotate / delete /
member-cap enforcement / host-can't-leave guard).

DB tests gate on ``TEST_PG_DSN``; unit tests always run.
"""

from __future__ import annotations

import secrets
from datetime import date
from typing import Any

import pytest

from setlist_stash.config import Settings
from setlist_stash.leagues import (
    SLUG_WORDLIST,
    LeagueDateWindowError,
    LeagueForbidden,
    LeagueFull,
    LeagueHostCannotLeave,
    LeagueNameError,
    create_league,
    generate_slug,
    get_league_by_slug,
    is_member,
    join_league,
    leave_league,
    list_league_members,
    list_user_leagues,
    member_count,
    normalize_name,
    rotate_slug,
    soft_delete_league,
    update_league,
    validate_window,
)
from tests.conftest import requires_pg

# ---------------------------------------------------------------------------
# Pure unit tests
# ---------------------------------------------------------------------------


def test_normalize_name_strips_and_caps_at_80() -> None:
    assert normalize_name("  Tweezerheads United  ") == "Tweezerheads United"


def test_normalize_name_rejects_empty() -> None:
    with pytest.raises(LeagueNameError):
        normalize_name("   ")


def test_normalize_name_rejects_too_long() -> None:
    with pytest.raises(LeagueNameError):
        normalize_name("x" * 81)


def test_validate_window_allows_both_none() -> None:
    assert validate_window(None, None) == (None, None)


def test_validate_window_allows_only_start() -> None:
    s = date(2026, 6, 1)
    assert validate_window(s, None) == (s, None)


def test_validate_window_allows_equal() -> None:
    d = date(2026, 6, 1)
    assert validate_window(d, d) == (d, d)


def test_validate_window_rejects_inverted() -> None:
    with pytest.raises(LeagueDateWindowError):
        validate_window(date(2026, 7, 1), date(2026, 6, 1))


# ---------------------------------------------------------------------------
# Slug generation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@requires_pg
async def test_generate_slug_shape(pg_pool: Any) -> None:
    slug = await generate_slug(pg_pool)
    word, _, _suffix = slug.partition("-")
    assert word in SLUG_WORDLIST or slug.startswith("x")
    assert len(slug) >= 3


@pytest.mark.asyncio
@requires_pg
async def test_generate_slug_retries_on_collision(
    pg_pool: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force the first candidate to collide and confirm the second is used.

    We seed a row with the candidate the next call to ``_candidate_slug``
    will produce, then verify the returned slug is different.
    """
    rng = secrets.SystemRandom()
    # Pre-create a user to host a league so the slug collision path uses
    # _slug_taken correctly.
    async with pg_pool.acquire() as conn:
        host_id = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ('host_a','host_a') RETURNING id"
        )
        # Plant a slug that matches the wordlist shape so we force at least
        # one retry. The choice is deterministic via a forced rng seed below.
        await conn.execute(
            "INSERT INTO leagues (slug, name, host_user_id) "
            "VALUES ('tweezer-aa', 'Seed', $1)",
            int(host_id),
        )

    # Build an rng that returns ``tweezer`` first, then ``ghost``, with
    # suffix chars 'a','a' (collision) then 'b','c' (fresh).
    seq: list[Any] = ["tweezer", "a", "a", "ghost", "b", "c"]

    class FakeRng:
        def choice(self, items: Any) -> Any:
            return seq.pop(0)

    fake = FakeRng()
    slug = await generate_slug(pg_pool, rng=fake)  # type: ignore[arg-type]
    assert slug == "ghost-bc"
    _ = rng  # the real rng exists; we don't use it here but keep the import shape


@pytest.mark.asyncio
@requires_pg
async def test_generate_slug_falls_back_after_max_attempts(
    pg_pool: Any,
) -> None:
    """If every candidate collides, the helper falls back to ``x<8 hex>``."""

    class AlwaysCollideRng:
        def choice(self, items: Any) -> Any:
            return "tweezer" if items[0] == SLUG_WORDLIST[0] else "a"

    # Pre-populate the slug for every retry attempt.
    async with pg_pool.acquire() as conn:
        host_id = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ('host_b','host_b') RETURNING id"
        )
        await conn.execute(
            "INSERT INTO leagues (slug, name, host_user_id) "
            "VALUES ('tweezer-aa', 'Collide', $1)",
            int(host_id),
        )

    slug = await generate_slug(
        pg_pool, rng=AlwaysCollideRng(), max_attempts=5  # type: ignore[arg-type]
    )
    assert slug.startswith("x")
    assert len(slug) == 9  # "x" + 8 hex


# ---------------------------------------------------------------------------
# DB-backed CRUD
# ---------------------------------------------------------------------------


def _settings(member_cap: int = 500) -> Settings:
    return Settings(league_member_cap=member_cap)


async def _make_user(pool: Any, handle: str) -> int:
    async with pool.acquire() as conn:
        return int(
            await conn.fetchval(
                "INSERT INTO users (handle, handle_lower) VALUES ($1, $2) RETURNING id",
                handle,
                handle,
            )
        )


@pytest.mark.asyncio
@requires_pg
async def test_create_league_makes_host_a_member(pg_pool: Any) -> None:
    host_id = await _make_user(pg_pool, "alice")
    league = await create_league(
        pg_pool,
        name="Tweezerheads United",
        host_user_id=host_id,
        settings=_settings(),
    )
    assert league.name == "Tweezerheads United"
    assert league.host_user_id == host_id
    assert league.member_cap == 500
    assert league.deleted_at is None
    assert await is_member(pg_pool, league.id, host_id) is True
    members = await list_league_members(pg_pool, league.id)
    assert any(m.user_id == host_id and m.role == "host" for m in members)


@pytest.mark.asyncio
@requires_pg
async def test_create_league_rejects_empty_name(pg_pool: Any) -> None:
    host_id = await _make_user(pg_pool, "alice")
    with pytest.raises(LeagueNameError):
        await create_league(
            pg_pool,
            name="   ",
            host_user_id=host_id,
            settings=_settings(),
        )


@pytest.mark.asyncio
@requires_pg
async def test_create_league_rejects_inverted_dates(pg_pool: Any) -> None:
    host_id = await _make_user(pg_pool, "alice")
    with pytest.raises(LeagueDateWindowError):
        await create_league(
            pg_pool,
            name="Bad dates",
            host_user_id=host_id,
            settings=_settings(),
            start_date=date(2026, 7, 1),
            end_date=date(2026, 6, 1),
        )


@pytest.mark.asyncio
@requires_pg
async def test_join_league_idempotent(pg_pool: Any) -> None:
    host_id = await _make_user(pg_pool, "alice")
    bob_id = await _make_user(pg_pool, "bob")
    league = await create_league(
        pg_pool,
        name="Pod",
        host_user_id=host_id,
        settings=_settings(),
    )
    await join_league(pg_pool, league, bob_id)
    await join_league(pg_pool, league, bob_id)  # second is a no-op
    assert await member_count(pg_pool, league.id) == 2


@pytest.mark.asyncio
@requires_pg
async def test_join_league_enforces_cap(pg_pool: Any) -> None:
    host_id = await _make_user(pg_pool, "alice")
    bob_id = await _make_user(pg_pool, "bob")
    carol_id = await _make_user(pg_pool, "carol")
    league = await create_league(
        pg_pool,
        name="Tiny",
        host_user_id=host_id,
        settings=_settings(member_cap=2),
    )
    # alice already in (host). bob joins fine. carol bounces.
    await join_league(pg_pool, league, bob_id)
    with pytest.raises(LeagueFull):
        await join_league(pg_pool, league, carol_id)


@pytest.mark.asyncio
@requires_pg
async def test_leave_league_works_for_non_host(pg_pool: Any) -> None:
    host_id = await _make_user(pg_pool, "alice")
    bob_id = await _make_user(pg_pool, "bob")
    league = await create_league(
        pg_pool,
        name="Pod",
        host_user_id=host_id,
        settings=_settings(),
    )
    await join_league(pg_pool, league, bob_id)
    await leave_league(pg_pool, league, bob_id)
    assert await is_member(pg_pool, league.id, bob_id) is False


@pytest.mark.asyncio
@requires_pg
async def test_host_cannot_leave(pg_pool: Any) -> None:
    host_id = await _make_user(pg_pool, "alice")
    league = await create_league(
        pg_pool,
        name="Pod",
        host_user_id=host_id,
        settings=_settings(),
    )
    with pytest.raises(LeagueHostCannotLeave):
        await leave_league(pg_pool, league, host_id)


@pytest.mark.asyncio
@requires_pg
async def test_rotate_slug_changes_url_keeps_members(pg_pool: Any) -> None:
    host_id = await _make_user(pg_pool, "alice")
    bob_id = await _make_user(pg_pool, "bob")
    league = await create_league(
        pg_pool,
        name="Pod",
        host_user_id=host_id,
        settings=_settings(),
    )
    await join_league(pg_pool, league, bob_id)
    old_slug = league.slug

    new_slug = await rotate_slug(pg_pool, league, host_user_id=host_id)
    assert new_slug != old_slug

    # Old slug 404s.
    assert await get_league_by_slug(pg_pool, old_slug) is None
    # New slug resolves and members survive.
    refreshed = await get_league_by_slug(pg_pool, new_slug)
    assert refreshed is not None
    assert await is_member(pg_pool, refreshed.id, bob_id) is True


@pytest.mark.asyncio
@requires_pg
async def test_rotate_slug_only_host(pg_pool: Any) -> None:
    host_id = await _make_user(pg_pool, "alice")
    bob_id = await _make_user(pg_pool, "bob")
    league = await create_league(
        pg_pool,
        name="Pod",
        host_user_id=host_id,
        settings=_settings(),
    )
    with pytest.raises(LeagueForbidden):
        await rotate_slug(pg_pool, league, host_user_id=bob_id)


@pytest.mark.asyncio
@requires_pg
async def test_soft_delete_makes_slug_404(pg_pool: Any) -> None:
    host_id = await _make_user(pg_pool, "alice")
    league = await create_league(
        pg_pool,
        name="Pod",
        host_user_id=host_id,
        settings=_settings(),
    )
    await soft_delete_league(pg_pool, league, host_user_id=host_id)
    assert await get_league_by_slug(pg_pool, league.slug) is None


@pytest.mark.asyncio
@requires_pg
async def test_soft_delete_only_host(pg_pool: Any) -> None:
    host_id = await _make_user(pg_pool, "alice")
    bob_id = await _make_user(pg_pool, "bob")
    league = await create_league(
        pg_pool,
        name="Pod",
        host_user_id=host_id,
        settings=_settings(),
    )
    with pytest.raises(LeagueForbidden):
        await soft_delete_league(pg_pool, league, host_user_id=bob_id)


@pytest.mark.asyncio
@requires_pg
async def test_update_league_name_and_dates(pg_pool: Any) -> None:
    host_id = await _make_user(pg_pool, "alice")
    league = await create_league(
        pg_pool,
        name="Old name",
        host_user_id=host_id,
        settings=_settings(),
    )
    new_league = await update_league(
        pg_pool,
        league,
        host_user_id=host_id,
        name="New name",
        start_date=date(2026, 6, 1),
        end_date=date(2026, 9, 1),
    )
    assert new_league.name == "New name"
    assert new_league.start_date == date(2026, 6, 1)
    assert new_league.end_date == date(2026, 9, 1)


@pytest.mark.asyncio
@requires_pg
async def test_list_user_leagues_excludes_deleted(pg_pool: Any) -> None:
    host_id = await _make_user(pg_pool, "alice")
    a = await create_league(
        pg_pool, name="Keep", host_user_id=host_id, settings=_settings()
    )
    b = await create_league(
        pg_pool, name="Trash", host_user_id=host_id, settings=_settings()
    )
    await soft_delete_league(pg_pool, b, host_user_id=host_id)
    out = await list_user_leagues(pg_pool, host_id)
    slugs = {row.slug for row in out}
    assert a.slug in slugs
    assert b.slug not in slugs
