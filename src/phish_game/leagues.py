"""Private leagues (Phase 4c).

A league is a private prediction pool. Anyone with the league URL can
join. Predictions are still GLOBAL: one prediction per user per show,
scored once, but counted in every league the user belongs to. The
leaderboard for a league filters to its members and (optionally) to a
tour-date window set by the host.

Design decisions baked in here:

- Slug is a short, readable, phish-themed token (e.g. ``tweezer-7k``).
  The slug doubles as the invite. There is no separate ``?code=`` param.
- Hosts can rotate the slug; old slug stops resolving immediately.
- 500-member soft cap by default; overridable per-league via
  ``leagues.member_cap`` and globally via ``LEAGUE_MEMBER_CAP``.
- Soft delete only. Predictions are never cascade-deleted; the league
  just disappears from member listings.
- Host-leave is blocked (delete or transfer-host instead). Transfer is
  out of scope for Phase 4c.

This module is stateless except for the ``slug_alphabet`` constants. All
DB writes go through the asyncpg pool the caller hands us.
"""

from __future__ import annotations

import logging
import secrets
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import asyncpg

from phish_game.config import Settings

logger = logging.getLogger("phish_game.leagues")


# ----- errors ---------------------------------------------------------------


class LeagueError(ValueError):
    """Base for user-fixable league validation errors."""


class LeagueNotFound(LeagueError):
    """Slug doesn't resolve to an active league."""


class LeagueNameError(LeagueError):
    """Name is empty, too long, or otherwise rejected."""


class LeagueDateWindowError(LeagueError):
    """start_date is after end_date."""


class LeagueFull(LeagueError):
    """Member cap reached."""


class LeagueHostCannotLeave(LeagueError):
    """Host tried to leave; must delete or transfer first."""


class LeagueForbidden(LeagueError):
    """A non-host user tried to perform a host-only action."""


# ----- types ----------------------------------------------------------------


@dataclass(frozen=True)
class League:
    id: int
    slug: str
    name: str
    host_user_id: int
    member_cap: int
    start_date: date | None
    end_date: date | None
    created_at: datetime
    deleted_at: datetime | None


@dataclass(frozen=True)
class LeagueMember:
    league_id: int
    user_id: int
    handle: str
    role: str
    joined_at: datetime


@dataclass(frozen=True)
class LeagueSummary:
    """Lightweight projection used by membership listings."""

    id: int
    slug: str
    name: str
    role: str
    member_count: int


# ----- slug generation ------------------------------------------------------

# Phish-themed wordlist. Kept short, recognizable, lowercase, hyphenless.
# Picking a word at random gives the slug a friendly, sharable feel; the
# 2-char alphanumeric suffix keeps collisions rare without forcing the slug
# above ~10 chars total.
SLUG_WORDLIST: tuple[str, ...] = (
    "tweezer",
    "ghost",
    "mikes",
    "harry",
    "fluff",
    "bouncing",
    "divided",
    "weekapaug",
    "hood",
    "antelope",
    "suzy",
    "bowie",
    "reba",
    "runlike",
    "possum",
    "free",
    "scent",
    "character",
    "alsosprach",
    "golgi",
    "runaway",
    "wilson",
    "llama",
    "sample",
    "slave",
    "tube",
    "simple",
    "yamar",
    "julius",
    "halleys",
    "horse",
    "silent",
    "moma",
    "stash",
    "melt",
    "swept",
    "fast",
    "cars",
    "loving",
    "cup",
)

# Lowercase alphanumeric, no look-alikes. Avoids 0/o, 1/l/i, etc., to keep
# slugs readable when shared verbally.
_SUFFIX_ALPHABET = "23456789abcdefghjkmnpqrstuvwxyz"


def _suffix(rng: secrets.SystemRandom, length: int = 2) -> str:
    return "".join(rng.choice(_SUFFIX_ALPHABET) for _ in range(length))


def _candidate_slug(rng: secrets.SystemRandom) -> str:
    """One candidate slug from the wordlist + 2-char suffix."""
    word = rng.choice(SLUG_WORDLIST)
    return f"{word}-{_suffix(rng)}"


async def _slug_taken(pool: asyncpg.Pool[Any], slug: str) -> bool:
    """Treat soft-deleted slugs as still-taken so old links never silently
    map to a brand-new league after a delete.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM leagues WHERE slug = $1",
            slug,
        )
    return row is not None


async def generate_slug(
    pool: asyncpg.Pool[Any],
    *,
    rng: secrets.SystemRandom | None = None,
    max_attempts: int = 5,
) -> str:
    """Return a unique league slug. Retries on collision.

    After ``max_attempts`` collisions, falls back to a fully-random
    8-char token (``slug = "x" + secrets.token_hex(4)``) so generation
    can never deadlock against an unlucky alphabet exhaustion.

    The fallback prefix ``x`` keeps the slug from ever colliding with a
    ``word-suffix`` shaped slug, so future word-list expansions are safe.
    """
    rng = rng or secrets.SystemRandom()
    for _ in range(max_attempts):
        candidate = _candidate_slug(rng)
        if not await _slug_taken(pool, candidate):
            return candidate
    # Fallback: random 8-char hex with a non-wordlist prefix.
    fallback = f"x{secrets.token_hex(4)}"
    while await _slug_taken(pool, fallback):
        # Vanishingly unlikely to loop, but stay deterministic.
        fallback = f"x{secrets.token_hex(4)}"
    return fallback


# ----- validation -----------------------------------------------------------


def normalize_name(raw: str) -> str:
    """Trim and validate a league name. 1-80 chars after strip."""
    name = (raw or "").strip()
    if not name:
        raise LeagueNameError("League name cannot be empty.")
    if len(name) > 80:
        raise LeagueNameError("League name is too long (max 80 characters).")
    return name


def validate_window(
    start_date: date | None, end_date: date | None
) -> tuple[date | None, date | None]:
    """Return the (start, end) pair after sanity-checking ordering."""
    if start_date and end_date and start_date > end_date:
        raise LeagueDateWindowError("Start date must be on or before end date.")
    return start_date, end_date


# ----- create / read --------------------------------------------------------


async def create_league(
    pool: asyncpg.Pool[Any],
    *,
    name: str,
    host_user_id: int,
    settings: Settings,
    start_date: date | None = None,
    end_date: date | None = None,
) -> League:
    """Create a league + add the host as a member. One transaction."""
    canonical = normalize_name(name)
    start, end = validate_window(start_date, end_date)
    slug = await generate_slug(pool)
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            """
            INSERT INTO leagues
                (slug, name, host_user_id, member_cap, start_date, end_date)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, slug, name, host_user_id, member_cap,
                      start_date, end_date, created_at, deleted_at
            """,
            slug,
            canonical,
            host_user_id,
            settings.league_member_cap,
            start,
            end,
        )
        if row is None:
            raise LeagueError("Could not create league (no row returned).")
        await conn.execute(
            """
            INSERT INTO league_members (league_id, user_id, role)
            VALUES ($1, $2, 'host')
            ON CONFLICT DO NOTHING
            """,
            int(row["id"]),
            host_user_id,
        )
    logger.info(
        "league created",
        extra={"slug": row["slug"], "host_user_id": host_user_id},
    )
    return _row_to_league(row)


async def get_league_by_slug(
    pool: asyncpg.Pool[Any], slug: str
) -> League | None:
    """Return the active league for a slug, or None.

    Soft-deleted leagues are NOT returned (the URL 404s).
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, slug, name, host_user_id, member_cap,
                   start_date, end_date, created_at, deleted_at
              FROM leagues
             WHERE slug = $1
               AND deleted_at IS NULL
            """,
            slug,
        )
    if row is None:
        return None
    return _row_to_league(row)


async def get_league_by_id(
    pool: asyncpg.Pool[Any], league_id: int
) -> League | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, slug, name, host_user_id, member_cap,
                   start_date, end_date, created_at, deleted_at
              FROM leagues
             WHERE id = $1
               AND deleted_at IS NULL
            """,
            league_id,
        )
    if row is None:
        return None
    return _row_to_league(row)


def _row_to_league(row: Any) -> League:
    return League(
        id=int(row["id"]),
        slug=str(row["slug"]),
        name=str(row["name"]),
        host_user_id=int(row["host_user_id"]),
        member_cap=int(row["member_cap"]),
        start_date=row["start_date"],
        end_date=row["end_date"],
        created_at=row["created_at"],
        deleted_at=row["deleted_at"],
    )


# ----- membership -----------------------------------------------------------


async def member_count(pool: asyncpg.Pool[Any], league_id: int) -> int:
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM league_members WHERE league_id = $1",
            league_id,
        )
    return int(n or 0)


async def is_member(
    pool: asyncpg.Pool[Any], league_id: int, user_id: int
) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT 1
              FROM league_members
             WHERE league_id = $1 AND user_id = $2
            """,
            league_id,
            user_id,
        )
    return row is not None


async def get_role(
    pool: asyncpg.Pool[Any], league_id: int, user_id: int
) -> str | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT role FROM league_members
             WHERE league_id = $1 AND user_id = $2
            """,
            league_id,
            user_id,
        )
    if row is None:
        return None
    return str(row["role"])


async def join_league(
    pool: asyncpg.Pool[Any], league: League, user_id: int
) -> None:
    """Add a member. Idempotent. Enforces member_cap.

    Race: between the count and the insert, another joiner could push the
    league over cap. We fold the cap check into the INSERT via a CTE so
    only one transaction can win the last slot.
    """
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            """
            WITH existing AS (
                SELECT 1 AS already_member
                  FROM league_members
                 WHERE league_id = $1 AND user_id = $2
            ),
            count_now AS (
                SELECT COUNT(*) AS n FROM league_members WHERE league_id = $1
            ),
            ins AS (
                INSERT INTO league_members (league_id, user_id, role)
                SELECT $1, $2, 'member'
                  FROM count_now
                 WHERE NOT EXISTS (SELECT 1 FROM existing)
                   AND count_now.n < $3
                RETURNING 1
            )
            SELECT
                (SELECT 1 FROM existing) AS was_member,
                (SELECT 1 FROM ins)      AS inserted,
                (SELECT n FROM count_now) AS count_before
            """,
            league.id,
            user_id,
            league.member_cap,
        )
    if row is None:
        # Defensive: the SELECT always returns a row.
        raise LeagueError("join_league produced no result row.")
    if row["was_member"]:
        return  # idempotent
    if row["inserted"]:
        return
    # Not a member, not inserted: cap was the blocker.
    raise LeagueFull(
        f"This league is full ({league.member_cap} members)."
    )


async def leave_league(
    pool: asyncpg.Pool[Any], league: League, user_id: int
) -> None:
    """Remove a member. Refuses to remove the host."""
    if user_id == league.host_user_id:
        raise LeagueHostCannotLeave(
            "Hosts can't leave their own league. "
            "Delete the league or transfer host first."
        )
    async with pool.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM league_members
             WHERE league_id = $1 AND user_id = $2
            """,
            league.id,
            user_id,
        )


async def list_user_leagues(
    pool: asyncpg.Pool[Any], user_id: int
) -> list[LeagueSummary]:
    """Return every active league the user is a member of."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT l.id, l.slug, l.name, lm.role,
                   (SELECT COUNT(*) FROM league_members
                     WHERE league_id = l.id) AS member_count
              FROM league_members lm
              JOIN leagues l ON l.id = lm.league_id
             WHERE lm.user_id = $1
               AND l.deleted_at IS NULL
             ORDER BY l.created_at DESC
            """,
            user_id,
        )
    return [
        LeagueSummary(
            id=int(r["id"]),
            slug=str(r["slug"]),
            name=str(r["name"]),
            role=str(r["role"]),
            member_count=int(r["member_count"]),
        )
        for r in rows
    ]


async def list_league_members(
    pool: asyncpg.Pool[Any], league_id: int, *, limit: int = 500
) -> list[LeagueMember]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT lm.league_id, lm.user_id, u.handle, lm.role, lm.joined_at
              FROM league_members lm
              JOIN users u ON u.id = lm.user_id
             WHERE lm.league_id = $1
             ORDER BY (lm.role = 'host') DESC, lm.joined_at ASC
             LIMIT $2
            """,
            league_id,
            limit,
        )
    return [
        LeagueMember(
            league_id=int(r["league_id"]),
            user_id=int(r["user_id"]),
            handle=str(r["handle"]),
            role=str(r["role"]),
            joined_at=r["joined_at"],
        )
        for r in rows
    ]


# ----- host-only actions ----------------------------------------------------


async def rotate_slug(
    pool: asyncpg.Pool[Any], league: League, *, host_user_id: int
) -> str:
    """Replace the league's slug with a freshly-generated one.

    Existing members keep their membership. Old slug 404s after this.
    Returns the new slug.
    """
    if league.host_user_id != host_user_id:
        raise LeagueForbidden("Only the host can rotate the invite slug.")
    new_slug = await generate_slug(pool)
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE leagues SET slug = $1 WHERE id = $2",
            new_slug,
            league.id,
        )
    logger.info(
        "league slug rotated",
        extra={
            "league_id": league.id,
            "old_slug": league.slug,
            "new_slug": new_slug,
        },
    )
    return new_slug


async def update_league(
    pool: asyncpg.Pool[Any],
    league: League,
    *,
    host_user_id: int,
    name: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> League:
    """Host-only edit of name + tour window.

    ``None`` (the default) for the date fields means "clear the date".
    Phase 4c form posts always re-submit both fields explicitly, so
    there's no need for an "unchanged" sentinel.
    """
    if league.host_user_id != host_user_id:
        raise LeagueForbidden("Only the host can edit the league.")

    new_name = league.name if name is None else normalize_name(name)
    new_start, new_end = validate_window(start_date, end_date)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE leagues
               SET name = $2,
                   start_date = $3,
                   end_date = $4
             WHERE id = $1
            RETURNING id, slug, name, host_user_id, member_cap,
                      start_date, end_date, created_at, deleted_at
            """,
            league.id,
            new_name,
            new_start,
            new_end,
        )
    if row is None:
        raise LeagueError("League update returned no row.")
    return _row_to_league(row)


async def soft_delete_league(
    pool: asyncpg.Pool[Any], league: League, *, host_user_id: int
) -> None:
    """Soft-delete the league. Predictions are NOT touched."""
    if league.host_user_id != host_user_id:
        raise LeagueForbidden("Only the host can delete the league.")
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE leagues SET deleted_at = now()
             WHERE id = $1 AND deleted_at IS NULL
            """,
            league.id,
        )
    logger.info(
        "league soft-deleted",
        extra={"league_id": league.id, "slug": league.slug},
    )
