"""DEMO TOOL: seed demo players and a scored past show into the DEMO database.

NOT a deploy artifact. It is the ``demo-seed`` step of
``docker-compose.demo.yml`` and runs once, before the game starts, so a visitor
lands on a leaderboard that already has people on it.

Safety: it REFUSES to touch any database whose name is not
``setlist_stash_demo`` (exit code 2, nothing written, not even migrations). The
name is read from the server with ``SELECT current_database()``, not from an
environment variable, so a mistyped ``PG_DB`` or a stray ``.env`` cannot point
it at a real deployment. Both production tenants use other database names.

What it does, through the same code paths a real show night uses:
  1. Applies migrations (idempotent).
  2. Asks the data server (``MCP_PHISH_URL``) which shows have already been
     played, and takes the two most recent.
  3. Creates four demo players (handles start with ``demo_``) and gives each
     one a set of picks for those shows. Picks are inserted while the show is
     still open, then the show's cutoff is moved back to its real, past value,
     because the database refuses picks for a locked show.
  4. Runs one resolver tick (``setlist_stash.resolve.run_tick``), which reads
     each setlist from the data server, scores every pick with the real
     scoring rules, finalizes the shows and rebuilds the leaderboards.
  5. Prints the all-time leaderboard, and exits non-zero if nothing was scored,
     so the demo never starts on an empty board without saying so.

Idempotent: a show that already has a lock row is left alone, so restarting
the demo does not duplicate anything.

Usage (the demo compose file runs this for you):
    docker compose -f docker-compose.demo.yml up --build
By hand, against a local Postgres that has a ``setlist_stash_demo`` database
and a running demo data server:
    PG_HOST=127.0.0.1 PG_DB=setlist_stash_demo PG_USER=... PG_PASSWORD=... \
    MCP_PHISH_URL=http://127.0.0.1:3790/mcp python scripts/seed_leaderboard_demo.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

# Make the script runnable from a checkout without `pip install -e .`.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent / "src"))

import asyncpg  # noqa: E402

from setlist_stash import db as db_module  # noqa: E402
from setlist_stash.config import Settings, get_settings  # noqa: E402
from setlist_stash.leaderboard import (  # noqa: E402
    derive_tour_season_key,
    fetch_leaderboard,
    latest_scope_key,
)
from setlist_stash.locks import compute_default_lock_at, resolve_venue_tz  # noqa: E402
from setlist_stash.mcp_client import McpPhishClient  # noqa: E402
from setlist_stash.migrate import run_migrations  # noqa: E402

logger = logging.getLogger("seed_leaderboard_demo")

#: The only database this script will write to.
DEMO_DB_NAME = "setlist_stash_demo"
#: Exit code when the target is not the demo database.
EXIT_NOT_DEMO = 2

DEMO_HANDLES = ("demo_alice", "demo_bob", "demo_carol", "demo_dana")

# Picks per player, for the most recent past show (index 0) and the one before
# it (index 1): (song slugs, encore call). The slugs are songs of the demo
# data server's made-up band. Whether each one scores is decided by the real
# scoring rules against the setlist the data server returns, not here.
DEMO_PICKS: dict[str, tuple[tuple[list[str], str] | None, ...]] = {
    "demo_alice": (
        (
            ["cardboard-comet", "echo-pocket", "placeholder-blues", "kite-engine", "moth-radio"],
            "placeholder-blues",
        ),
        (["moth-radio", "kite-engine", "the-lighthouse-is-a-rumor"], "the-lighthouse-is-a-rumor"),
    ),
    "demo_bob": (
        (["the-long-hallway", "velvet-weather", "hello-world-jam"], "velvet-weather"),
        (["static-garden", "velvet-weather", "cardboard-comet", "echo-pocket"], "cardboard-comet"),
    ),
    "demo_carol": (
        (["static-garden", "lemon-telegram", "tin-can-telescope", "borrowed-thunder"],
         "static-garden"),
        (
            [
                "borrowed-thunder",
                "midnight-inventory",
                "lorem-ipsum-shuffle",
                "the-lighthouse-is-a-rumor",
                "paper-satellites",
            ],
            "paper-satellites",
        ),
    ),
    "demo_dana": (
        (
            ["slow-parade", "gravel-and-honey", "sample-text-stomp", "example-dot-com",
             "hello-world-jam"],
            "hello-world-jam",
        ),
        None,  # sat this one out, so the boards show different show counts
    ),
}

McpFactory = Callable[[Settings], McpPhishClient]


class NotDemoDatabase(RuntimeError):
    """The connected database is not the demo database."""


def check_demo_database(name: str) -> None:
    """Raise :class:`NotDemoDatabase` unless ``name`` is exactly the demo DB."""
    if name != DEMO_DB_NAME:
        raise NotDemoDatabase(
            f"refusing to seed database {name!r}: this script only writes to "
            f"{DEMO_DB_NAME!r}, the database created by docker-compose.demo.yml"
        )


async def assert_demo_database(conn: Any) -> str:
    """Ask the server which database this connection is on, and check it."""
    name = str(await conn.fetchval("SELECT current_database()"))
    check_demo_database(name)
    return name


def _default_mcp(settings: Settings) -> McpPhishClient:
    return McpPhishClient(
        settings.mcp_phish_url, timeout_seconds=settings.mcp_phish_timeout_seconds
    )


async def _recent_past_shows(
    mcp: McpPhishClient, today: date, count: int
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for year in (today.year - 1, today.year):
        rows.extend(await mcp.search_shows(year=year, limit=200))
    past: list[tuple[date, dict[str, Any]]] = []
    for row in rows:
        try:
            d = date.fromisoformat(str(row["date"]))
        except (KeyError, ValueError):
            continue
        if d < today:
            past.append((d, row))
    past.sort(key=lambda pair: pair[0], reverse=True)
    return [row for _, row in past[:count]]


async def _seed_show(
    pool: asyncpg.Pool[Any],
    settings: Settings,
    show: dict[str, Any],
    picks_index: int,
    user_ids: dict[str, int],
) -> bool:
    """Open, fill and re-lock one past show. False if it was already seeded."""
    show_date = date.fromisoformat(str(show["date"]))
    venue_tz = resolve_venue_tz(show.get("location"), settings.default_lock_tz)
    real_lock = compute_default_lock_at(show_date, settings, venue_tz=venue_tz)
    async with pool.acquire() as conn, conn.transaction():
        exists = await conn.fetchval(
            "SELECT 1 FROM prediction_locks WHERE show_date = $1", show_date
        )
        if exists:
            return False
        # Open the show briefly: the lock-guard trigger rejects any pick
        # written after the cutoff, which for a past show has already passed.
        await conn.execute(
            """
            INSERT INTO prediction_locks
                (show_date, show_id, lock_at, venue_tz, tour_season_key)
            VALUES ($1, $2, $3, $4, $5)
            """,
            show_date,
            str(show.get("show_id") or "") or None,
            datetime.now(UTC) + timedelta(hours=1),
            venue_tz,
            derive_tour_season_key(show.get("tour_name"), show_date),
        )
        for handle, per_show in DEMO_PICKS.items():
            entry = per_show[picks_index]
            if entry is None:
                continue
            slugs, encore = entry
            await conn.execute(
                """
                INSERT INTO predictions (user_id, show_date, pick_song_slugs, encore_slug)
                VALUES ($1, $2, $3, $4)
                """,
                user_ids[handle],
                show_date,
                sorted(slugs),
                encore,
            )
        # Close it again at its real cutoff.
        await conn.execute(
            "UPDATE prediction_locks SET lock_at = $2 WHERE show_date = $1",
            show_date,
            real_lock,
        )
    return True


async def seed_demo(
    settings: Settings, *, mcp_factory: McpFactory = _default_mcp
) -> int:
    """Seed the demo database. Returns a process exit code."""
    # Guard first, on a bare connection, before migrations or anything else.
    conn = await asyncpg.connect(dsn=settings.pg_dsn)
    try:
        name = await assert_demo_database(conn)
    except NotDemoDatabase as exc:
        print(f"seed_leaderboard_demo: {exc}", file=sys.stderr)
        return EXIT_NOT_DEMO
    finally:
        await conn.close()
    logger.info("target is the demo database", extra={"db": name})

    from setlist_stash.resolve import run_tick  # after the guard: imports the app

    pool = await db_module.init_pool(settings)
    try:
        # init_pool reuses a pool that already exists in this process, and the
        # resolver tick below writes through that global pool, so check the
        # database it is actually on as well, not only the probe connection.
        try:
            await assert_demo_database(pool)
        except NotDemoDatabase as exc:
            print(f"seed_leaderboard_demo: {exc}", file=sys.stderr)
            return EXIT_NOT_DEMO
        await run_migrations(pool)
        today = datetime.now(tz=ZoneInfo(settings.display_tz)).date()
        async with mcp_factory(settings) as mcp:
            shows = await _recent_past_shows(mcp, today, count=2)
        if not shows:
            print("seed_leaderboard_demo: the data server has no past shows", file=sys.stderr)
            return 1

        user_ids: dict[str, int] = {}
        async with pool.acquire() as conn2:
            for handle in DEMO_HANDLES:
                user_ids[handle] = int(
                    await conn2.fetchval(
                        """
                        INSERT INTO users (handle, handle_lower) VALUES ($1, $2)
                        ON CONFLICT (handle_lower) DO UPDATE SET last_seen_at = now()
                        RETURNING id
                        """,
                        handle,
                        handle.lower(),
                    )
                )
        for i, show in enumerate(shows):
            seeded = await _seed_show(pool, settings, show, i, user_ids)
            logger.info(
                "demo show %s: %s", show["date"], "seeded" if seeded else "already seeded"
            )

        result = await run_tick(settings)
        logger.info(
            "resolver tick: status=%s shows_resolved=%s predictions_scored=%s",
            result.status,
            result.shows_resolved,
            result.predictions_scored,
        )

        key = await latest_scope_key(pool, "all_time")
        rows = await fetch_leaderboard(pool, "all_time", key, limit=20) if key else []
        print("\nAll-time leaderboard (demo):")
        for r in rows:
            print(f"  #{r.rank}  {r.handle:<12} score={r.total_score:>3}  shows={r.shows_played}")
        if not rows:
            print("seed_leaderboard_demo: nothing was scored", file=sys.stderr)
            return 1
        return 0
    finally:
        await db_module.close_pool()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return asyncio.run(seed_demo(get_settings()))


if __name__ == "__main__":
    sys.exit(main())
