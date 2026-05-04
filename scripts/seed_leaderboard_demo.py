"""DEV TOOL — seed demo leaderboard data and rebuild snapshots.

NOT a deploy artifact. Use to verify the leaderboard renders without waiting
for a real show resolver tick.

What this does:
  1. Inserts 3 fake users (or upserts if they exist).
  2. Creates a `prediction_locks` row for a clearly-past show date.
  3. Inserts 3 predictions for that date with hand-picked scores.
  4. Calls every leaderboard rebuilder.
  5. Prints the resulting snapshot rows.

Demo data is marked: handles start with ``demo_`` and the show_date is
``1990-12-31`` so it's obvious. Cleanup is a single SQL line:

    DELETE FROM prediction_locks WHERE show_date = '1990-12-31';

(predictions cascade via show_date FK; users can be deleted explicitly).

Usage (local with TEST_PG_DSN):
    export PG_HOST=127.0.0.1 PG_PORT=5434 PG_DB=tweezer_picks \
           PG_USER=tweezer_picks PG_PASSWORD=...
    python -m scripts.seed_leaderboard_demo

Usage on nix1 (inside the container):
    docker compose exec tweezer-picks python -m scripts.seed_leaderboard_demo
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

# Make the script runnable without `pip install -e .` by adding src/ to path.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent / "src"))

import asyncpg  # noqa: E402

from tweezer_picks.config import get_settings  # noqa: E402
from tweezer_picks.leaderboard import (  # noqa: E402
    fetch_leaderboard,
    list_scope_keys,
    rebuild_all,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("seed_leaderboard_demo")

DEMO_SHOW_DATE = date(1990, 12, 31)
DEMO_HANDLES = ("demo_alice", "demo_bob", "demo_carol")
# (handle, picks, opener, closer, encore, score, breakdown_json)
_DemoPred = tuple[str, list[str], str | None, str | None, str | None, int, dict[str, Any]]
DEMO_PREDICTIONS: tuple[_DemoPred, ...] = (
    (
        "demo_alice",
        ["divided-sky", "fluffhead", "tweezer"],
        "tweezer",
        "harry-hood",
        "loving-cup",
        140,
        {"picks": [{"slug": "tweezer", "played": True, "base": 47}], "total": 140, "demo": True},
    ),
    (
        "demo_bob",
        ["bouncing-around-the-room", "harry-hood", "weekapaug-groove"],
        "bouncing-around-the-room",
        "weekapaug-groove",
        "show-of-life",
        92,
        {"picks": [{"slug": "harry-hood", "played": True, "base": 22}], "total": 92, "demo": True},
    ),
    (
        "demo_carol",
        ["antelope", "reba", "you-enjoy-myself"],
        "antelope",
        "you-enjoy-myself",
        "good-times-bad-times",
        58,
        {"picks": [{"slug": "reba", "played": True, "base": 18}], "total": 58, "demo": True},
    ),
)


async def _make_pool() -> asyncpg.Pool[Any]:
    settings = get_settings()
    pool = await asyncpg.create_pool(dsn=settings.pg_dsn, min_size=1, max_size=2)
    if pool is None:
        raise RuntimeError("could not open Postgres pool")
    return pool


async def _seed(pool: asyncpg.Pool[Any]) -> None:
    """Insert/update the demo rows. Idempotent.

    Strategy:
    - users: upsert by handle_lower
    - prediction_locks: upsert by show_date with lock_at far in the past so the
      lock-guard trigger does NOT block (UPDATE on existing pred is fine when
      lock_at < now since picks didn't change)
    - predictions: best-effort; if predictions already exist for this date,
      update score + breakdown only (avoid the lock-guard trigger entirely).
    """
    past_lock = datetime.now(UTC) - timedelta(days=365 * 30)
    async with pool.acquire() as conn, conn.transaction():
        # Users.
        user_ids: dict[str, int] = {}
        for handle in DEMO_HANDLES:
            row = await conn.fetchrow(
                """
                INSERT INTO users (handle, handle_lower)
                VALUES ($1, $2)
                ON CONFLICT (handle_lower) DO UPDATE
                    SET last_seen_at = now()
                RETURNING id
                """,
                handle,
                handle.lower(),
            )
            if row is None:
                raise RuntimeError(f"could not upsert user {handle}")
            user_ids[handle] = int(row["id"])

        # Prediction lock for the demo show.
        await conn.execute(
            """
            INSERT INTO prediction_locks (show_date, lock_at, venue_tz, summary, resolved_at)
            VALUES ($1, $2, 'UTC', $3::jsonb, now())
            ON CONFLICT (show_date) DO UPDATE
                SET lock_at = EXCLUDED.lock_at,
                    summary = EXCLUDED.summary,
                    resolved_at = COALESCE(prediction_locks.resolved_at, EXCLUDED.resolved_at)
            """,
            DEMO_SHOW_DATE,
            past_lock,
            json.dumps({"demo": True}),
        )

        # Predictions. Insert with score baked in so the lock-guard trigger
        # path is exercised the same way the resolver does it (UPDATE of
        # score column post-lock is allowed by migration 002's tightened
        # trigger). We insert WITHOUT a score first while lock_at could be
        # in the past — but since lock_at IS in the past, any INSERT raises.
        # Workaround: temporarily lift the lock, insert, then drop it back.
        # Cleaner approach: bypass via a NULL lock_at row briefly (not great).
        # Best: detect existing pred and UPDATE only.
        for handle, picks, opener, closer, encore, score, breakdown in DEMO_PREDICTIONS:
            uid = user_ids[handle]
            existing = await conn.fetchrow(
                "SELECT id FROM predictions WHERE user_id = $1 AND show_date = $2",
                uid,
                DEMO_SHOW_DATE,
            )
            if existing is None:
                # Briefly move lock_at to the future to allow the insert,
                # then move it back. This is a dev tool; keep it transactional.
                future_lock = datetime.now(UTC) + timedelta(hours=1)
                await conn.execute(
                    "UPDATE prediction_locks SET lock_at = $2 WHERE show_date = $1",
                    DEMO_SHOW_DATE,
                    future_lock,
                )
                await conn.execute(
                    """
                    INSERT INTO predictions (
                        user_id, show_date, pick_song_slugs,
                        opener_slug, closer_slug, encore_slug
                    )
                    VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                    uid,
                    DEMO_SHOW_DATE,
                    sorted(picks),
                    opener,
                    closer,
                    encore,
                )
                await conn.execute(
                    "UPDATE prediction_locks SET lock_at = $2 WHERE show_date = $1",
                    DEMO_SHOW_DATE,
                    past_lock,
                )
            # Now stamp the score (allowed by the post-migration-002 trigger).
            await conn.execute(
                """
                UPDATE predictions
                   SET score = $2,
                       score_breakdown = $3::jsonb
                 WHERE user_id = $1 AND show_date = $4
                """,
                uid,
                score,
                json.dumps({**breakdown, "demo": True}),
                DEMO_SHOW_DATE,
            )

    logger.info("demo data seeded", extra={"show_date": DEMO_SHOW_DATE.isoformat()})


async def _print_snapshots(pool: asyncpg.Pool[Any]) -> None:
    print("\n=== leaderboard_snapshots ===")
    for scope in ("weekly", "tour", "all_time"):
        keys = await list_scope_keys(pool, scope)
        print(f"\nscope={scope}, scope_keys={keys}")
        for key in keys:
            rows = await fetch_leaderboard(pool, scope, key, limit=20)
            print(f"  {key}:")
            for r in rows:
                print(
                    f"    #{r.rank}  {r.handle:<20}  "
                    f"score={r.total_score:>5}  shows={r.shows_played:>3}"
                )


async def _amain() -> int:
    pool = await _make_pool()
    try:
        await _seed(pool)
        counts = await rebuild_all(pool)
        logger.info("rebuilders done", extra={"counts": counts})
        await _print_snapshots(pool)
        print(
            "\nCleanup (when done):\n"
            "  DELETE FROM prediction_locks WHERE show_date = '1990-12-31';\n"
            "  DELETE FROM users WHERE handle_lower LIKE 'demo_%';\n"
            "  -- then re-run rebuilders or wait for next resolver tick."
        )
    finally:
        await pool.close()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_amain()))
