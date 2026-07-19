"""One-shot idempotent seed of ``prediction_locks`` for an upcoming tour.

Enumerates upcoming shows via mcp-phish (``search_shows`` for the current and
next year, so a tour that straddles a year boundary is covered), resolves each
show's venue-local timezone and default lock instant with the SAME helpers the
app uses at request time (``resolve_venue_tz`` + ``compute_default_lock_at``),
and inserts one ``prediction_locks`` row per show with ``ON CONFLICT DO
NOTHING``. Existing rows (e.g. a show that already has picks) are left
untouched.

It can also purge a phantom lock row for a date that has no show upstream
(``--purge-date``, repeatable) — but only when no predictions reference it, so
it can never orphan real player data.

Read-only against the vault (through mcp-phish); writes only to the
setlist-stash Postgres. Safe to re-run. The image does not ship ``scripts/``,
so run it by streaming this file into the container's Python (deps live there):

    docker exec -i setlist-stash python - \
        --tour "2026 Summer Tour" --purge-date 2026-07-09 < scripts/seed_tour.py
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import date

from setlist_stash.config import get_settings
from setlist_stash.db import close_pool, init_pool
from setlist_stash.locks import compute_default_lock_at, resolve_venue_tz
from setlist_stash.mcp_client import McpPhishClient

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("seed_tour")


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--tour",
        default=None,
        help="only seed shows whose tour_name contains this (case-insensitive)",
    )
    ap.add_argument(
        "--from-date",
        default=None,
        help="ISO date lower bound (inclusive); defaults to today",
    )
    ap.add_argument(
        "--purge-date",
        action="append",
        default=[],
        help="delete a phantom lock row for this ISO date (repeatable)",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    settings = get_settings()
    start = date.fromisoformat(args.from_date) if args.from_date else date.today()

    # --- Enumerate + enrich candidate shows via mcp-phish (reads the vault) ---
    targets: list[tuple[date, str | None, object, str, str, str]] = []
    async with McpPhishClient(
        settings.mcp_phish_url,
        timeout_seconds=settings.mcp_phish_timeout_seconds,
    ) as mcp:
        seen: dict[str, dict] = {}
        for yr in (start.year, start.year + 1):
            for row in await mcp.search_shows(year=yr, limit=300):
                d = str(row.get("date") or "")
                if d:
                    seen[d] = row
        for d, row in sorted(seen.items()):
            try:
                dd = date.fromisoformat(d)
            except ValueError:
                continue
            if dd < start:
                continue
            tour = str(row.get("tour_name") or "")
            if args.tour and args.tour.lower() not in tour.lower():
                continue
            # search_shows leaves location blank for some rows; get_show carries
            # the full venue object, which we need for the tz resolution.
            location = str(row.get("location") or "")
            if not location:
                try:
                    gs = await mcp.get_show(d)
                    venue = gs.get("venue") or {}
                    location = str(venue.get("location") or "")
                except Exception:  # noqa: BLE001 — best-effort; tz falls back
                    location = ""
            show_id = str(row.get("show_id")) if row.get("show_id") else None
            venue_tz = resolve_venue_tz(location, settings.default_lock_tz)
            lock_at = compute_default_lock_at(dd, settings, venue_tz=venue_tz)
            targets.append(
                (dd, show_id, lock_at, venue_tz, str(row.get("venue_name") or ""), tour)
            )

    # --- Write to the setlist-stash Postgres ---
    pool = await init_pool(settings)
    inserted = skipped = purged = 0
    try:
        async with pool.acquire() as conn:
            for dd, show_id, lock_at, venue_tz, venue_name, tour in targets:
                if args.dry_run:
                    log.info(
                        "would seed %s show_id=%s lock_at=%s tz=%s venue=%r",
                        dd, show_id, lock_at, venue_tz, venue_name,
                    )
                    continue
                res = await conn.execute(
                    """
                    INSERT INTO prediction_locks (show_date, show_id, lock_at, venue_tz)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (show_date) DO NOTHING
                    """,
                    dd, show_id, lock_at, venue_tz,
                )
                if res.endswith("1"):
                    inserted += 1
                    log.info(
                        "seeded %s lock_at=%s tz=%s venue=%r", dd, lock_at, venue_tz, venue_name
                    )
                else:
                    skipped += 1

            for pd in args.purge_date:
                pdd = date.fromisoformat(pd)
                cnt = await conn.fetchval(
                    "SELECT count(*) FROM predictions WHERE show_date = $1", pdd
                )
                if cnt and int(cnt) > 0:
                    log.warning(
                        "refusing to purge %s: %s prediction(s) reference it", pdd, cnt
                    )
                    continue
                if args.dry_run:
                    log.info("would purge phantom lock row %s", pdd)
                    continue
                res = await conn.execute(
                    "DELETE FROM prediction_locks WHERE show_date = $1 AND resolved_at IS NULL",
                    pdd,
                )
                if res.endswith("1"):
                    purged += 1
                    log.info("purged phantom lock row %s", pdd)
                else:
                    log.info("no phantom row to purge for %s", pdd)
    finally:
        await close_pool()

    log.info("done: inserted=%d skipped=%d purged=%d", inserted, skipped, purged)


if __name__ == "__main__":
    asyncio.run(main())
