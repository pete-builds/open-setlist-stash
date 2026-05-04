"""Tiny migrations runner.

Reads ``migrations/NNN_*.sql`` files in lexicographic order, applies any
whose version is missing from ``schema_version``. Idempotent — safe to call
on every app start.

We don't pull in alembic here. The schema is stable and the resolver session
adds at most one or two more files. If the migration set ever grows past 5
files, switch to alembic and delete this.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import asyncpg

logger = logging.getLogger("setlist_stash.migrate")

_VERSION_RE = re.compile(r"^(\d+)_")


def _migrations_dir() -> Path:
    """Locate ``migrations/`` next to the project root.

    The Dockerfile copies the repo into ``/app``; running from source the
    ``migrations/`` dir is two levels up from this file.
    """
    here = Path(__file__).resolve().parent
    # src/setlist_stash/migrate.py -> repo root is two dirs up
    candidate = here.parent.parent / "migrations"
    if candidate.is_dir():
        return candidate
    # Container layout: Dockerfile copies migrations to /app/migrations.
    candidate = Path("/app/migrations")
    if candidate.is_dir():
        return candidate
    raise RuntimeError(f"migrations dir not found (looked near {here} and /app)")


def discover_migrations() -> list[tuple[int, Path]]:
    """Return ``(version, path)`` pairs sorted ascending."""
    out: list[tuple[int, Path]] = []
    for p in sorted(_migrations_dir().glob("*.sql")):
        m = _VERSION_RE.match(p.name)
        if not m:
            continue
        out.append((int(m.group(1)), p))
    return out


async def applied_versions(pool: asyncpg.Pool[Any]) -> set[int]:
    """Return the set of versions already in ``schema_version``."""
    async with pool.acquire() as conn:
        # schema_version is created by 001_initial.sql; bootstrap by hand.
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        rows = await conn.fetch("SELECT version FROM schema_version")
    return {int(r["version"]) for r in rows}


async def run_migrations(pool: asyncpg.Pool[Any]) -> int:
    """Apply pending migrations. Returns count applied."""
    pending = []
    seen = await applied_versions(pool)
    for version, path in discover_migrations():
        if version in seen:
            continue
        pending.append((version, path))

    if not pending:
        logger.info("setlist-stash migrations: nothing to do")
        return 0

    for version, path in pending:
        sql = path.read_text(encoding="utf-8")
        logger.info("applying migration", extra={"version": version, "file": path.name})
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute(sql)
            # 001 self-stamps; later migrations should too, but be defensive.
            await conn.execute(
                "INSERT INTO schema_version (version) VALUES ($1) "
                "ON CONFLICT (version) DO NOTHING",
                version,
            )
    logger.info("setlist-stash migrations applied", extra={"count": len(pending)})
    return len(pending)
