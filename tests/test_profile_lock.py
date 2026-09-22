"""``/u/{handle}`` must hide picks until the EFFECTIVE lock.

Every other reader of ``prediction_locks`` uses
``COALESCE(lock_at_override, lock_at)``. The profile page read the raw
``lock_at`` column, so when an operator moved a cutoff later (the Ravinia
co-bill case migration 010 documents) every entrant's picks were public on
their profile from the default cutoff until the real one, while still
editable. The fake pool below answers the way Postgres would: if the query
asks for the override, the effective lock is in the future; if it only asks
for ``lock_at``, the default cutoff has passed.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr

from setlist_stash import db as db_module
from setlist_stash.config import Settings
from setlist_stash.server import build_app
from tests.conftest import requires_pg

PICK = "harry-hood"
SHOW = datetime(2026, 9, 12, tzinfo=UTC).date()


class _FakeConn:
    def __init__(self) -> None:
        self.queries: list[str] = []

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        self.queries.append(sql)
        if "FROM users" in sql:
            return {"id": 1, "handle": "alice"}
        return None

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.queries.append(sql)
        now = datetime.now(tz=UTC)
        default_lock = now - timedelta(minutes=30)
        override_lock = now + timedelta(minutes=60)
        effective = override_lock if "lock_at_override" in sql else default_lock
        return [
            {
                "show_date": SHOW,
                "pick_song_slugs": [PICK],
                "encore_slug": "tweezer-reprise",
                "score": None,
                "lock_at": effective,
                "resolved_at": None,
            }
        ]

    async def fetchval(self, sql: str, *args: Any) -> Any:
        self.queries.append(sql)
        return None

    async def execute(self, sql: str, *args: Any) -> str:
        self.queries.append(sql)
        return "OK"


class _Acquire:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *_: object) -> None:
        return None


class _FakePool:
    def __init__(self) -> None:
        self.conn = _FakeConn()

    def acquire(self) -> _Acquire:
        return _Acquire(self.conn)


@pytest.mark.asyncio
async def test_profile_hides_picks_until_the_effective_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _FakePool()
    previous = db_module._pool  # type: ignore[attr-defined]
    db_module._pool = pool  # type: ignore[attr-defined,assignment]
    try:
        app = build_app(Settings(session_secret=SecretStr("test-secret")))  # type: ignore[call-arg]
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/u/alice")
    finally:
        db_module._pool = previous  # type: ignore[attr-defined]
    assert resp.status_code == 200
    body = resp.text
    assert "Hidden until showtime" in body
    assert PICK not in body
    history_sql = [q for q in pool.conn.queries if "FROM predictions" in q]
    assert history_sql, "profile route did not query predictions"
    assert "lock_at_override" in history_sql[0]


@requires_pg
async def test_profile_hides_picks_when_override_is_later_pg(
    pg_pool: Any, async_client: AsyncClient
) -> None:
    now = datetime.now(tz=UTC)
    async with pg_pool.acquire() as conn:
        uid = await conn.fetchval(
            "INSERT INTO users (handle, handle_lower) VALUES ('alice','alice') RETURNING id"
        )
        await conn.execute(
            """
            INSERT INTO prediction_locks (show_date, lock_at, lock_at_override, venue_tz)
            VALUES ($1, $2, $3, 'UTC')
            """,
            SHOW,
            now - timedelta(minutes=30),
            now + timedelta(minutes=60),
        )
        await conn.execute(
            """
            INSERT INTO predictions (
                user_id, show_date, pick_song_slugs, opener_slug, closer_slug, encore_slug
            )
            VALUES ($1, $2, ARRAY[$3::text], NULL, NULL, 'tweezer-reprise')
            """,
            int(uid),
            SHOW,
            PICK,
        )
    resp = await async_client.get("/u/alice")
    assert resp.status_code == 200
    assert "Hidden until showtime" in resp.text
    assert PICK not in resp.text
