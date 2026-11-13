"""The demo seed refuses to write to any database that is not the demo one.

``scripts/seed_leaderboard_demo.py`` creates players, picks and scores. Pointed
at a real deployment it would put fake players on a real leaderboard, so it
checks ``current_database()`` before doing anything, migrations included.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.parse import unquote, urlsplit

import asyncpg
import pytest

from setlist_stash.config import Settings
from tests.conftest import _test_pg_dsn, requires_pg

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "seed_leaderboard_demo.py"


def _load_seed() -> ModuleType:
    spec = importlib.util.spec_from_file_location("seed_leaderboard_demo", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


seed = _load_seed()


@pytest.mark.parametrize(
    "name",
    [
        "setlist_stash",
        "setlist_stash_test",
        "postgres",
        "setlist_stash_demo_old",
        "SETLIST_STASH_DEMO",
        "",
    ],
)
def test_only_the_demo_database_name_passes(name: str) -> None:
    with pytest.raises(seed.NotDemoDatabase):
        seed.check_demo_database(name)


def test_demo_database_name_passes() -> None:
    seed.check_demo_database("setlist_stash_demo")


class _FakeConn:
    """Answers ``current_database()`` and records every other statement."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.queries: list[str] = []
        self.closed = False

    async def fetchval(self, sql: str, *args: Any) -> Any:
        self.queries.append(sql)
        if sql == "SELECT current_database()":
            return self.name
        raise AssertionError(f"unexpected query before the guard: {sql}")

    async def execute(self, sql: str, *args: Any) -> str:
        self.queries.append(sql)
        raise AssertionError(f"write attempted: {sql}")

    async def close(self) -> None:
        self.closed = True


class _StopAfterGuard(Exception):
    pass


def _patch(monkeypatch: pytest.MonkeyPatch, conn: _FakeConn) -> list[str]:
    calls: list[str] = []

    async def fake_connect(*_a: Any, **_kw: Any) -> _FakeConn:
        return conn

    async def fake_init_pool(_settings: Settings) -> Any:
        calls.append("init_pool")
        raise _StopAfterGuard

    monkeypatch.setattr(seed.asyncpg, "connect", fake_connect)
    monkeypatch.setattr(seed.db_module, "init_pool", fake_init_pool)
    return calls


async def test_refuses_a_non_demo_database_before_any_write(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    conn = _FakeConn("setlist_stash")
    calls = _patch(monkeypatch, conn)

    rc = await seed.seed_demo(Settings())

    assert rc == seed.EXIT_NOT_DEMO == 2
    assert conn.queries == ["SELECT current_database()"]
    assert conn.closed
    assert calls == []  # never opened the pool, so never migrated or seeded
    assert "refusing to seed database 'setlist_stash'" in capsys.readouterr().err


async def test_control_the_demo_database_gets_past_the_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Control for the test above: the same harness with the demo name proceeds
    # to open the pool, so the refusal is the guard, not a broken harness.
    conn = _FakeConn("setlist_stash_demo")
    calls = _patch(monkeypatch, conn)

    with pytest.raises(_StopAfterGuard):
        await seed.seed_demo(Settings())

    assert calls == ["init_pool"]


def _settings_from_dsn(dsn: str) -> Settings:
    parts = urlsplit(dsn)
    return Settings(
        pg_host=parts.hostname or "localhost",
        pg_port=parts.port or 5432,
        pg_user=unquote(parts.username or ""),
        pg_password=unquote(parts.password or ""),
        pg_db=parts.path.lstrip("/"),
    )


@requires_pg
async def test_refuses_a_real_database_and_writes_nothing(
    pg_pool: asyncpg.Pool[Any] | None,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert pg_pool is not None
    dsn = _test_pg_dsn()
    assert dsn is not None
    settings = _settings_from_dsn(dsn)
    assert settings.pg_db != "setlist_stash_demo"
    # Make sure a refusal cannot be an artifact of reusing the test's pool.
    monkeypatch.setattr(seed.db_module, "_pool", None)

    rc = await seed.seed_demo(settings)

    assert rc == 2
    assert "refusing to seed database" in capsys.readouterr().err
    async with pg_pool.acquire() as conn:
        for table in ("users", "prediction_locks", "predictions", "scoring_runs"):
            count = await conn.fetchval(f"SELECT count(*) FROM {table}")  # noqa: S608
            assert count == 0, table
