"""Show-day pick reminders.

The tests that matter here are the ones that prove the job stays QUIET. A
reminder job is trivial to write so that it sends; the expensive failures are
all in the other direction, and each has its own arm below:

* mailing on a day with no show,
* mailing someone who already made their picks,
* mailing someone who unsubscribed,
* mailing the same person twice because the container restarted,
* mailing a reminder that lands after the pick cutoff.

DB-backed arms are skipped unless ``TEST_PG_DSN`` is set (see conftest).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any
from unittest.mock import patch
from zoneinfo import ZoneInfo

import asyncpg
import pytest
from pydantic import SecretStr

from setlist_stash.config import Settings
from setlist_stash.locks import ShowTarget, read_lock
from setlist_stash.reminders import (
    KIND,
    Recipient,
    TickResult,
    claim_send,
    next_run_at,
    render_message,
    run_schedule,
    run_tick,
    select_recipients,
    within_catchup_window,
)
from setlist_stash.unsubscribe import read_unsubscribe_token
from tests.conftest import requires_pg

ET = ZoneInfo("America/New_York")


def _settings(**over: Any) -> Settings:
    base: dict[str, Any] = {
        "session_secret": SecretStr("test-secret-aaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        "base_url": "https://tweezerpicks.test",
        "site_name": "Tweezer Picks",
        "reminder_enabled": True,
        "display_tz": "America/New_York",
    }
    base.update(over)
    return Settings(**base)


class CapturingProvider:
    """Records sends instead of performing them."""

    name = "capture"

    def __init__(self, *, fail_on: str | None = None) -> None:
        self.sent: list[dict[str, Any]] = []
        self.fail_on = fail_on

    async def send(
        self,
        *,
        to: str,
        subject: str,
        body: str,
        headers: Any = None,
    ) -> None:
        if self.fail_on is not None and to == self.fail_on:
            raise RuntimeError("smtp exploded")
        self.sent.append(
            {"to": to, "subject": subject, "body": body, "headers": headers or {}}
        )


# ----- message shape --------------------------------------------------------


def test_message_carries_a_working_unsubscribe_link() -> None:
    cfg = _settings()
    show = ShowTarget(
        show_date=date(2026, 9, 5),
        show_id=None,
        venue_name="Dick's Sporting Goods Park",
        location="Commerce City, CO",
        tour_name=None,
    )
    subject, body = render_message(
        cfg,
        recipient=Recipient(user_id=17, handle="pete", email="p@example.test"),
        show=show,
        lock_at=datetime(2026, 9, 5, 23, 25, tzinfo=UTC),
    )
    assert "2026-09-05" in subject
    assert "Tweezer Picks" in subject
    assert "pete" in body
    assert "Dick's Sporting Goods Park" in body
    assert "https://tweezerpicks.test/predict/2026-09-05" in body

    # The unsubscribe URL in the body must actually resolve to this user, not
    # merely look like a link. A body built with the wrong id unsubscribes a
    # stranger, and nothing else in the system would ever notice.
    line = next(ln for ln in body.splitlines() if "/email/unsubscribe" in ln)
    token = line.split("t=", 1)[1]
    assert read_unsubscribe_token(cfg, token) == 17


def test_message_is_tenant_scoped() -> None:
    """The Wappy tenant runs the same image and must not link to Tweezer."""
    cfg = _settings(base_url="https://wappypicks.test", site_name="Wappy Picks")
    _, body = render_message(
        cfg,
        recipient=Recipient(user_id=1, handle="x", email="x@example.test"),
        show=ShowTarget(date(2026, 9, 5), None, None, None, None),
        lock_at=datetime(2026, 9, 5, 23, 25, tzinfo=UTC),
    )
    assert "wappypicks.test" in body
    assert "tweezerpicks" not in body


def test_lock_time_renders_in_display_tz_not_utc() -> None:
    """A 23:25 UTC cutoff is 7:25 PM Eastern; the mail must say the latter.

    Containers run TZ=UTC. A body formatted in the container's zone tells
    players their picks close four hours later than they do.
    """
    cfg = _settings()
    _, body = render_message(
        cfg,
        recipient=Recipient(user_id=1, handle="x", email="x@example.test"),
        show=ShowTarget(date(2026, 9, 5), None, None, None, None),
        lock_at=datetime(2026, 9, 5, 23, 25, tzinfo=UTC),
    )
    assert "7:25 PM EDT" in body
    assert "11:25 PM" not in body


# ----- scheduling -----------------------------------------------------------


def test_next_run_is_today_when_the_hour_has_not_passed() -> None:
    cfg = _settings()
    now = datetime(2026, 9, 5, 6, 30, tzinfo=ET)
    assert next_run_at(cfg, now) == datetime(2026, 9, 5, 9, 0, tzinfo=ET)


def test_next_run_rolls_to_tomorrow_after_the_hour() -> None:
    cfg = _settings()
    now = datetime(2026, 9, 5, 9, 30, tzinfo=ET)
    assert next_run_at(cfg, now) == datetime(2026, 9, 6, 9, 0, tzinfo=ET)


def test_next_run_holds_local_9am_across_the_dst_change() -> None:
    """Wall-clock arithmetic, not "add 86400 seconds".

    The night US clocks fall back, a fixed 24-hour step lands the job at 8am.
    Nobody would notice for months, and the fix is one line, so pin it here.
    """
    cfg = _settings()
    now = datetime(2026, 11, 1, 10, 0, tzinfo=ET)  # after that day's run
    due = next_run_at(cfg, now)
    assert (due.hour, due.minute) == (9, 0)
    assert due.date() == date(2026, 11, 2)
    # 23 real hours from 10:00 to the next day's 09:00, computed from the
    # zone rather than assumed: a naive +86400 would put the run at 08:00.
    assert (due - now).total_seconds() == 23 * 3600


def test_catchup_window_covers_a_restart_just_after_the_hour() -> None:
    cfg = _settings(reminder_catchup_minutes=120)
    assert within_catchup_window(cfg, datetime(2026, 9, 5, 9, 4, tzinfo=ET))
    assert within_catchup_window(cfg, datetime(2026, 9, 5, 10, 59, tzinfo=ET))


def test_catchup_window_excludes_before_and_long_after() -> None:
    cfg = _settings(reminder_catchup_minutes=120)
    assert not within_catchup_window(cfg, datetime(2026, 9, 5, 8, 59, tzinfo=ET))
    assert not within_catchup_window(cfg, datetime(2026, 9, 5, 15, 0, tzinfo=ET))


# ----- recipient selection --------------------------------------------------


# Sentinel so ``email=None`` can mean "this user has NO email" rather than
# "use the default", which is the distinction the no-email arm below tests.
_DEFAULT_EMAIL = object()


async def _make_user(
    pool: asyncpg.Pool[Any],
    handle: str,
    *,
    email: Any = _DEFAULT_EMAIL,
    verified: bool = True,
    opted_out: bool = False,
) -> int:
    async with pool.acquire() as conn:
        uid = await conn.fetchval(
            """
            INSERT INTO users
                (handle, handle_lower, email, email_verified_at, email_opt_out_at)
            VALUES ($1, lower($1), $2,
                    CASE WHEN $3 THEN now() ELSE NULL END,
                    CASE WHEN $4 THEN now() ELSE NULL END)
            RETURNING id
            """,
            handle,
            f"{handle}@example.test" if email is _DEFAULT_EMAIL else email,
            verified,
            opted_out,
        )
    return int(uid)


async def _make_lock(
    pool: asyncpg.Pool[Any], show_date: date, lock_at: datetime
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO prediction_locks (show_date, lock_at) VALUES ($1, $2) "
            "ON CONFLICT (show_date) DO UPDATE SET lock_at = EXCLUDED.lock_at",
            show_date,
            lock_at,
        )


async def _make_prediction(
    pool: asyncpg.Pool[Any], user_id: int, show_date: date
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO predictions (user_id, show_date, pick_song_slugs) "
            "VALUES ($1, $2, $3)",
            user_id,
            show_date,
            ["tweezer", "sand", "ghost"],
        )


@requires_pg
@pytest.mark.asyncio
async def test_select_recipients_excludes_the_four_wrong_audiences(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """One eligible player among four who must not be mailed."""
    assert pg_pool is not None
    show_date = date(2026, 9, 5)
    # This lock is relative to the REAL clock on purpose, and is the one
    # place in the file that should be. ``predictions`` has a foreign key to
    # ``prediction_locks`` and a trigger that refuses an INSERT after the
    # cutoff, judged by the database's own ``now()`` at write time. The
    # "alreadypicked" fixture below has to get past that trigger today, not
    # on 2026-09-05. Recipient selection itself never reads the lock, so no
    # assertion here depends on what day the suite runs.
    await _make_lock(
        pg_pool, show_date, datetime.now(tz=UTC) + timedelta(hours=8)
    )

    wanted = await _make_user(pg_pool, "eligible")
    await _make_user(pg_pool, "noemail", email=None)
    await _make_user(pg_pool, "unverified", verified=False)
    await _make_user(pg_pool, "unsubbed", opted_out=True)
    already = await _make_user(pg_pool, "alreadypicked")
    await _make_prediction(pg_pool, already, show_date)

    got = await select_recipients(pg_pool, show_date)
    assert [r.user_id for r in got] == [wanted]


@requires_pg
@pytest.mark.asyncio
async def test_claim_send_is_the_double_send_guard(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """Second claim for the same (user, show) loses. This is the restart story."""
    assert pg_pool is not None
    uid = await _make_user(pg_pool, "claimed")
    assert await claim_send(pg_pool, uid, "2026-09-05") is True
    assert await claim_send(pg_pool, uid, "2026-09-05") is False
    # A different show is a different claim.
    assert await claim_send(pg_pool, uid, "2026-09-06") is True


# ----- tick -----------------------------------------------------------------


def _patch_show(show: ShowTarget | None) -> Any:
    """Stand in for the MCP lookup so no test reaches phish.net."""
    return patch(
        "setlist_stash.reminders.select_form_show",
        new=_async_return(show),
    )


def _async_return(value: Any) -> Any:
    async def _inner(*_args: Any, **_kwargs: Any) -> Any:
        return value

    return _inner


@pytest.mark.asyncio
async def test_tick_is_inert_when_disabled() -> None:
    result = await run_tick(_settings(reminder_enabled=False))
    assert result.status == "disabled"
    assert result.sent == 0


@requires_pg
@pytest.mark.asyncio
async def test_tick_sends_nothing_when_the_next_show_is_not_today(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    await _make_user(pg_pool, "waiting")
    provider = CapturingProvider()
    now = datetime(2026, 9, 5, 9, 0, tzinfo=ET)
    with _patch_show(ShowTarget(date(2026, 9, 9), None, None, None, None)):
        result = await run_tick(
            _settings(), pool=pg_pool, provider=provider, now=now
        )
    assert result.status == "noop"
    assert provider.sent == []


@requires_pg
@pytest.mark.asyncio
async def test_tick_sends_nothing_when_picks_have_already_closed(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    assert pg_pool is not None
    show_date = date(2026, 9, 5)
    await _make_user(pg_pool, "latecomer")
    # Cutoff two hours before the 9am tick.
    await _make_lock(pg_pool, show_date, datetime(2026, 9, 5, 11, 0, tzinfo=UTC))
    provider = CapturingProvider()
    with _patch_show(ShowTarget(show_date, None, None, None, None)):
        result = await run_tick(
            _settings(),
            pool=pg_pool,
            provider=provider,
            now=datetime(2026, 9, 5, 9, 0, tzinfo=ET),
        )
    assert result.status == "noop"
    assert provider.sent == []


@requires_pg
@pytest.mark.asyncio
async def test_tick_sends_once_and_only_once(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """The positive arm, plus the restart that must not re-send.

    Running the tick twice is exactly what a container restart inside the
    catch-up window does.
    """
    assert pg_pool is not None
    show_date = date(2026, 9, 5)
    uid = await _make_user(pg_pool, "player")
    await _make_lock(pg_pool, show_date, datetime(2026, 9, 5, 23, 25, tzinfo=UTC))
    provider = CapturingProvider()
    cfg = _settings()
    now = datetime(2026, 9, 5, 9, 0, tzinfo=ET)

    with _patch_show(ShowTarget(show_date, None, None, None, None)):
        first = await run_tick(cfg, pool=pg_pool, provider=provider, now=now)
        second = await run_tick(cfg, pool=pg_pool, provider=provider, now=now)

    assert first.status == "sent"
    assert first.sent == 1
    assert len(provider.sent) == 1
    assert provider.sent[0]["to"] == "player@example.test"
    # RFC 8058 headers, without which the recipient's only button is "spam".
    headers = provider.sent[0]["headers"]
    assert headers["List-Unsubscribe-Post"] == "List-Unsubscribe=One-Click"
    assert headers["List-Unsubscribe"].startswith("<https://tweezerpicks.test/")

    assert second.sent == 0
    assert second.skipped_already_sent == 1
    assert len(provider.sent) == 1

    async with pg_pool.acquire() as conn:
        status = await conn.fetchval(
            "SELECT status FROM email_sends WHERE user_id = $1 AND kind = $2",
            uid,
            KIND,
        )
    assert status == "sent"


@requires_pg
@pytest.mark.asyncio
async def test_one_failed_send_does_not_silence_the_batch(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """A bad address must cost one recipient, not all of them."""
    assert pg_pool is not None
    show_date = date(2026, 9, 5)
    await _make_user(pg_pool, "abad", email="bad@example.test")
    await _make_user(pg_pool, "bgood", email="good@example.test")
    await _make_lock(pg_pool, show_date, datetime(2026, 9, 5, 23, 25, tzinfo=UTC))
    provider = CapturingProvider(fail_on="bad@example.test")

    with _patch_show(ShowTarget(show_date, None, None, None, None)):
        result = await run_tick(
            _settings(),
            pool=pg_pool,
            provider=provider,
            now=datetime(2026, 9, 5, 9, 0, tzinfo=ET),
        )

    assert result.failed == 1
    assert result.sent == 1
    assert [m["to"] for m in provider.sent] == ["good@example.test"]
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT status FROM email_sends ORDER BY user_id"
        )
    assert [r["status"] for r in rows] == ["failed", "sent"]


# ----- one clock ------------------------------------------------------------
#
# The tick takes an injectable ``now``. Every decision it makes (which show is
# today's, whether picks have closed, how much lead time is left) has to come
# from that one value. A single stray read of the real clock, in Python or in
# Postgres (``SELECT now()``), turns a fixed-date test into a time bomb that
# passes the day it is written and fails once real time walks past the
# fixture. This PR shipped exactly that bug: the lock check read the database
# clock, and the 2026-09-05 fixtures went red once that evening's cutoff
# passed in real time.
#
# The guard runs the same scenarios decades in the past AND decades in the
# future. Whatever today's real date is, it sits on the wrong side of one of
# those runs, so any remaining real-clock read flips a verdict and fails here
# loudly instead of rotting quietly.


@requires_pg
@pytest.mark.asyncio
@pytest.mark.parametrize("year", [2001, 2099])
@pytest.mark.parametrize("lock_source", ["row", "default"])
@pytest.mark.parametrize("state", ["open", "closed"])
async def test_tick_reads_only_the_injected_clock(
    pg_pool: asyncpg.Pool[Any] | None,
    year: int,
    lock_source: str,
    state: str,
) -> None:
    assert pg_pool is not None
    show_date = date(year, 7, 1)
    now = datetime(year, 7, 1, 9, 0, tzinfo=ET)
    # "closed" puts the cutoff at 08:00 ET, an hour before the 9am tick;
    # "open" puts it at 19:25 ET, comfortably past the minimum lead.
    lock_local = "08:00" if state == "closed" else "19:25"
    cfg = _settings(
        default_lock_time_local=lock_local,
        default_lock_tz="America/New_York",
        # Pinning the show through the operator override runs the real
        # select_form_show, whose own notion of "today" must also be ``now``.
        admin_show_date=show_date,
    )
    if lock_source == "row":
        hh, mm = (int(p) for p in lock_local.split(":"))
        await _make_lock(
            pg_pool, show_date, datetime(year, 7, 1, hh, mm, tzinfo=ET)
        )
    await _make_user(pg_pool, "timetraveler")
    provider = CapturingProvider()

    # The override must be honored without an MCP lookup. If the tick judged
    # the pinned date against the real calendar it would fall through to the
    # network, so make that path fail fast and visibly.
    async def _no_mcp(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("tick fell through to an MCP show lookup")

    with patch("setlist_stash.locks._shows_on_or_after", new=_no_mcp):
        result = await run_tick(cfg, pool=pg_pool, provider=provider, now=now)

    assert result.show_date == show_date, result
    if state == "open":
        assert result.status == "sent", result
        assert [m["to"] for m in provider.sent] == ["timetraveler@example.test"]
    else:
        assert result.status == "noop", result
        assert result.note == "picks already closed", result
        assert provider.sent == []


@requires_pg
@pytest.mark.asyncio
async def test_read_lock_keeps_the_database_clock_unless_told_otherwise(
    pg_pool: asyncpg.Pool[Any] | None,
) -> None:
    """``now=`` is opt-in. The request handlers that call ``read_lock`` without
    it must keep judging against the database clock exactly as before."""
    assert pg_pool is not None
    show_date = date(2001, 7, 1)
    cutoff = datetime(2001, 7, 1, 23, 25, tzinfo=UTC)
    await _make_lock(pg_pool, show_date, cutoff)

    real = await read_lock(pg_pool, show_date)
    assert real is not None and real.is_locked  # 2001 is long gone

    before = await read_lock(
        pg_pool, show_date, now=cutoff - timedelta(minutes=1)
    )
    assert before is not None
    assert not before.is_locked
    assert before.seconds_until_lock == 60
    assert before.lock_at == cutoff


# ----- the daily loop -------------------------------------------------------
#
# ``run_schedule`` is driven here on a fake clock: ``sleep`` advances the clock
# instead of waiting, so days of wall time run in milliseconds. The bug these
# pin was a loop that recomputed "the next target" on every wake. The wake
# that ended the last sleep landed at or just past 09:00, where the next
# target is already TOMORROW, so it slept again and the scheduled tick never
# ran. Only a restart inside the catch-up window ever sent anything.


class _StopLoop(Exception):
    """Raised by the fake sleep to end an otherwise endless loop."""


class _FakeClock:
    def __init__(
        self,
        start: datetime,
        stop_at: datetime,
        *,
        jump_on_first_sleep: timedelta = timedelta(0),
    ) -> None:
        self.t = start.astimezone(UTC)
        self.stop_at = stop_at.astimezone(UTC)
        self.jump = jump_on_first_sleep
        self.sleeps = 0

    def now(self) -> datetime:
        return self.t

    async def sleep(self, seconds: float) -> None:
        assert seconds > 0, f"non-positive sleep {seconds} would spin"
        self.sleeps += 1
        # A loop that never reaches stop_at would hang the suite instead of
        # failing it; 100k sleeps is decades of 15-minute naps.
        if self.sleeps > 100_000:
            raise _StopLoop("runaway loop")
        self.t += timedelta(seconds=seconds)
        if self.sleeps == 1:
            self.t += self.jump  # a host suspend or clock step mid-sleep
        if self.t >= self.stop_at:
            raise _StopLoop


async def _drive(
    clock: _FakeClock, *, cfg: Settings | None = None, fail_first: bool = False
) -> list[datetime]:
    fired: list[datetime] = []

    async def _tick() -> TickResult:
        fired.append(clock.now().astimezone(ET))
        if fail_first and len(fired) == 1:
            raise RuntimeError("tick blew up")
        return TickResult("noop")

    with pytest.raises(_StopLoop):
        await run_schedule(
            cfg or _settings(), tick=_tick, clock=clock.now, sleep=clock.sleep
        )
    return fired


def _when(fired: list[datetime]) -> list[tuple[date, int, int]]:
    return [(t.date(), t.hour, t.minute) for t in fired]


@pytest.mark.asyncio
async def test_loop_fires_once_a_day_at_the_target() -> None:
    clock = _FakeClock(
        datetime(2026, 9, 4, 20, 0, tzinfo=ET),
        datetime(2026, 9, 7, 12, 0, tzinfo=ET),
    )
    assert _when(await _drive(clock)) == [
        (date(2026, 9, 5), 9, 0),
        (date(2026, 9, 6), 9, 0),
        (date(2026, 9, 7), 9, 0),
    ]


@pytest.mark.asyncio
async def test_loop_holds_9am_local_across_the_fall_back() -> None:
    """2026-11-01 is the US fall-back Sunday; both runs land at 09:00 local."""
    clock = _FakeClock(
        datetime(2026, 10, 31, 20, 0, tzinfo=ET),
        datetime(2026, 11, 2, 12, 0, tzinfo=ET),
    )
    assert _when(await _drive(clock)) == [
        (date(2026, 11, 1), 9, 0),
        (date(2026, 11, 2), 9, 0),
    ]


@pytest.mark.asyncio
async def test_loop_restart_inside_the_window_runs_once_not_twice() -> None:
    """Startup at 09:30 runs today's pass now, then waits for TOMORROW."""
    clock = _FakeClock(
        datetime(2026, 9, 5, 9, 30, tzinfo=ET),
        datetime(2026, 9, 6, 12, 0, tzinfo=ET),
    )
    assert _when(await _drive(clock)) == [
        (date(2026, 9, 5), 9, 30),
        (date(2026, 9, 6), 9, 0),
    ]


@pytest.mark.asyncio
async def test_loop_skips_a_run_it_wakes_up_too_late_for() -> None:
    """A suspend that carries the host past the catch-up window skips that
    day rather than mailing people at 2pm, and the next day still fires."""
    clock = _FakeClock(
        datetime(2026, 9, 5, 8, 0, tzinfo=ET),
        datetime(2026, 9, 6, 12, 0, tzinfo=ET),
        jump_on_first_sleep=timedelta(hours=6),
    )
    cfg = _settings(reminder_catchup_minutes=120)
    assert _when(await _drive(clock, cfg=cfg)) == [(date(2026, 9, 6), 9, 0)]


@pytest.mark.asyncio
async def test_loop_survives_a_tick_that_raises() -> None:
    clock = _FakeClock(
        datetime(2026, 9, 4, 20, 0, tzinfo=ET),
        datetime(2026, 9, 6, 12, 0, tzinfo=ET),
    )
    assert _when(await _drive(clock, fail_first=True)) == [
        (date(2026, 9, 5), 9, 0),
        (date(2026, 9, 6), 9, 0),
    ]
