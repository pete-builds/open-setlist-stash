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
from setlist_stash.locks import ShowTarget
from setlist_stash.reminders import (
    KIND,
    Recipient,
    claim_send,
    next_run_at,
    render_message,
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
