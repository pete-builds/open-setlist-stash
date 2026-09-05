"""Show-day pick reminders.

One message, once, on the morning of a show, to players who have a verified
email, have not opted out, and have not made their picks yet. Nothing on a day
with no show. Nothing to a player who already submitted. Nothing at all unless
the operator set ``REMINDER_ENABLED=true`` for this deployment.

Shape mirrors the resolver (``resolve.py``): a small tick function plus an
in-container loop, run as its own service off the same image. Two reasons it
is not folded into the resolver's loop. The resolver's cadence is show-aware
and ranges from 60s to 30 minutes, so "fire at 09:00" would land up to half an
hour late; and a bug in reminder mail must not be able to stall the loop that
scores the game.

Three things this module is deliberately careful about:

* **"Today" is DISPLAY_TZ, never the container's zone.** Containers run
  ``TZ=UTC``, where ``date.today()`` has already rolled over to tomorrow by
  8pm Eastern. The same trap ``select_form_show`` documents.
* **The claim is written before the send.** ``email_sends`` has a UNIQUE on
  (user_id, kind, dedupe_key) and we INSERT ... ON CONFLICT DO NOTHING first.
  A restart mid-run therefore resumes rather than re-mails, and a crash loop
  cannot turn into a mail bomb aimed at ten people.
* **A send that would land after the pick cutoff is not sent.** Asking someone
  to do something they can no longer do is worse than staying quiet.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import asyncpg

from setlist_stash import __version__
from setlist_stash.config import Settings, get_settings
from setlist_stash.db import close_pool, get_pool, init_pool
from setlist_stash.email import EmailProvider, build_provider
from setlist_stash.locks import (
    LockState,
    ShowTarget,
    compute_default_lock_at,
    read_lock,
    resolve_venue_tz,
    select_form_show,
)
from setlist_stash.logging_setup import configure_logging
from setlist_stash.mcp_client import McpPhishClient
from setlist_stash.migrate import run_migrations
from setlist_stash.unsubscribe import unsubscribe_url
from setlist_stash.web_helpers import display_dt

logger = logging.getLogger("setlist_stash.reminders")

__all__ = ["KIND", "Recipient", "TickResult", "main", "run_tick"]

# Message family, and the dedupe namespace that goes with it. A future digest
# gets its own KIND and cannot collide with this one.
KIND = "pick_reminder"

# Longest the loop will sleep in one go. The target time is recomputed from the
# wall clock on every wake, so a host suspend, a clock step or a DST shift
# costs at most one short interval of drift instead of a whole missed day.
_MAX_SLEEP_SECONDS = 900


@dataclass(frozen=True)
class Recipient:
    user_id: int
    handle: str
    email: str


@dataclass
class TickResult:
    status: str                 # sent | noop | disabled | error
    show_date: date | None = None
    candidates: int = 0
    sent: int = 0
    failed: int = 0
    skipped_already_sent: int = 0
    note: str = ""
    errors: list[str] = field(default_factory=list)


# ----- recipient selection --------------------------------------------------


async def select_recipients(
    pool: asyncpg.Pool[Any], show_date: date
) -> list[Recipient]:
    """Players who should hear about ``show_date`` this morning.

    The filters, and why each one is here:

    * ``email_verified_at IS NOT NULL`` — an unverified address is one somebody
      typed, not one somebody proved they own. Mailing it is mailing a stranger.
      Google sign-ins land here already verified, which is why this covers the
      "signed in with Google" audience without naming Google.
    * ``email_opt_out_at IS NULL`` — the unsubscribe, honored at the source.
    * no row in ``predictions`` for this show — the reminder is for people who
      have not picked. Someone who submitted at 8am should not be nagged at 9.

    Ordered by id so a partial run (SMTP dies halfway) resumes in a stable
    order and the dedupe table shows an obvious prefix rather than a scatter.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT u.id, u.handle, u.email
              FROM users u
             WHERE u.email IS NOT NULL
               AND u.email_verified_at IS NOT NULL
               AND u.email_opt_out_at IS NULL
               AND NOT EXISTS (
                   SELECT 1 FROM predictions p
                    WHERE p.user_id = u.id AND p.show_date = $1
               )
             ORDER BY u.id
            """,
            show_date,
        )
    return [
        Recipient(user_id=r["id"], handle=r["handle"], email=r["email"])
        for r in rows
    ]


async def claim_send(
    pool: asyncpg.Pool[Any], user_id: int, dedupe_key: str
) -> bool:
    """Reserve the right to mail ``user_id`` about ``dedupe_key``.

    True means this process owns the send. False means a row already exists —
    an earlier tick, or an earlier life of this container, already handled it,
    including the case where it FAILED. A failed row is not retried
    automatically on purpose: the realistic cause is a misconfigured or
    rate-limited SMTP account, and a loop that retries that on every tick
    turns one outage into a sending-reputation problem. Clearing the row by
    hand is the deliberate retry.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO email_sends (user_id, kind, dedupe_key, status)
            VALUES ($1, $2, $3, 'pending')
            ON CONFLICT (user_id, kind, dedupe_key) DO NOTHING
            RETURNING id
            """,
            user_id,
            KIND,
            dedupe_key,
        )
    return row is not None


async def _mark_send(
    pool: asyncpg.Pool[Any],
    user_id: int,
    dedupe_key: str,
    *,
    status: str,
    error: str | None = None,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE email_sends
               SET status = $4,
                   error = $5,
                   sent_at = CASE WHEN $4 = 'sent' THEN now() ELSE sent_at END
             WHERE user_id = $1 AND kind = $2 AND dedupe_key = $3
            """,
            user_id,
            KIND,
            dedupe_key,
            status,
            error[:500] if error else None,
        )


# ----- message --------------------------------------------------------------


def render_message(
    settings: Settings,
    *,
    recipient: Recipient,
    show: ShowTarget,
    lock_at: datetime,
) -> tuple[str, str]:
    """Build (subject, plain-text body) for one recipient.

    Plain text only. The whole message is four lines and a link; an HTML part
    would add a spam-filter surface and a rendering matrix for no gain, and
    plain text is what a phone lock screen previews well.

    Every deployment-specific string comes from settings (``site_name``,
    ``base_url``), so the Wappy tenant's mail says Wappy Picks and links to
    wappypicks.com without a second template.
    """
    where = " - ".join(
        part for part in (show.venue_name, show.location) if part
    )
    when = show.show_date.isoformat()
    subject = f"{settings.site_name}: picks close tonight for {when}"

    lock_str = display_dt(lock_at, tz=settings.display_tz)
    base = settings.base_url.rstrip("/")
    lines = [
        f"Hi {recipient.handle},",
        "",
        f"There's a show tonight{': ' + where if where else ''} ({when}),",
        f"and you haven't made your picks yet. Picks close at {lock_str}.",
        "",
        f"Make your picks: {base}/predict/{when}",
        "",
        "Good luck.",
        "",
        "--",
        f"You're getting this because you signed in to {settings.site_name}",
        "with a verified email address. One click to stop these reminders:",
        unsubscribe_url(settings, recipient.user_id),
        "",
        "This only turns off show-day reminders. Sign-in links still work.",
    ]
    return subject, "\n".join(lines)


def _list_unsubscribe_headers(settings: Settings, user_id: int) -> dict[str, str]:
    """RFC 8058 one-click unsubscribe headers.

    ``List-Unsubscribe-Post`` is what makes Gmail and Apple Mail render their
    own native unsubscribe control and POST to the URL directly. Without the
    Post header, the same URL is treated as a plain link and some clients
    prefetch it with a GET, which is exactly why the GET route renders a
    confirmation page instead of unsubscribing anybody.
    """
    return {
        "List-Unsubscribe": f"<{unsubscribe_url(settings, user_id)}>",
        "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
    }


# ----- tick -----------------------------------------------------------------


async def _effective_lock(
    pool: asyncpg.Pool[Any], show: ShowTarget, settings: Settings
) -> tuple[datetime, bool]:
    """The pick cutoff for ``show`` and whether it has passed.

    Reads an existing ``prediction_locks`` row when there is one so an
    operator's ``lock_at_override`` is honored, and otherwise COMPUTES the
    default without writing. The reminder must not be the thing that creates a
    lock row: that row is meant to appear on the first prediction, and a job
    that quietly materializes it changes what ``read_lock``-based views show
    for a show nobody has played yet.
    """
    existing: LockState | None = await read_lock(pool, show.show_date)
    if existing is not None:
        return existing.lock_at, existing.is_locked
    venue_tz = resolve_venue_tz(show.location, settings.default_lock_tz)
    lock_at = compute_default_lock_at(show.show_date, settings, venue_tz=venue_tz)
    now = datetime.now(tz=ZoneInfo("UTC"))
    return lock_at, now > lock_at


async def run_tick(
    settings: Settings,
    *,
    pool: asyncpg.Pool[Any] | None = None,
    provider: EmailProvider | None = None,
    now: datetime | None = None,
) -> TickResult:
    """One reminder pass. Safe to call repeatedly; the dedupe row is the guard.

    ``pool``, ``provider`` and ``now`` are injectable so the send decision can
    be tested against a fixed clock and a recording transport. In the container
    all three come from the process: the global pool, the configured provider,
    and the real clock.
    """
    if not settings.reminder_enabled:
        return TickResult("disabled", note="REMINDER_ENABLED is false")

    tz = ZoneInfo(settings.display_tz)
    now_local = now.astimezone(tz) if now is not None else datetime.now(tz=tz)
    today = now_local.date()

    try:
        async with McpPhishClient(
            settings.mcp_phish_url,
            timeout_seconds=settings.mcp_phish_timeout_seconds,
        ) as mcp:
            show = await select_form_show(settings, mcp)
    except Exception as exc:
        logger.exception("show lookup failed")
        return TickResult("error", note=f"show lookup failed: {exc!s}"[:200])

    if show is None:
        return TickResult("noop", note="no upcoming show")
    if show.show_date != today:
        return TickResult(
            "noop",
            show_date=show.show_date,
            note=f"next show is {show.show_date.isoformat()}, not today",
        )

    db = pool if pool is not None else get_pool()
    lock_at, is_locked = await _effective_lock(db, show, settings)
    if is_locked:
        return TickResult(
            "noop", show_date=show.show_date, note="picks already closed"
        )
    lead_seconds = (lock_at - now_local).total_seconds()
    if lead_seconds < settings.reminder_min_lead_minutes * 60:
        return TickResult(
            "noop",
            show_date=show.show_date,
            note=(
                f"only {int(lead_seconds // 60)}m before lock; under the "
                f"{settings.reminder_min_lead_minutes}m minimum lead"
            ),
        )

    send_provider = provider or build_provider(settings)
    if send_provider.name == "disabled":
        return TickResult(
            "noop",
            show_date=show.show_date,
            note="EMAIL_PROVIDER=disabled; nothing can be sent",
        )

    dedupe_key = show.show_date.isoformat()
    recipients = await select_recipients(db, show.show_date)
    result = TickResult(
        "sent", show_date=show.show_date, candidates=len(recipients)
    )

    for person in recipients:
        if not await claim_send(db, person.user_id, dedupe_key):
            result.skipped_already_sent += 1
            continue
        subject, body = render_message(
            settings, recipient=person, show=show, lock_at=lock_at
        )
        try:
            await send_provider.send(
                to=person.email,
                subject=subject,
                body=body,
                headers=_list_unsubscribe_headers(settings, person.user_id),
            )
        except Exception as exc:
            # Broad on purpose, and wider than EmailSendError: a transport that
            # raises something unexpected must still leave a 'failed' row and
            # let the remaining recipients go out. One bad address cannot be
            # allowed to silence the batch.
            result.failed += 1
            result.errors.append(f"{person.user_id}: {exc!s}"[:200])
            await _mark_send(
                db, person.user_id, dedupe_key, status="failed", error=str(exc)
            )
            logger.exception(
                "reminder send failed", extra={"user_id": person.user_id}
            )
            continue
        result.sent += 1
        await _mark_send(db, person.user_id, dedupe_key, status="sent")

    if result.sent == 0 and result.failed == 0:
        result.status = "noop"
        result.note = result.note or "no eligible recipients"
    return result


# ----- entrypoint -----------------------------------------------------------


def next_run_at(settings: Settings, now_local: datetime) -> datetime:
    """The next instant the daily check should fire, in DISPLAY_TZ.

    Wall-clock arithmetic on an aware datetime, so the run stays at 09:00
    local across a DST boundary rather than drifting to 08:00 or 10:00.
    """
    target = now_local.replace(
        hour=settings.reminder_hour_local,
        minute=settings.reminder_minute_local,
        second=0,
        microsecond=0,
    )
    if now_local < target:
        return target
    return target + timedelta(days=1)


def within_catchup_window(settings: Settings, now_local: datetime) -> bool:
    """True when a tick starting now still counts as today's scheduled run.

    Covers the ordinary case of a container that restarted a few minutes after
    the target time. Outside the window the loop waits for tomorrow rather than
    mailing people hours late about a show whose lock is closing.
    """
    target = now_local.replace(
        hour=settings.reminder_hour_local,
        minute=settings.reminder_minute_local,
        second=0,
        microsecond=0,
    )
    if now_local < target:
        return False
    return now_local <= target + timedelta(
        minutes=settings.reminder_catchup_minutes
    )


def _log_tick(result: TickResult) -> None:
    logger.info(
        "reminder tick",
        extra={
            "status": result.status,
            "show_date": (
                result.show_date.isoformat() if result.show_date else None
            ),
            "candidates": result.candidates,
            "sent": result.sent,
            "failed": result.failed,
            "skipped_already_sent": result.skipped_already_sent,
            "note": result.note,
        },
    )


async def _amain(loop: bool) -> int:
    settings = get_settings()
    configure_logging(settings.log_format)
    pool = await init_pool(settings)
    await run_migrations(pool)
    try:
        if not loop:
            _log_tick(await run_tick(settings))
            return 0

        tz = ZoneInfo(settings.display_tz)
        logger.info(
            "reminder loop starting",
            extra={
                "enabled": settings.reminder_enabled,
                "local_time": (
                    f"{settings.reminder_hour_local:02d}:"
                    f"{settings.reminder_minute_local:02d}"
                ),
                "display_tz": settings.display_tz,
                "catchup_minutes": settings.reminder_catchup_minutes,
                "min_lead_minutes": settings.reminder_min_lead_minutes,
                "version": __version__,
            },
        )
        # A restart inside the catch-up window runs today's pass immediately;
        # the dedupe row makes that harmless if it already ran.
        if within_catchup_window(settings, datetime.now(tz=tz)):
            try:
                _log_tick(await run_tick(settings))
            except Exception:
                logger.exception("startup catch-up tick raised; continuing")

        while True:
            now_local = datetime.now(tz=tz)
            due = next_run_at(settings, now_local)
            remaining = (due - now_local).total_seconds()
            if remaining > 0:
                await asyncio.sleep(min(remaining, _MAX_SLEEP_SECONDS))
                continue
            try:
                _log_tick(await run_tick(settings))
            except Exception:
                # Never let a bad tick end the loop; tomorrow deserves a try.
                logger.exception("reminder tick raised; continuing loop")
            # Step past the target so the next next_run_at lands on tomorrow.
            await asyncio.sleep(60)
    finally:
        await close_pool()


def main() -> None:
    """CLI: ``python -m setlist_stash.reminders [--loop]``."""
    parser = argparse.ArgumentParser(
        prog="setlist-stash-reminders",
        description="Email show-day pick reminders to opted-in players.",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help=(
            "Run forever, waking at REMINDER_HOUR_LOCAL:REMINDER_MINUTE_LOCAL "
            "in DISPLAY_TZ. Without it, run one pass and exit."
        ),
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(_amain(loop=args.loop)))


if __name__ == "__main__":
    main()
