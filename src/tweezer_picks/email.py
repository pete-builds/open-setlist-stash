"""Email transport provider abstraction.

Phase 4b ships behind ``EMAIL_PROVIDER`` env var with three modes:

- ``disabled`` (default): the magic-link UI is wired up but ``send`` raises
  HTTP 503. No outbound traffic of any kind.
- ``log``: writes the full message (recipient, subject, body) to the logger
  at INFO. Used on nix1 today so Pete can verify the full flow without an
  app password configured. No outbound traffic.
- ``smtp``: sends via SMTP using the standard ``aiosmtplib`` STARTTLS flow.
  Reads ``SMTP_HOST/PORT/USER/PASS/FROM`` from settings. This is the
  production path; flip to it once Gmail app password is provisioned.

The provider is selected at app startup via ``build_provider(settings)`` and
stored once on the app state. Routes call ``provider.send(...)`` rather
than caring about the transport.

Auth boundary note: the platform is Tailscale/LAN-only through Phase 5
(see PHASE-4-PLAN.md and phish-platform.md). Magic links emailed today
will only work for Pete's Gmail -> Tailscale browser. Documented; that
is the intended use case for Phase 4b.
"""

from __future__ import annotations

import logging
from email.message import EmailMessage
from typing import Protocol

from tweezer_picks.config import Settings

logger = logging.getLogger("tweezer_picks.email")


class EmailSendError(RuntimeError):
    """Raised when a provider cannot deliver a message.

    Routes catch this and surface a user-facing 503 / error template so an
    SMTP outage never silently swallows a verification request.
    """


class EmailProvider(Protocol):
    """Minimum surface every provider must implement.

    ``send`` is async to keep the door open for SMTP / HTTP transports that
    don't want to block the event loop. For the in-process LogProvider it's
    a no-op coroutine.
    """

    name: str

    async def send(self, *, to: str, subject: str, body: str) -> None: ...


class DisabledProvider:
    """No-op provider that refuses all sends.

    Routes that depend on an active provider check ``provider.name`` (or
    catch ``EmailSendError``) to decide whether to surface the email UI.
    """

    name = "disabled"

    async def send(self, *, to: str, subject: str, body: str) -> None:
        logger.info(
            "email send blocked (provider=disabled)",
            extra={"to": to, "subject": subject},
        )
        raise EmailSendError(
            "Email is disabled. Set EMAIL_PROVIDER=log (dev) or "
            "EMAIL_PROVIDER=smtp (prod) and configure SMTP_* settings."
        )


class LogProvider:
    """Logs the full message instead of sending it.

    Useful for dev and for the nix1-without-app-password mode. The full
    body is written at INFO so the magic link is grep-able from
    ``docker logs tweezer-picks``.

    Caveat: do NOT use in production. Anyone with log access can complete
    a verification flow. Documented in the env var help text.
    """

    name = "log"

    async def send(self, *, to: str, subject: str, body: str) -> None:
        # Single-line preamble + indented body; easy to spot in container logs.
        logger.info(
            "EMAIL (log provider) -> %s | subject: %s",
            to,
            subject,
        )
        for line in body.splitlines():
            logger.info("EMAIL body | %s", line)


class SmtpProvider:
    """SMTP transport via ``aiosmtplib`` (async) with STARTTLS.

    Configuration (all required when EMAIL_PROVIDER=smtp):
      - SMTP_HOST, SMTP_PORT (default 587 for STARTTLS)
      - SMTP_USER, SMTP_PASS  (Gmail: an app password, NOT the account password)
      - SMTP_FROM             (envelope-from + Header From)

    Failure modes are mapped to ``EmailSendError`` so routes can render a
    clean user-facing message rather than a 500 with a backtrace.
    """

    name = "smtp"

    def __init__(
        self,
        *,
        host: str,
        port: int,
        username: str,
        password: str,
        sender: str,
        starttls: bool = True,
        timeout_seconds: float = 10.0,
    ) -> None:
        if not host:
            raise ValueError("SMTP_HOST must be set when EMAIL_PROVIDER=smtp")
        if not sender:
            raise ValueError("SMTP_FROM must be set when EMAIL_PROVIDER=smtp")
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.sender = sender
        self.starttls = starttls
        self.timeout_seconds = timeout_seconds

    def _build_message(self, *, to: str, subject: str, body: str) -> EmailMessage:
        msg = EmailMessage()
        msg["From"] = self.sender
        msg["To"] = to
        msg["Subject"] = subject
        msg.set_content(body)
        return msg

    async def send(self, *, to: str, subject: str, body: str) -> None:
        # Imported lazily so Disabled/Log providers don't pay the import cost
        # in environments where aiosmtplib isn't installed (e.g. minimal CI
        # paths). aiosmtplib IS in requirements.lock, so this always works
        # at runtime in our actual containers.
        try:
            import aiosmtplib
        except ImportError as exc:  # pragma: no cover - install-time guard
            raise EmailSendError(
                "aiosmtplib is not installed; cannot send via SMTP"
            ) from exc

        msg = self._build_message(to=to, subject=subject, body=body)
        try:
            await aiosmtplib.send(
                msg,
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                start_tls=self.starttls,
                timeout=self.timeout_seconds,
            )
        except Exception as exc:
            # aiosmtplib raises a family of SMTP-specific errors plus
            # OSError / TimeoutError. Wrap them so callers don't have to
            # import aiosmtplib themselves.
            logger.exception(
                "smtp send failed",
                extra={"to": to, "host": self.host, "port": self.port},
            )
            raise EmailSendError(f"SMTP send failed: {exc}") from exc
        logger.info(
            "smtp send ok",
            extra={"to": to, "host": self.host, "port": self.port},
        )


def build_provider(settings: Settings) -> EmailProvider:
    """Factory: pick an EmailProvider based on settings.

    Always returns a provider; defaults to DisabledProvider so a missing
    env var never accidentally enables email delivery.
    """
    mode = (settings.email_provider or "disabled").strip().lower()
    if mode == "log":
        return LogProvider()
    if mode == "smtp":
        return SmtpProvider(
            host=settings.smtp_host,
            port=settings.smtp_port,
            username=settings.smtp_user,
            password=settings.smtp_pass.get_secret_value(),
            sender=settings.smtp_from,
        )
    if mode != "disabled":
        logger.warning(
            "unknown EMAIL_PROVIDER=%r; falling back to disabled", mode
        )
    return DisabledProvider()
