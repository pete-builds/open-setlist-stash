"""Unit tests for the email provider abstraction.

Covers Disabled / Log / Smtp providers and the ``build_provider`` factory.
SmtpProvider is tested by mocking ``aiosmtplib.send`` so we don't actually
open a TCP connection — that's an integration concern, not unit.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import SecretStr

from phish_game.config import Settings
from phish_game.email import (
    DisabledProvider,
    EmailSendError,
    LogProvider,
    SmtpProvider,
    build_provider,
)


@pytest.mark.asyncio
async def test_disabled_provider_raises() -> None:
    p = DisabledProvider()
    assert p.name == "disabled"
    with pytest.raises(EmailSendError):
        await p.send(to="x@example.com", subject="hi", body="hi")


@pytest.mark.asyncio
async def test_log_provider_writes_message_to_logger(
    caplog: pytest.LogCaptureFixture,
) -> None:
    p = LogProvider()
    assert p.name == "log"
    body = "Click here: http://example.com/auth/verify?token=abc\nthanks"
    with caplog.at_level(logging.INFO, logger="phish_game.email"):
        await p.send(to="user@example.com", subject="Hello", body=body)
    # Subject + recipient on the preamble line.
    preamble = [r for r in caplog.records if "EMAIL (log provider)" in r.getMessage()]
    assert len(preamble) == 1
    assert "user@example.com" in preamble[0].getMessage()
    assert "Hello" in preamble[0].getMessage()
    # Each body line emitted separately.
    body_lines = [r.getMessage() for r in caplog.records if "EMAIL body" in r.getMessage()]
    assert any("token=abc" in line for line in body_lines)
    assert any("thanks" in line for line in body_lines)


def test_smtp_provider_requires_host_and_sender() -> None:
    with pytest.raises(ValueError, match="SMTP_HOST"):
        SmtpProvider(
            host="", port=587, username="u", password="p", sender="from@x.com",
        )
    with pytest.raises(ValueError, match="SMTP_FROM"):
        SmtpProvider(
            host="smtp.example.com", port=587, username="u", password="p",
            sender="",
        )


@pytest.mark.asyncio
async def test_smtp_provider_send_invokes_aiosmtplib() -> None:
    p = SmtpProvider(
        host="smtp.example.com",
        port=587,
        username="u",
        password="p",
        sender="from@example.com",
    )
    with patch("aiosmtplib.send", new=AsyncMock(return_value=None)) as mock_send:
        await p.send(to="to@example.com", subject="Hi", body="Body")
    assert mock_send.await_count == 1
    # Inspect what it was called with: first positional arg is the EmailMessage.
    args, kwargs = mock_send.await_args
    assert kwargs["hostname"] == "smtp.example.com"
    assert kwargs["port"] == 587
    assert kwargs["username"] == "u"
    assert kwargs["password"] == "p"
    assert kwargs["start_tls"] is True
    msg = args[0]
    assert msg["From"] == "from@example.com"
    assert msg["To"] == "to@example.com"
    assert msg["Subject"] == "Hi"
    # Body content is set via set_content; the payload should round-trip.
    payload = msg.get_content().rstrip()
    assert payload == "Body"


@pytest.mark.asyncio
async def test_smtp_provider_wraps_failures() -> None:
    p = SmtpProvider(
        host="smtp.example.com",
        port=587,
        username="u",
        password="p",
        sender="from@example.com",
    )
    boom = ConnectionRefusedError("nope")
    with (
        patch("aiosmtplib.send", new=AsyncMock(side_effect=boom)),
        pytest.raises(EmailSendError, match="SMTP send failed"),
    ):
        await p.send(to="to@example.com", subject="Hi", body="Body")


def _settings(**overrides: Any) -> Settings:
    defaults = {
        "session_secret": SecretStr("test-secret"),
        "pg_password": SecretStr("test-pw"),
        "smtp_pass": SecretStr(""),
    }
    defaults.update(overrides)
    return Settings(**defaults)  # type: ignore[arg-type]


def test_build_provider_disabled_default() -> None:
    p = build_provider(_settings(email_provider="disabled"))
    assert isinstance(p, DisabledProvider)


def test_build_provider_log_mode() -> None:
    p = build_provider(_settings(email_provider="log"))
    assert isinstance(p, LogProvider)


def test_build_provider_smtp_mode() -> None:
    p = build_provider(
        _settings(
            email_provider="smtp",
            smtp_host="smtp.example.com",
            smtp_port=587,
            smtp_user="user",
            smtp_pass=SecretStr("pw"),
            smtp_from="from@example.com",
        )
    )
    assert isinstance(p, SmtpProvider)
    assert p.host == "smtp.example.com"
    assert p.sender == "from@example.com"


def test_build_provider_unknown_mode_falls_back_to_disabled() -> None:
    p = build_provider(_settings(email_provider="not-a-real-mode"))
    assert isinstance(p, DisabledProvider)


def test_build_provider_case_insensitive() -> None:
    p = build_provider(_settings(email_provider="LOG"))
    assert isinstance(p, LogProvider)
