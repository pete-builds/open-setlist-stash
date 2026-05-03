"""Configuration via pydantic-settings.

All values come from environment variables (or `.env` in dev). Secrets use
`SecretStr` so they never reach logs or repr output.
"""

from __future__ import annotations

from datetime import date

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """phish-game runtime settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- HTTP server ---
    app_host: str = Field(default="0.0.0.0")
    app_port: int = Field(default=3706, ge=1, le=65535)
    log_format: str = Field(default="json")

    # --- Postgres (game state only) ---
    pg_host: str = Field(default="postgres")
    pg_port: int = Field(default=5432, ge=1, le=65535)
    pg_db: str = Field(default="phish_game")
    pg_user: str = Field(default="phish_game")
    pg_password: SecretStr = Field(default=SecretStr("changeme"))

    # --- mcp-phish read path ---
    mcp_phish_url: str = Field(default="http://mcp-phish:3705/mcp")
    mcp_phish_timeout_seconds: float = Field(default=15.0, gt=0)

    # --- Showtime lock policy ---
    default_lock_time_local: str = Field(default="22:00")
    default_lock_tz: str = Field(default="America/New_York")

    # --- Auto-resolve cron ---
    # Legacy (Phase 4 plan §5 Option A naming). Kept for backwards-compat
    # with .env files that already set it.
    resolve_interval_minutes: int = Field(default=30, ge=1)
    # Inside-container loop interval (PHASE-4-PLAN.md §5 Option B; used by
    # the phish-game-resolver service).
    resolver_interval_seconds: int = Field(default=1800, ge=60)
    # Conservative cancelled-show window. A show whose lock_at is older than
    # this and still has no setlist data gets stamped cancelled. Don't drop
    # below 24h: phish.net's setlist publish can lag, especially overnight.
    resolver_cancel_after_hours: int = Field(default=72, ge=24)

    # --- Session / handle ---
    session_secret: SecretStr = Field(default=SecretStr("dev-only-do-not-use-in-prod"))

    # --- Magic-link email (Phase 4b) ---
    # Provider selector. ``disabled`` (default) hides the email UI behind a
    # 503; ``log`` writes the full message to logger at INFO (used on nix1
    # until Pete provisions a Gmail app password); ``smtp`` sends via the
    # configured SMTP_* settings below.
    email_provider: str = Field(default="disabled")
    # Base URL used to construct magic-link URLs in the email body.
    # Defaults to the Tailscale-only nix1 URL. Override per environment
    # (e.g. ``http://localhost:3706`` for dev, https URL for Phase 6).
    base_url: str = Field(default="http://nix1:3706")
    # Magic-link token TTL. 24h matches "click the link from your inbox
    # later today" expectations without leaving long-lived bearer tokens
    # outstanding.
    magic_link_ttl_hours: int = Field(default=24, ge=1, le=168)
    # Max outstanding (un-consumed, un-expired) magic links per user. New
    # requests beyond this expire the oldest. Stops accidental "spam me 30
    # links" loops.
    magic_link_max_outstanding: int = Field(default=3, ge=1, le=10)
    # SMTP settings — only consulted when EMAIL_PROVIDER=smtp.
    smtp_host: str = Field(default="")
    smtp_port: int = Field(default=587, ge=1, le=65535)
    smtp_user: str = Field(default="")
    smtp_pass: SecretStr = Field(default=SecretStr(""))
    smtp_from: str = Field(default="")

    # --- Smart-pick assist gate ---
    # MUST stay False during the prediction window. See PHASE-4-PLAN.md.
    assist_pre_lock: bool = Field(default=False)

    # --- Predict form show selection ---
    # Operator override. When set, the predict form targets this show.
    # When unset, ``select_form_show`` walks ``recent_shows`` for the next
    # future date.
    admin_show_date: date | None = Field(default=None)
    admin_show_venue: str | None = Field(default=None)
    admin_show_location: str | None = Field(default=None)

    @property
    def pg_dsn(self) -> str:
        """Build an asyncpg-compatible Postgres DSN."""
        return (
            f"postgresql://{self.pg_user}:{self.pg_password.get_secret_value()}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )


def get_settings() -> Settings:
    """Construct a fresh Settings instance.

    Wrapped so tests can monkeypatch envvars and reload.
    """
    return Settings()
