"""Configuration via pydantic-settings.

All values come from environment variables (or `.env` in dev). Secrets use
`SecretStr` so they never reach logs or repr output.
"""

from __future__ import annotations

from datetime import date

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """setlist-stash runtime settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Branding (deployment-level override) ---
    site_name: str = Field(default="Open Setlist Stash")
    # Path under /static/ to an additional CSS file loaded after style.css.
    # Empty disables; e.g. "themes/lot-poster.css" loads the bundled Lot Poster look.
    theme_file: str = Field(default="")
    # Optional deployment credit shown in the footer (e.g. "A Brooks New Media
    # Production"). Empty (default) hides the credit line entirely so a third
    # party who self-hosts the OSS image sees no operator branding.
    footer_credit: str = Field(default="")
    # Optional URL the footer credit links to. Only used when footer_credit is
    # set; if empty the credit renders as plain text.
    footer_credit_url: str = Field(default="")
    # Google Analytics 4 measurement ID (e.g. "G-XXXXXXXXXX"). Deployment-level
    # override: when set, every page renders the gtag.js snippet; when empty
    # (the default) NO analytics tag renders at all, so the OSS image and any
    # third-party self-host stay clean. Never bake a real ID into the repo —
    # set it per deployment via the ANALYTICS_ID env var (oss-platform-split).
    analytics_id: str = Field(default="")
    # Optional beta notice rendered as a small banner on the home page only.
    # Deployment-level override (oss-platform-split): when set, the home hero
    # shows the text in a subtle ``.beta-notice`` banner; when empty (the
    # default) NOTHING renders, so the OSS image and any third-party self-host
    # (and the Phish demo) stay clean. Edit/clear it per deployment via the
    # BETA_NOTICE env var with no rebuild.
    beta_notice: str = Field(default="")
    # Whether the private-leagues / shareable-game feature is exposed at all.
    # Deployment-level gate (oss-platform-split): True (the default) keeps the
    # full games experience (the Phish demo, the OSS image, any third-party
    # self-host). Set ENABLE_GAMES=false to strip every league/game route and
    # link, turning the deployment into a single global per-show contest
    # (Wappy Picks). The league code, tables, and routes still exist when
    # gated off — the routes just 404/redirect and the templates hide the
    # links — so nothing is deleted and no migration is needed.
    enable_games: bool = Field(default=True)
    # Public Streamable-HTTP endpoint for this deployment's read-only MCP
    # server, surfaced on the /connect docs page so visitors can wire the
    # band's setlist data into their own MCP client (Claude Code, Claude
    # Desktop, etc.). Empty (the default) hides the /connect nav link and
    # serves a "no public MCP on this deployment" panel — so the OSS image and
    # the Phish demo stay clean (oss-platform-split). Set per deployment via
    # the MCP_PUBLIC_URL env var (e.g. https://www.wappypicks.com/mcp).
    mcp_public_url: str = Field(default="")
    # Short human name for the band/catalog the MCP serves, used in the
    # /connect docs copy (e.g. "Umphrey's McGee"). Falls back to a generic
    # phrase when empty.
    mcp_subject: str = Field(default="")
    # Directory the blog engine reads ``*.md`` posts from. Deployment-specific:
    # the content is NOT in the image, it's bind-mounted here per deployment
    # (same pattern as THEME_FILE). With nothing mounted the dir is missing,
    # the blog shows no posts, and the nav "Blog" link does not render — so the
    # Phish demo and any third-party self-host stay clean. Absolute path inside
    # the container; the OSS default is an empty mount point.
    blog_dir: str = Field(default="/app/content/blog")

    # --- HTTP server ---
    app_host: str = Field(default="0.0.0.0")
    app_port: int = Field(default=3706, ge=1, le=65535)
    log_format: str = Field(default="json")

    # --- Postgres (game state only) ---
    pg_host: str = Field(default="postgres")
    pg_port: int = Field(default=5432, ge=1, le=65535)
    pg_db: str = Field(default="setlist_stash")
    pg_user: str = Field(default="setlist_stash")
    pg_password: SecretStr = Field(default=SecretStr("changeme"))

    # --- mcp-phish read path ---
    mcp_phish_url: str = Field(default="http://mcp-phish:3705/mcp")
    mcp_phish_timeout_seconds: float = Field(default=15.0, gt=0)

    # --- Public MCP reverse proxy (/mcp) ---
    # Upstream Streamable-HTTP MCP endpoint that public /mcp traffic is proxied
    # to. Empty (the default) disables the proxy entirely: /mcp is not mounted,
    # so the OSS image and the Phish demo never expose an upstream (they simply
    # don't route public traffic there — oss-platform-split). Set per deployment
    # (e.g. http://mcp-umphreys:3717/mcp on the Wappy Picks game) to turn the
    # public reverse proxy on. This is the internal docker-network URL; the
    # public-facing URL advertised on /connect is MCP_PUBLIC_URL.
    mcp_upstream_url: str = Field(default="")
    # Per-request timeout (seconds) for the upstream MCP proxy. Streaming SSE
    # responses can stay open, so this bounds connect/read on the upstream
    # rather than the full stream duration; keep it generous but finite so a
    # hung upstream can't pin a worker forever.
    mcp_proxy_timeout_seconds: float = Field(default=30.0, gt=0)
    # Per-IP rate limit for the public /mcp proxy ONLY (the game UI is never
    # rate-limited). Fixed-window: at most ``mcp_rate_limit_per_minute`` requests
    # per 60s window per client IP, returning 429 when exceeded. The app sits
    # behind Cloudflare, so the client IP is taken from the first X-Forwarded-For
    # hop (falling back to the socket peer). 0 disables the limiter.
    mcp_rate_limit_per_minute: int = Field(default=60, ge=0)

    # --- Showtime lock policy ---
    default_lock_time_local: str = Field(default="22:00")
    default_lock_tz: str = Field(default="America/New_York")

    # --- Auto-resolve cron ---
    # Legacy (Phase 4 plan §5 Option A naming). Kept for backwards-compat
    # with .env files that already set it.
    resolve_interval_minutes: int = Field(default=30, ge=1)
    # Inside-container loop interval (PHASE-4-PLAN.md §5 Option B; used by
    # the setlist-stash-resolver service).
    resolver_interval_seconds: int = Field(default=1800, ge=60)
    # Conservative cancelled-show window. A show whose lock_at is older than
    # this and still has no setlist data gets stamped cancelled. Don't drop
    # below 24h: phish.net's setlist publish can lag, especially overnight.
    resolver_cancel_after_hours: int = Field(default=72, ge=24)

    # --- Setlist-completeness gate (game-night scoring) ---
    # phish.net setlists are typed in live DURING the show and grow set by set,
    # encore entered last. Scoring on the first non-empty setlist would score
    # everyone's encore pick against the end of Set 1 and lock those wrong
    # scores in forever. The resolver therefore scores a show ONLY when its
    # setlist looks final. A setlist is COMPLETE when an encore is detected AND
    # the track count has held steady across this many consecutive polls...
    resolver_stable_polls_required: int = Field(default=6, ge=1)
    # ...OR this many hours have elapsed since the effective lock (time
    # backstop). A Phish show is ~3h and the setlist settles well within this,
    # so 6h guarantees eventual scoring even if the stability signal never
    # converges (e.g. phish.net edits trickle for days).
    resolver_backstop_hours: int = Field(default=6, ge=1)
    # Fast poll cadence used while an open unresolved lock has an active show
    # window (between effective lock and lock + backstop). Default 5 min,
    # matching the phish-vault active-poll cadence so stable-poll math lines up
    # (6 stable polls * 5 min = 30 min of no new tracks).
    resolver_active_interval_seconds: int = Field(default=300, ge=30)
    # How long after the effective lock the show window stays "active" for the
    # fast cadence. Defaults to the backstop so the coarse interval resumes
    # once the backstop would have fired anyway.
    resolver_active_window_hours: int = Field(default=6, ge=1)

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

    # --- Private leagues (Phase 4c) ---
    # Soft cap on members per league. Enforced at join time. Existing leagues
    # keep their cap value (column ``leagues.member_cap``); this default is
    # used when a new league is created without an explicit override.
    league_member_cap: int = Field(default=500, ge=1, le=10000)

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
