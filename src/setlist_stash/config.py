"""Configuration via pydantic-settings.

All values come from environment variables (or `.env` in dev). Secrets use
`SecretStr` so they never reach logs or repr output.
"""

from __future__ import annotations

from datetime import date

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

#: The session-signing key the repo ships with. It is public: it is in this
#: file, in ``.env.example``, and in every published image. Anything signed
#: with it can be forged by anyone, so a deployment that reaches the public
#: internet while still using it has no session integrity at all. See
#: ``Settings._reject_default_session_secret_in_production``.
# The S105 suppression below is justified, not lazy: ruff is right that this is
# a hardcoded secret, and that is the entire point of the constant. It exists so
# the validator can recognise the shipped default and refuse to boot, which is
# the opposite of the leak S105 guards against.
DEV_SESSION_SECRET = "dev-only-do-not-use-in-prod"  # noqa: S105


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
    # One-line blurb used for the social-preview meta tags (og:description,
    # twitter:description). Empty (the default) DERIVES a band-neutral line from
    # ``mcp_subject`` / ``site_name`` via ``site_description_effective``. The
    # base template used to hardcode a Phish-specific sentence, which every
    # deployment inherited: wappypicks.com advertised itself to social crawlers
    # as a "Phish setlist picks game". Nothing band-specific belongs in the
    # platform. Set SITE_DESCRIPTION per deployment to override the derivation.
    site_description: str = Field(default="")
    # Path under /static/ to an additional CSS file loaded after style.css.
    # Empty disables; e.g. "themes/lot-poster.css" loads the bundled Lot Poster look.
    theme_file: str = Field(default="")
    # --- Icons + social card (deployment-level override) ---
    # Paths under /static/ to the site's mark. Same reasoning as
    # SITE_DESCRIPTION above: these used to be hardcoded filenames in
    # base.html, so every tenant of the shared image served the SAME artwork.
    # One deployment's mark is not the platform's mark. The defaults below are
    # the neutral platform placeholders, so a deployment that sets nothing
    # keeps exactly what it renders today; brand artwork is opted into by
    # naming a file here, never by overwriting the default in the image.
    favicon_svg: str = Field(default="logo-t.svg")
    # Multi-size .ico for clients that ignore the SVG (older browsers, some
    # bookmark bars, link unfurlers). Empty (default) renders no link tag at
    # all rather than pointing at a file the image may not carry.
    favicon_ico: str = Field(default="")
    # PNG fallback icon, used for `rel="alternate icon"`.
    favicon_png: str = Field(default="logo-t-192.png")
    # Home-screen icon for iOS/Android. Rendered large, so it can carry more
    # detail than the favicon.
    apple_touch_icon: str = Field(default="logo-t-192.png")
    # 1200x630 social preview card (og:image / twitter:image).
    og_image: str = Field(default="og-image.png")
    # Optional deployment credit shown in the footer (e.g. "A Brooks New Media
    # Production"). Empty (default) hides the credit line entirely so a third
    # party who self-hosts the OSS image sees no operator branding.
    footer_credit: str = Field(default="")
    # Optional URL the footer credit links to. Only used when footer_credit is
    # set; if empty the credit renders as plain text.
    footer_credit_url: str = Field(default="")
    # Optional data-source attribution shown in the footer (e.g. "Phish.net").
    # Deployment-level override: the phish.net API terms require crediting them
    # as the setlist data source, but the engine is band-agnostic (an Umphreys
    # deployment uses a different source), so this is env-driven, not hardcoded.
    # Empty (default) hides the line so the OSS image / third-party self-host
    # stays clean. Set via DATA_SOURCE_NAME + DATA_SOURCE_URL per deployment.
    data_source_name: str = Field(default="")
    # URL the data-source credit links to. Only used when data_source_name is
    # set; if empty the credit renders as plain text.
    data_source_url: str = Field(default="")
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
    # Whether the per-show comment threads render at all. True (the default)
    # exposes the read-open, handle-gated comment section under each show's
    # predictions page and mounts its routes. Set ENABLE_COMMENTS=false to hide
    # the section and 404 the /show/{date}/comments + /comment/{id}/delete
    # routes for a deployment that doesn't want threads. Same empty/false-means-
    # off idiom as enable_games; the table + module still exist when gated off,
    # so nothing is deleted and no migration is needed.
    enable_comments: bool = Field(default=True)
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
    # Local MCP-client alias suggested on the /connect docs page (the name used
    # in ``claude mcp add ... <alias> <url>`` and as the JSON key in the Claude
    # Desktop example). Deployment-level override; when empty (the default) it
    # is derived from ``mcp_subject`` via ``mcp_alias_effective`` (first word,
    # slugified, e.g. "Umphrey's McGee" -> "umphreys", "Phish" -> "phish"), so
    # each tenant renders its own band automatically with no per-deployment
    # config. Set MCP_ALIAS explicitly only to override that derivation.
    mcp_alias: str = Field(default="")
    # Directory the blog engine reads ``*.md`` posts from. Deployment-specific:
    # the content is NOT in the image, it's bind-mounted here per deployment
    # (same pattern as THEME_FILE). With nothing mounted the dir is missing,
    # the blog shows no posts, and the nav "Blog" link does not render — so the
    # Phish demo and any third-party self-host stay clean. Absolute path inside
    # the container; the OSS default is an empty mount point.
    blog_dir: str = Field(default="/app/content/blog")
    # Named runs of shows that get their own leaderboard board — a residency,
    # a festival, a venue stand. Deployment-level (oss-platform-split): these
    # are inherently band- and tour-specific ("the MSG summer run" means
    # nothing on an Umphrey's deployment), so they are config, never code.
    # Empty (the default) builds no run boards at all, keeping the OSS image
    # and any third-party self-host clean.
    #
    # Format: ``key=YYYY-MM-DD,YYYY-MM-DD;key2=YYYY-MM-DD``
    #   - ``;`` separates runs, ``,`` separates dates within a run
    #   - keys are slugs (``[a-z0-9][a-z0-9-]*``) and become the scope_key
    # e.g. LEADERBOARD_RUNS=msg-summer-26=2026-07-22,2026-07-24,2026-07-25
    leaderboard_runs: str = Field(default="")
    # The leaderboard's tab bar. Deployment-level for the same reason as
    # LEADERBOARD_RUNS: which boards matter is a per-game editorial call.
    # Empty (the default) renders the platform's original three tabs
    # (Weekly / Season / All-time), so every existing deployment and the OSS
    # image are unchanged unless they opt in.
    #
    # Format: ``Label|scope|scope_key`` per tab, comma-separated. ``scope_key``
    # is optional; omit it to track the newest bucket for that scope (the old
    # behavior), or pin it to freeze a tab on one bucket. Pinning is what lets
    # two tabs share a scope, e.g. a Summer and a Fall board both on ``tour``.
    # e.g. LEADERBOARD_TABS=Summer Tour|tour|2026-summer,MSG Summer 26|run|
    #      msg-summer-26,All Time|all_time,Fall Tour|tour|2026-fall
    #      (one line in the .env; wrapped here only to fit the line limit)
    leaderboard_tabs: str = Field(default="")

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
    # per 60s window per client IP, returning 429 when exceeded. 0 disables the
    # limiter. Which address counts as "the client" is TRUSTED_CLIENT_IP_HEADER
    # below -- and on a public deployment that setting is what makes this limit
    # real, because an unkeyed-to-anything limiter is not a limit.
    mcp_rate_limit_per_minute: int = Field(default=60, ge=0)

    # --- Client address resolution ---
    # The ONE request header this deployment's edge is known to set and to
    # overwrite for inbound requests, e.g. CF-Connecting-IP behind Cloudflare.
    # Used for the /mcp rate-limit key and the magic-link audit trail.
    #
    # Empty (the default) means "trust no header, use the socket peer", which is
    # correct for LAN/Tailscale and is the safe default for any self-hoster who
    # has not told us what fronts them. Set this ONLY when the app cannot be
    # reached around that edge; otherwise a caller can set the header directly
    # and choose their own rate-limit bucket. See client_addr.py.
    trusted_client_ip_header: str = Field(default="")

    # --- Showtime lock policy ---
    # DEFAULT_LOCK_TIME_LOCAL is interpreted in DEFAULT_LOCK_TZ to compute the
    # stored lock instant (a UTC TIMESTAMPTZ). This is the *anchor* tz — the
    # zone the wall-clock lock time is expressed in when it's set.
    default_lock_time_local: str = Field(default="22:00")
    default_lock_tz: str = Field(default="America/New_York")
    # DISPLAY_TZ is the zone lock/show times are *rendered* in for viewers,
    # independent of the anchor tz above. Defaults to US Eastern; ZoneInfo is
    # DST-aware, so it renders EDT in summer and EST in winter automatically
    # (never a hardcoded abbreviation). Most players are Eastern, so times are
    # shown in Eastern regardless of where the venue is.
    display_tz: str = Field(default="America/New_York")

    # --- Auto-resolve cron ---
    # Legacy (Phase 4 plan §5 Option A naming). Kept for backwards-compat
    # with .env files that already set it.
    resolve_interval_minutes: int = Field(default=30, ge=1)
    # Inside-container loop interval (docs/PHASE-4-PLAN.md §5 Option B; used by
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
    #
    # !! COUPLED TO ``resolver_active_interval_seconds`` !!
    # What actually protects a show is the QUIET WINDOW this buys:
    #     quiet_window = resolver_stable_polls_required * active_interval
    # The default pair (30 x 60s) is 30 minutes of no new tracks after the
    # encore is seen. Speeding the poll cadence up WITHOUT raising this count
    # silently shortens that window — at a 60s cadence, 6 polls is 6 minutes,
    # short enough that a gap between encore song 1 and encore song 2 reads as
    # "final" and freezes an incomplete score. Change the two together and keep
    # the product at ~30 minutes. The resolver logs the derived quiet window on
    # startup so the effective value is observable, not inferred.
    resolver_stable_polls_required: int = Field(default=30, ge=1)
    # ...OR this many hours have elapsed since the effective lock (time
    # backstop). A Phish show is ~3h and the setlist settles well within this,
    # so 6h guarantees eventual scoring even if the stability signal never
    # converges (e.g. phish.net edits trickle for days).
    resolver_backstop_hours: int = Field(default=6, ge=1)
    # Fast poll cadence used while an open unresolved lock has an active show
    # window (between effective lock and lock + backstop). Default 60s: fast
    # enough that the live page feels live, and the only thing that ever talks
    # to the upstream MCP during a show (page renders read the snapshot from
    # Postgres, so viewers add no upstream load at any refresh rate).
    #
    # An interval shorter than the upstream MCP's hot-window cache TTL just
    # re-reads a cached response and buys nothing. mcp-phish ships
    # HOT_WINDOW_CACHE_TTL_SECONDS=90, so lower that to <= this value if you go
    # below 90s — otherwise the completeness gate counts cache hits as "stable"
    # polls, which is exactly the failure mode documented in completeness.py.
    #
    # This is the REAL ceiling on how fresh the live show page can be: the page
    # renders the setlist snapshot the resolver scored, so tightening the
    # browser refresh below this value buys nothing. Speed both up together, or
    # neither. See resolver_stable_polls_required for the coupling that keeps
    # the completeness gate honest at a faster cadence.
    resolver_active_interval_seconds: int = Field(default=60, ge=30)
    # How long after the effective lock the show window stays "active" for the
    # fast cadence. Defaults to the backstop so the coarse interval resumes
    # once the backstop would have fired anyway.
    resolver_active_window_hours: int = Field(default=6, ge=1)

    # --- Live show page refresh ---
    # Seconds between browser refreshes of the live show board (setlist +
    # standings) on /show/<date>/predictions. ONE htmx poll re-renders both
    # halves from a single server response, so scores can never lag the setlist
    # on screen.
    #
    # It only runs while the show is actually live (post-lock, unresolved, and
    # inside RESOLVER_ACTIVE_WINDOW_HOURS of the lock) — outside that window the
    # page is static, exactly as before. Set to 0 to disable auto-refresh.
    #
    # Costs nothing upstream: the board reads the resolver's setlist snapshot
    # out of Postgres, so extra viewers and faster polling never touch
    # phish.net / allthings.umphreys.com. Only the resolver does.
    # There is no point setting this lower than
    # RESOLVER_ACTIVE_INTERVAL_SECONDS — that's how often the data behind it
    # actually changes.
    live_refresh_seconds: int = Field(default=60, ge=0, le=3600)

    # --- Session / handle ---
    session_secret: SecretStr = Field(default=SecretStr(DEV_SESSION_SECRET))
    # Send the ``Secure`` flag on session/flash cookies. False (the default)
    # keeps LAN/Tailscale-over-HTTP dev working; set COOKIE_SECURE=true on any
    # HTTPS deployment (e.g. tweezerpicks.com behind Cloudflare) so the cookies
    # are only ever sent over TLS.
    cookie_secure: bool = Field(default=False)
    # How long a session cookie stays valid, enforced SERVER-side against the
    # timestamp inside the signed token (see auth.py). The browser's own
    # expiry is a hint a client can ignore; this is the one that decides.
    # 30 days keeps a casual player signed in across a tour without leaving a
    # captured cookie usable for a year. Per-user revocation is separate: bump
    # users.session_epoch (migration 012) to drop that user's cookies now.
    session_max_age_days: int = Field(default=30, ge=1, le=365)

    # --- Response security headers ---
    # Master switch for the CSP / framing / referrer / HSTS header set (see
    # security_headers.py). On by default. The escape hatch exists for an
    # operator fronting the app with a proxy that sets its own policy and does
    # not want two Content-Security-Policy headers intersecting, which is a
    # genuinely confusing failure mode.
    security_headers: bool = Field(default=True)
    # Strict-Transport-Security max-age, in seconds. Only ever emitted when
    # COOKIE_SECURE is true (this app's "I am HTTPS-only" declaration), so a
    # LAN or Tailscale-over-HTTP deployment never sends it. 0 disables it even
    # on an HTTPS deployment. Default is one year, the conventional value.
    # NOTE this is sticky: a browser that sees it will refuse plain HTTP to the
    # host for the full duration, so lower it BEFORE any planned move off TLS.
    # includeSubDomains/preload are deliberately not offered here.
    hsts_max_age_seconds: int = Field(default=31_536_000, ge=0)
    # Extra origins to append to the CSP's script-src / connect-src, as a
    # space-separated list. Empty by default: the shipped policy names no third
    # party unless that deployment configured one.
    #
    # This exists because an edge/CDN in front of the app can inject a script
    # the app never rendered and therefore cannot know about. Found live rather
    # than in review: Cloudflare's Web Analytics injects
    # ``static.cloudflareinsights.com/beacon.min.js`` into the HTML at the
    # edge, so the first CSP deploy blocked it on both public deployments while
    # every local and CI check passed. Hardcoding Cloudflare into a
    # band-agnostic platform would be wrong; letting a deployment name its own
    # edge is not.
    #
    #   CSP_EXTRA_SCRIPT_SRC=https://static.cloudflareinsights.com
    #   CSP_EXTRA_CONNECT_SRC=https://cloudflareinsights.com
    csp_extra_script_src: str = Field(default="")
    csp_extra_connect_src: str = Field(default="")

    # --- Google SSO (Phase 1) ---
    # OAuth 2.0 / OpenID Connect "Web application" client credentials. Both
    # empty (the default) hides "Sign in with Google" entirely, so the OSS
    # image, the Wappy sibling deployment, and any third-party self-host stay
    # unaffected until they provision their own client (same empty-string-means-
    # disabled gating idiom used for MCP_PUBLIC_URL etc). The redirect URI is
    # derived from BASE_URL as ``{base_url}/auth/google/callback``.
    google_client_id: str = Field(default="")
    google_client_secret: SecretStr = Field(default=SecretStr(""))

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
    # MUST stay False during the prediction window. See docs/PHASE-4-PLAN.md.
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

    @model_validator(mode="after")
    def _reject_default_session_secret_in_production(self) -> Settings:
        """Refuse to boot on a public deployment still using the shipped key.

        ``SESSION_SECRET`` signs the ``phishgame_session`` identity cookie and
        keys the OAuth state cookie. The default is published in this repo and
        baked into every image on ghcr.io, so a deployment that keeps it lets
        anyone mint a cookie for any user id. Nothing in the app misbehaves
        when that happens, which is exactly why it would go unnoticed.

        Fail closed rather than warn. A log line at startup is the wrong shape
        for this: it is emitted once, into a stream nobody reads on the happy
        path, and the site keeps serving. Two independent signals say "this is
        not a laptop":

        * ``COOKIE_SECURE=true`` - the operator has declared HTTPS-only.
        * ``BASE_URL`` is https - the operator has a public origin.

        Either one plus the default secret is a hard error at construction, so
        the container exits instead of serving forgeable sessions. Local and
        LAN development (http base_url, COOKIE_SECURE unset) is untouched, and
        so is the whole test suite.
        """
        if self.session_secret.get_secret_value() != DEV_SESSION_SECRET:
            return self
        reasons = []
        if self.cookie_secure:
            reasons.append("COOKIE_SECURE=true")
        if self.base_url.strip().lower().startswith("https://"):
            reasons.append("BASE_URL is https")
        if not reasons:
            return self
        raise ValueError(
            "SESSION_SECRET is still the shipped development default, but "
            f"this looks like a production deployment ({' and '.join(reasons)}). "
            "That key is public, so session cookies signed with it can be "
            "forged by anyone. Set SESSION_SECRET to a long random string, "
            "e.g. `python -c \"import secrets; print(secrets.token_urlsafe(48))\"`. "
            "Rotating it signs every existing session out, which is the "
            "correct outcome here."
        )

    @property
    def google_oauth_enabled(self) -> bool:
        """True only when a Google OAuth client is fully configured.

        Mirrors the empty-string-means-disabled gate used elsewhere: when
        either the id or the secret is unset, the "Sign in with Google" entry
        points disappear and the /auth/google/* routes redirect home.
        """
        return bool(
            self.google_client_id and self.google_client_secret.get_secret_value()
        )

    @property
    def google_redirect_uri(self) -> str:
        """The OAuth redirect URI, derived from ``base_url``."""
        return f"{self.base_url.rstrip('/')}/auth/google/callback"

    @property
    def mcp_alias_effective(self) -> str:
        """Resolve the /connect MCP-client alias for this deployment.

        Explicit ``MCP_ALIAS`` wins. Otherwise derive a slug from the first
        word of ``mcp_subject`` (e.g. "Umphrey's McGee" -> "umphreys",
        "Phish" -> "phish"). Falls back to a generic "setlist" when neither is
        usable, so a deployment that exposes an MCP without a subject still
        renders a sane, band-neutral alias.
        """
        if self.mcp_alias:
            return self.mcp_alias
        if self.mcp_subject:
            first = self.mcp_subject.split()[0] if self.mcp_subject.split() else ""
            slug = "".join(ch for ch in first.lower() if ch.isalnum())
            if slug:
                return slug
        return "setlist"

    @property
    def site_description_effective(self) -> str:
        """Social-preview blurb for this deployment.

        Explicit ``SITE_DESCRIPTION`` wins. Otherwise derive from the band name
        the deployment already declares for the /connect page
        (``MCP_SUBJECT``), falling back to the site name and finally to a fully
        generic line. Never mentions a band the deployment does not serve.
        """
        if self.site_description:
            return self.site_description
        if self.mcp_subject:
            return (
                f"{self.mcp_subject} setlist picks game. "
                "Make your calls before the lights go down."
            )
        return (
            f"{self.site_name}: a setlist prediction game. "
            "Make your calls before the lights go down."
        )

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
