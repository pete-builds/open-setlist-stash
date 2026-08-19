"""Response security headers.

Pure header construction, kept out of ``server.py`` so the policy can be unit
tested without standing up an app or a request.

Why this app in particular needs a CSP: every page renders **user-controlled
strings**. Handles appear on the leaderboard, on every per-show standings row,
and on ``/u/{handle}``; comment bodies appear verbatim under each show. Jinja
autoescaping is the primary defence and it is on, but a CSP is the layer that
still holds if a single ``|safe`` gets added to the wrong template later.

The frontend is server-rendered Jinja plus HTMX, with no bundler. Everything
the policy allows was read off the templates rather than guessed:

* ``htmx.min.js`` is **vendored** under ``/static/vendor/`` (no CDN), so
  ``script-src 'self'`` covers it.
* Google Fonts stylesheets come from ``fonts.googleapis.com`` and the font
  files from ``fonts.gstatic.com``.
* ``gtag.js`` loads from ``www.googletagmanager.com`` and beacons back to the
  ``*.google-analytics.com`` hosts, but ONLY when ``ANALYTICS_ID`` is set.
  A deployment with no analytics id gets a policy that never names Google
  Analytics at all.
* Blog posts are operator-authored markdown that may embed remote images, so
  ``img-src`` allows any ``https:`` origin. Images are not a script-execution
  sink; the directives that matter for XSS stay tight.

What the templates cannot tell you is what an **edge injects**. Cloudflare Web
Analytics rewrites HTML at the CDN to add ``static.cloudflareinsights.com/
beacon.min.js``, which no local or CI check can see. ``CSP_EXTRA_SCRIPT_SRC``
and ``CSP_EXTRA_CONNECT_SRC`` exist for exactly that: a deployment declares its
own edge rather than the platform hardcoding one vendor.

**Known weakness, stated plainly:** ``script-src`` includes ``'unsafe-inline'``.
The templates carry inline ``<script>`` blocks; a nonce-based policy needs those
moved into ``/static/*.js`` first, and adding a nonce makes browsers ignore
``'unsafe-inline'``, so a half-migration is worse than either end state.

Inline event handlers are gone (0.3.0). The three
``onsubmit="return confirm(...)"`` attributes became ``data-confirm`` read by a
delegated listener in ``base.html``. That was not just nonce groundwork: one of
them interpolated a user-chosen league name into a JS string literal, and
autoescaping does not defend that context -- the HTML parser turns ``&#39;``
back into a quote before the attribute is compiled as script, so the name broke
out and ran in every other member's session. Values belong in plain attributes,
where autoescaping is the right tool. The policy is still worth shipping: ``default-src
'self'`` blocks an injected ``<script src="https://evil/">`` and blocks
exfiltration to an attacker host, ``base-uri`` blocks ``<base>`` hijacking,
``object-src 'none'`` kills plugin vectors, and ``form-action 'self'`` stops a
form from being repointed at an attacker. Removing ``'unsafe-inline'`` means
moving those inline blocks into ``/static/*.js`` and replacing the three
``confirm()`` handlers with listeners; that is a template change, not a header
change.

``'unsafe-eval'`` is deliberately NOT allowed. HTMX's ``hx-on:`` attributes are
evaluated with ``new Function`` and would have required it; the one use in
``_comments.html`` was replaced with a delegated listener instead.
"""

from __future__ import annotations

from setlist_stash.config import Settings

#: gtag.js itself.
_GA_SCRIPT_HOST = "https://www.googletagmanager.com"
#: Where gtag.js sends measurement beacons (XHR/fetch, hence connect-src).
_GA_CONNECT_HOSTS = (
    "https://www.google-analytics.com",
    "https://analytics.google.com",
    "https://region1.google-analytics.com",
)
_FONT_CSS_HOST = "https://fonts.googleapis.com"
_FONT_FILE_HOST = "https://fonts.gstatic.com"


def build_csp(settings: Settings) -> str:
    """Assemble the Content-Security-Policy for this deployment.

    The Google Analytics origins are only added when ``ANALYTICS_ID`` is set,
    so the OSS default and any self-host without analytics ship a policy with
    no third-party script origin in it whatsoever.
    """
    script_src = ["'self'", "'unsafe-inline'"]
    connect_src = ["'self'"]
    if settings.analytics_id:
        script_src.append(_GA_SCRIPT_HOST)
        connect_src.extend(_GA_CONNECT_HOSTS)
    # Deployment-declared extras, for scripts an edge/CDN injects that the app
    # never rendered and cannot know about. See config.csp_extra_script_src.
    script_src.extend(settings.csp_extra_script_src.split())
    connect_src.extend(settings.csp_extra_connect_src.split())

    directives: list[tuple[str, str]] = [
        # Everything not named below falls back to same-origin only.
        ("default-src", "'self'"),
        # Neutralise <base href> injection, which can silently repoint every
        # relative URL on the page.
        ("base-uri", "'self'"),
        # No plugins, ever.
        ("object-src", "'none'"),
        # Clickjacking. Duplicated as X-Frame-Options for older browsers.
        ("frame-ancestors", "'none'"),
        ("frame-src", "'none'"),
        # A form on this site may only post back to this site.
        ("form-action", "'self'"),
        ("script-src", " ".join(script_src)),
        ("connect-src", " ".join(connect_src)),
        ("style-src", f"'self' 'unsafe-inline' {_FONT_CSS_HOST}"),
        ("font-src", f"'self' data: {_FONT_FILE_HOST}"),
        # data: covers inline SVG/PNG favicons; https: covers remote images in
        # operator-authored blog markdown.
        ("img-src", "'self' data: https:"),
    ]
    return "; ".join(f"{name} {value}" for name, value in directives)


def build_security_headers(settings: Settings) -> dict[str, str]:
    """The full header set applied to every response.

    ``Strict-Transport-Security`` is emitted **only** when ``COOKIE_SECURE`` is
    true. That flag is this app's existing "the operator has declared this
    deployment HTTPS-only" signal, and HSTS on a plain-HTTP LAN or Tailscale
    deployment would be either ignored or actively wrong. ``includeSubDomains``
    and ``preload`` are deliberately omitted: both are far harder to walk back
    than a bare max-age, and neither is this app's call to make for an
    operator's apex domain.
    """
    headers = {
        "Content-Security-Policy": build_csp(settings),
        "X-Frame-Options": "DENY",
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "strict-origin-when-cross-origin",
        # Google sign-in is a full-page redirect here, not a popup, so an
        # isolated browsing-context group costs nothing.
        "Cross-Origin-Opener-Policy": "same-origin",
        # Nothing in this app asks for hardware. Say so.
        "Permissions-Policy": (
            "accelerometer=(), camera=(), geolocation=(), gyroscope=(), "
            "magnetometer=(), microphone=(), payment=(), usb=()"
        ),
    }
    if settings.cookie_secure and settings.hsts_max_age_seconds > 0:
        headers["Strict-Transport-Security"] = (
            f"max-age={settings.hsts_max_age_seconds}"
        )
    return headers
