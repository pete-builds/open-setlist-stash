# Security Policy

## Supported versions

The latest commit on `main` is the only supported version. Older tags and releases are not patched.

## Reporting a vulnerability

If you find a security issue, **please do not open a public GitHub issue**.

Email **pstergion@gmail.com** with:

- A description of the vulnerability
- Steps to reproduce (or a proof of concept)
- Affected versions, if known
- Your assessment of impact and severity

I will acknowledge receipt within 72 hours and aim to ship a fix within 14 days for high-severity issues. Once the fix lands I will publish a GitHub Security Advisory crediting you (with your permission).

## What the app enforces for you

- **The shipped session key is fail-closed.** `SESSION_SECRET` signs the identity cookie, and its default is published in this repo and baked into every image. The app **refuses to start** if that default is still in place while `COOKIE_SECURE=true` or `BASE_URL` is https. A warning would not have been enough: nothing misbehaves when sessions are forgeable, so it would go unnoticed.
- **Security headers on every response** (`src/setlist_stash/security_headers.py`): a Content-Security-Policy, `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, `Referrer-Policy: strict-origin-when-cross-origin`, `Cross-Origin-Opener-Policy: same-origin`, and a deny-all `Permissions-Policy`. HSTS is added only when `COOKIE_SECURE=true`, so an HTTP LAN deployment never sends it. Disable the whole set with `SECURITY_HEADERS=false` if a fronting proxy owns the policy.
- **Known CSP limitation:** `script-src` includes `'unsafe-inline'`. The templates still carry inline `<script>` blocks, so a nonce policy needs those moved to static files first. Inline event handlers are gone as of 0.3.0: every `onsubmit="return confirm(...)"` became a `data-confirm` attribute read by one delegated listener, which removes the whole class of "escaped for HTML, then parsed as JavaScript" bugs. `'unsafe-eval'` is *not* allowed. `default-src 'self'`, `base-uri 'self'`, `object-src 'none'`, `frame-ancestors 'none'`, and `form-action 'self'` still block remote script loading, exfiltration, base-tag hijacking, and form repointing.
- **User-controlled content** (handles, comment bodies, league names) is rendered through Jinja autoescaping, never `|safe`, and never inside a `<script>` block or an event-handler attribute. Autoescaping defends HTML context only; a value that lands in a JS string nested in an attribute is *not* protected by it, because the HTML parser decodes entities before the JavaScript is compiled.
- **League invite slugs are credentials.** A private league's slug is the only thing gating it, so it carries 8 characters of `secrets.SystemRandom` entropy (`SLUG_SUFFIX_LENGTH`), not a collision-avoidance suffix. Do not shorten it.
- **Every submitted song slug** is validated against the upstream MCP server before any database write.
- **The public `/mcp` reverse proxy is rate limited per IP** (`MCP_RATE_LIMIT_PER_MINUTE`). The game UI is not.

## Scope

In scope: code in this repository, the container image (if published), and any deployment configuration shipped here.

Out of scope: third-party dependencies (please report those upstream), social engineering, denial of service via volumetric attacks, and issues that require attacker-controlled physical access to the host.
