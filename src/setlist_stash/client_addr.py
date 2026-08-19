"""Resolving the real client IP behind a proxy.

This app began as a LAN/Tailscale deployment where the socket peer *was* the
client, and two call sites drifted apart once it went public behind Cloudflare
Tunnel:

* ``mcp_proxy`` trusted the **first** ``X-Forwarded-For`` hop. Cloudflare
  *appends* the connecting address to whatever XFF the caller already sent, so
  that first hop is caller-controlled. Keying a rate limiter on it means an
  abuser rotates one header and gets a fresh bucket per request.
* the magic-link verifier trusted the **socket peer**, which behind a tunnel is
  the connector container, so every sign-in was audited to the same address.

Both are the same bug: the app never declared which hop it trusts. It does now,
and it declines to guess.

``TRUSTED_CLIENT_IP_HEADER`` names the one header the operator's edge is known
to set and to overwrite on the way in (``CF-Connecting-IP`` on Cloudflare).
When it is unset the socket peer is used, which is correct for a direct LAN or
Tailscale deployment and is the safe default for a self-hoster who has not told
us what fronts them: a wrong-but-unspoofable address beats a spoofable one.

Trusting a header is only sound when the app cannot be reached *around* the
edge that sets it. That is an ingress property, not something this code can
check, so it stays an explicit operator declaration rather than a default.
"""

from __future__ import annotations

from starlette.requests import Request

from setlist_stash.config import Settings

#: Returned when neither the trusted header nor the socket peer yields anything.
UNKNOWN = "unknown"


def resolve_client_ip(request: Request, settings: Settings) -> str:
    """Best-effort client address, honouring only an operator-declared header.

    Never falls back to ``X-Forwarded-For``: an undeclared deployment gets the
    socket peer rather than a value the caller could have chosen.
    """
    header = settings.trusted_client_ip_header.strip()
    if header:
        raw = request.headers.get(header)
        if raw:
            # Even a trusted header may carry a list (an operator pointing this
            # at X-Forwarded-For on a single-proxy deployment). The edge appends
            # the address it observed, so the LAST hop is the one it vouches for.
            candidate = raw.split(",")[-1].strip()
            if candidate:
                return candidate
    if request.client is not None and request.client.host:
        return request.client.host
    return UNKNOWN
