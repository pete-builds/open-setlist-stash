"""HTTP middleware wired onto the app in ``server.build_app``.

Split out only to keep ``server.py`` at the wiring-only ceiling; behavior
is identical to the pre-split inline definitions (same order, same policy).
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from setlist_stash.client_addr import resolve_client_ip
from setlist_stash.config import Settings
from setlist_stash.mcp_proxy import FixedWindowRateLimiter, McpReverseProxy


def install_mcp_rate_limit(
    app: FastAPI, limiter: FixedWindowRateLimiter, cfg: Settings
) -> None:
    """Per-IP rate limit, scoped to the public /mcp proxy ONLY.

    The game UI, static assets, and every other route are never touched by
    this middleware.

    The limiter is only as good as its key. ``resolve_client_ip`` honours just
    the operator-declared ``TRUSTED_CLIENT_IP_HEADER`` and otherwise falls back
    to the socket peer, so a caller cannot pick their own bucket by setting a
    header. See client_addr.py for why that declaration is required rather than
    inferred.
    """

    @app.middleware("http")
    async def _mcp_rate_limit(request: Request, call_next: Any) -> Response:
        path = request.url.path
        if (
            limiter.enabled
            and (path == "/mcp" or path.startswith("/mcp/"))
            and not limiter.allow(resolve_client_ip(request, cfg))
        ):
            return JSONResponse(
                {"error": "rate limit exceeded"},
                status_code=429,
                headers={"Retry-After": "60"},
            )
        resp: Response = await call_next(request)
        return resp


def install_security_headers(app: FastAPI, headers: dict[str, str]) -> None:
    """Stamp response security headers (CSP, framing, referrer, HSTS).

    Registered LAST at the call site so it runs OUTERMOST: Starlette applies
    http middleware in reverse registration order, so this wraps everything
    below it and stamps the headers onto rate-limit 429s, static files, and
    error responses too, not just the routes that happen to return normally.
    """
    if not headers:
        return

    @app.middleware("http")
    async def _security_headers(request: Request, call_next: Any) -> Response:
        resp: Response = await call_next(request)
        # setdefault, not assignment: an operator fronting this with a
        # proxy that already sets a policy keeps theirs rather than
        # silently getting two intersecting CSPs.
        for name, value in headers.items():
            resp.headers.setdefault(name, value)
        return resp


def install_mcp_reverse_proxy(app: FastAPI, mcp_proxy: McpReverseProxy) -> None:
    """Mount the /mcp streaming reverse-proxy passthrough routes.

    Only invoked when ``mcp_proxy`` is not None — the OSS image / Phish demo
    never mount these because their ``MCP_UPSTREAM_URL`` is empty.
    """

    @app.api_route(
        "/mcp",
        methods=["GET", "POST", "DELETE"],
        include_in_schema=False,
    )
    async def mcp_root(request: Request) -> Response:
        resp: Response = await mcp_proxy.handle(request)
        return resp

    @app.api_route(
        "/mcp/{path:path}",
        methods=["GET", "POST", "DELETE"],
        include_in_schema=False,
    )
    async def mcp_subpath(request: Request, path: str) -> Response:
        resp: Response = await mcp_proxy.handle(request, path)
        return resp
