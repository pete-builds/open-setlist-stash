"""Public reverse proxy for a deployment's read-only Streamable-HTTP MCP.

This module exposes the deployment's internal MCP server (e.g. ``mcp-umphreys``
on the docker network) at the app's public ``/mcp`` path so visitors can wire
the band's setlist data into their own MCP client. It is a generic platform
feature: it only activates when ``MCP_UPSTREAM_URL`` is set, so the OSS image
and the Phish demo never proxy anywhere (oss-platform-split).

Design notes:
- MCP Streamable HTTP negotiates over ``GET``/``POST``/``DELETE`` and replies
  with either JSON or an SSE ``text/event-stream`` body. We therefore **stream**
  the upstream response back rather than buffering it, so long-lived SSE streams
  work.
- The ``mcp-session-id`` header is the session token. It MUST be forwarded in
  both directions or sessions break, so we pass request headers through (minus
  hop-by-hop) and copy the upstream response headers back verbatim.
- A small dependency-free fixed-window rate limiter guards the public endpoint
  (it is authless). It is scoped to ``/mcp`` only; the game UI is never limited.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import AsyncIterator
from threading import Lock

import httpx
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse

logger = logging.getLogger("setlist_stash.mcp_proxy")

# Hop-by-hop headers must not be forwarded across a proxy (RFC 7230 §6.1).
# ``host`` is dropped so httpx sets it from the upstream URL; ``content-length``
# is dropped because we hand httpx the raw body and let it recompute.
_HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        "host",
        "content-length",
    }
)


class FixedWindowRateLimiter:
    """In-memory fixed-window per-key rate limiter. Dependency-free.

    Tracks request timestamps per key in a 60-second sliding deque and allows up
    to ``per_minute`` requests within any trailing 60s window. Thread-safe via a
    single lock (uvicorn may run multiple worker threads for sync work; the lock
    keeps the bookkeeping consistent). State is per-process, which is fine for a
    single-container deployment — it caps abuse without an external store.
    """

    def __init__(self, per_minute: int, window_seconds: float = 60.0) -> None:
        self._per_minute = per_minute
        self._window = window_seconds
        self._hits: dict[str, deque[float]] = {}
        self._lock = Lock()

    @property
    def enabled(self) -> bool:
        return self._per_minute > 0

    def allow(self, key: str, *, now: float | None = None) -> bool:
        """Return True if a request from ``key`` is allowed; record it if so."""
        if self._per_minute <= 0:
            return True
        ts = time.monotonic() if now is None else now
        cutoff = ts - self._window
        with self._lock:
            bucket = self._hits.get(key)
            if bucket is None:
                bucket = deque()
                self._hits[key] = bucket
            # Drop timestamps that have aged out of the window.
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()
            if len(bucket) >= self._per_minute:
                return False
            bucket.append(ts)
            return True


def client_ip(request: Request) -> str:
    """Best-effort client IP for rate-limiting.

    The app sits behind Cloudflare, so the real client is the FIRST hop of
    ``X-Forwarded-For``. Fall back to the socket peer when the header is absent
    (e.g. direct LAN access or tests).
    """
    xff = request.headers.get("x-forwarded-for")
    if xff:
        first = xff.split(",", 1)[0].strip()
        if first:
            return first
    if request.client is not None:
        return request.client.host
    return "unknown"


def _forward_request_headers(request: Request) -> dict[str, str]:
    headers: dict[str, str] = {}
    for name, value in request.headers.items():
        if name.lower() in _HOP_BY_HOP:
            continue
        headers[name] = value
    return headers


def _forward_response_headers(upstream: httpx.Response) -> dict[str, str]:
    headers: dict[str, str] = {}
    for name, value in upstream.headers.items():
        if name.lower() in _HOP_BY_HOP:
            continue
        # Let Starlette set content-length/transfer-encoding for the streamed
        # body; copy everything else (critically mcp-session-id, content-type).
        if name.lower() == "content-encoding":
            # httpx has already decoded the body for us; a stale encoding header
            # would make the client try to decode again.
            continue
        headers[name] = value
    return headers


class McpReverseProxy:
    """Streaming reverse proxy to an upstream Streamable-HTTP MCP server.

    One long-lived ``httpx.AsyncClient`` per proxy instance. The upstream base
    URL is the full ``/mcp`` endpoint; an optional sub-path from the request is
    appended.
    """

    def __init__(self, upstream_url: str, *, timeout_seconds: float) -> None:
        self._upstream = upstream_url.rstrip("/")
        # Bound connect/read/write; the overall SSE stream may outlive this
        # because httpx applies read timeout per-chunk, not to the whole stream.
        self._timeout = httpx.Timeout(timeout_seconds, connect=10.0)
        self._client = httpx.AsyncClient(timeout=self._timeout)

    async def aclose(self) -> None:
        await self._client.aclose()

    def _target_url(self, subpath: str) -> str:
        if subpath:
            return f"{self._upstream}/{subpath.lstrip('/')}"
        return self._upstream

    async def handle(self, request: Request, subpath: str = "") -> Response:
        url = self._target_url(subpath)
        body = await request.body()
        req_headers = _forward_request_headers(request)
        upstream_req = self._client.build_request(
            request.method,
            url,
            params=dict(request.query_params),
            headers=req_headers,
            content=body,
        )
        try:
            upstream_resp = await self._client.send(upstream_req, stream=True)
        except httpx.TimeoutException:
            logger.warning("mcp proxy upstream timeout", extra={"url": url})
            return Response(
                content=b'{"error":"upstream MCP timed out"}',
                status_code=504,
                media_type="application/json",
            )
        except httpx.HTTPError as exc:
            logger.warning(
                "mcp proxy upstream error", extra={"url": url, "error": str(exc)[:200]}
            )
            return Response(
                content=b'{"error":"upstream MCP unreachable"}',
                status_code=502,
                media_type="application/json",
            )

        resp_headers = _forward_response_headers(upstream_resp)

        async def _body_iter() -> AsyncIterator[bytes]:
            # aiter_bytes (not aiter_raw): httpx decodes any content-encoding,
            # and we already strip the upstream content-encoding header so the
            # client doesn't try to decode again. This streams SSE chunks as the
            # upstream emits them (per-chunk read timeout, not whole-stream).
            try:
                async for chunk in upstream_resp.aiter_bytes():
                    yield chunk
            finally:
                await upstream_resp.aclose()

        return StreamingResponse(
            _body_iter(),
            status_code=upstream_resp.status_code,
            headers=resp_headers,
        )
