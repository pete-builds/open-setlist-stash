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
import posixpath
import time
from collections import deque
from collections.abc import AsyncIterator
from threading import Lock
from urllib.parse import unquote, urlsplit, urlunsplit

import httpx
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse

logger = logging.getLogger("setlist_stash.mcp_proxy")


#: Bound on re-decoding a subpath when hunting for hidden dot-segments. Three
#: rounds covers single, double, and triple encoding; a request needing more is
#: not a real MCP client.
_MAX_DECODE_ROUNDS = 3


def _has_dot_segment(path: str) -> bool:
    """True if any path segment is exactly ``..`` (also handling backslashes)."""
    return any(
        seg == ".." for seg in path.replace("\\", "/").split("/")
    )


class _TraversalRejected(ValueError):
    """The requested subpath resolved outside the upstream MCP endpoint."""

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

    **Keys are evicted, not just drained.** Timestamps ageing out of a bucket is
    not the same as the bucket going away: ``allow`` only ever touches the key
    being asked about, so an address that is seen once and never returns leaves
    an empty deque and a dict entry behind forever.

    That is not a real leak today, and it is worth being precise about why,
    because it changes with a config flag rather than with any code here. With
    ``TRUSTED_CLIENT_IP_HEADER`` unset — the shipped default — the key is the
    socket peer, which behind a tunnel is the connector container: one entry,
    for the life of the process. Declaring the trusted header is what makes the
    key a real client address, and the moment it does, this dict grows by one
    entry per distinct visitor IP and never shrinks. So the growth is *armed by*
    the very setting that makes the rate limit mean anything.

    A stale key is swept at most once per window, which bounds retained keys to
    roughly the distinct addresses seen in one window.
    """

    def __init__(self, per_minute: int, window_seconds: float = 60.0) -> None:
        self._per_minute = per_minute
        self._window = window_seconds
        self._hits: dict[str, deque[float]] = {}
        self._lock = Lock()
        # Sweeps are scheduled off the first observed timestamp rather than a
        # clock read here, so a limiter constructed at import time and first
        # used much later does not sweep on its very first request.
        self._next_sweep: float | None = None

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
                # A blocked caller still leaves a populated bucket, so there is
                # nothing to evict here; the sweep below reclaims it once the
                # caller gives up and the timestamps age out.
                self._sweep_if_due(ts)
                return False
            bucket.append(ts)
            self._sweep_if_due(ts)
            return True

    def _sweep_if_due(self, ts: float) -> None:
        """Drop keys with no timestamps left inside the window. Caller holds the lock.

        Runs at most once per window. The cost is proportional to the number of
        tracked keys, which is the thing being bounded, and it is paid by one
        request per window rather than by every request.
        """
        if self._next_sweep is None:
            self._next_sweep = ts + self._window
            return
        if ts < self._next_sweep:
            return
        self._next_sweep = ts + self._window
        cutoff = ts - self._window
        # A bucket's timestamps are appended in order, so the LAST one is the
        # newest: if even that has aged out, the whole key is dead. Checking
        # only bucket[-1] keeps the sweep O(keys) rather than O(timestamps).
        stale = [key for key, hits in self._hits.items() if not hits or hits[-1] <= cutoff]
        for key in stale:
            del self._hits[key]

    def tracked_keys(self) -> int:
        """Number of keys currently held. Exposed for tests and diagnostics."""
        with self._lock:
            return len(self._hits)


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
        """Join ``subpath`` under the upstream endpoint, or raise on escape.

        ``/mcp/{path:path}`` hands us a URL-decoded segment, so a caller can put
        real ``..`` segments in it via ``%2e%2e``. Pasted on naively they would
        survive to httpx, which normalises dot-segments per RFC 3986 and would
        resolve them *upward* -- turning ``/mcp/../foo`` into a request for
        ``/foo`` on the internal MCP host. The upstream host is fixed, so this
        is not full SSRF, but it reaches paths on that host we never mounted.

        Normalise here instead, and require the result to still sit under the
        configured endpoint. Anything else is a client error, not a proxy job.
        """
        if not subpath:
            return self._upstream
        # Reject before normalising if the segment still hides dot-segments in
        # percent-encoding. Starlette decodes the path param once, so a single
        # %2e%2e arrives here as real dots and is caught below -- but a DOUBLE
        # encoding (%252e%252e) survives that decode and would be forwarded to
        # the upstream verbatim as %2e%2e. Whether that resolves upward is then
        # the upstream server's decoding behaviour, which is not ours to assume.
        # Decode repeatedly and refuse anything that ever looks like traversal.
        probe = subpath
        for _ in range(_MAX_DECODE_ROUNDS):
            decoded = unquote(probe)
            if decoded == probe:
                break
            probe = decoded
            if _has_dot_segment(probe):
                raise _TraversalRejected(subpath)
        base = urlsplit(self._upstream)
        base_path = base.path or "/"
        joined = posixpath.normpath(f"{base_path}/{subpath}")
        # normpath collapses ".." but leaves a leading one in place when it
        # would escape the root, so check the result rather than the input.
        if joined != base_path and not joined.startswith(f"{base_path.rstrip('/')}/"):
            raise _TraversalRejected(subpath)
        # normpath drops a meaningful trailing slash; restore it.
        if subpath.endswith("/") and not joined.endswith("/"):
            joined = f"{joined}/"
        return urlunsplit(base._replace(path=joined))

    async def handle(self, request: Request, subpath: str = "") -> Response:
        try:
            url = self._target_url(subpath)
        except _TraversalRejected:
            logger.warning("mcp proxy rejected traversal", extra={"subpath": subpath[:200]})
            return Response(
                content=b'{"error":"invalid MCP path"}',
                status_code=400,
                media_type="application/json",
            )
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
