"""Thin async JSON-RPC client for mcp-phish.

Calls the FastMCP Streamable HTTP endpoint. We use synchronous request/response
only — no SSE streaming. Returns plain dicts (not Pydantic models) so the
game keeps its own narrower internal shapes.

Pre-lock policy: the autocomplete used by the picks form returns ONLY
``{slug, title}``. ``times_played``, ``gap_current``, etc. are stripped here
so they can't leak into a pre-lock UI. Post-lock callers (resolver, post-lock
assist views) call ``get_song`` directly and get the full payload.

Field-name normalization: upstream mcp-phish exposes a song field called
``gap``. The plan's scoring formula uses ``gap_current``. We normalize at the
boundary: ``get_song`` always returns ``gap_current``, regardless of what
upstream calls it. This way the scoring code, score_breakdown JSONB, and any
future analytics code all speak one name.
"""

from __future__ import annotations

import json
import logging
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx

logger = logging.getLogger("phish_game.mcp_client")


class McpPhishError(RuntimeError):
    """Base class for mcp-phish client errors."""


class McpPhishUnavailable(McpPhishError):
    """Network / 5xx / unparseable response."""


class McpPhishNotFound(McpPhishError):
    """4xx-style failure — e.g. unknown slug."""


class McpPhishClient:
    """Async client for mcp-phish JSON-RPC over HTTP.

    Use as an async context manager:
        async with McpPhishClient(url) as client:
            shows = await client.recent_shows(limit=10)
    """

    PROTOCOL_VERSION = "2025-06-18"

    def __init__(
        self,
        url: str,
        *,
        timeout_seconds: float = 15.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._url = url.rstrip("/")
        self._timeout = timeout_seconds
        # Allow tests to inject a transport-mocked client.
        self._client = client
        self._owns_client = client is None
        self._session_id: str | None = None
        self._initialized: bool = False

    async def __aenter__(self) -> McpPhishClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None
        self._session_id = None
        self._initialized = False

    async def _initialize(self) -> None:
        """Run the FastMCP Streamable HTTP handshake.

        1. POST ``initialize`` (response carries ``mcp-session-id`` header)
        2. POST ``notifications/initialized`` (notification; 202 expected)

        Idempotent — subsequent calls re-use the session id.
        """
        if self._initialized:
            return
        if self._client is None:
            raise McpPhishError("client not entered (use async with)")
        init_body = {
            "jsonrpc": "2.0",
            "id": secrets.token_hex(8),
            "method": "initialize",
            "params": {
                "protocolVersion": self.PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "phish-game", "version": "0.1.0"},
            },
        }
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        try:
            resp = await self._client.post(self._url, json=init_body, headers=headers)
        except httpx.HTTPError as exc:
            raise McpPhishUnavailable(f"initialize: network error: {exc}") from exc
        if resp.status_code >= 500:
            raise McpPhishUnavailable(
                f"initialize: upstream {resp.status_code}: {resp.text[:200]}"
            )
        if resp.status_code >= 400:
            raise McpPhishNotFound(
                f"initialize: upstream {resp.status_code}: {resp.text[:200]}"
            )
        session_id = resp.headers.get("mcp-session-id")
        if not session_id:
            raise McpPhishError("initialize: server did not return mcp-session-id")
        # Drain the body to keep the connection clean; we don't need it.
        _ = _parse_response(resp)
        self._session_id = session_id

        # Send the initialized notification. No response body expected
        # (it's a JSON-RPC notification).
        notify_body = {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {},
        }
        notify_headers = dict(headers)
        notify_headers["mcp-session-id"] = session_id
        try:
            ack = await self._client.post(
                self._url, json=notify_body, headers=notify_headers
            )
        except httpx.HTTPError as exc:
            raise McpPhishUnavailable(
                f"initialized notification: {exc}"
            ) from exc
        if ack.status_code >= 400:
            # Some FastMCP versions return 202; others 200. Either is fine.
            raise McpPhishError(
                f"initialized notification rejected: {ack.status_code}"
            )
        self._initialized = True

    # ----- public tool wrappers ---------------------------------------------

    async def health(self) -> dict[str, Any]:
        """mcp-phish self-health (cache, vault staleness, throttle)."""
        result = await self._call_tool("health", {})
        if not isinstance(result, dict):
            raise McpPhishError(f"health: unexpected shape {type(result).__name__}")
        return result

    async def recent_shows(self, limit: int = 10) -> list[dict[str, Any]]:
        """Return the N most recent shows (descending by date)."""
        result = await self._call_tool("recent_shows", {"limit": limit})
        if isinstance(result, list):
            return result
        # mcp-phish returns ``{"data": [...]}``; _call_tool strips the wrapper.
        raise McpPhishError(f"recent_shows: unexpected shape {type(result).__name__}")

    async def search_songs_pre_lock(
        self, query: str, limit: int = 10
    ) -> list[dict[str, str]]:
        """Pre-lock autocomplete: returns ONLY {slug, title}.

        See PHASE-4-PLAN.md §7. Stripping happens here so a careless caller
        can't leak gap counts or play counts into a pre-lock UI.
        """
        rows = await self._call_tool("search_songs", {"query": query, "limit": limit})
        if not isinstance(rows, list):
            raise McpPhishError(f"search_songs: unexpected shape {type(rows).__name__}")
        return [
            {"slug": str(r["slug"]), "title": str(r["title"])}
            for r in rows
            if "slug" in r and "title" in r
        ]

    async def search_songs_full(
        self, query: str, limit: int = 25
    ) -> list[dict[str, Any]]:
        """Post-lock or resolver use: full song search payload."""
        rows = await self._call_tool("search_songs", {"query": query, "limit": limit})
        if not isinstance(rows, list):
            raise McpPhishError(f"search_songs: unexpected shape {type(rows).__name__}")
        return rows

    async def get_song(self, slug: str) -> dict[str, Any]:
        """Single-song record. Normalizes ``gap`` -> ``gap_current``."""
        row = await self._call_tool("get_song", {"slug": slug})
        if not isinstance(row, dict):
            raise McpPhishError(f"get_song: unexpected shape {type(row).__name__}")
        out = dict(row)
        if "gap" in out and "gap_current" not in out:
            out["gap_current"] = out["gap"]
        return out

    async def get_show(self, date_or_id: str) -> dict[str, Any]:
        """Full show record (setlist, ratings, venue)."""
        row = await self._call_tool("get_show", {"date_or_id": date_or_id})
        if not isinstance(row, dict):
            raise McpPhishError(f"get_show: unexpected shape {type(row).__name__}")
        return row

    async def songs_by_gap(self, limit: int = 25) -> list[dict[str, Any]]:
        """Top-N songs ordered by current gap (descending).

        Post-lock assist only; gap counts must NOT leak pre-lock (see
        PHASE-4-PLAN.md §7). The route layer enforces the gate via
        ``assist_allowed`` before calling this.
        """
        rows = await self._call_tool("songs_by_gap", {"limit": limit})
        if not isinstance(rows, list):
            raise McpPhishError(
                f"songs_by_gap: unexpected shape {type(rows).__name__}"
            )
        return rows

    async def validate_song_slugs(self, slugs: list[str]) -> set[str]:
        """Return the subset of ``slugs`` that correspond to real songs.

        Server-side validation gate for the predict form: the picker UI hands
        us slugs the user clicked from the autocomplete, but we still treat
        every submitted slug as untrusted (curl, JS-disabled, race against
        an alias removal). Any slug that isn't found is dropped from the
        returned set; the caller compares against the input to surface
        per-pick errors.

        Single round-trip: calls the upstream ``validate_song_slugs`` tool
        on mcp-phish (vault-backed when enabled, live-API fallback
        otherwise). Empty input short-circuits with no network call. Input
        is normalized (strip + lower) and deduped client-side; the upstream
        tool caps at 50 slugs per call, so we slice defensively (the
        predict form only ever sends 6).
        """
        seen: list[str] = []
        seen_set: set[str] = set()
        for raw in slugs:
            if not raw:
                continue
            slug = raw.strip().lower()
            if not slug or slug in seen_set:
                continue
            seen_set.add(slug)
            seen.append(slug)
        if not seen:
            return set()
        # Upstream caps at 50; slice defensively (form only sends 6).
        if len(seen) > 50:
            seen = seen[:50]
        result = await self._call_tool(
            "validate_song_slugs", {"slugs": seen}
        )
        if not isinstance(result, dict):
            raise McpPhishError(
                f"validate_song_slugs: unexpected shape {type(result).__name__}"
            )
        valid_raw = result.get("valid", [])
        if not isinstance(valid_raw, list):
            raise McpPhishError(
                f"validate_song_slugs: 'valid' is not a list: "
                f"{type(valid_raw).__name__}"
            )
        return {str(s).strip().lower() for s in valid_raw if s}

    async def venue_history(
        self, venue_slug: str, limit: int = 25
    ) -> list[dict[str, Any]]:
        """Recent shows at a venue, most-recent first.

        Returns ``[]`` for an unknown venue slug rather than raising
        ``McpPhishNotFound``; the post-lock assist UI degrades gracefully
        when the venue's slug isn't yet populated in the vault.
        """
        try:
            rows = await self._call_tool(
                "venue_history",
                {"venue_slug": venue_slug, "limit": limit},
            )
        except McpPhishNotFound:
            return []
        if not isinstance(rows, list):
            raise McpPhishError(
                f"venue_history: unexpected shape {type(rows).__name__}"
            )
        return rows

    # ----- transport --------------------------------------------------------

    async def _call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        """Invoke an mcp-phish tool via the ``tools/call`` JSON-RPC method.

        FastMCP returns tool output wrapped in ``content[0].text`` as a JSON
        string of the shape ``{"data": <actual>}``. We unwrap here so callers
        get the actual payload.
        """
        if self._client is None:
            raise McpPhishError("client not entered (use async with)")
        await self._initialize()
        request_id = secrets.token_hex(8)
        body = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self._session_id:
            headers["mcp-session-id"] = self._session_id
        try:
            resp = await self._client.post(self._url, json=body, headers=headers)
        except httpx.HTTPError as exc:
            raise McpPhishUnavailable(f"network error: {exc}") from exc

        if resp.status_code >= 500:
            raise McpPhishUnavailable(
                f"upstream {resp.status_code}: {resp.text[:200]}"
            )
        if resp.status_code >= 400:
            raise McpPhishNotFound(f"upstream {resp.status_code}: {resp.text[:200]}")

        payload = _parse_response(resp)
        if "error" in payload:
            err = payload["error"]
            raise McpPhishError(f"jsonrpc error: {err}")
        result = payload.get("result")
        if result is None:
            raise McpPhishError("jsonrpc result missing")
        # FastMCP shape: {"content": [{"type":"text","text":"<json>"}], ...}
        content = result.get("content") if isinstance(result, dict) else None
        if isinstance(content, list) and content:
            text = content[0].get("text") if isinstance(content[0], dict) else None
            if isinstance(text, str):
                try:
                    inner = json.loads(text)
                except json.JSONDecodeError as exc:
                    raise McpPhishError(f"bad inner JSON: {exc}") from exc
                if isinstance(inner, dict) and "data" in inner:
                    return inner["data"]
                return inner
        # Fallback: maybe the server already returns structured content.
        if isinstance(result, dict) and "data" in result:
            return result["data"]
        return result


def _parse_response(resp: httpx.Response) -> dict[str, Any]:
    """Parse a JSON-RPC response. Streamable-HTTP allows two encodings."""
    ctype = resp.headers.get("content-type", "")
    text = resp.text
    if "application/json" in ctype:
        try:
            return resp.json()  # type: ignore[no-any-return]
        except json.JSONDecodeError as exc:
            raise McpPhishError(f"non-json response: {exc}") from exc
    # SSE fallback: lines like ``data: {...}``. Pull the last data line.
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("data:"):
            chunk = line[5:].strip()
            try:
                return json.loads(chunk)  # type: ignore[no-any-return]
            except json.JSONDecodeError as exc:
                raise McpPhishError(f"sse decode error: {exc}") from exc
    raise McpPhishError(f"unrecognized response content-type {ctype!r}")


@asynccontextmanager
async def mcp_client_from_settings(
    url: str, timeout_seconds: float
) -> AsyncIterator[McpPhishClient]:
    """Convenience helper for FastAPI dependency wiring."""
    async with McpPhishClient(url, timeout_seconds=timeout_seconds) as client:
        yield client
