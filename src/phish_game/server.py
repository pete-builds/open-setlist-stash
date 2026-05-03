"""FastAPI app entrypoint.

Phase 4 kickoff scope: only ``/healthz`` is implemented. Subsequent Link
sessions add the picks form, leaderboard pages, lock UI, and resolve worker
hooks.
"""

from __future__ import annotations

import logging

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from phish_game import __version__
from phish_game.config import Settings, get_settings
from phish_game.logging_setup import configure_logging

logger = logging.getLogger("phish_game.server")


def build_app(settings: Settings | None = None) -> FastAPI:
    """Construct the FastAPI app.

    Factory pattern so tests can inject a custom Settings instance without
    touching the environment.
    """
    cfg = settings or get_settings()
    configure_logging(cfg.log_format)

    app = FastAPI(
        title="phish-game",
        version=__version__,
        description=(
            "Setlist prediction game for Phish shows. "
            "Phase 4 of the Phish Data Platform."
        ),
    )

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> JSONResponse:
        """Liveness probe.

        Phase 4 kickoff: status-only. Future versions extend with DB ping
        and mcp-phish reachability.
        """
        return JSONResponse(
            {"status": "ok", "version": __version__},
            status_code=200,
        )

    logger.info(
        "phish-game booted",
        extra={"version": __version__, "port": cfg.app_port},
    )
    return app


# Module-level app for ``uvicorn phish_game.server:app`` usage.
app = build_app()


def main() -> None:
    """Run the app under uvicorn. Used by the Docker entrypoint."""
    cfg = get_settings()
    uvicorn.run(
        "phish_game.server:app",
        host=cfg.app_host,
        port=cfg.app_port,
        log_config=None,
        access_log=True,
    )


if __name__ == "__main__":
    main()
