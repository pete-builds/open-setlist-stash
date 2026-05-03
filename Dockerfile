# syntax=docker/dockerfile:1.7

# ---------------------------------------------------------------------------
# Builder stage: install pinned deps into a wheel directory.
# ---------------------------------------------------------------------------
# Pin the digest the same way mcp-unifi/mcp-phish do. Refresh weekly via
# Dependabot once the GitHub Actions workflow ships.
FROM python:3.14-slim AS builder

WORKDIR /build

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Phase 4 kickoff uses requirements.in directly. Subsequent sessions will
# generate a hash-pinned requirements.lock with `uv pip compile` and switch
# this stage to `pip install --require-hashes -r requirements.lock` to match
# mcp-phish and mcp-unifi.
COPY requirements.in ./requirements.in
RUN pip install --no-cache-dir --target /wheels -r requirements.in

COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir --target /wheels --no-deps .

# ---------------------------------------------------------------------------
# Runtime stage: slim image with only the installed package + UID 1000 user.
# ---------------------------------------------------------------------------
FROM python:3.14-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/site-packages \
    PATH=/app/site-packages/bin:$PATH

# Non-root user with pinned UID 1000 (no shell, no home).
RUN groupadd --system --gid 1000 game \
    && useradd --system --uid 1000 --gid 1000 --no-create-home --shell /usr/sbin/nologin game

WORKDIR /app
COPY --from=builder /wheels /app/site-packages
# Migrations live next to the package so the migrate runner can read them
# without packaging SQL into the wheel.
COPY migrations/ /app/migrations/
RUN chown -R game:game /app

USER game

EXPOSE 3706

HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=10s \
    CMD ["python", "-m", "phish_game.healthcheck"]

ENTRYPOINT ["python", "-m", "phish_game.server"]
