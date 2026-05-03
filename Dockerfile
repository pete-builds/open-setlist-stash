# syntax=docker/dockerfile:1.7

# ---------------------------------------------------------------------------
# Builder stage: install pinned deps into a wheel directory.
# ---------------------------------------------------------------------------
# Pin the digest the same way mcp-unifi/mcp-phish do. Refresh weekly via
# Dependabot once the GitHub Actions workflow ships.
FROM python:3.13-slim AS builder

WORKDIR /build

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Hash-pinned reproducible install (session 9 onwards). Lockfile generated
# via `uv pip compile requirements.in --generate-hashes --python-version 3.13
# --python-platform linux` inside a python:3.13-slim container so the hash
# set covers the same Linux wheels we install at build time. Mirrors
# mcp-phish + mcp-unifi.
COPY requirements.lock ./requirements.lock
RUN pip install --no-cache-dir --require-hashes --target /wheels -r requirements.lock

COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir --target /wheels --no-deps .

# ---------------------------------------------------------------------------
# Runtime stage: slim image with only the installed package + UID 1000 user.
# ---------------------------------------------------------------------------
FROM python:3.13-slim AS runtime

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
