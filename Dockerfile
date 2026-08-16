# syntax=docker/dockerfile:1.7

# ---------------------------------------------------------------------------
# Builder stage: install pinned deps into a wheel directory.
# ---------------------------------------------------------------------------
# Pin the digest the same way mcp-unifi/mcp-phish do. Refresh weekly via
# Dependabot once the GitHub Actions workflow ships.
FROM python:3.13.14-slim AS builder

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
FROM python:3.13.14-slim AS runtime

# Apply Debian security patches on top of the pinned base. Keeps the digest
# pin for reproducibility while picking up CVE fixes between base rebuilds.
# CACHE_BUST is referenced inside the RUN so BuildKit's gha cache (cache-from/
# cache-to: type=gha in CI) cannot replay a stale apt layer that predates the
# latest Debian security fixes. Bump the date to force a fresh apt upgrade.
ARG CACHE_BUST=2026-06-15
RUN echo "cache-bust: ${CACHE_BUST}" && apt-get update && apt-get -y upgrade && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/site-packages \
    PATH=/app/site-packages/bin:$PATH

# Non-root user with pinned UID 1000 (no shell, no home).
RUN groupadd --system --gid 1000 game \
    && useradd --system --uid 1000 --gid 1000 --no-create-home --shell /usr/sbin/nologin game

# Drop pip from the runtime image. Nothing at runtime uses it: dependencies are built
# in the builder stage and reach this stage via PYTHONPATH, and the entrypoint
# and healthcheck are plain `python -m` calls.
#
# This is also the only fix for two recurring Trivy HIGHs. pip ships a vendored
# dependency set (see pip/_vendor/vendor.txt) that Trivy scans as real packages:
# msgpack 1.1.2 (GHSA-6v7p-g79w-8964) and setuptools 70.3.0 (CVE-2025-47273).
# Neither is an application dependency, so no lockfile change can move them, and
# no pip release ships fixed versions. Removing the unused component is the fix.
RUN python -m pip uninstall -y pip \
    && rm -rf /usr/local/lib/python3.*/site-packages/pip \
              /usr/local/lib/python3.*/site-packages/pip-*.dist-info \
              /usr/local/bin/pip /usr/local/bin/pip3 /usr/local/bin/pip3.*

WORKDIR /app
COPY --from=builder /wheels /app/site-packages
# Migrations live next to the package so the migrate runner can read them
# without packaging SQL into the wheel.
COPY migrations/ /app/migrations/
RUN chown -R game:game /app

USER game

EXPOSE 3706

HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=10s \
    CMD ["python", "-m", "setlist_stash.healthcheck"]

ENTRYPOINT ["python", "-m", "setlist_stash.server"]
