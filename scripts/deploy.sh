#!/usr/bin/env bash
# Deploy tweezerpicks.com (and optionally wappypicks.com) to nix1.
# Run from your local machine: bash scripts/deploy.sh
# Requires key-based SSH access to nix1 (alias configured in ~/.ssh/config).
set -euo pipefail

HOST="${DEPLOY_HOST:-nix1}"
TWEEZER_DIR="\$HOME/docker/open-setlist-stash"
WAPPY_DIR="\$HOME/docker/open-setlist-stash-umphreys"
DEPLOY_WAPPY="${DEPLOY_WAPPY:-1}"

echo "==> Connecting to $HOST ..."

ssh "$HOST" bash -s <<'REMOTE'
set -euo pipefail

TWEEZER_DIR="$HOME/docker/open-setlist-stash"
WAPPY_DIR="$HOME/docker/open-setlist-stash-umphreys"

# ---- 1. Ensure BASE_URL is the public domain --------------------------------
CURRENT_BASE=$(grep '^BASE_URL=' "$TWEEZER_DIR/.env" | cut -d= -f2-)
if [[ "$CURRENT_BASE" != "https://tweezerpicks.com" ]]; then
  echo "==> Fixing BASE_URL: was '$CURRENT_BASE'"
  sed -i "s|^BASE_URL=.*|BASE_URL=https://tweezerpicks.com|" "$TWEEZER_DIR/.env"
  echo "    Set to https://tweezerpicks.com"
else
  echo "==> BASE_URL already correct: $CURRENT_BASE"
fi

# ---- 2. Pull latest code ----------------------------------------------------
echo "==> Pulling latest code ..."
cd "$TWEEZER_DIR"
git fetch origin main
git checkout main
git pull --ff-only origin main

NEW_SHA=$(git rev-parse --short HEAD)
NEW_TAG="setlist-stash:0.2.0-${NEW_SHA}"
echo "==> Building image $NEW_TAG ..."

# ---- 3. Build ---------------------------------------------------------------
docker build -t "$NEW_TAG" .

# ---- 4. Update pin in override file -----------------------------------------
echo "==> Updating image pin in docker-compose.override.yml ..."
sed -i "s|setlist-stash:0\.2\.0-[a-f0-9]*|${NEW_TAG}|g" \
  "$TWEEZER_DIR/docker-compose.override.yml"

# ---- 5. Restart Tweezer -----------------------------------------------------
echo "==> Restarting Tweezer Picks (setlist-stash) ..."
docker compose up -d --force-recreate setlist-stash setlist-stash-resolver

# ---- 6. Promote to Wappy if directory exists --------------------------------
if [[ -d "$WAPPY_DIR" ]]; then
  echo "==> Promoting same image to Wappy Picks ..."
  sed -i "s|setlist-stash:0\.2\.0-[a-f0-9]*|${NEW_TAG}|g" \
    "$WAPPY_DIR/docker-compose.yml"
  cd "$WAPPY_DIR"
  docker compose up -d --force-recreate app resolver 2>/dev/null || \
    docker compose up -d --force-recreate setlist-stash setlist-stash-resolver
  echo "==> Wappy promoted."
fi

# ---- 7. Verify og-image is served -------------------------------------------
echo ""
echo "==> Waiting 5s for container to start ..."
sleep 5
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  "http://localhost:3706/static/og-image.png")
if [[ "$STATUS" == "200" ]]; then
  echo "OK: og-image.png is being served (HTTP $STATUS)"
else
  echo "WARN: og-image.png returned HTTP $STATUS — check container logs"
fi

echo ""
echo "Deploy complete. Image tag: $NEW_TAG"
REMOTE

echo ""
echo "==> Verifying og-image via public URL ..."
sleep 3
curl -sI "https://tweezerpicks.com/static/og-image.png" | grep -E "HTTP|content-type|cf-"
echo ""
echo "Done. Share https://tweezerpicks.com and check the preview."
