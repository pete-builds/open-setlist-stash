# phish-game

[![CI](https://github.com/pete-builds/phish-game/actions/workflows/ci.yml/badge.svg)](https://github.com/pete-builds/phish-game/actions/workflows/ci.yml)

Setlist prediction game for Phish shows. Phase 4 of the Phish Data Platform.

## What it is

Pick three songs, an opener, a closer, and an encore for an upcoming show.
Predictions lock at showtime. After the show resolves on phish.net, scores
post to a public leaderboard (weekly, tour, all-time).

Scoring is rarity-weighted: rarer songs are worth more points, and getting
a slot right (opener/closer/encore) earns a bonus.

This is a fair human contest. AI smart-pick assist is disabled during the
prediction window. Gap stats and venue history unlock after lock.

## Stack

- Python 3.13, FastAPI, Jinja2 templates, HTMX
- PostgreSQL (game state only; vault data is read via mcp-phish)
- Docker, multi-stage build, Tailscale/LAN only through Phase 5

## Reading vault data

The game never touches the phish-vault Postgres directly. Every vault read
goes through the [mcp-phish](https://github.com/pete-builds/mcp-phish) HTTP
endpoint at `http://mcp-phish:3705/mcp` (MCP Streamable HTTP, JSON-RPC).
A small async wrapper in `src/phish_game/mcp_client.py` calls the 14 tools
exposed by mcp-phish.

## Phase 4 plan

See `PHASE-4-PLAN.md` for the full design: data model, scoring formula,
showtime lock policy, auto-resolve cron, auth flow, and the verification
checklist.

## Run locally

```bash
cp .env.example .env
# edit .env: set PG_PASSWORD and (eventually) MCP_PHISH_URL
docker compose up -d
curl http://localhost:3706/healthz
```

## License

MIT — see [LICENSE](./LICENSE).

## Attribution

This project consumes data from
[phish.net](https://phish.net) and [phish.in](https://phish.in) via the
[mcp-phish](https://github.com/pete-builds/mcp-phish) server. Not affiliated
with phish.net, phish.in, or Phish.
