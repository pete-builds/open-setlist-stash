# open-setlist-stash

[![CI](https://github.com/pete-builds/open-setlist-stash/actions/workflows/ci.yml/badge.svg)](https://github.com/pete-builds/open-setlist-stash/actions/workflows/ci.yml)

Open-source setlist prediction game for Phish shows. The reference deployment is [Tweezer Picks](https://github.com/pete-builds/open-setlist-stash); fork it and brand your own instance via the `SITE_NAME` env var.

## Branding your instance

Every page title, the brand wordmark, and the marketing copy read from the `SITE_NAME` environment variable. Set it in your `.env`:

```
SITE_NAME=Your Site Name
```

### Themes

The platform ships with a clean minimal default theme (`static/style.css`). To layer on a custom look, drop a CSS file into `static/` and point `THEME_FILE` at it:

```
THEME_FILE=themes/your-theme.css
```

The repo bundles one alternate theme out of the box: `themes/lot-poster.css` (Phish-fan hoodie-patch aesthetic, Honk + Anton + Special Elite typography, halftone overlays, silkscreen offsets). It's the look used on the reference deployment, [Tweezer Picks](http://192.168.86.20:3706). Set `THEME_FILE=themes/lot-poster.css` to use it.

To fully reskin, write your own CSS file and override the class hooks documented in `static/style.css`.

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
A small async wrapper in `src/setlist_stash/mcp_client.py` calls the 14 tools
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
