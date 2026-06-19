# open-setlist-stash

[![CI](https://github.com/pete-builds/open-setlist-stash/actions/workflows/ci.yml/badge.svg)](https://github.com/pete-builds/open-setlist-stash/actions/workflows/ci.yml)

Open-source, self-hostable **setlist-prediction game** for live-music fans. It is **band-agnostic**: the game engine reads setlist data from an MCP server, so you can run it for any act that has a data source. Two reference deployments run on this same codebase:

- **Wappy Picks** (Umphrey's McGee): https://www.wappypicks.com, data via [mcp-umphreys](https://github.com/pete-builds/mcp-umphreys)
- **Tweezer Picks** (Phish): data via [mcp-phish](https://github.com/pete-builds/mcp-phish)

They differ only by environment config, branding, and which MCP data source they point at. There is no per-band fork: one engine, many deployments.

## What it is

Pick **up to 5 songs** for an upcoming show (at least one required). Each song that gets played is worth **2 points**. You also make **one encore call**: tap one of your picks as your encore guess. If it lands in the encore you get **+5**; if it plays elsewhere it still earns its 2.

Predictions **lock** at a configurable showtime. The game then **scores live during the show**, re-reading the (partial) setlist and rebuilding the leaderboard on a short interval, so scores climb in real time as songs are played. Leaderboards run per-league and global.

The song picker shows each song's **show gap** (how many shows since it was last played) so you can avoid burning a pick on something just played. It is a fair human contest; any optional AI smart-pick assist is disabled during the prediction window.

## Bring your own data (MCP)

The game never touches a setlist database directly. Every read goes through an **MCP server** (Streamable HTTP, JSON-RPC) that implements the setlist contract, configured via `MCP_PHISH_URL` (the env name is historical; point it at any compatible server). A small async wrapper in `src/setlist_stash/mcp_client.py` calls the tools the server exposes.

Reference data servers:
- [mcp-umphreys](https://github.com/pete-builds/mcp-umphreys) — Umphrey's McGee, backed by [umphreys-vault](https://github.com/pete-builds/umphreys-vault) (All Things Umphreys data)
- [mcp-phish](https://github.com/pete-builds/mcp-phish) — Phish (phish.net / phish.in data)

To support a new band, stand up an MCP server that satisfies the same contract and point the game at it.

## Branding your instance

All branding is deployment-specific (config + mounted assets), so the public repo carries no operator-specific identity.

- **Name:** every page title and the brand wordmark read from `SITE_NAME`. Emoji work (e.g. `SITE_NAME="🎸 Wappy Picks 🤘"`).
- **Footer credit:** set `FOOTER_CREDIT` and `FOOTER_CREDIT_URL` to add an attribution line (defaults empty, so a self-host shows none).
- **Theme:** the platform ships a clean default (`static/style.css`). Layer your own CSS on top via either:
  - *Bundle it (good for forks):* drop a file in `src/setlist_stash/static/themes/your-theme.css`, set `THEME_FILE=themes/your-theme.css`, rebuild.
  - *Keep it private (good if the branding is yours):* mount the CSS at runtime via `docker-compose.override.yml` (see `docker-compose.override.yml.example`). The CSS lives outside the repo on the host; compose merges the override on `up`.
- **Email signup** is gated on `EMAIL_PROVIDER`: with it `disabled` (the default), the magic-link UI is hidden and players join with an anonymous handle + cookie. Set a real provider to enable email magic-link auth.

### Blog (optional)

Drop markdown files (optional `title`/`date`/`summary` frontmatter) into the directory named by `BLOG_DIR` (default `content/blog`, typically a mounted volume so posts stay deployment-specific). Posts render at `/blog` and `/blog/{slug}`; the nav "Blog" link only appears when at least one post is present.

## Stack

- Python 3.13, FastAPI, Jinja2, HTMX
- PostgreSQL (game state only; setlist data is read via the MCP server)
- Docker, multi-stage build; mypy-strict, Trivy-scanned in CI
- Deployable LAN-only, over Tailscale, or publicly (e.g. a Cloudflare Tunnel)

## Run locally

```bash
cp .env.example .env
# edit .env: set PG_PASSWORD and MCP_PHISH_URL (your MCP data server)
docker compose up -d
curl http://localhost:3706/healthz
```

## License

MIT — see [LICENSE](./LICENSE).

## Attribution

This project consumes setlist data through MCP servers. The Phish deployment uses data from [phish.net](https://phish.net) and [phish.in](https://phish.in) via mcp-phish; the Umphrey's deployment uses [All Things Umphreys](https://allthings.umphreys.com) via mcp-umphreys. Not affiliated with those data sources or with the artists.
