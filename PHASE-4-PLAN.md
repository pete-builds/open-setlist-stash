# Phase 4 Plan — `tweezer-picks`

Setlist prediction game for Phish shows. Phase 4 of the Phish Data Platform.
This document is the source of truth for the build. Subsequent Link sessions
iterate against this plan.

> **Hard architectural rules (carried from `phish-platform.md`):**
> - Every consumer reads the corpus via mcp-phish. Game NEVER touches the
>   `phish-vault` Postgres directly.
> - Auth boundary: Tailscale/LAN only through Phase 5. Public exposure is
>   Phase 6 and requires an auth-token layer.
> - Smart-pick assist is disabled during the prediction window. Game is a
>   fair human contest until showtime lock.
> - Pydantic tool models on mcp-phish are frozen contracts. Treat them as
>   external API surface; do not ship code that breaks if a field is added.

---

## 1. Stack choice + rationale

**FastAPI + Jinja2 + HTMX + PostgreSQL + asyncpg**, multi-stage Docker.

Why over Astro:

- **Server-rendered fits the use case.** Picks form, leaderboards, show
  pages: all simple state. HTMX delivers partial-page updates (lock
  countdown, leaderboard refresh, optimistic form ack) without an SPA
  framework.
- **Pete already runs FastAPI on nix1** (Model Arena, nfl-web,
  anthropic-tracker). One stack family, one deploy pattern, known
  operational footprint.
- **Cron-friendly.** A second Python entrypoint (`tweezer-picks-resolve`)
  runs as a cron-profile container. Astro would need an extra Node
  runtime for the resolver.
- **Direct path to mcp-phish.** httpx + asyncio is the same idiom we use
  in the platform's MCP servers. No JS adapter layer.
- **Lower deploy complexity.** Single image build, mirrors `mcp-unifi`
  and `mcp-phish` Dockerfile structure verbatim.

Astro stays viable for **Phase 5** (`phish-web`), where content pages
dominate. We can split the platform: Phase 4 = stateful game on FastAPI,
Phase 5 = content + chat on Astro. They share nothing but the MCP
contract.

---

## 2. Data model (game-side Postgres)

Migration `migrations/001_initial.sql`. Five tables plus `schema_version`.

### `users`

Anonymous handle by default. Magic-link email is **Phase 4b**.

| Column | Type | Notes |
|---|---|---|
| `id` | BIGSERIAL PK | |
| `handle` | TEXT UNIQUE | display name, regex-checked: `^[A-Za-z0-9_-]{2,32}$` |
| `handle_lower` | TEXT UNIQUE | case-insensitive lookup |
| `email` | TEXT NULL | Phase 4b |
| `email_verified_at` | TIMESTAMPTZ NULL | non-NULL = magic link confirmed |
| `created_at` / `last_seen_at` | TIMESTAMPTZ | |

Why no foreign key into the vault: handles are local identity. Vault has
no concept of users.

### `prediction_locks`

One row per show. Created lazily when the first prediction comes in. The
resolver also creates rows when checking for newly-published setlists.

| Column | Type | Notes |
|---|---|---|
| `show_date` | DATE PK | mcp-phish key for the show |
| `show_id` | TEXT | mcp-phish show id, denormalized |
| `lock_at` | TIMESTAMPTZ | effective cutoff (UTC) |
| `lock_at_override` | TIMESTAMPTZ NULL | manual override (operator only) |
| `venue_tz` | TEXT NULL | e.g. `America/New_York`; NULL = default |
| `resolved_at` | TIMESTAMPTZ NULL | non-NULL = setlist scored |

Partial index on `resolved_at IS NULL` makes the resolver's "what's open"
query a fast scan.

### `predictions`

One row per (`user_id`, `show_date`). Picks are arrays of song slugs
(strings). Storing slugs not FKs is intentional: songs live in the vault,
not here. The resolver validates slug existence against
`mcp__phish__get_song`.

| Column | Type | Notes |
|---|---|---|
| `id` | BIGSERIAL PK | |
| `user_id` | BIGINT FK → users | |
| `show_date` | DATE FK → prediction_locks | |
| `pick_song_slugs` | TEXT[] CHECK cardinality=3 | the 3 any-set picks; sorted at write |
| `opener_slug` | TEXT NULL | NULL = user skipped this slot |
| `closer_slug` | TEXT NULL | NULL = user skipped this slot |
| `encore_slug` | TEXT NULL | NULL = user skipped this slot |
| `submitted_at` | TIMESTAMPTZ | |
| `score` | INTEGER NULL | NULL until resolved |
| `score_breakdown` | JSONB NULL | per-pick rarity / slot bonus split |
| UNIQUE | (user_id, show_date) | one prediction per user per show |

**Lock-guard trigger** (`reject_post_lock_predictions`) blocks INSERT or
UPDATE when `now() > lock_at`. Application code is the first defense; this
trigger is the last. A bug, race, or compromised app session cannot slip
a late prediction past it.

### `scoring_runs`

Audit table mirroring `phish-vault.etl_runs`. One row per resolver tick.

| Column | Type |
|---|---|
| `id` | BIGSERIAL PK |
| `started_at` / `finished_at` | TIMESTAMPTZ |
| `status` | TEXT enum: `running`, `success`, `partial`, `error`, `noop` |
| `shows_resolved` | INTEGER |
| `predictions_scored` | INTEGER |
| `summary` | JSONB (per-show counts and any errors) |
| `error_message` | TEXT NULL |

`status='noop'` covers "ran but found nothing to score" — we want one row
per tick for monitoring.

### `leaderboard_snapshots`

Materialized leaderboards. Refreshed after each scoring_run. Three scopes:

- `weekly` with `scope_key = '2026-W19'`
- `tour` with `scope_key = 'fall-2026'` (slug from vault)
- `all_time` with `scope_key = 'all'`

| Column | Type |
|---|---|
| `id` | BIGSERIAL PK |
| `scope` | TEXT enum |
| `scope_key` | TEXT |
| `user_id` | BIGINT FK |
| `handle` | TEXT (denormalized) |
| `total_score` | INTEGER |
| `shows_played` | INTEGER |
| `rank` | INTEGER |
| `refreshed_at` | TIMESTAMPTZ |
| UNIQUE | (scope, scope_key, user_id) |

Reads serve directly off this table; writes rebuild it from `predictions`
every resolve. ~10K predictions × 3 scopes is trivial; no need for a
real materialized view yet.

---

## 3. Scoring formula

> "Weighted by song rarity (vault knows the gap), bonuses for slot-correct
> picks." — Roadmap

Inputs from `mcp__phish__get_song(slug)`:
- `times_played` — historical performance count.
- `gap_current` — shows since last play. Note: upstream MCP returns this
  field as `gap`. Normalized to `gap_current` inside
  `mcp_client.get_song()` so the rest of the codebase speaks one name.

### Per-pick base score (rarity points)

```
base(slug) = round( 10 * log2(1 + gap_current) * (200 / max(20, times_played)) )
```

Reasoning:

- `log2(1 + gap_current)`: a song bagged for 50 shows is meaningfully
  rarer than one bagged for 5, but log dampens the curve so a
  "never played in 800 shows" doesn't drown out everything else.
- `200 / max(20, times_played)`: songs played 20+ times anchor at
  scale 1; songs played 200+ times max at 1.0; songs played <20
  (rare deep cuts) cap the multiplier at 10. The `max(20, ...)`
  floor stops a 1-time-play song from getting an absurd 200×.
- `round` and the leading `10` keep typical scores in the 5–80 range
  for a played song, big numbers for a true bust-out, zero if the
  pick wasn't played.

If the pick **wasn't played** in the show: `base = 0`.

### Slot bonuses

| Slot | Bonus condition | Bonus |
|---|---|---|
| Opener | predicted slug == set 1 first song | `+25` flat |
| Closer | predicted slug == set N last song before encore | `+25` flat |
| Encore | predicted slug == any encore song | `+30` flat |

Slot bonuses are independent of base score: an opener pick that opens AND
is rare scores `base + 25`. A correct slot pick that's a common song still
scores something (the bonus alone, since `gap_current` is small).

A user can use the same slug as both a "pick" and a slot pick. Scoring is
applied independently. Example: pick Tweezer in the 3-song bag AND mark it
as the opener. If Tweezer opens, the user gets `base(Tweezer)` for the
bag pick AND `base(Tweezer) + 25` for the slot pick. Pete's call: this
could be tightened to "no double-dipping" later. **Recommended for now:
allow it.** Keeps the form simpler.

### Total score

```
total = sum(base(pick) for pick in pick_song_slugs)
      + slot_score(opener_slug, played_opener)
      + slot_score(closer_slug, played_closer)
      + slot_score(encore_slug, played_encore_set)
```

### `score_breakdown` JSONB shape

```json
{
  "picks": [
    {"slug": "tweezer", "played": true, "base": 47},
    {"slug": "fluffhead", "played": false, "base": 0},
    {"slug": "harry-hood", "played": true, "base": 22}
  ],
  "opener": {"pick": "tweezer", "actual": "tweezer", "bonus": 25},
  "closer": {"pick": "harry-hood", "actual": "slave", "bonus": 0},
  "encore": {"pick": "loving-cup", "actual": "loving-cup", "bonus": 30},
  "total": 124
}
```

This shape is stable for the front-end "your prediction scored" page.

### Edge cases

- **Phish.net says a song was played but no slug match:** treat as not
  played. Log to scoring_runs.summary as an unmatched-slug warning.
- **Show was cancelled or rescheduled:** resolver sees no setlist for the
  date past a long timeout. Decision: leave `predictions.score = NULL`,
  set `prediction_locks.resolved_at = now()` with a sentinel
  `summary.cancelled = true`. Picks don't count for any leaderboard.
- **Setlist has unusual structure (no encore, three sets, etc.):** bonus
  is calculated per the actual structure: closer = last song of the last
  non-encore set; encore bonus = ANY song in any encore set; opener =
  first song of set 1. If there's no encore, encore-pick bonus is 0.

---

## 4. Showtime lock policy

**Default cutoff: 22:00 ET on the show date.** Configurable via
`DEFAULT_LOCK_TIME_LOCAL=22:00` and `DEFAULT_LOCK_TZ=America/New_York`.

Why a default-and-override model:

- Phish has variable start times.
- Vault doesn't carry venue tz reliably yet (Phase 2 deferred this).
- A blanket "22:00 ET" is conservative: late enough that east-coast
  shows haven't started, early enough that west-coast shows can't have
  finished set 1 (which would leak the opener).
- Operator can override per-show via `prediction_locks.lock_at_override`.
  Phase 4b will surface this in an admin UI; Phase 4 ships without one
  (operator just runs SQL).

**Lock enforcement is layered:**
1. Application reads `prediction_locks.lock_at` before accepting a
   submission. Returns 409 Conflict if locked.
2. Database trigger `predictions_lock_guard` rejects INSERT/UPDATE when
   `now() > lock_at`. Last line of defense; applies to direct SQL,
   migrations, anything.
3. Audit: every lock breach attempt at the app layer logs to scoring_runs
   summary as a `lock_violation_attempt` (not a DB row in scoring_runs;
   that table is for resolves, not user actions — a `prediction_attempts`
   audit table is a future addition).

**Edge case: rescheduled shows.** If a show is rescheduled before the
default cutoff, the operator updates `prediction_locks.lock_at_override`.
If after, predictions are already locked — they ride to the new date or
get refunded by SQL hand-edit. Documented as a known operator burden;
Phase 4b adds a CLI for it.

---

## 5. Auto-resolve cron design

A separate process (`tweezer-picks-resolve`) runs every 30 minutes via either:

- **Option A:** docker compose `cron` profile + a host-level `cron` entry
  invoking `docker compose --profile cron run --rm tweezer-picks-resolve`.
  Mirrors `phish-vault`. **Recommended.**
- **Option B:** an APScheduler thread inside the FastAPI app process.
  Smaller ops surface, but complicates restarts (a deploy mid-resolve
  would silently kill the run).

Going with **Option A** for parity with `phish-vault` and clean separation.

### Resolve loop

```
1. INSERT scoring_runs (status='running', started_at=now())
2. Find open shows: SELECT show_date FROM prediction_locks
                    WHERE resolved_at IS NULL AND lock_at < now()
3. For each open show:
   a. Call mcp__phish__get_show(show_date)
   b. If no setlist data yet -> skip, do not write resolved_at
   c. If setlist present:
      - Extract opener / closer / encore songs
      - For each prediction on this show:
        * Score per the formula (calls mcp__phish__get_song per unique slug,
          batched + cached for the run)
        * UPDATE predictions SET score, score_breakdown
      - UPDATE prediction_locks SET resolved_at = now()
4. Recalculate leaderboard_snapshots for all three scopes
5. UPDATE scoring_runs SET status='success', finished_at, summary, counts
```

Failure handling:

- mcp-phish unreachable: fail the whole run, status='error',
  error_message logged. Cron retries in 30 min.
- Per-show MCP error: skip that show, status='partial' for the run, log
  show_date to summary.errors. Other shows still resolve.
- Network blip mid-run: scoring_runs row stays at 'running' if the
  process dies. A startup task at next tick stamps any older 'running'
  rows as 'error' with a "watchdog" message.

### Setlist freshness window

Phish.net publishes setlists at varying delays (often 30 min, sometimes
hours, sometimes overnight if the show ran late). The resolver simply
re-checks every 30 min. No special "wait N hours" logic. The hot-window
fallthrough rule on mcp-phish (last 24h reads live) means we always get
the freshest setlist available without bypassing the MCP contract.

---

## 6. Auth flow

### Phase 4: anonymous handle

1. New visitor → form: "pick a handle (2–32 chars, A-Z 0-9 _ -)".
2. Server creates `users` row, sets a signed cookie:
   `phishgame_handle=<handle>` (signed with `SESSION_SECRET` via
   `itsdangerous`). HttpOnly, SameSite=Lax. Lifetime: 365d.
3. Returning visitor → cookie valid → `last_seen_at` updates.
4. Cookie invalid / missing → form again.

No password, no email. Handles are public.

**Squatting:** first-come first-served. A user can "claim" a fresh handle
at any time (creates a new account; old one becomes orphaned but
predictions stay attached). Phase 4b adds rename + email-bound recovery.

### Phase 4b: optional email magic-link

- `users.email` + `email_verified_at`.
- `/auth/email` form → user types email → server generates a one-time
  signed token (15-min TTL via `itsdangerous`) → emails a `/auth/verify?token=...`
  link.
- On verify: stamp `email_verified_at`. Cookie is now bound to the email.
- Future logins from a new device: enter email → new magic link.
- Email transport: SMTP via `msmtp` on nix1 (Pete already has notes on
  Gmail app password setup in `zion-patching-notifications.md` —
  same pattern).

**Out of scope for kickoff session.** Tagged 4b explicitly.

### Auth boundary

The whole game is **Tailscale/LAN only** through Phase 5. Public exposure
is a Phase 6 decision. Until then, "auth" is a soft layer over a network
that's already trusted.

---

## 7. Smart-pick assist gating

Roadmap rule: **assist disabled during prediction window**. Implementation:

- Single env flag `ASSIST_PRE_LOCK` (default `false`). MUST stay `false`
  in production.
- Game UI features that surface vault analytics are gated by a single
  helper: `assist_allowed(show_date) -> bool`:
  ```
  if now() >= prediction_locks.lock_at:    -> True   (post-lock retro)
  elif ASSIST_PRE_LOCK:                    -> True   (dev override only)
  else:                                    -> False
  ```
- Assist features (post-lock only):
  - "Show your picks vs the songs-by-gap chart"
  - "Venue history for tonight's room"
  - "Tour-to-date opener/closer heatmap"
- Pre-lock UI shows the form, the lock countdown, and the user's previous
  predictions. **Nothing else.** Specifically: no "rare songs" hints, no
  gap counts, no last-played indicators on song search.
- Search-songs autocomplete for the picks form is allowed (it's a typing
  affordance, not analytics) but returns ONLY title and slug. No
  `times_played`, no `gap_current` in the autocomplete response payload.

Tested via:
- A unit test verifies the songs-search endpoint payload shape during
  pre-lock contains only `{slug, title}`.
- A second test patches `now()` past `lock_at` and confirms the same
  endpoint returns the full payload.

---

## 8. MCP client integration approach

A small async wrapper (`src/tweezer_picks/mcp_client.py`, **stub for kickoff**)
calls the mcp-phish HTTP endpoint over JSON-RPC.

### Transport

- mcp-phish runs FastMCP Streamable HTTP at `http://mcp-phish:3705/mcp`.
- The game's compose file joins the `mcp-phish_default` external network
  so DNS resolves `mcp-phish` to the container IP.
- Wire format: JSON-RPC 2.0 POST, `Content-Type: application/json`,
  `Accept: application/json, text/event-stream`. The Streamable HTTP
  spec allows both request/response and SSE; we use synchronous
  request/response only.
- **Session handshake required.** FastMCP Streamable HTTP refuses
  `tools/call` without a session. Sequence per process:
  1. POST `initialize` to `/mcp`. Capture the `mcp-session-id` response
     header.
  2. POST `notifications/initialized` to `/mcp` with that header.
  3. Carry `mcp-session-id` on every subsequent `tools/call`.
  Skip this and the server returns `400 Missing session ID` (caught by
  build session 1's post-deploy smoke test).

### Wrapper shape

```python
class McpPhishClient:
    async def __aenter__(self) -> McpPhishClient: ...
    async def __aexit__(self, *_) -> None: ...

    async def get_show(self, date_or_id: str) -> dict[str, Any]: ...
    async def get_song(self, slug: str) -> dict[str, Any]: ...
    async def search_songs(self, query: str, limit: int = 10) -> list[dict]: ...
    async def recent_shows(self, limit: int = 10) -> list[dict]: ...
    async def venue_history(self, slug: str, limit: int = 25) -> list[dict]: ...
    async def health(self) -> dict[str, Any]: ...
```

Returns are plain dicts, not Pydantic models. The mcp-phish models are
upstream's contract; the game has its own narrower internal shapes for
the form, the leaderboard, and the score breakdown.

### Caching

- Per-request memo for the resolver loop (one show resolve = ~3-7 unique
  song slug fetches; deduplicate inside the request).
- No long-lived game-side cache yet. mcp-phish has its own opaque KV
  cache + vault read path, so a per-tick fetch is cheap.
- If we ever see resolver loops timing out, add a Redis layer. Not now.

### Failure handling

- 5xx or network error: bubble up as `McpPhishUnavailable`.
- 4xx (e.g. unknown slug): bubble up as `McpPhishNotFound`.
- Hot-window fallthrough means a show that's "live" (today) still
  resolves on time as long as phish.net publishes the setlist before our
  next tick.

### Phase 4 kickoff scope for the client

Just the file stub + a `/healthz` extension that pings mcp-phish health.
Full implementation lands in the next Link session along with the picks
form.

---

## 9. Out of scope for Phase 4

Phase 4 ships the game core. These belong to **4b** or later:

- **Magic-link email auth** (Phase 4b)
- **Operator admin UI** for lock overrides, handle moderation, prediction
  refunds on rescheduled shows (Phase 4b)
- **Per-tour and per-venue leaderboards** beyond the three required
  scopes (Phase 4b)
- **Game-side gap-stat assist UI** (Phase 5; cleaner alongside `phish-web`)
- **Public exposure** with Cloudflare/Tailscale Funnel + auth tokens
  (Phase 6)
- **Push notifications** for "your show locks in 30 min" (Phase 6)
- **Mobile-optimized UI polish** (iterative, not a phase)
- **Fantasy-league-style season scoring** (post-Phase 6, optional)
- **Prediction discussion / comments** (out scope; opens moderation
  burden)
- **Anti-bot rate limiting beyond LAN trust** (Phase 6)

---

## 10. Verification checklist (Phase 4 done = all of these)

Mirrors the Phase 1 nine-point list in `phish-platform-roadmap.md`.

1. **Container health.** `docker compose ps` shows `tweezer-picks` healthy
   and `tweezer-picks-pg` healthy on nix1; `curl http://nix1:3706/healthz`
   returns 200 with `{"status":"ok","version":"<v>"}`.
2. **Schema migrated.** `tweezer-picks-pg` has `schema_version` row 1, all
   five tables exist, `predictions_lock_guard` trigger exists.
3. **MCP path works.** A curl from inside the `tweezer-picks` container to
   `http://mcp-phish:3705/mcp` returns a JSON-RPC response (not a DNS
   error). `/healthz` extension reports `mcp_phish.reachable=true`.
4. **Game flow smoke (manual).** A new visitor:
   - Picks a handle, gets a session cookie
   - Picks 3 songs + opener + closer + encore for an upcoming show
   - Sees their submission echoed back
   - Cannot submit again for the same show (one prediction per
     user-show)
   - Cannot submit after `lock_at` (returns 409, trigger blocks
     direct DB write too)
5. **Pre-lock assist gate.** Songs autocomplete returns `{slug, title}`
   only. No `gap_current` leak. Verified by unit test AND HTTP smoke.
6. **Resolver works.** A scripted test: insert a fake `prediction_locks`
   row + a fake `predictions` row with a real past show date (e.g.
   1995-12-30 with known setlist). Run `tweezer-picks-resolve`. Confirm:
   - `scoring_runs` row written with `status='success'`
   - `predictions.score` populated
   - `predictions.score_breakdown` JSON shape matches plan
   - `prediction_locks.resolved_at` set
7. **Leaderboards refresh.** After step 6, `leaderboard_snapshots` has
   rows for all three scopes; the user appears at rank 1 in `all_time`.
8. **Lock guard fires.** Direct SQL `INSERT INTO predictions` with a
   `show_date` whose `lock_at` is in the past raises a
   `check_violation`. Tested in pytest.
9. **CI green.** ruff, mypy --strict, pytest >= 80% coverage. (CI
   workflow lands in a follow-up session — kickoff omits it
   intentionally to keep scope tight, same way mcp-phish kickoff did.)
10. **Memory updated.** `phish-platform.md` shows Phase 4 status
    advanced; `homelab-webapps.md` claims port 3706 as `tweezer-picks`.

When all 10 pass, Phase 4 is done. Phase 5 (`phish-web`) can start.

---

## Build sequence after kickoff

This is the queue for subsequent Link sessions. Each row is a session-sized
chunk, in order.

| # | Session focus | Output |
|---|---|---|
| 1 | This kickoff | scaffold + healthz + plan + repo + deploy on 3706 |
| 2 | mcp-phish client wrapper + DB connection pool + migrations runner | `mcp_client.py`, `db.py`, `migrate` CLI |
| 3 | Auth (anonymous handle) + session cookie | handle form, signed cookie, `users` write path |
| 4 | Picks form (3 songs + opener + closer + encore) + lock-aware submit | `/predict/<show_date>` flow |
| 5 | Resolver entrypoint + scoring formula + scoring_runs audit | `tweezer-picks-resolve` CLI |
| 6 | Leaderboards (weekly / tour / all-time) + HTMX refresh | `/leaderboard` |
| 7 | Lock countdown + post-lock assist UI (gap stats, venue history) | gated assist views |
| 8 | CI workflow (ruff + mypy + pytest + Trivy + Dependabot) | `.github/workflows/ci.yml` |
| 9 | Hash-pinned `requirements.lock` + Docker switch to require-hashes | reproducible build |
| 10 | Phase 4b: magic-link email auth | `email`, `email_verified_at`, msmtp wiring |

Pete sets the priority. This is one valid order; Phase 4 done = items
1–9 shipped. Item 10 is 4b.
