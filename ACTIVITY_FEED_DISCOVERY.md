# Activity Feed — Discovery (READ-ONLY)

Scope: how the dashboard "COMMAND FEED" / Activity views are wired today, and what `agent_log`
actually provides. No code was changed.

## Frontend

All frontend lives in a single file: `public/dashboard.html`.

There are **two** distinct UI surfaces that read activity, both already live:

### A) Right-panel "COMMAND FEED" (the live ticker)
- **Feed DOM container**: `public/dashboard.html:3376` → `<div class="activity-feed" id="activityFeed">`
  - Inner list element: `public/dashboard.html:3382` → `<div id="feedItems">`
  - Placeholder markup: `public/dashboard.html:3383` → `<div class="feed-item" ...>Loading activity...</div>`
    (static text only; replaced on first fetch)
  - Filter toggle (All / Warm signals only): `public/dashboard.html:3378-3381`
- **Populator function**: `loadActivityFeed()` at `public/dashboard.html:4374`
  - Renderer: `renderActivityFeedItems()` at `public/dashboard.html:4148`
  - Row event delegation (hover preview, click modal, detail expand): `bindActivityFeedDelegation()` at `public/dashboard.html:4178`
  - Per-item expand detail: `toggleActivityDetail()` `:4228` → `fetchActivityDetails()` `:4216` → `GET /api/activity/:id/details`
- **Data source URL**: `fetch('/api/activity')` — `public/dashboard.html:4376`
- **Refresh interval**: `setInterval(loadActivityFeed, 30000)` — `public/dashboard.html:6463` → **30,000 ms (30s)**.
  Also fired on load (`:6460`), after running an agent (`:4088`, `:4118`), and on client switch (`:3724`, `:3894`).

### B) Center "ACTIVITY" tab (full timeline + sequence panel)
- **DOM container**: `public/dashboard.html:3088` → `<div class="activity-area" id="activityArea">`
  - Sub-header status line: `id="activitySub"` `:3092`
  - "Load more" button: `id="activityLoadMore"` `:3160` → `loadMoreTimeline()`
- **Populator function**: `loadActivity()` at `public/dashboard.html:5646` (invoked by tab switch at `:3771`)
  - Pagination helper: `_fetchActivityPage(offset)` at `:5600`
- **Data source URLs**:
  - `GET /api/activity-panel` — `public/dashboard.html:5654` (primary: sequences + timeline)
  - `GET /api/activity?limit=&offset=` — `:5601` (paged "load more")
  - `GET /api/activity-timeline?offset=` — `:5606` (fallback/merge for older rows)
- **Refresh**: no auto-interval; loads on tab activation, with a 10s min-gap guard
  (`LOAD_ACTIVITY_MIN_INTERVAL_MS = 10000`, `:5642`) + manual "Load more".

### Is the data placeholder/hardcoded or live?
**Live.** Both surfaces fetch from real endpoints backed by the `agent_log` table.

One thing that *looks* like a placeholder but is not feed data: the
**"Random agent activity simulation"** at `public/dashboard.html:4057-4069` — a `setInterval(..., 2500)`
that only pulses the spinner ring on `.agent-card.live` cards for visual life. It does **not** inject
fake rows into the feed. The only literal placeholder is the static `"Loading activity..."` /
`"No activity yet."` strings (`:3383`, `:4156`), shown before/instead of fetched rows.

## Backend Endpoints

All four activity endpoints live in `routes/api.js`, gated by `requireDashboardRead`, and scoped to
`client_id` via `getRequestClientId(req)`. A shared SQL fragment
`EXCLUDE_COMMAND_FEED_ACTIONS_SQL` (`routes/api.js:22-27`) filters out Max `daily_brief`/`daily_digest`
rows from feeds.

| Endpoint | File:line | Source | Order | Limit |
|---|---|---|---|---|
| `GET /api/activity` | `routes/api.js:1022` | `agent_log al` LEFT JOIN `prospects p` | `al.ran_at DESC` | `limit` (1–100, default 20) + `offset` |
| `GET /api/activity/:id/details` | `routes/api.js:1150` | single `agent_log` row + per-agent enrichment | n/a (by id) | 1 |
| `GET /api/activity-panel` | `routes/api.js:1427` | `prospects/touchpoints` (sequences) **+** `agent_log` (timeline) | `ran_at DESC` / `MAX(t.created_at) DESC` | seq 100, timeline 50 |
| `GET /api/activity-timeline` | `routes/api.js:1542` | `agent_log al` LEFT JOIN `prospects p` | `al.ran_at DESC` | 50 + `offset` |

### `GET /api/activity` (drives the live COMMAND FEED)
- **Query** (`routes/api.js:1027-1051`): selects `id, agent_name, action, status, ran_at, payload`,
  resolves a `prospect_id` from either `al.prospect_id` or a UUID embedded in `payload->>'prospect_id'`,
  LEFT JOINs `prospects` for `first_name/last_name/notes`. Filters
  `WHERE al.client_id = $1 AND <not max daily_brief/daily_digest>`, `ORDER BY al.ran_at DESC LIMIT $2 OFFSET $3`.
- **Response shape** (array; mapped at `:1058-1103`):
  ```json
  [{
    "id": 12345,
    "agent": "Emmett",
    "action": "sent email sequence · Acme Cleaning",
    "raw_action": "outbound",
    "icon": "✉️",
    "color": "fi-o",
    "time": "12m",
    "ran_at": "2026-06-13T14:20:00.000Z",
    "status": "success",
    "prospect_id": "…uuid or null…",
    "prospect": "Acme Cleaning",
    "is_warm_signal": false
  }]
  ```
  - `agent` is humanized via `agentNameMap` (`:1053`); `action` is humanized via `actionLabels` (`:1068`).
  - `is_warm_signal=true` for actions in `[open, email_opened, click, email_clicked, reply, inbound, call_answered, triage]` (`:1094`) — these get the amber SIGNAL highlight.

### `GET /api/activity/:id/details` (expand panel)
- `routes/api.js:1150`. Loads one `agent_log` row by id, builds a labeled field list + per-agent
  enrichment (sequence step, prospect context). Returns `{ title, fields[], actions[], ran_at, ... }`.
  404s if the id/client doesn't match.

### `GET /api/activity-panel` (center Activity tab)
- `routes/api.js:1427`. Runs two queries in parallel:
  - **sequences**: `prospects` + email `touchpoints` aggregation (emails_sent, opens, clicks, next_due_at). LIMIT 100.
  - **timeline**: `agent_log` LEFT JOIN `prospects`, same exclude filter, `ORDER BY ran_at DESC LIMIT 50`.
- **Response shape**: `{ sequences: [...], timeline: [...] }` (timeline rows: `{id, agent, icon, action, prospect, status, ran_at}`).

### `GET /api/activity-timeline` (load-more pages)
- `routes/api.js:1542`. `agent_log` LEFT JOIN `prospects`, exclude filter,
  `ORDER BY ran_at DESC LIMIT 50 OFFSET $1`. Returns array of `{id, agent, icon, action, prospect, status, ran_at}`.

> Note: a **separate** setter-scoped `GET /api/activity` exists in `routes/setter.js:611` reading the
> `activity_log` table (setter call/email log) — different table, different surface. Not part of the
> agent_log command feed. `routes/client.js` also exposes a public `/:clientId/api/activity` (`:750`).

## agent_log Schema

No `CREATE TABLE agent_log` (or `schema.sql`) exists in the repo — the table is **pre-provisioned in
the Railway Postgres DB**, not migrated from code. Columns below are inferred from every INSERT/SELECT
and the `logAgentAction` helper.

- **Helper**: `dbClient.js:59` `logAgentAction(agentName, action, prospectId, targetUrl, payload, status, errorMsg=null, durationMs=null)` →
  `INSERT INTO agent_log (agent_name, action, prospect_id, target_url, payload, status, error_msg, duration_ms, client_id)` (`dbClient.js:62-66`).

### Columns (inferred types)
| Column | Type | Notes |
|---|---|---|
| `id` | serial/int PK | Returned to frontend as feed item id; `/api/activity/:id/details` keys on it |
| `agent_name` | text | e.g. `emmett_agent`, `scout`, `max`, `riley`; frontend strips `_agent` and maps to display name |
| `action` | text | e.g. `outbound`, `generate_comment`, `daily_digest`, `open`, `triage`, `cron_run` |
| `prospect_id` | uuid (nullable) | FK→`prospects.id`; sometimes only present inside `payload->>'prospect_id'` |
| `target_url` | text (nullable) | written by `logAgentAction`; not surfaced in feeds |
| `payload` | jsonb | arbitrary per-action data (`prospect_name`, `company`, insights, scores, etc.) |
| `status` | text | CHECK constraint: `success, failed, skipped, pending, completed, posted, in_progress` (`routes/api.js:42-55`) |
| `error_msg` | text (nullable) | failure detail |
| `duration_ms` | int (nullable) | run duration |
| `ran_at` | timestamptz | primary sort key (`ORDER BY ran_at DESC` everywhere) |
| `client_id` | int FK→`clients` | multi-tenant scope; every feed query filters on it |

### Sample row (sanitized, reconstructed from write/read sites)
```json
{
  "id": 84213,
  "agent_name": "emmett_agent",
  "action": "outbound",
  "prospect_id": "3f1c9a2e-7b4d-4e21-9c88-aa1122334455",
  "target_url": null,
  "payload": { "prospect_name": "Acme Cleaning", "sequence": "cleaning", "step": 1 },
  "status": "success",
  "error_msg": null,
  "duration_ms": 742,
  "ran_at": "2026-06-13T14:20:00.000Z",
  "client_id": 1
}
```

### Indexes
**None defined in the repo.** No `CREATE INDEX ... agent_log ...` exists anywhere (verified — other
tables like `call_dispositions`, `icp_score_history`, `scout_unenriched` create indexes in code, but
`agent_log` does not). There is therefore **no code-managed index on `ran_at DESC` or `client_id`**.
Any index that exists was created directly on the database and is not visible from this repo; this
cannot be confirmed read-only from the codebase.

## Gap Analysis

- **The feed is already live, not placeholder.** `/api/activity` returns real `agent_log` rows, polled
  every 30s. The only "fake" motion is the cosmetic agent-card ring pulse (`dashboard.html:4057`), and the
  only literal placeholders are the `"Loading activity..."` / `"No activity yet."` strings. If the task
  assumed a hardcoded feed, that premise is outdated — verify against live DB data instead.
- **No new endpoint is needed for basic wire-up.** `GET /api/activity` already delivers the correct
  shape (humanized agent/action, warm-signal flag, prospect linkage, pagination). Center tab is also
  wired via `/api/activity-panel` + `/api/activity-timeline`.
- **Performance risk: unindexed sort.** Every feed query is `WHERE client_id=$1 ... ORDER BY ran_at DESC LIMIT N`,
  but the repo defines no `(client_id, ran_at DESC)` index. As `agent_log` grows this becomes a full
  scan + sort on every 30s poll. A composite index is the highest-value change and must be applied at the
  DB level (not currently code-managed).
- **Label coverage drift.** `actionLabels` (`api.js:1068`) and `ACTION_LABELS` (`api.js:1510`, `:1567`) are
  hand-maintained allow-lists; unmapped actions fall through as the raw `action` string. New agent actions
  (e.g. Mira/Penny variants) will render as raw snake_case until added.
- **Two overlapping data paths in the center tab.** `loadActivity()` merges `/api/activity`,
  `/api/activity-panel`, and `/api/activity-timeline` client-side (`dashboard.html:5600-5611`). This is
  redundant and a candidate for consolidation if the timeline is reworked.

**Recommended approach:** Treat this as already-wired and live — the immediate win is a DB-level
`CREATE INDEX ON agent_log (client_id, ran_at DESC)` plus widening the action-label maps so new agents
render cleanly. No new endpoint or frontend rewire is required; if anything is consolidated, collapse the
center tab's three-call merge onto the single paginated `/api/activity`.
