# Pulseforge Lead Gen App — Architecture Reference

## What This Is
An AI-powered lead generation and outreach CRM for Pulseforge. It scrapes leads, scores them, runs multi-channel outreach campaigns via a team of named AI agents, and surfaces everything through an authenticated Express dashboard. Deployed on Railway at `node server.js`.

---

## File Map

| File | Purpose |
|---|---|
| `server.js` | Entry point. ~370 lines after route split. Handles auth, session, login/logout, /dashboard, /demo, /preview, /api/status, /api/search, /api/export/csv, /api/post-comment, and app.listen. |
| `routes/api.js` | All /api/* dashboard routes (19 endpoints) |
| `routes/cron.js` | CRON_MODULES, runCronAgent, GET/POST /cron/:agent handlers |
| `routes/webhooks.js` | POST /webhooks/brevo and /webhooks/bland with BREVO_EVENT_MAP and checkAndUpdateWarmStatus |
| `routes/approvals.js` | /approvals dashboard, /api/pending-comments, /api/approve-comment/:id, /api/reject-comment/:id |
| `leadgen.js` | CLI tool — scrapes leads from Google Custom Search + Prospeo, scores them, exports CSV/Sheets. Run via `node leadgen.js --industry "cleaning" --location "Manchester NH"` |
| `db.js` | Creates and exports the shared `pg.Pool` using `DATABASE_URL`. All agents and server import from here. Never call `pool.end()` in agents — it closes the shared pool. |
| `dbClient.js` | Helper functions wrapping common DB operations: `checkDNC`, `logTouchpoint`, `logAgentAction`, `addProspect`, `addCompany`, `savePendingComment`, etc. |
| `railway.json` | Railway deploy config: `node server.js` |
| `warmSignalAgent.js` | Warm Signal Agent — detects prospects with 2+ opens in 7 days, flags them in Setter Lead List Google Sheet |
| `setterHandoffAgent.js` | Handoff Utility — runs after Scout or manually to mark qualified prospects as setter-visible and backfill the setter queue |
| `calBatchAgent.js` | Cal Batch — Bland.ai batch calling for cold prospects with phone numbers |
| `getSheetsToken.js` | Helper script to generate GOOGLE_SHEETS_REFRESH_TOKEN OAuth token with spreadsheets scope |
| `getRileyToken.js` | Helper script to generate Gmail OAuth token for Riley |
| `utils/publishPipeline.js` | Publishes approved content to Google Business Profile, Facebook Page, LinkedIn Page (via Buffer GraphQL), and handles comment publishing for Faye/Link. |
| `utils/blogPublisher.js` | GitHub-based blog post publisher. |
| `utils/demoData.js` | Generates fake live-feed data for the unauthenticated `/demo` route. |

---

## Agent Roster

Each agent is a standalone JS module that reads from the DB and writes results back. All are triggered via `POST /api/run/:agent` (dashboard button) or `POST/GET /cron/:agent` (Railway cron jobs).

| File | Agent Name | Role |
|---|---|---|
| `maxAgent.js` | Max | Daily manager briefing — pulls system snapshot, generates AI summary of pipeline health. Has read-only API access to /api/setter/metrics and /api/setter/feed for pipeline monitoring. No write permissions. |
| `paigeAgent.js` | Paige | Content creation — writes posts for GBP, Facebook, LinkedIn; queues to `pending_comments` for approval |
| `emmettAgent.js` | Emmett | Email outreach — writes and sends cold email sequences via Brevo API |
| `rileyAgent.js` | Riley | Inbound triage — reads Gmail inbox, classifies replies from known prospects (interested/not_now/unsubscribe/out_of_office/wrong_person/negative), updates prospect status, logs inbound touchpoints, deposits action cards for interested replies. Also processes Brevo email event webhooks. |
| `warmSignalAgent.js` | Warm Signal | Detects 2+ opens in 7 days, writes 🔥 2ND OPEN flag to Setter Lead List Google Sheet |
| `setterHandoffAgent.js` | handoff_utility | Runs after Scout to evaluate newly inserted prospect rows. Applies scoring threshold and writes qualified leads to the setter queue with `setter_status = 'new'` and `setter_visible = true`. Also serves as a backfill agent for historical prospects that were never handed off. Triggered post-Scout via n8n/Railway cron or manual invocation. |
| `rexAgent.js` | Rex | Reporting — weekly performance summaries |
| `samAgent.js` | Sam | SMS outreach via Twilio |
| `ivyAgent.js` | Ivy | Instagram engagement |
| `linkedinAgent.js` | Link | LinkedIn — monitors local biz owner posts, drafts comments for approval |
| `facebookAgent.js` | Faye | Facebook — monitors local group posts, drafts comments for approval |
| `veraAgent.js` | Vera | Review monitoring — watches GBP reviews, drafts response copy |
| `calAgent.js` | Cal | Google Calendar — books discovery calls |
| `calBatchAgent.js` | Cal Batch | Bland.ai batch calling for cold prospects with phone numbers |
| `analyticsAgent.js` | Analytics | Pulls post performance data, writes back to `post_analytics` table |
| `sketchAgent.js` | Sketch | Site mockup generator using Puppeteer |
| `pennyAgent.js` | Penny | (check file for current role) |

---

## Key Route Groups

Routes are now split across `routes/api.js`, `routes/cron.js`, `routes/webhooks.js`, and `routes/approvals.js`. Only auth, session, and a handful of core routes remain in `server.js`.

- **Auth**: `GET /login`, `POST /login`, `GET /logout` — bcrypt session auth, `requireAuth` middleware gates all `/api/*` and `/dashboard`
- **Webhooks**: `POST /webhooks/brevo` (email tracking), `POST /webhooks/bland` (call transcripts) — in `routes/webhooks.js`
- **Cron**: `POST /cron/:agent` and `GET /cron/:agent` — Railway cron triggers. Protected by `CRON_SECRET` query param. — in `routes/cron.js`
- **Agent run**: `POST /api/run/:agent` — fires any agent on demand from dashboard — in `routes/api.js`
- **Prospects**: `GET /api/prospects`, `GET /api/prospects/:id/touchpoints` — in `routes/api.js`
- **Approvals**: `GET /api/approvals`, `POST /api/approvals/:id` (approve/reject content before publish) — in `routes/approvals.js`
- **Pending comments**: `GET /api/pending-comments`, `POST /api/approve-comment/:id`, `POST /api/reject-comment/:id` — in `routes/approvals.js`
- **Analytics**: `/api/analytics`, `/api/analytics/posts`, `/api/analytics/summary`, `/api/analytics/top-posts`, `/api/analytics/email` — in `routes/api.js`
- **Activity**: `/api/activity`, `/api/activity-panel`, `/api/activity-timeline` — in `routes/api.js`
- **Dashboard UI**: `GET /dashboard` → serves `public/dashboard.html`; `GET /demo` → unauthenticated demo mode — in `server.js`
- **Setter dashboard**: `GET /setter` → authenticated setter UI (queue, pipeline, activity log, metrics strip, Scout feed). Requires setter or admin role. — in `routes/setter.js`
- **Setter API (read-only)**: `GET /api/setter/metrics`, `GET /api/setter/feed` — consumed by Max for pipeline monitoring. No write access. `PATCH /api/setter/leads/:id/notes`, `PATCH /api/setter/leads/:id/callback`, `PATCH /api/setter/leads/:id/hot`, `POST /api/setter/leads/:id/quick-log-call`, `POST /api/setter/leads/:id/enrich-phone`, `GET /api/setter/stats/today` — in `routes/setter.js`
- **Brevo warm signal**: Brevo POSTs email events here → Riley logs touchpoints and upgrades cold→warm automatically — in `routes/webhooks.js`

---

## Database Schema (Key Tables)

| Table | What it holds |
|---|---|
| `companies` | Scraped companies: name, industry, size, location, website, icp_score, tech_stack |
| `prospects` | Individual contacts: name, email, phone, job_title, linkedin_url, icp_score, status (cold/warm/hot), do_not_contact, last_contacted_at, vertical (TEXT — populated by Scout at insert time based on CONFIG.industry), setter_status (enum: new \| contacted \| follow_up \| booked \| dead), setter_visible (boolean — true = qualifies for setter queue, set by setterHandoffAgent), notes (text — setter scratchpad, auto-saved on blur), callback_at (timestamptz — scheduled follow-up, shown as "Due Today" queue section), is_hot (boolean — hot lead flag, sorts to top of stage) |
| `activity_log` | Setter contact log: id, lead_id (FK → prospects), action_type (call \| email \| text), notes, created_at, setter_id |
| `users` | Auth accounts: id, name, email, password_hash, role (admin \| manager \| setter), active, created_at, last_login_at |
| `touchpoints` | Every agent action: channel, action_type, content_summary, outcome, sentiment, external_ref |
| `agent_log` | Audit trail for every agent run: agent_name, action, prospect_id, payload, status, error_msg, duration_ms |
| `agent_actions` | Actionable items deposited by agents for dashboard review: id, created_by, action_type, title, description, payload, status, executed_at, result, created_at — used by Max and Riley |
| `pending_comments` | Content queued for human approval before publish: post_content, comment, post_url, channel, status |
| `post_analytics` | Published post performance: platform, post_id, impressions, clicks, etc. |
| `prospect_summary` | View that joins prospects + companies for agent reads |
| `session` | connect-pg-simple session store |

**DNC rule**: Always call `checkDNC(prospectId)` before any agent sends or posts on behalf of a prospect. `do_not_contact = true` is set automatically on bounces, unsubscribes, and spam events from Brevo.

---

## Environment Variables

**Core**
- `DATABASE_URL` — PostgreSQL connection string (Railway Postgres)
- `SESSION_SECRET` — Express session signing key
- `ANTHROPIC_API_KEY` — All AI agents use this
- `CRON_SECRET` — Guards `/cron/:agent` endpoints from unauthenticated triggers
- `DASHBOARD_PASSWORD` — Bcrypt-hashed password for dashboard login
- `APP_URL` — Public base URL of the deployed app
- `DASHBOARD_URL` — URL shown in notifications/emails

**Lead Scraping**
- `GOOGLE_API_KEY`, `GOOGLE_CX` — Google Custom Search
- `GOOGLE_PLACES_KEY` — Google Places API
- `SERPAPI_KEY` — SerpAPI fallback
- `HUNTER_API_KEY` — Email enrichment
- `PROSPEO_API_KEY` — Contact data enrichment (Prospeo API for email lookup)
- `GOOGLE_SHEET_ID` — Sheet to write leads into

**Email / Outreach**
- `BREVO_API_KEY` — Email sending + webhook tracking (Emmett)
- `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_PHONE_NUMBER` — SMS (Sam)
- `BLAND_API_KEY` — AI phone calls
- `BLAND_PHONE_NUMBER` — Bland.ai caller ID for Cal batch calls

**Social Publishing**
- `FACEBOOK_PAGE_ACCESS_TOKEN`, `FACEBOOK_PAGE_ID`, `FACEBOOK_APP_SECRET` — Facebook Page posts (Faye/Paige)
- `FACEBOOK_SESSION` — Puppeteer session cookie (stored as env var on Railway, never commit `facebook_session.json`)
- `LINKEDIN_SESSION` — Puppeteer session cookie (same — never commit `linkedin_session.json`)
- `BUFFER_ACCESS_TOKEN`, `BUFFER_CHANNEL_ID` — LinkedIn Page publishing via Buffer GraphQL
- `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `GOOGLE_REFRESH_TOKEN` — Google Business Profile and Calendar (Vera/Paige/Cal)
- `GOOGLE_SHEETS_REFRESH_TOKEN` — OAuth refresh token with spreadsheets scope for warmSignalAgent (separate from GOOGLE_REFRESH_TOKEN)
- `GOOGLE_SERVICE_ACCOUNT_JSON` — Service account for GBP API calls
- `GBP_ACCOUNT_ID`, `GBP_LOCATION_ID` — Specific GBP location being managed
- `GMAIL_CREDENTIALS`, `GMAIL_TOKEN` — Gmail OAuth (stored as env vars on Railway, never commit `gmail_credentials.json` / `gmail_token.json`)

**Calendar**
- `GOOGLE_CALENDAR_REFRESH_TOKEN`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET` — Cal agent booking

**Content Publishing**
- `GITHUB_TOKEN`, `GITHUB_REPO` — Blog post publishing (blogPublisher)

**Google Ads** (future)
- `GOOGLE_ADS_CLIENT_ID`, `GOOGLE_ADS_CLIENT_SECRET`, `GOOGLE_ADS_DEVELOPER_TOKEN`, `GOOGLE_ADS_MANAGER_ACCOUNT_ID`

---

## Auth Model
Session-based multi-user auth. `POST /login` accepts email + password, checks against users table, stores { id, name, email, role } in session. `requireAuth` middleware gates all protected routes. `requireRole(...roles)` enforces per-route role access. Roles: admin (full access), manager (full access except user management), setter (/setter only). Admin UI at `/admin/users` for creating and managing accounts. Falls back to `DASHBOARD_PASSWORD` env var if users table is empty.

---

## Approval Flow
Agents that generate content (Paige, Link, Faye, Vera) do NOT post directly. They write to `pending_comments` with `status = 'pending'`. The dashboard surfaces these under the Approvals tab. `POST /api/approve-comment/:id` triggers `publishPipeline.js` to actually push to the platform. This is intentional — nothing goes public without human sign-off.

---

## Known Architectural Notes
- **Route split (May 10 2026)** — server.js was split into 4 route files. Do not add routes back to server.js directly. All new routes go in the appropriate file under `routes/`.
- **Duplicate middleware** — `express.json()` and `express.urlencoded()` are registered twice in server.js. Harmless but worth cleaning next time that section is touched.
- **Sensitive JSON files** — `gmail_token.json`, `gmail_credentials.json`, `facebook_session.json`, `linkedin_session.json` are in `.gitignore` — never remove them from there. They are injected as env vars on Railway.
- **`pool.end()` is forbidden in agents** — the pool is shared across the server process. Calling `pool.end()` in an agent kills all subsequent DB connections.
- **Vertical routing** — Scout tags every prospect with `vertical` at insert time based on `CONFIG.industry`. Emmett reads `vertical` to pick the correct email sequence. Sequences exist for: cleaning, restaurant, salon, fitness, property, landscaping. All others fall back to cleaning.
- **Email sequences** — live entirely in `emmettAgent.js` in the `SEQUENCES` object. To add a new vertical: add the sequence array, add a routing condition in `getSequenceForProspect`.
- **GMAIL_CREDENTIALS format** — the Railway env var must be the full credentials JSON starting with `{"web":` — not just the client secret file.
- **GOOGLE_SHEETS_REFRESH_TOKEN vs GOOGLE_REFRESH_TOKEN** — `GOOGLE_REFRESH_TOKEN` is for GBP/Calendar. `GOOGLE_SHEETS_REFRESH_TOKEN` is for Sheets write access in warmSignalAgent. They use different OAuth clients and scopes — do not conflate them.
- **Scout → setter pipeline (fixed May 14 2026)** — Scout writes prospects to the DB but does not self-qualify leads for the setter. setterHandoffAgent.js handles the handoff: it evaluates icp_score against a threshold and sets setter_visible = true on qualifying rows. Prior to this fix, 170 leads had accumulated in prospects without ever surfacing in the setter queue. Backfill was run manually to resolve the gap. Always run setterHandoffAgent after a Scout batch or wire it into the Scout cron chain.
- **calBatchAgent.js was present in an earlier version of CLAUDE.md but may have been dropped during a recent edit.** Confirm it is restored in both the File Map and Agent Roster.
- **Multi-user auth (added May 2026)** — replaced single DASHBOARD_PASSWORD with users table. DASHBOARD_PASSWORD fallback remains for empty-DB safety. First setter: William Hernandez (created via admin UI post-deploy). setter_id on activity_log rows references users.id.
- **Phone enrichment on setter dashboard uses Prospeo API (PROSPEO_API_KEY).** Matches the same request pattern as leadgen.js. Logs to agent_log with agent_name = 'setter'.
- **Setter dashboard call logging uses the existing activity_log table (action_type='call').** attempt_count is computed at query time — not stored. Daily goal tracker scopes to setter_id + today's date so it resets automatically at midnight without a cron job.
