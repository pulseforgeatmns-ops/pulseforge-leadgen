# Pulseforge Lead Gen App — Architecture Reference

## What This Is
An AI-powered lead generation and outreach CRM for Pulseforge. It scrapes leads, scores them, runs multi-channel outreach campaigns via a team of named AI agents, and surfaces everything through an authenticated Express dashboard. Deployed on Railway at `node server.js`.

---

## File Map

| File | Purpose |
|---|---|
| `server.js` | Entry point. Handles auth, session, login/logout, /dashboard, /preview, /api/status, /api/search, /api/export/csv, /api/post-comment, and app.listen. |
| `routes/api.js` | All /api/* dashboard routes (19 endpoints) |
| `routes/cron.js` | CRON_MODULES, runCronAgent, GET/POST /cron/:agent handlers |
| `routes/webhooks.js` | POST /webhooks/brevo and /webhooks/bland with BREVO_EVENT_MAP and checkAndUpdateWarmStatus |
| `routes/approvals.js` | /approvals dashboard, /api/pending-comments, /api/approve-comment/:id, /api/reject-comment/:id |
| `routes/closer.js` | /closer dashboard and closer APIs for booked-call pipeline, call status updates, closing prospects, and commission tracking |
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
| `utils/clientContext.js` | `getClientConfig(clientId)` loads full client config from `clients`; also owns idempotent client architecture migration/backfill helpers. Called by agents/routes to scope behavior and queries. |
| `utils/closerSchema.js` | Idempotent closer-role migration helper: users role constraint, prospect closer fields, and `commissions` table. |
| `utils/emailPerformance.js` | Email performance tracking. `ensureEmailPerformanceTable()` startup migration + `recordSend` / `recordEvent` upserts keyed on client_id/vertical/sequence/step/subject_line. Emmett calls `recordSend` after each send; `routes/webhooks.js` calls `recordEvent` on open/click/bounce. Powers Max's weekly EMAIL PERFORMANCE digest section. |

---

## Agent Roster

Each agent is a standalone JS module that reads from the DB and writes results back. All are triggered via `POST /api/run/:agent` (dashboard button) or `POST/GET /cron/:agent` (Railway cron jobs).

| File | Agent Name | Role |
|---|---|---|
| `maxAgent.js` | Max | Daily manager briefing — pulls system snapshot, generates AI summary of pipeline health. Has read-only API access to /api/setter/metrics and /api/setter/feed for pipeline monitoring. No write permissions. |
| `paigeAgent.js` | Paige | Content creation — writes posts for GBP, Facebook, LinkedIn; queues to `pending_comments` for approval. LinkedIn Page posts queue to `pending_comments` (`status = 'pending'`) for human approval before publishing. Does NOT call Buffer directly. Buffer is called by `publishPipeline.js` on approval. Includes anti-repetition logic (pulls last 30 posts per channel, injects avoid-list into prompt) and a 3-dimension quality scoring step (specificity, originality, hook_strength). Regenerates once if total score < 21/30. Logs scores to agent_log for Max reporting. |
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

## Clients

| Client ID | Client | Contact | Location | Notes |
|---|---|---|---|---|
| 1 | Pulseforge | jacob@gopulseforge.com | Manchester NH | Existing NH pipeline. All pre-migration data backfilled to `client_id=1`. |
| 2 | MSHI | mshomeinnovations@gmail.com | Charleston WV | Mountain State Home Innovations. Owners: Brad Hudson & Dustin Allison. License: WV065578. Avg job: $10k-$25k. Sequence: `home_renovation`. Sender: Brad & Dustin. Max briefing: 8AM EST → mshomeinnovations@gmail.com. Pending: website, Facebook page, Riley forwarding. |
| 5 | Pulseforge Nashville | jacob@gopulseforge.com | Nashville TN | Slug `pulseforge-nashville`. Service area `{Nashville}`. Declared verticals: cleaning, restaurant, salon, fitness, home_services, auto, landscaping, med_spa. Sender: Jacob Maynard. Added May 2026. |
| 10 | Anchor Cleaning | — | Manchester NH | Slug `cleaning-co`. **Separate LLC — commercial-cleaning BUYER, not a Pulseforge lead-gen client.** `scoring_profile='cleaning_buyer'` (its own ICP rubric — see Known Architectural Notes). Scout targets law firms + accounting practices in the Greater Manchester NH ring. `enabled_agents=['scout']` only (no Emmett/social until entity name, domain, and sender are set). `service_area` = Manchester ~20mi ring (radius is a default pending Jacob's confirmation). Entity name set to Anchor Cleaning (Jun 2026); service-area radius still TBD. Added Jun 2026. |

Pulseforge Scout markets for William's setter territory: Manchester NH and Charleston WV. Charleston WV runs as `client_id=1` with a 15-prospect cap per vertical for: cleaning, hvac, roofing, auto_repair, dental, salon, fitness, restaurant, landscaping. Setter visibility still requires `icp_score >= 70`.

MSHI Scout customer-channel map (`client_id=2`): Scout plans seed `property_management`, `probate_attorney`, `renovation_lender`, `insurance_restoration`, `home_inspector`, and `listing_agent` across Charleston, South Charleston, St. Albans, Dunbar, Nitro, Cross Lanes, and Hurricane WV. These are customer/referral-partner verticals, not MSHI service types. Google Places query seeds live in `leadgen.js` `CLIENT_SCOUT_PLANS`. Do not add hand-built channels `real_estate_investor`, `property_preservation`, or `bank_reo` to Scout. MSHI Max cron: `POST /cron/max?client_id=2&secret={CRON_SECRET}` at 8:00 AM EST.

Pulseforge Nashville Scout cron targets (`client_id=5`, location `Nashville%20TN`, schedule on Railway):
- `POST /cron/scout?client_id=5&industry=cleaning&location=Nashville%20TN`
- `POST /cron/scout?client_id=5&industry=restaurant&location=Nashville%20TN`
- `POST /cron/scout?client_id=5&industry=salon&location=Nashville%20TN`
- `POST /cron/scout?client_id=5&industry=fitness&location=Nashville%20TN`
- `POST /cron/scout?client_id=5&industry=home_services&location=Nashville%20TN`
- `POST /cron/scout?client_id=5&industry=auto&location=Nashville%20TN`
- `POST /cron/scout?client_id=5&industry=landscaping&location=Nashville%20TN`
- `POST /cron/scout?client_id=5&industry=med_spa&location=Nashville%20TN`

---

## Key Route Groups

Routes are now split across `routes/api.js`, `routes/cron.js`, `routes/webhooks.js`, and `routes/approvals.js`. Only auth, session, and a handful of core routes remain in `server.js`.

- **Auth**: `GET /login`, `POST /login`, `GET /logout` — bcrypt session auth, `requireAuth` middleware gates all `/api/*` and `/dashboard`
- **Webhooks**: `POST /webhooks/brevo` (email tracking), `POST /webhooks/bland` (call transcripts) — in `routes/webhooks.js`
- **Cron**: `POST /cron/:agent` and `GET /cron/:agent` — Railway cron triggers. Protected by `CRON_SECRET` query param. — in `routes/cron.js`
- **Agent run**: `POST /api/run/:agent` — fires any agent on demand from dashboard — in `routes/api.js`
- **Prospects**: `GET /api/prospects`, `GET /api/prospects/:id/touchpoints` — in `routes/api.js`
- **Prospect feed context**: `GET /api/prospects/:id/preview` — compact prospect data for activity feed hover cards. 30s cache. `GET /api/prospects/:id/detail` — full prospect detail including touchpoint history for click modal. — in `routes/api.js`
- **Approvals**: `GET /api/approvals`, `POST /api/approvals/:id` (approve/reject content before publish) — in `routes/approvals.js`
- **Pending comments**: `GET /api/pending-comments`, `POST /api/approve-comment/:id`, `POST /api/reject-comment/:id` — in `routes/approvals.js`
- **Analytics**: `/api/analytics`, `/api/analytics/posts`, `/api/analytics/summary`, `/api/analytics/top-posts`, `/api/analytics/email` — in `routes/api.js`
- **Pipeline view**: `GET /api/pipeline` → full business snapshot across clients, revenue, setters, closers, and agent health. `GET /api/prospect-pipeline` → read-only kanban data for dashboard viewers. — in `routes/api.js`
- **Activity**: `/api/activity`, `/api/activity-panel`, `/api/activity-timeline` — in `routes/api.js`
- **Dashboard UI**: `GET /dashboard` → serves `public/dashboard.html` with authenticated live data only — in `server.js`
- **Setter dashboard**: `GET /setter` → authenticated setter UI (queue, pipeline, activity log, metrics strip, Scout feed). Requires setter or admin role. — in `routes/setter.js`
- **Closer dashboard**: `GET /closer` → authenticated closer UI (pipeline, commission tracker, metrics strip). `requireRole('admin', 'manager', 'closer')` — in `routes/closer.js`
- **Setter API (read-only)**: `GET /api/setter/metrics`, `GET /api/setter/feed` — consumed by Max for pipeline monitoring. No write access. `PATCH /api/setter/leads/:id/notes`, `PATCH /api/setter/leads/:id/callback`, `PATCH /api/setter/leads/:id/hot`, `POST /api/setter/leads/:id/quick-log-call`, `POST /api/setter/leads/:id/enrich-phone`, `GET /api/setter/stats/today` — in `routes/setter.js`
- **Brevo warm signal**: Brevo POSTs email events here → Riley logs touchpoints and upgrades cold→warm automatically — in `routes/webhooks.js`

---

## Database Schema (Key Tables)

| Table | What it holds |
|---|---|
| `clients` | Master client registry. All agents and data tables reference `client_id`. `client_id=1` = Pulseforge (NH). `client_id=2` = MSHI (WV). Stores all per-client config: brand voice, sequences, service area, agent settings. |
| `companies` | Scraped companies: name, industry, size, location, website, icp_score, tech_stack, `client_id` |
| `prospects` | Individual contacts: name, email, phone, job_title, linkedin_url, icp_score, status (cold/warm/hot), do_not_contact, last_contacted_at, vertical (TEXT — populated by Scout at insert time based on CONFIG.industry), service_area_match, setter_status (enum: new \| contacted \| follow_up \| booked \| dead), setter_visible (boolean — true = qualifies for setter queue, set by setterHandoffAgent), notes (text — setter scratchpad, auto-saved on blur), callback_at (timestamptz — scheduled follow-up, shown as "Due Today" queue section), is_hot (boolean — hot lead flag, sorts to top of stage), closer_id (FK → users), booked_at, closed_at, mrr_value, close_notes, closer_status, `client_id` |
| `activity_log` | Setter contact log: id, lead_id (FK → prospects), action_type (call \| email \| text), notes, created_at, setter_id, `client_id` |
| `users` | Auth accounts: id, name, email, password_hash, role (admin \| manager \| viewer \| setter \| closer \| sales), active, created_at, last_login_at |
| `touchpoints` | Every agent action: channel, action_type, content_summary, outcome, sentiment, external_ref, `client_id` |
| `agent_log` | Audit trail for every agent run: agent_name, action, prospect_id, payload, status, error_msg, duration_ms, `client_id` |
| `agent_actions` | Actionable items deposited by agents for dashboard review: id, created_by, action_type, title, description, payload, status, executed_at, result, created_at, `client_id` — used by Max and Riley |
| `pending_comments` | Content queued for human approval before publish: post_content, comment, post_url, channel, status, `client_id` |
| `post_analytics` | Published post performance: platform, post_id, impressions, clicks, etc., `client_id` |
| `email_performance` | Email outreach performance per subject line / sequence step / vertical: sends, opens, clicks, replies, bounces, open_rate, reply_rate, `client_id`. Upserted by `utils/emailPerformance.js` (Emmett on send, Brevo webhook on event). |
| `commissions` | Closer commission records: closer_id, prospect_id, mrr_amount, commission_rate (0.15 default), commission_amt (generated), status, closed_at, paid_at, `client_id` |
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
Session-based multi-user auth. `POST /login` accepts email + password, checks against users table, stores { id, name, email, role } in session. `requireAuth` middleware gates all protected routes. `requireRole(...roles)` enforces per-route role access. Roles: admin (full access), manager (full access except user management), viewer (dashboard read-only: agents, activity, pipeline, analytics), setter (/setter only), closer (/closer only), sales (/sales only). Admin UI at `/admin/users` for creating and managing accounts. Falls back to `DASHBOARD_PASSWORD` env var if users table is empty.

## Team Roster

| Name | Role | Email | Notes |
|---|---|---|---|
| Levi TBD | closer | TBD | 15% MRR commission. Create active closer account in `/admin/users` once last name/email are confirmed. |

---

## Approval Flow
Agents that generate content (Paige, Link, Faye, Vera) do NOT post directly. They write to `pending_comments` with `status = 'pending'`. This includes LinkedIn Page posts — Paige queues them, Buffer publishes them only after dashboard approval via `publishPipeline.js`. The dashboard surfaces these under the Approvals tab. This is intentional — nothing goes public without human sign-off.

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
- **Charleston WV Pulseforge Scout market (added May 2026)** — Charleston WV is part of William's setter territory for `client_id=1` Pulseforge. Scout runs are capped at 15 prospects per vertical and staggered across cleaning, hvac, roofing, auto_repair, dental, salon, fitness, restaurant, and landscaping. The active setter threshold remains `icp_score >= 70`.
- **Per-client ICP scoring profiles (added Jun 2026)** — `clients.scoring_profile` selects which ICP rubric Scout's `scoreLead()` applies. NULL = the default Pulseforge lead-gen rubric (vertical/location/contact/web/size, plus MSHI `client_id=2` boosts and probate weighting). `'cleaning_buyer'` = a fully separate rubric (`scoreCleaningLead` in `leadgen.js`) for the **cleaning company (`client_id=10`)**, which scores commercial-cleaning BUYERS, not lead-gen targets. The cleaning rubric drops ALL Pulseforge signals (probate, MSHI, tech/web-presence) and scores: vertical match 0–35 (single-tenant professional-services office — law firm / CPA practice), geography 0–25 (Manchester pilot cluster), reachable decision-maker 0–25 (owner/office-manager name + email/phone), single-tenant signal 0–10 (address-derived heuristic — flagged as a limitation when undetectable), size proxy 0–5 (prefer small offices — flagged when thin), minus a **disqualifier penalty**: a detectable multi-office/national firm (−25) and/or a high-floor/large-suite multi-tenant tower address (−12) actively subtract so the wrong beachhead falls BELOW threshold instead of merely zero-awarding (e.g. a Beacon-National-type lead: 72 → 35). The single-tenant address heuristic is intentionally left as-is for the ambiguous middle (those are filtered at walkthrough booking — do not over-engineer detection). `CONFIG.scoringProfile` is loaded from `CLIENT_CONFIG.scoring_profile` in `run()`. Setter-qualifying threshold for `client_id=10` is **60** (`CLEANING_SETTER_THRESHOLD` / `getSetterThreshold()`), not 70 — pilot needs volume and Jacob is the human final-filter early on; live scores also run lower than the synthetic harness (which assumes full enrichment). To tune the rubric, edit `scoreCleaningLead`; eyeball it offline with `node scripts/testCleaningScout.js`, and report a live pass with `node scripts/cleaningPilotReport.js` (no DB/API needed for the former; DB for the latter).
- **Cleaning client Scout config (`client_id=10`)** — Google Places is **PRIMARY** for this client (SerpAPI is skipped — it underperforms on professional offices and runs dry); see the `placesPrimary` branch in `leadgen.js`'s search loop. Law firms and accounting practices run as **separate passes, one vertical per run** (`CLIENT_SCOUT_PLANS[10]`, verticals `law_firm` and `accounting`). Prospeo title filter is widened to owner OR office manager (plus principal/partner/founder/president) via `getProspeoTitleIncludes()`. Out-of-area leads are culled by the existing `service_area` gate (`clients.service_area` = Manchester pilot cluster: Manchester, Bedford, Goffstown, Hooksett, Londonderry, Auburn — wider ring held for post-pilot; keep in sync with `CLEANING_AREA_CITIES`). **Run the test pass first** — `node leadgen.js --client_id 10 --industry "law firm" --location "Manchester NH" --max 15 | tee /tmp/cleaning-pass.log` then `node scripts/cleaningPilotReport.js --vertical law_firm` — and let Jacob validate quality/scoring/enrichment-hit-rate before any full multi-vertical run. **Partitioning note:** setter users must have `client_id` assigned (e.g. William = 1) so client-10's `setter_visible` leads never surface in another client's setter queue — the setter queue only scopes by client when `session.user.client_id` is set.
- **calBatchAgent.js was present in an earlier version of CLAUDE.md but may have been dropped during a recent edit.** Confirm it is restored in both the File Map and Agent Roster.
- **Multi-user auth (added May 2026)** — replaced single DASHBOARD_PASSWORD with users table. DASHBOARD_PASSWORD fallback remains for empty-DB safety. First setter: William Hernandez (created via admin UI post-deploy). setter_id on activity_log rows references users.id.
- **Phone enrichment on setter dashboard uses Prospeo API (PROSPEO_API_KEY).** Matches the same request pattern as leadgen.js. Logs to agent_log with agent_name = 'setter'.
- **Setter dashboard call logging uses the existing activity_log table (action_type='call').** attempt_count is computed at query time — not stored. Daily goal tracker scopes to setter_id + today's date so it resets automatically at midnight without a cron job.
- **Paige content scoring (added May 2026)** — after generation, a second Claude API call scores each draft on specificity, originality, and hook_strength (max 30). Drafts scoring below 21 are regenerated once. Max reads these scores from agent_log WHERE agent_name='paige' AND action='content_scored' for weekly quality trend reporting.
- **Multi-client architecture (added May 2026)** — all primary data tables carry `client_id` FK to `clients`. Agents accept `client_id` via `POST /api/run/:agent?client_id=1` or `/cron/:agent?client_id=2` and scope DB reads/writes accordingly. Dashboard has a client selector in the header and stores the active client in `req.session.active_client_id`. Existing data backfilled to `client_id=1` (Pulseforge). MSHI is `client_id=2`. To add a new client: INSERT into `clients`, add client-specific cron jobs with `client_id`, and add any needed email sequence to `emmettAgent.js` `SEQUENCES`.
- **Pipeline dashboard (added May 2026)** — combines client status, revenue metrics, setter/closer performance, agent health, and a prospect kanban. Admin/manager can edit pipeline state; viewer can see it read-only. Auto-refreshes every 5 min. McLeod Legal seeded as inactive client (`active=false`).
- **Dark/light mode (added May 2026)** — toggle in nav, persisted via localStorage key `pulseforge-theme`. Applies to dashboard, setter, and closer views. Default: dark.
- **Client dropdown (May 2026)** — scoped to admin/manager roles only. Setter and closer roles cannot switch client context.
- **Agent visibility on client dashboard** — agents greyed out (opacity 0.4) when not configured for active client. Logic based on clients table field population, not hardcoded.
- **Live feed hover/click (added May 2026)** — activity feed items with `prospect_id` show hover preview card (280px, fetches `/preview` endpoint) and click modal (full history, sequence status, action buttons). Warm signal items (opens, clicks, replies) are visually highlighted with amber accent and SIGNAL badge. Feed has `[All]` / `[Warm signals only]` filter toggle.
- **MSHI notes** — no website yet (`clients.website = 'PENDING_BUILD'`). No Facebook page yet; Faye and Paige social posting are disabled for MSHI until Brad/Dustin create the Facebook Business Page, add jacob@gopulseforge.com as editor, and `clients.facebook_url` is updated. Riley does not monitor the MSHI inbox; reply triage is manual until forwarding to jacob@gopulseforge.com or a second Gmail OAuth is configured. Max briefing sends to mshomeinnovations@gmail.com at 8:00 AM EST daily.
- **MSHI website to-do** — separate build task. Follow the Whittaker pattern: GitHub Pages deploy, services for siding/decks/windows/interior renovation/emergency repair, lead form to mshomeinnovations@gmail.com, prominent GBP link, visible license WV065578, before/after gallery, and service area list/map for Kanawha, Putnam, and Cabell Counties.
- **Setter → closer handoff (added May 2026)** — when William marks a prospect as `booked`, the system auto-assigns `closer_id`, writes `booked_at`, creates an `agent_actions` row of type `closer_handoff`, and emails Levi with prospect details and setter notes. Commission tracking lives in the `commissions` table. Rate: 15% MRR. Closer role has access to `/closer` only.
