# Pulseforge Lead Gen App — Architecture Reference

## What This Is
An AI-powered lead generation and outreach CRM for Pulseforge. It scrapes leads, scores them, runs multi-channel outreach campaigns via a team of named AI agents, and surfaces everything through an authenticated Express dashboard. Deployed on Railway at `node server.js`.

---

## File Map

| File | Purpose |
|---|---|
| `server.js` | All Express routes (~1,988 lines). Entry point for the app. See route map below. |
| `leadgen.js` | CLI tool — scrapes leads from Google Custom Search + Prospeo, scores them, exports CSV/Sheets. Run via `node leadgen.js --industry "cleaning" --location "Manchester NH"` |
| `db.js` | Creates and exports the shared `pg.Pool` using `DATABASE_URL`. All agents and server import from here. Never call `pool.end()` in agents — it closes the shared pool. |
| `dbClient.js` | Helper functions wrapping common DB operations: `checkDNC`, `logTouchpoint`, `logAgentAction`, `addProspect`, `addCompany`, `savePendingComment`, etc. |
| `railway.json` | Railway deploy config: `node server.js` |
| `utils/publishPipeline.js` | Publishes approved content to Google Business Profile, Facebook Page, LinkedIn Page (via Buffer GraphQL), and handles comment publishing for Faye/Link. |
| `utils/blogPublisher.js` | GitHub-based blog post publisher. |
| `utils/demoData.js` | Generates fake live-feed data for the unauthenticated `/demo` route. |

---

## Agent Roster

Each agent is a standalone JS module that reads from the DB and writes results back. All are triggered via `POST /api/run/:agent` (dashboard button) or `POST/GET /cron/:agent` (Railway cron jobs).

| File | Agent Name | Role |
|---|---|---|
| `maxAgent.js` | Max | Daily manager briefing — pulls system snapshot, generates AI summary of pipeline health |
| `paigeAgent.js` | Paige | Content creation — writes posts for GBP, Facebook, LinkedIn; queues to `pending_comments` for approval |
| `emmettAgent.js` | Emmett | Email outreach — writes and sends cold email sequences via Brevo API |
| `rileyAgent.js` | Riley | Receptionist — auto-responds to inbound inquiries; also processes Brevo email event webhooks to update warm/DNC status |
| `rexAgent.js` | Rex | Reporting — weekly performance summaries |
| `samAgent.js` | Sam | SMS outreach via Twilio |
| `ivyAgent.js` | Ivy | Instagram engagement |
| `linkedinAgent.js` | Link | LinkedIn — monitors local biz owner posts, drafts comments for approval |
| `facebookAgent.js` | Faye | Facebook — monitors local group posts, drafts comments for approval |
| `veraAgent.js` | Vera | Review monitoring — watches GBP reviews, drafts response copy |
| `calAgent.js` | Cal | Google Calendar — books discovery calls |
| `analyticsAgent.js` | Analytics | Pulls post performance data, writes back to `post_analytics` table |
| `sketchAgent.js` | Sketch | Site mockup generator using Puppeteer |
| `pennyAgent.js` | Penny | (check file for current role) |

---

## Key Route Groups in server.js

- **Auth**: `GET /login`, `POST /login`, `GET /logout` — bcrypt session auth, `requireAuth` middleware gates all `/api/*` and `/dashboard`
- **Webhooks**: `POST /webhooks/brevo` (email tracking), `POST /webhooks/bland` (call transcripts)
- **Cron**: `POST /cron/:agent` and `GET /cron/:agent` — Railway cron triggers. Protected by `CRON_SECRET` query param.
- **Agent run**: `POST /api/run/:agent` — fires any agent on demand from dashboard
- **Prospects**: `GET /api/prospects`, `GET /api/prospects/:id/touchpoints`
- **Approvals**: `GET /api/approvals`, `POST /api/approvals/:id` (approve/reject content before publish)
- **Pending comments**: `GET /api/pending-comments`, `POST /api/approve-comment/:id`, `POST /api/reject-comment/:id`
- **Analytics**: `/api/analytics`, `/api/analytics/posts`, `/api/analytics/summary`, `/api/analytics/top-posts`, `/api/analytics/email`
- **Activity**: `/api/activity`, `/api/activity-panel`, `/api/activity-timeline`
- **Dashboard UI**: `GET /dashboard` → serves `public/dashboard.html`; `GET /demo` → unauthenticated demo mode
- **Brevo warm signal**: Brevo POSTs email events here → Riley logs touchpoints and upgrades cold→warm automatically

---

## Database Schema (Key Tables)

| Table | What it holds |
|---|---|
| `companies` | Scraped companies: name, industry, size, location, website, icp_score, tech_stack |
| `prospects` | Individual contacts: name, email, phone, job_title, linkedin_url, icp_score, status (cold/warm/hot), do_not_contact, last_contacted_at |
| `touchpoints` | Every agent action: channel, action_type, content_summary, outcome, sentiment, external_ref |
| `agent_log` | Audit trail for every agent run: agent_name, action, prospect_id, payload, status, error_msg, duration_ms |
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
- `PROSPEO_API_KEY` — Contact data enrichment
- `GOOGLE_SHEET_ID` — Sheet to write leads into

**Email / Outreach**
- `BREVO_API_KEY` — Email sending + webhook tracking (Emmett)
- `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_PHONE_NUMBER` — SMS (Sam)
- `BLAND_API_KEY` — AI phone calls

**Social Publishing**
- `FACEBOOK_PAGE_ACCESS_TOKEN`, `FACEBOOK_PAGE_ID`, `FACEBOOK_APP_SECRET` — Facebook Page posts (Faye/Paige)
- `FACEBOOK_SESSION` — Puppeteer session cookie (stored as env var on Railway, never commit `facebook_session.json`)
- `LINKEDIN_SESSION` — Puppeteer session cookie (same — never commit `linkedin_session.json`)
- `BUFFER_ACCESS_TOKEN`, `BUFFER_CHANNEL_ID` — LinkedIn Page publishing via Buffer GraphQL
- `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `GOOGLE_REFRESH_TOKEN` — Google Business Profile (Vera/Paige)
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
Session-based. `POST /login` checks `DASHBOARD_PASSWORD` env var via bcrypt. `requireAuth` middleware redirects unauthenticated requests to `/login`. The `/demo` and `/webhooks/*` and `/cron/*` routes are intentionally unauthenticated (cron is guarded by `CRON_SECRET` instead).

---

## Approval Flow
Agents that generate content (Paige, Link, Faye, Vera) do NOT post directly. They write to `pending_comments` with `status = 'pending'`. The dashboard surfaces these under the Approvals tab. `POST /api/approve-comment/:id` triggers `publishPipeline.js` to actually push to the platform. This is intentional — nothing goes public without human sign-off.

---

## Known Architectural Notes
- **`server.js` is large (~1,988 lines)** — next logical refactor is splitting into route files (`routes/prospects.js`, `routes/approvals.js`, `routes/analytics.js`, `routes/cron.js`, `routes/webhooks.js`). Don't add more routes directly to server.js without considering this.
- **Duplicate middleware** — `express.json()` and `express.urlencoded()` are registered twice (lines ~80-84). Harmless but worth cleaning next time that section is touched.
- **Duplicate route** — `GET /api/agent-status` is registered twice (lines 609 and 655). The second one wins. Fix when touching that route.
- **Sensitive JSON files** — `gmail_token.json`, `gmail_credentials.json`, `facebook_session.json`, `linkedin_session.json` live locally for dev but are injected as env vars on Railway. They are in `.gitignore` — never remove them from there.
- **`pool.end()` is forbidden in agents** — the pool is shared across the server process. Calling `pool.end()` in an agent kills all subsequent DB connections. Keep cleanup out of agent files.
