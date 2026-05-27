# AGENT_RULES.md
## Pulseforge Operational Rules & Known Failure Modes
*Hard-won corrections from live system operation. Reference this before modifying any agent.*

---

## SCOUT (leadgen.js)

### Data Sources
- Primary: SerpAPI (`SERPAPI_KEY`) — good for service businesses, weak for retail/wellness/auto
- Secondary: Google Places API (`GOOGLE_PLACES_KEY`) — Phase 4 addition, better for location-based brick-and-mortar
- Email enrichment: Hunter.io domain search (`HUNTER_API_KEY`) — returns full unmasked emails on `/v2/domain-search`
- If `GOOGLE_PLACES_KEY` is not set, skip Places search silently — do not break the existing SerpAPI flow

### Validation Rules
Scout MUST reject a prospect before saving if the name:
- Contains a street address pattern (digits + Rd, St, Ave, Blvd, Dr, Route, Unit, Suite, NH + zip)
- Contains job-related keywords: "Jobs", "Employment", "Hiring", "Career"
- Is a single generic word: "Sitemap", "Home", "Contact", "Index"
- Is shorter than 4 characters
- Starts with "CONTACT:"
- Contains a year pattern (2024, 2025, 2026)

### Domain Blacklist
Always skip: facebook.com, instagram.com, twitter.com, linkedin.com, youtube.com, yelp.com, yellowpages.com, bbb.org, indeed.com, glassdoor.com, ziprecruiter.com, thumbtack.com, homeadvisor.com, angieslist.com, and all job boards/directories.

### Schema
- Prospects table uses `vertical` not `industry` — this applies everywhere in the codebase
- Company name lives in the `companies` table — join via `company_id`, never assume a `company` column on `prospects`
- `source` field should be set to `'serpapi'` or `'google_places'` to track performance by source

### Known Issues Fixed
- Google Custom Search API (persistent 403 errors) — replaced with SerpAPI permanently
- Prospeo deprecated endpoint — replaced with Hunter.io
- Agent log entries were using inconsistent names: standardized to `'scout'` everywhere

### Email Validation (May 27 2026)
- Email validation rejects: file-extension domains, placeholder domains like `godaddy.com`, invalid `@` count, under 6 chars, or any string containing spaces
- Invalid emails are nulled but the prospect record is kept

### Saturation Tracking (scout_queue)
- `scout_queue` table now tracks saturation per client / vertical / location
- Saturation thresholds: auto 50, cleaning 50, restaurant 60, fitness 40, salon 40, med_spa 30, landscaping 30, property_management 40, home_services 30, auto_repair 50, default 40
- `scout_queue` must be populated with all active verticals before each run
- `prospect_count` must update after each run
- `saturated = true` and `status = 'saturated'` must be set together — never one without the other

---

## EMMETT (emmettAgent.js)

### Sending Rules
- Sending window: Tuesday–Thursday, 9am–2pm ET only
- If outside window, exit early and log `skipped_outside_window` to agent_log — do not send
- Daily cap is enforced at the DB level: query `agent_log` for `action = 'email_sent'` where `DATE(ran_at) = CURRENT_DATE` before selecting prospects — subtract from cap to get remaining capacity for the run
- Do NOT rely solely on in-memory `dailyLimit` counter

### Per-Client Caps
```javascript
const clientConfig = {
  1: { dailyCap: 100, verticalCap: 15 },         // Manchester NH
  2: { dailyCap: 40,  verticalCap: 10 },          // Charleston WV
  5: { dailyCap: 30,  verticalCap: 8,             // Nashville TN
       ramp: { afterDays: 14, bounceCeiling: 0.03, newDailyCap: 50 } }
}
```

### Nashville Ramp Logic
- After 14 days from first client_id=5 email sent, check bounce rate (bounced / total sent)
- If bounce rate < 3%, automatically switch to 50/day cap
- Log a `cap_ramped` entry to agent_log when this triggers

### Schema
- Use `prospect.vertical` — NOT `prospect.industry` (column does not exist)
- Per-vertical cap logic must reference `vertical` or all prospects fall into "unknown" and the cap never works correctly

### Logging
- Create each send attempt as a `pending` entry in agent_log at start of send
- On success: update status to `completed`
- On failure: update status to `failed`, persist error message to `payload` column
- Never console.log failures only — they must be persisted to DB

### Known Issues Fixed
- `prospect.industry` → `prospect.vertical` (all prospects were falling into "unknown" vertical cap)
- In-memory only daily cap → DB-level cap check added
- All agent_log entries stuck at `pending` → now updated to `completed` or `failed` after each send
- No sending window enforcement in code → Tuesday–Thursday 9am–2pm ET now enforced in emmettAgent.js

### Updates (May 27 2026)
- Best performing subject line format: `business_name — honest question`
- Cleaning, salon, and auto sequences fully rewritten May 27 2026
- `WARM_STEP` is now defined as the cleaning Day 4 fallback
- "Jake" corrected to "Jacob" in all signatures
- Cron runs 4x daily: 9am, 11am, 1pm, 3pm ET via cron-jobs.org
- Email body is NOT logged to agent_log payload — only subject and metadata are persisted
- Zero clicks on cold outreach is expected — reply-only CTA, no links in body

---

## MAX (maxAgent.js)

### Prospect Data Rules
- Max may ONLY reference businesses that exist in the DB snapshot passed into the prompt
- The prompt must include a CRITICAL instruction: only reference companies from the provided snapshot, copy `company_name_with_market` verbatim, say "none" if a section has no qualifying prospects
- After LLM generation, run `verifyDigestProspects()` to validate all mentioned company names against the DB allow-list
- Any company name not found in the allow-list must be stripped and replaced with `[unverified prospect removed]`
- Log all strips to agent_log under action `digest_prospect_validation`

### Market Labels
Every prospect mentioned in the digest must include their market in parentheses:
- client_id = 1 → "Manchester NH"
- client_id = 2 → "Charleston WV"
- client_id = 5 → "Nashville TN"

Apply to: warm signals, top priorities, watch list, recommendations sections.

### Data Sourcing
- `getSystemSnapshot()` fetches all prospect data via real `prospects`/`companies` joins scoped by `client_id`
- The full snapshot must be passed into the LLM prompt as context
- Max has read-only API access — no writes

### Known Issues Fixed
- LLM was hallucinating prospect names (e.g. "TT Hair Salon") not present in DB → validation pass added
- Prospect names were missing market context → market label appended to all prospect references

### Agent Naming Rules (May 27 2026)
- Emmett sends emails, Vera handles GBP reviews only, Riley triages inbound, Paige posts content, Sam sends SMS, Cal calls, Scout scrapes
- Never attribute email open rates or send volume to Vera
- Market labels are required on all prospect mentions: client_id 1 = Manchester NH, client_id 2 = Charleston WV, client_id 5 = Nashville TN

---

## PAIGE (paigeAgent.js)

### Channel Strategy
Each channel has a distinct job — never produce variations of the same angle across channels on the same day:

| Channel | Format | Tone | Length |
|---|---|---|---|
| LinkedIn | Thought leadership, first-person POV, contrarian take | Professional, no hard sell | 150–250 words, max 3 hashtags |
| Google Business | Local proof, search-intent | Short, specific to Manchester NH | 75–100 words max |
| Facebook | Story-first, relatable moment or client scenario | Warm, conversational | 100–175 words |
| Blog | Educational, teaches something useful | Authoritative, Pulseforge as solution | 400–600 words |

If the topic rotation selects the same angle bucket, each channel must express a different facet of it — not the same narrative arc with different wording.

### Topic Rotation
- Maintain a 12-topic rotation bank, cycling by day
- Topics include: behind the scenes, client result story, common mistake, tool explainer, contrarian take, the human side (bartender builds AI agency), before/after workflow, industry-specific pain point, FAQ, Manchester NH local observation, cost of doing nothing, day in the life of an automated system
- Track used angles per client with a 14-day rolling window — do not reuse a hook or premise within that window

### Blog Closers
- Never use engagement-bait questions as closers ("Drop a comment", "what do you think", "let us know below")
- Use the CTA rotation bank (8 distinct closers) seeded by date + channel for deterministic but varied output
- Closers should drive action (contact, audit, walkthrough) not social engagement

### Scoring
- Scoring is out of 30 (10 per dimension: originality, specificity, hook_strength)
- Originality is the chronic weak dimension — the topic rotation and used-angles memory exist specifically to fix this
- Originality below 7 should only occur if the post reuses a hook seen in the last 14 days

### Known Issues Fixed
- All channels producing variations of the same angle → per-channel strategy enforced in prompt
- "Drop a comment — what's the one part of your follow-up process you wish ran itself?" hardcoded closer → removed, replaced with CTA rotation bank
- LLM hallucinating prospect names → validation pass (shared pattern with Max)
- Originality scoring chronically low (4–6) → topic rotation bank + used-angles memory added

### Updates (May 27 2026)
- All content for client_id 1 must be written from Pulseforge's POV — never as the client business
- One post per channel per run maximum — 5 posts total
- Channels: `facebook_page`, `google_business`, `linkedin_page`, `linkedin_personal`, `blog`
- `attempt_count` is now logged to the `content_scored` payload
- Passing threshold: total 24 or above AND all three dimensions 7 or above
- LinkedIn personal channel ID: `6a15b291c687a22dd42a62ad` — first-person Jacob Maynard voice, not Pulseforge brand voice
- MSHI content (client_id 2): Facebook, Google Business, Blog only — no LinkedIn

---

## PROSPECTS

### Status Progression (May 27 2026)
- `cold` → `contacted` on Day 0 send
- `contacted` → `warm` on 2+ opens or a click
- `warm` → `dead` on bounce or unsubscribe
- `contacted` status added to the constraint May 27 2026
- Legacy values migrated: `hot` → `warm`, `booked` → `closed`
- Riley resets `contacted` prospects with no touchpoint in 21 days back to `cold`

### Kanban
- Columns: Cold, Contacted, Warm, Booked, Closed
- Drag and drop updates `prospects.status` via `PATCH /api/prospects/:id/status`

---

## SAM (samAgent.js)

### Warm Signal Notifications (May 27 2026)
- Sam owns all Twilio SMS warm signal notifications — NOT Riley
- Riley deposits `warm_signal` into `agent_actions`; Sam picks it up and sends
- Quiet hours: 8:30am to 10:30pm ET — queue overnight signals and send at 8:30am
- Dedup: one SMS per prospect per day max
- Check `do_not_contact` before sending — log `sms_skipped_dnc` if blocked

---

## GENERAL RULES (all agents)

### Logging
- All agent_log entries must be updated from `pending` to `completed` or `failed` after execution
- Failures must be persisted to DB with error in `payload` — never console.log only
- Agent names in agent_log are standardized: `scout`, `emmett`, `max`, `paige`, `rex`, `riley`, `cal`, `link`, `faye` — no `_agent` suffix

### Schema Reminders
- `vertical` not `industry` on prospects table
- Company name is in `companies` table, joined via `company_id`
- `do_not_contact` flag must be respected by all outreach agents before sending
- `setter_visible` flag controls what appears in the setter dashboard

### Railway / Infrastructure
- All env vars live on Railway — never hardcode
- PORT must come from `process.env.PORT` — never hardcode 3000
- Pool connections must complete before `pool.end()` — use `require.main === module` guards
- Cron secret: `pulseforge-cron-2024`

### Updates (May 27 2026)
- `touchpoints.channel` constraint: email, linkedin, facebook, manual, call, sms
- Dashboard triggers bypass Emmett's sending window — manual triggers always send regardless of time
- Nashville sending window: 9am to 4pm ET (extended from 2pm)
- Viewer role added to user management — read-only access to agents, activity, pipeline, analytics
- GBP API reapplication submitted May 27 2026, case ID 7-5222000041067, expect response by June 6

---

*Last updated: May 2026*
*Update this file whenever a correction is made to any agent in production.*
