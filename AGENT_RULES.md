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

Scout MUST reject an email before saving if it:
- Contains URL-encoded characters such as `%20`
- Uses a generic inbox prefix blocked by Emmett send-time rules

### Domain Blacklist
Always skip: facebook.com, instagram.com, twitter.com, linkedin.com, youtube.com, yelp.com, yellowpages.com, bbb.org, indeed.com, glassdoor.com, ziprecruiter.com, thumbtack.com, homeadvisor.com, angieslist.com, and all job boards/directories.

Also reject surveymonkey.com and surveymonkey.co.uk domains.

### Schema
- Prospects table uses `vertical` not `industry` — this applies everywhere in the codebase
- Company name lives in the `companies` table — join via `company_id`, never assume a `company` column on `prospects`
- `source` field should be set to `'serpapi'` or `'google_places'` to track performance by source

### Known Issues Fixed
- Google Custom Search API (persistent 403 errors) — replaced with SerpAPI permanently
- Prospeo deprecated endpoint — replaced with Hunter.io
- Agent log entries were using inconsistent names: standardized to `'scout'` everywhere
- Anton's Cleaners email manually corrected to `aanton@antons.com`

---

## EMMETT (emmettAgent.js)

### Sending Rules
- Sending window: Tuesday–Thursday, 9am–2pm ET only
- If outside window, exit early and log `skipped_outside_window` to agent_log — do not send
- Daily cap is enforced at the DB level: query `agent_log` for `action = 'email_sent'` where `DATE(ran_at) = CURRENT_DATE` before selecting prospects — subtract from cap to get remaining capacity for the run
- Do NOT rely solely on in-memory `dailyLimit` counter
- Generic inbox email blacklist is enforced at send time and at Scout save time
- Blocked prefixes: `support`, `info`, `admin`, `webmaster`, `contact`, `noreply`, `no-reply`, `hello`, `help`, `sales`, `billing`, `accounts`, `reception`, `office`, `enquiries`, `mail`, `postmaster`, `hostmaster`, `abuse`, `donotreply`, `feedback`, `service`, `team`, `staff`, `general`, `media`, `press`, `marketing`, `jobs`, `careers`, `hr`, `legal`, `privacy`, `security`, `newsletter`, `notifications`, `notify`, `alerts`, `updates`, `news`

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
- `restaurant_b` A/B variant removed from sequences
- `WARM_STEP` defined as cleaning Day 4 fallback
- Jake corrected to Jacob in all signatures

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

### Auto-Execution
- Max runs pre-digest auto-execution before generating the brief via `runAutoExecuteActions()`
- Auto-executes without human approval:
  - Hard bounces in last 24h → `status = 'dead'`, `do_not_contact = true`, logged as `auto_marked_dead`
  - Generic inbox emails (`info`, `contact`, `support`, `office`, `admin`, `hello`) → nulled, logged as `auto_email_nulled`
  - Warm/hot prospects with no Cal call in 7 days → inserted into `cal_queue` priority 1, logged as `auto_queued_cal`
  - 5+ touchpoint cold/contacted prospects with no reply → `cal_queue` priority 2 reason `5_touch_no_reply`, logged as `auto_queued_cal_nurture`
  - Contacted prospects untouched 21+ days → reset to cold, logged as `auto_reset_stale`

### Digest Rules
- Digest is capped at 200 words
- Digest has four sections only: Actions Executed, Exceptions, Pipeline Snapshot, one Recommendation
- Never flag 0% click rate as a problem — reply-only sequences have no links, zero clicks is expected
- Remove from digest: click rate commentary, generic content quality observations, routine bounce mentions, untouched prospect lists

### Known Issues Fixed
- LLM was hallucinating prospect names (e.g. "TT Hair Salon") not present in DB → validation pass added
- Prospect names were missing market context → market label appended to all prospect references

---

## CAL (calAgent.js)

### Queue Rules
- `cal_queue` table exists and Cal pulls pending queue rows first, ordered by `priority` then `created_at`, before selecting normal batch prospects
- After successful dial, Cal marks the `cal_queue` row as completed via `markCalQueueCompleted`
- Missing `cal_queue` table errors are swallowed so Cal works on a fresh DB

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
- `cal_queue` table columns: `prospect_id`, `client_id`, `priority`, `reason`, `created_at`, `status` default `pending`
- Max feeds `cal_queue`; Cal drains it — this is the warm handoff pipeline

### Phone Enrichment
- Phone enrichment job runs daily for prospects with null email and null phone added in the last 7 days via Hunter.io domain search

### Railway / Infrastructure
- All env vars live on Railway — never hardcode
- PORT must come from `process.env.PORT` — never hardcode 3000
- Pool connections must complete before `pool.end()` — use `require.main === module` guards
- Cron secret: `pulseforge-cron-2024`

---

*Last updated: May 2026*
*Update this file whenever a correction is made to any agent in production.*
