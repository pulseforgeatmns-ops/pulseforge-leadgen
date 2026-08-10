# SPEC-077 — Scout Public-Source Prospect Sourcing Execution

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v1.x |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-08-10 |
| **Depends on** | Max → Scout Handoff Brief / work request (PR #207), SPEC-024 Places provider, SPEC-060 acquisition independence |
| **Consumed by** | Client Intelligence campaign planning, Scout work-request APIs |

## Objective

When Max creates and the operator approves a Scout prospect-sourcing handoff, Scout inspects **public sources** and returns an evidenced prospect batch for operator review.

## Scope

1. Read approved Scout work request by `workRequestId` / `handoffId`
2. Execute public-source sourcing for the approved market and criteria
3. Return **15–25** candidate prospects
4. Each prospect includes:
   - company / property manager name
   - website / source URL
   - location
   - segment / subtype
   - fit rationale
   - disqualifying risk or uncertainty
   - suggested contact role
   - confidence level
5. Mark work request status: `queued` → `in_progress` → `completed` | `failed` | `needs_operator_review`
6. Store results as **review-only** Scout output
7. Do **not** write to CRM
8. Do **not** create outreach copy
9. Do **not** send messages
10. Do **not** change accounts / DNS / GBP / social / tracking

## Out of scope

- Composer / CRM import of candidates before operator approval
- Outreach sequence generation
- Enrichment beyond public listing signals
- Private / gated scrapes

## Architecture

```text
Operator: "Hand this brief to Scout"
        ↓
scoutHandoff.handBriefToScout
  → work request queued (status=queued)
  → persisted in scoutWorkRequestStore
        ↓
executeScoutWorkRequest(workRequestId | handoffId)
  → status=in_progress
  → scoutPublicSourcing (Google Places / injected search)
  → filterValidScoutCandidates (source URL required)
  → candidate batch (review_only)
  → status=completed | failed
        ↓
Operator reviews → approveScoutResults
  (still no CRM / outreach)
```

### Modules

| Module | Role |
|---|---|
| `services/scoutPublicSourcing.js` | Public-source search + candidate mapping |
| `services/scoutWorkRequestStore.js` | In-memory work-request / handoff store |
| `services/scoutHandoff.js` | Queue + execute + approve lifecycle |
| `routes/clientIntelligence.js` | GET/POST Scout work-request APIs |

### Wiring

- Sourcing is **wired** when `GOOGLE_PLACES_KEY` is set, or when `publicSearchFn` / `searchProvider` / `scoutSourcingFn` is injected.
- Without tooling, handoff still creates a work request and returns the clear not-wired boundary (no placeholders).

## APIs

- `GET /api/v1/scout/work-requests/:id`
- `GET /api/v1/scout/handoffs/:handoffId`
- `POST /api/v1/scout/work-requests/:id/execute`
- `POST /api/v1/scout/work-requests/:id/approve-results`
- `GET /api/v1/scout/places-diagnostic` — safe Places connectivity probe (operator auth)
- `GET|POST /cron/scout-places-diagnostic?secret=CRON_SECRET` — same probe on Railway runtime

## Places diagnostic

When Scout fails with `google_places_status_REQUEST_DENIED`, run:

```bash
npm run scout:places:diagnostic -- --json
# or against Railway:
curl -sS "$APP_URL/cron/scout-places-diagnostic?secret=$CRON_SECRET"
```

Reports endpoint family (legacy Text Search), host/path only, HTTP status, Google status / error_message, key fingerprint (first4…last4), key presence, and Railway service/environment. Never logs the full key; never writes CRM/outreach/placeholders. Optionally compares Places API (New) to detect New-only keys.

**Deploy note:** production must be on a commit that includes this route. Until PR #211 is merged/deployed, `/cron/scout-places-diagnostic` falls through to `/cron/:agent` and returns `Unknown agent: scout-places-diagnostic`. The agent is also registered in `CRON_SPECIAL_HANDLERS` and intercepted in the `:agent` dispatcher so that failure mode cannot recur after deploy.

## Quality gates (Anchor / NH property-manager campaigns)

Scout public sourcing applies hard quality gates before a batch is reviewable:

1. Market is interpreted as **New Hampshire, USA** (never UK Greater Manchester)
2. Every Places query includes `{town} NH` or `New Hampshire` and Text Search is biased with `region=us`
3. Priority towns: Bedford, Hooksett, Londonderry, Auburn, Goffstown; Manchester NH is nearby/fill (`review_required`) unless needed to fill the batch
4. Concord / Derry / other NH towns outside the cluster are `review_required` unless explicitly allowed — never `accepted`
5. Hard-reject UK / non-US geography, including bare “Manchester” without an NH/USA token and UK place markers (Salford, Stockport, Deansgate, +44, `.co.uk`)
6. Hard-reject cleaning / maid / housekeeping / janitorial / carpet-cleaning competitors when targeting property managers
7. Hard-reject large institutional/national firms unless `allowInstitutional` is set on the work request
8. `confidence=high` only when source URL + NH location + property-management fit + reachable contact signal are all present on a priority town and no exclusion risk remains
9. Fit rationale must be source-specific (not copied inclusion/exclusion criteria)
10. Contact roles are labeled `Suggested contact role: …` unless a verified named+title contact is present — never overclaim `Owner / decision-maker`
11. Results are grouped as `accepted` / `review_required` / `rejected` with `rejectionReason` on rejected rows
12. Injected `scoutSourcingFn` paths are re-gated — they cannot bypass quality rules
13. Completed handoffs and CIE brief/UI replace draft-brief guardrail text with completed-result guardrails

## Acceptance

- [x] Approved Scout handoff creates/runs Scout sourcing when tooling is available
- [x] Scout returns real prospects with source URLs (no placeholders)
- [x] Failure is explicit and preserves the work request
- [x] Operator must approve results before any downstream use
- [x] No CRM writes, outreach copy, sends, or account changes
- [x] Places diagnostic probes Scout's legacy Text Search path and reports status without logging the full key
- [x] Quality gates reject UK / cleaning / institutional misses and block undeserved high confidence

## Tests

- `node --test test/scoutQualityGate.test.js test/scoutPublicSourcing.test.js test/scoutHandoff.test.js test/scoutPlacesDiagnostic.test.js test/scoutPlacesDiagnosticCronRoute.test.js`
