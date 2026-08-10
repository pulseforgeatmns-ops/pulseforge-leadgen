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
- If Google Places returns `REQUEST_DENIED` (or similar), Scout returns an **operator-facing setup checklist** (Railway `GOOGLE_PLACES_KEY`, Places API enablement, billing, key restrictions). SerpAPI / Custom Search fallback is **not wired** on this path and is marked unavailable. The work request stays **retryable**.

## APIs

- `GET /api/v1/scout/work-requests/:id`
- `GET /api/v1/scout/handoffs/:handoffId`
- `POST /api/v1/scout/work-requests/:id/execute`
- `POST /api/v1/scout/work-requests/:id/approve-results`

## Acceptance

- [x] Approved Scout handoff creates/runs Scout sourcing when tooling is available
- [x] Scout returns real prospects with source URLs (no placeholders)
- [x] Failure is explicit and preserves the work request
- [x] Operator must approve results before any downstream use
- [x] No CRM writes, outreach copy, sends, or account changes

## Tests

- `node --test test/scoutPublicSourcing.test.js test/scoutHandoff.test.js`
