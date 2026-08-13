# SPEC-092 — Content Outcome Intelligence

| Field | Value |
|---|---|
| **Status** | Implemented (thin slice) |
| **Priority** | High |
| **Depends on** | SPEC-013 Outcome Intelligence, Paige content artifacts |
| **Draft label** | Originally drafted as SPEC-085; renumbered because SPEC-085/086 were already assigned |
| **Next** | SPEC-093 (deferred) Paige Outcome Learning Loop — draft referred to as SPEC-086 |

## Purpose

Pulseforge can create content through Paige, but previously had no durable feedback loop connecting published content to what happened afterward.

This spec closes that loop:

```text
Content → Intent → Publication → Observed Response → Business Outcome
```

It extends SPEC-013 Outcome Intelligence rather than introducing a separate Paige analytics system.

**Attention is not the outcome.** Impressions and business outcomes remain distinct. No vanity composite score.

## Thin-slice scope

Operator can:

1. Identify a published Paige content artifact (`pending_comments.id`)
2. Record publication details
3. Record quantitative performance snapshots (immutable, partial OK)
4. Record downstream business outcomes with attribution
5. Record qualitative observations
6. Retrieve complete outcome history
7. Compare content outcomes deterministically
8. Expose records to existing intelligence consumers (`/api/v1/content-outcomes/intelligence`)

Manual input is sufficient. No LinkedIn API.

## Explicitly out of scope

- LinkedIn API / OAuth / automated polling
- Autonomous publishing or Paige strategy mutation
- New analytics warehouse / agent / event bus
- Content recommendations (belongs to the learning-loop follow-on)

## Data model

| Table | Role |
|---|---|
| `content_publications` | One publish instance of a Paige artifact |
| `content_performance_snapshots` | Immutable metric observations over time |
| `content_business_outcomes` | Downstream business results + attribution |
| `content_qualitative_signals` | Non-numeric observations |

Tenant isolation via `tenant_id` (stringified `client_id`). Soft refs to Evidence / Person / Company / Interaction — no CRM duplication.

Business outcomes may optionally set `canonical_outcome_id` by recording into the SPEC-013 Outcome Engine (evaluate-only).

## APIs

```text
POST /api/content-publications
GET  /api/content-publications/:id
POST /api/content-publications/:id/performance
POST /api/content-publications/:id/outcomes
POST /api/content-publications/:id/signals
GET  /api/content-publications/:id/outcomes
GET  /api/content-outcomes
GET  /api/content-outcomes/compare
GET  /api/v1/content-outcomes/intelligence
GET  /content-outcomes   # minimal capture UI
```

## CLI

```text
npm run content:outcome -- publish|performance|add-outcome|add-signal|show|list|compare
```

## Files

- `services/contentOutcomeIntelligence.js`
- `routes/contentOutcomeIntelligence.js`
- `scripts/contentOutcome.js`
- `public/content-outcomes.html`
- `migrations/2026-08-13-content-outcome-intelligence.sql`
- `test/contentOutcomeIntelligence.test.js`
- `test/contentOutcomeCli.test.js`

## Guardrails

- No `post_score = impressions + reactions + …`
- No automatic causation / attribution inflation
- No autonomous Paige behavior changes
- Existing approval requirements unchanged
- Tenant isolation enforced on every read/write

## Acceptance

- [x] Paige artifact → publication
- [x] Objective / intent recorded
- [x] Multiple immutable performance snapshots
- [x] Business outcomes + attribution
- [x] Qualitative signals
- [x] Evidence / entity soft references
- [x] Full history retrieval + timeline
- [x] Deterministic comparison
- [x] Tenant isolation
- [x] Intelligence accessor for Max consumers
- [x] No LinkedIn API / no Paige strategy mutation
- [x] Tests + migration

**SPEC-092 records reality. The follow-on learning loop reasons over it.**
