# SPEC-092 — Content Outcome Intelligence

| Field | Value |
|---|---|
| **Status** | Implemented (thin slice) |
| **Planning draft** | SPEC-085 (number taken in-repo by Executive Business Brief) |
| **Depends on** | SPEC-013 Outcome Intelligence, Paige `pending_comments`, knowledge Evidence/Outcome nodes |
| **Explicitly out of scope** | LinkedIn API, autonomous publishing, new analytics subsystem, Paige strategy mutation (→ future learning loop) |

## Purpose

Close the loop:

**Content → Intent → Publication → Observed Response → Business Outcome**

Pulseforge can create content through Paige but previously had no durable feedback connecting published content to what happened afterward. This spec records that evidence for Max and future Paige learning — without becoming a social-media analytics product.

## Core principle

**Attention is not the outcome.** Performance metrics and business outcomes remain distinct. No vanity composite score.

## Model

```text
ContentArtifact  (Paige pending_comment or manual backfill)
    ↓
ContentPublication
    ↓
ContentOutcome
    ├── PerformanceSnapshots  (immutable, time-series)
    ├── BusinessOutcomes      (attribution-aware)
    └── QualitativeSignals    (observations, not conclusions)
```

## Integration with SPEC-013

- Reuses tenant isolation (`tenantId = String(clientId)`).
- Optionally records evaluate-only entries in the in-process OutcomeEngine (`meta.kind = content_publication | content_business_outcome`).
- Soft-links Evidence via `evidence_id`; operator vs external evidence kinds are distinguishable.
- Business outcomes may set `canonical_outcome_id` when OutcomeEngine dual-write succeeds.
- **Never** mutates Paige generation, confidence, or strategy.

## Surfaces

| Surface | Path |
|---|---|
| Operator UI | `GET /content-outcome` |
| APIs | `/api/content-publications`, `/api/content-outcomes`, `/api/v1/content-outcomes/recent` |
| CLI | `npm run content:outcome -- <command>` |
| Max/Mira read | `miraContext.content_outcomes` (optional table; empty until migration) |

## Migration

`migrations/2026-08-13-content-outcome-intelligence.sql`

## Guardrails

- No LinkedIn API dependency
- No automatic causation
- No autonomous posting changes
- No strategy mutation
- Tenant isolation on every read/write

## Next

A future learning-loop spec may allow Paige to reason over these records under operator control. SPEC-092 only records reality.
