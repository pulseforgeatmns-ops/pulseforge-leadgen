# SPEC-092 — Content Outcome Intelligence

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Pulseforge |
| **Created** | 2026-08-13 |

> **Numbering note:** The product brief titled this work “SPEC-085”. Repository SPEC-085 is already [Executive Business Brief](SPEC-085_Executive_Business_Brief.md), so this work is numbered **SPEC-092**. The follow-on learning loop is [SPEC-093 Paige Outcome Learning Loop](SPEC-093_Paige_Outcome_Learning_Loop.md) — not [SPEC-086 Growth Conversation](SPEC-086_Growth_Conversation.md).

## Objective

Close the loop from Paige content to observed response and business outcome:

**Content → Intent → Publication → Observed Response → Business Outcome**

Operators can record publication details, quantitative performance snapshots, downstream business outcomes, and qualitative observations; retrieve complete history; run deterministic comparisons; and expose records to existing intelligence consumers (Max). No LinkedIn API, no autonomous publishing, no Paige strategy mutation.

## Vision References

- [SPEC-013 Outcome Intelligence](SPEC-013_Outcome_Intelligence.md) / ADR-008
- [SPEC-036 Outcome Intelligence (campaign)](SPEC-036_Outcome_Intelligence.md) / ADR-023
- [Intelligence Architecture](../vision/Intelligence_Architecture.md)
- ADR-045 Evidence Before Reasoning

## Problem

Pulseforge creates content through Paige but has no durable feedback loop connecting published content to what happened afterward. Attention metrics and business outcomes are not preserved as evidence for future reasoning.

## Scope

- Content publication records linked to Paige artifacts (`pending_comments` id) or manual keys
- Immutable performance snapshots (partial metrics allowed)
- Business outcomes with attribution (`direct|likely|possible|unknown`)
- Qualitative signals (observations, not conclusions)
- Soft evidence / person / company / interaction references
- Retrieval + timeline + deterministic comparison APIs
- Minimal operator capture UI (`/content-outcomes`)
- CLI using the same service
- Tenant isolation via `client_id` / `tenant_id`
- Optional knowledge dual-write of operational events (non-blocking)

## Out of Scope

- LinkedIn API / OAuth / automated metric polling
- Automated comment or DM ingestion
- Sentiment-analysis subsystem / ML scoring / predictive virality
- Autonomous content strategy or publishing
- New Paige agent or analytics dashboard product
- Cross-platform social management
- Content recommendations ([SPEC-093 Paige Outcome Learning Loop](SPEC-093_Paige_Outcome_Learning_Loop.md))

## Dependencies

- SPEC-013 / SPEC-036 outcome patterns
- Existing Paige artifacts (`pending_comments`)
- Auth / client scoping (`requireAuth`, `requireRole`, `getRequestClientId`)
- Optional `utils/knowledgeDualWrite`

## Architecture

Thin extension of Outcome Intelligence — not a new analytics subsystem.

```text
Paige artifact (pending_comments / manual key)
        ↓
content_publications
        ↓
   ┌────┼────────────────┐
   ↓    ↓                ↓
snapshots  business    qualitative
           outcomes    signals
        ↓
services/contentOutcomeIntelligence.js
        ↓
API / CLI / UI  →  toIntelligencePayload() for Max consumers
```

Core principle: **attention is not the outcome**. No vanity composite score.

## Data Model

Tables (migration `2026-08-13-content-outcome-intelligence.sql`):

- `content_publications`
- `content_performance_snapshots` (immutable; no `updated_at`)
- `content_business_outcomes`
- `content_qualitative_signals`

All scoped by `client_id` + `tenant_id` (`String(client_id)` for intelligence packages). Soft TEXT refs only — no hard CRM FKs.

## Implementation Plan

1. Spec + migration + service (memory + Postgres stores)
2. Routes + CLI + operator UI
3. Tests (tenant isolation, snapshots, outcomes, comparison, SPEC-013 safety)
4. Docs (README, CURRENT_STATE, CHANGELOG)

## Migration Strategy

Forward SQL + rollback SQL. Additive. Apply on Railway before production capture. Manual LinkedIn backfill after deploy (operator-driven; not automated).

## Testing

- `test/contentOutcomeIntelligence.test.js`
- `test/contentOutcomeRoutes.test.js`
- `test/contentOutcomeCli.test.js`

## Acceptance Criteria

- [x] Paige content artifact can be represented as a published content instance
- [x] Publication intent/objective can be recorded
- [x] Multiple performance snapshots can be stored (immutable)
- [x] Business outcomes can be associated with attribution
- [x] Qualitative observations can be recorded
- [x] Evidence / Person / Company / Interaction soft refs supported
- [x] Complete publication outcome history can be retrieved
- [x] Deterministic comparison across posts works (no vanity score)
- [x] Tenant isolation enforced
- [x] Intelligence payload available for Max consumers
- [x] No autonomous Paige behavior / no LinkedIn API dependency
- [x] Tests pass

## Future Work

- Optional Postgres integration suite behind env flag
- Dashboard deep-link from Content Performance section
- Structured Evidence Laboratory attachment UX

**SPEC-092 records reality. [SPEC-093](SPEC-093_Paige_Outcome_Learning_Loop.md) reasons over it.**
