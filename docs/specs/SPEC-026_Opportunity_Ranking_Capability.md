# SPEC-026 — Opportunity Ranking Capability

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v1.0.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-022, SPEC-023, SPEC-024, SPEC-030 (Company Intelligence; enrichment stub until live), SPEC-031 (Business Signals / Active set when live), ADR-010, ADR-011, ADR-018 |
| **Consumed by** | Mission Engine, Campaign Builder, Command Deck Operations |

## Objective

Convert enriched prospects into a prioritized work queue that answers one question:

**Who should we contact first?**

Every score is explainable. Ranking references evidence only — no black-box scoring. Operators review before Campaign Builder.

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- [ADR-010](../adr/ADR-010_Mission_Engine.md) — Mission Engine orchestration
- [ADR-011](../adr/ADR-011_Capability_Framework.md) — capabilities as the stable API
- [ADR-003](../adr/ADR-003_Human_Approval.md) — review before outreach
- [SPEC-022](SPEC-022_Mission_Engine_and_Agent_Orchestration.md)
- [SPEC-023](SPEC-023_Capability_Framework.md)
- [SPEC-024](SPEC-024_Prospect_Discovery_Capability.md)
- [SPEC-015A](SPEC-015A_Reasoning_Runtime_Decoupling.md) — optional Reasoning Runtime backing (future)

## Problem

Discovery and enrichment produce candidates, but operators still need a transparent priority queue. A stub sort by discovery confidence is not an opportunity ranking: it does not surface buying signals, decision-maker confidence, personalization angles, risks, or an actionable brief. Without explainable ranking, Campaign Builder cannot be fed a reviewed work queue.

## Scope

- Opportunity Ranking capability (`opportunity_ranking`) replacing the SPEC-023 stub
- Eight explainable ranking factors (weighted 0–100 overall)
- Priority bands: High / Medium / Low
- Per-prospect confidence, top reasons, risks, recommended next action
- Opportunity Brief per ranked prospect
- Review package with operator actions (approve / re-rank / exclude / lock / continue to Campaign Builder)
- Progress stages: Scoring → Briefing → Prioritizing → Completed
- Deterministic scoring from evidence fields only (no LLM required for v1)
- Graceful use of Discovery Profile, enrichment fields, knowledge snapshot, and historical outcomes when present

## Out of Scope

- Live Company Intelligence (SPEC-030) — ranking accepts enrichment stub or live intelligence packages
- Campaign Builder implementation (still stub; consumes ranked outputs)
- Autonomous outreach / send
- Mutating Reasoning Runtime confidence (ranking is a capability adapter; SPEC-015A remains optional backing later)
- Learning-loop weight auto-tuning (SPEC-021 may inform weights later)

## Dependencies

- SPEC-024 Discovery Profiles + prospect evidence / rankingSignals
- SPEC-030 intelligence package fields when available (`email`, `phone`, `contacts`, `buyingSignals`, `enriched`, firmographics) — consumed without modification
- Capability Framework contract (SPEC-023)
- Mission flow: Discovery → Enrichment → **Opportunity Ranking** → Campaign Builder

## Architecture

```text
Discovery Results
Enrichment Results
Discovery Profile
Knowledge (optional)
Historical Outcomes (optional)
        ↓
Opportunity Ranking Capability
        ↓
Explainable factor scores (evidence-backed)
        ↓
Overall Score · Priority · Confidence · Reasons · Risks · Next Action
        ↓
Opportunity Brief
        ↓
Review Package → Campaign Builder
```

### Design rules

1. **Evidence only** — a factor may score 0 when evidence is absent; it must not invent signals.
2. **Explainable** — every factor contributes `{ factor, score, max, detail, evidenceRefs }`.
3. **Deterministic** — same inputs → same scores and briefs.
4. **Review-gated** — outputs include operator actions; no outbound side effects.

### Ranking factors (max 100)

| Factor | Max | Evidence sources |
|---|---|---|
| Profile Match | 20 | Discovery rankingSignals, industry, profile industryTargets |
| Buying Signals | 15 | Enrichment / knowledge hiring, tech, website freshness, engagement flags |
| Company Size | 10 | Employee count, multi-location, size proxy fields |
| Decision Maker Confidence | 15 | Contact title, email/phone presence, enrichment confidence |
| Personalization Opportunities | 10 | Website, industry, address, review snippets, tech stack |
| Geographic Fit | 10 | Address vs profile geography |
| Historical Success | 10 | Prior outcomes / knowledge analogs for similar vertical+geo |
| Evidence Confidence | 10 | Discovery confidence, verification, enrichment completeness |

### Priority bands

| Priority | Overall score |
|---|---|
| High | ≥ 70 |
| Medium | ≥ 45 |
| Low | < 45 |

### Opportunity Brief

Every ranked prospect generates:

- Why they're a fit
- Best outreach angle
- Three talking points
- Potential objections
- Suggested first action

## Data Model

No new durable tables in v1. Ranking is a mission capability result artifact.

```text
packages/capabilities/ranking/
  types.js
  factors.js
  brief.js
  OpportunityRanking.js
  index.js
```

### Ranked opportunity shape

```ts
interface RankedOpportunity {
  id: string
  companyName: string
  overallScore: number          // 0–100
  priority: 'high' | 'medium' | 'low'
  confidence: number            // 0–1
  topReasons: string[]
  risks: string[]
  recommendedNextAction: string
  factorScores: FactorScore[]
  opportunityBrief: OpportunityBrief
  // passthrough enrichment / discovery fields
}
```

## Implementation Plan

1. Spec + types + factor scorer + brief builder
2. `createOpportunityRankingCapability` with progress events + review package
3. Replace stub registration in built-ins (keep stub export for explicit tests)
4. Capability tests: explainable scores, empty-evidence zeros, priority bands, review actions
5. Update CURRENT_STATE / CHANGELOG / specs index

## Migration Strategy

- No schema migration
- Missions already plan `opportunity_ranking`; executor picks up the live capability via registry
- Stub remains available as `createOpportunityRankingStub` for isolation tests

## Testing

- Unit: each factor with / without evidence
- Capability: end-to-end Discovery → Enrichment stub → Ranking
- Assert every prospect has overallScore, priority, factorScores with details, brief, review package
- Assert no fabricated buying signals when enrichment is empty

## Acceptance Criteria

- [x] Every prospect receives an explainable score (0–100) with factor breakdown
- [x] Ranking references evidence only (absent evidence → zero / risk, never invented)
- [x] No black-box scoring
- [x] Operator can review before continuing (review package + actions)
- [x] Results feed Campaign Builder (`prospects` ranked list in outputs)
- [x] Opportunity Brief present for each ranked prospect
- [x] Progress visible via capability progress events

## Future Work

- Wire live SPEC-030 Company Intelligence packages (hiring, commercial footprint, decision-maker graph)
- Prefer structured Active Business Signals from [SPEC-031](SPEC-031_Business_Signals_Capability.md) / [ADR-018](../adr/ADR-018_Time_Matters.md) over boolean enrichment flags for Buying Signals / timing / outreach angle
- Optional SPEC-015A Reasoning Runtime strategy pack for ranking
- SPEC-021 calibration of factor weights from outcome history
- Persist ranked queues as first-class mission artifacts in Postgres
