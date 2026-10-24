# SPEC-036 — Outcome Intelligence

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-022, SPEC-023, SPEC-026, SPEC-028, SPEC-030, SPEC-032, SPEC-035, ADR-002, ADR-003, ADR-010, ADR-011, ADR-015, ADR-023 |
| **Consumed by** | Mission Engine, Command Deck Operations, Opportunity Ranking, Client Playbook, Discovery Strategy, Campaign Templates |
| **Related (distinct)** | [SPEC-013](SPEC-013_Outcome_Intelligence.md) / [ADR-008](../adr/ADR-008_Outcome_Intelligence.md) — Max *recommendation* evaluation (was the intelligence right?). SPEC-036 captures *campaign/mission operational* outcomes and turns experience into reusable strategy after evidence + approval ([ADR-023](../adr/ADR-023_Experience_Becomes_Intelligence.md)). |

## Objective

Capture the outcome of every campaign and convert operational results into reusable intelligence that improves future decisions — only after evidence has been collected and operator approval has been granted ([ADR-023](../adr/ADR-023_Experience_Becomes_Intelligence.md)).

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- [ADR-023](../adr/ADR-023_Experience_Becomes_Intelligence.md) — experience becomes intelligence
- [ADR-015](../adr/ADR-015_Strategy_Lives_in_the_Playbook.md) — strategy lives in the playbook
- [ADR-002](../adr/ADR-002_Explainable_AI.md) — explainable scores and learnings
- [ADR-003](../adr/ADR-003_Human_Approval.md) — human approval before strategy mutation
- [ADR-010](../adr/ADR-010_Mission_Engine.md) — Mission Engine
- [ADR-011](../adr/ADR-011_Capability_Framework.md) — capabilities as the stable API
- [SPEC-035](SPEC-035_Direct_Mail_Execution.md) — Direct Mail Execution
- [SPEC-032](SPEC-032_Mission_Memory.md) — Mission Memory
- [SPEC-026](SPEC-026_Opportunity_Ranking_Capability.md) — Opportunity Ranking
- [SPEC-028](SPEC-028_Client_Playbook_Capability.md) — Client Playbook
- [SPEC-030](SPEC-030_Company_Intelligence_Capability.md) — Company Intelligence

## Problem

Direct Mail Execution (SPEC-035) records per-prospect responses and campaign metrics, but operators lack a durable system that:

1. Normalizes every execution outcome into a structured Outcome Record
2. Generates evidence-backed learnings (not assumptions)
3. Produces playbook / ranking / discovery recommendations that stay **pending** until approved
4. Feeds structured feedback into Opportunity Ranking
5. Tracks personalization effectiveness (opening, facts, offer, CTA, inserts)
6. Concludes the Mission with an Outcome Summary (objective, lessons, recommendations)

Without Outcome Intelligence, campaign results die in execution metrics and never improve future targeting, ranking, or playbooks.

## Scope

- Outcome Intelligence capability (`outcome_intelligence`) as a first-class Mission capability
- Mission type `outcome_intelligence`
- Inputs: Direct Mail Execution, Response Events, Mission Timeline, Company Intelligence, Opportunity Ranking, Client Playbook
- Outcome types (positive / neutral / negative)
- Outcome Records (mission, campaign, prospect, company, type, timestamp, operator, notes, evidence, confidence)
- Learning Engine — structured learnings from evidence only
- Playbook Recommendations — pending until operator approval
- Ranking Feedback — structured feedback for Opportunity Ranking
- Personalization Feedback — opening / facts / offer / CTA / inserts
- Campaign Analytics — response / walkthrough / proposal / win rates + cost metrics
- Mission Integration — outcome events + Outcome Summary on Mission Memory shapes
- Operator Review — approve / reject recommendations before mutating playbook, ranking weights, discovery strategy, or campaign templates

## Out of Scope

- Mutating Client Playbook / Ranking weights / Discovery profiles without operator approval (ADR-023)
- Max recommendation calibration ([SPEC-013](SPEC-013_Outcome_Intelligence.md) / ADR-008) — separate evaluation layer
- Full Command Deck HTML UI chrome (v1 returns outcome view model; UI binds later)
- Full Mission Memory Postgres tables (SPEC-032) — local event shapes mirror the contract
- Autonomous ML / black-box model training
- Live carrier / CRM webhook ingest beyond execution-supplied response events

## Dependencies

| Dependency | Role |
|---|---|
| SPEC-023 Capability Framework | Capability contract + registry |
| SPEC-035 Direct Mail Execution | Per-prospect responses + campaign metrics |
| SPEC-032 Mission Memory | Append outcome events / Outcome Summary |
| SPEC-026 Opportunity Ranking | Consumes structured ranking feedback |
| SPEC-028 Client Playbook | Target of approved recommendations |
| SPEC-030 Company Intelligence | Vertical / geo / company attributes for learnings |
| ADR-023 | Experience becomes intelligence only after evidence + approval |

## Architecture

```text
Direct Mail Execution · Response Events
Mission Timeline · Company Intelligence · Ranking · Playbook
      ↓
Outcome Intelligence
      ↓
Normalize Outcome Records
      ↓
Learning Engine (evidence-backed only)
      ↓
Recommendations (pending) · Ranking Feedback · Personalization Feedback
      ↓
Campaign Analytics · Outcome Summary
      ↓
Operator Review → approve → Playbook / Ranking / Discovery / Templates
Mission Memory events (SPEC-032 shape)
```

### Design rules

1. **Every execution outcome captured** — no silent drops; missing fields fail closed or score confidence down.
2. **Learning only from evidence** — minimum sample size + measurable lift vs baseline; assumptions never promote.
3. **Recommendations require approval** — pending until operator approve/reject; no silent playbook mutation ([ADR-023](../adr/ADR-023_Experience_Becomes_Intelligence.md)).
4. **Ranking receives structured feedback** — successful characteristics increase future scores; unsuccessful reduce confidence.
5. **Mission concludes with Outcome Summary** — objective achieved, lessons learned, recommendations generated.
6. **Distinct from SPEC-013** — this layer improves *strategy from operational results*; SPEC-013 evaluates whether *Max recommendations* were right.

### Outcome types

**Positive**

| Type |
|---|
| Delivered |
| Phone Call |
| Email Reply |
| Walkthrough Scheduled |
| Proposal Requested |
| Proposal Sent |
| Closed Won |
| Referral |

**Neutral**

| Type |
|---|
| No Response |
| Follow Up Required |
| Delayed Decision |

**Negative**

| Type |
|---|
| Returned Mail |
| Wrong Contact |
| Business Closed |
| Not Interested |
| Closed Lost |

### Outcome Record

Every outcome contains:

| Field | Description |
|---|---|
| Mission | Mission id |
| Campaign | Campaign id / name |
| Prospect | Prospect id |
| Company | Company id / name |
| Outcome Type | Canonical type |
| Timestamp | When recorded |
| Operator | Who recorded |
| Notes | Free text |
| Evidence | Refs (execution snapshot, response event, etc.) |
| Confidence | 0–1 |

### Learning Engine

Generate structured learnings from campaign outcomes. Examples:

- Property managers respond at 2.3× baseline.
- Medical offices outperform dental offices.
- Handwritten notes increase response rate.
- Law firms underperform in this region.
- Tuesday mailings outperform Friday.

Only evidence-backed conclusions may be promoted (minimum sample + lift threshold).

### Playbook Recommendations

Generate recommendations that remain **pending** until operator approval. Examples:

- Increase property manager targeting.
- Reduce restaurant targeting.
- Update opening paragraph.
- Increase handwritten personalization.
- Remove ineffective insert.

Approved recommendations may update: Client Playbook · Ranking Weights · Discovery Strategy · Campaign Templates.

### Ranking Feedback

Feed outcomes into Opportunity Ranking:

- Successful characteristics → increase future scores
- Unsuccessful characteristics → reduce future confidence

### Personalization Feedback

Track effectiveness of:

- Opening paragraph
- Personalization facts
- Offer
- CTA
- Insert package

### Campaign Analytics

| Metric |
|---|
| Response Rate |
| Walkthrough Rate |
| Proposal Rate |
| Win Rate |
| ROI |
| Cost Per Response |
| Cost Per Walkthrough |
| Cost Per Customer |

### Mission Integration

Outcome events append to Mission Memory. Mission concludes with:

- Objective achieved
- Lessons learned
- Recommendations generated

## Data Model

```text
packages/capabilities/outcomeIntelligence/
  types.js
  validate.js
  learn.js
  recommend.js
  rankingFeedback.js
  personalization.js
  analytics.js
  assemble.js
  actions.js
  OutcomeIntelligenceStore.js
  OutcomeIntelligence.js
  index.js
```

### Recommendation status

```ts
type RecommendationStatus = 'pending' | 'approved' | 'rejected' | 'applied'
```

### Learning status

```ts
type LearningStatus = 'candidate' | 'evidence_backed' | 'promoted' | 'rejected'
```

## Implementation Plan

1. File SPEC-036 + ADR-023 + types / validate / learn / recommend / rankingFeedback / personalization / analytics / assemble / actions / store
2. Outcome Intelligence capability + register builtin
3. Mission type `outcome_intelligence`; IntentRouter patterns; playbook pin optional
4. Tests: capture from execution, evidence gate on learnings, pending recommendations, approve gate, ranking feedback shape, analytics, outcome summary
5. Later: Command Deck UI, Mission Memory live attach, auto-ingest from SPEC-035 response webhooks

## Migration Strategy

- No required migration in v1 (in-memory store, like DirectMailExecutionStore)
- Forward: `campaign_outcomes` + `outcome_learnings` + `outcome_recommendations` tables when durability is required
- SPEC-032: map outcome events → Mission Memory timeline; pin Outcome Summary on mission completion

## Testing

- Unit: outcome normalization; learning evidence thresholds; analytics rates; ranking feedback polarity
- Capability: capture from Direct Mail Execution; generate evidence-backed learnings; recommendations stay pending; approve/reject; Outcome Summary
- Mission: IntentRouter + planner chain for `outcome_intelligence`
- Manual: inspect outcome view model JSON + analytics + pending recommendations

## Acceptance Criteria

- [x] Every execution outcome captured
- [x] Learning generated only from evidence
- [x] Recommendations require approval
- [x] Ranking receives structured feedback
- [x] Playbooks updated through approved recommendations only
- [x] Mission concludes with an Outcome Summary

## Future Work

- Command Deck Outcome Intelligence UI
- Mission Memory live attach (SPEC-032)
- Auto-publish from Direct Mail Execution response events
- Persist outcomes / learnings / recommendations in Postgres
- Apply approved recommendations into live Client Playbook versions + Discovery Profiles
- Cost / ROI inputs from finance when available
