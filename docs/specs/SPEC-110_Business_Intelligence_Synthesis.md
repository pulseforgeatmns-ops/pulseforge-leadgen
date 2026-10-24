# SPEC-110 — Business Intelligence Synthesis

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | Critical — Anchor Pilot 0 |
| **Owner** | Max Core Reasoning |
| **Created** | 2026-08-18 |
| **Depends on** | [SPEC-102](SPEC-102_Max_Retrieval_Before_Delegation.md), [SPEC-102F](SPEC-102F_Max_Development_Framework.md), [SPEC-105](SPEC-105_Max_Operating_Evidence_Retrieval.md), [SPEC-107](SPEC-107_Evidence_Grounded_Recommendation_Orchestration.md), [SPEC-107A](SPEC-107A_Recommendation_Claim_Grounding.md), [SPEC-108](SPEC-108_Claim_Grounding_Competency_Graduation.md), [SPEC-109](SPEC-109_Intent_Bound_Response_Selection.md) |
| **ADR** | [ADR-047 Intelligence Before Evidence](../adr/ADR-047_Intelligence_Before_Evidence.md) |

## Objective

Transform grounded operating evidence into concise operator intelligence **before** presenting supporting evidence.

Max should communicate conclusions, not merely inventories. Evidence remains fully available and attributable, but intelligence comes first.

## Problem

Current Max responses correctly retrieve and ground evidence but often present operators with raw inventories.

**Current**

```text
72 prospects
69 Scout companies
20 AO leads
25 touchpoints
```

All true. But the operator must perform the reasoning.

**Desired**

```text
Prospect generation is no longer the primary bottleneck.
You have sufficient qualified inventory to begin validating a repeatable acquisition motion.
The largest uncertainty is execution and conversion rather than prospect discovery.

Evidence
72 prospects
69 Scout companies
...
```

## Principle

Evidence exists to support intelligence.
Intelligence exists to support operator decisions.
Max should optimize for operator understanding, not evidence presentation.

Operators don't recite databases. They synthesize: "Here's what matters." Then they explain why.

## Architecture

```text
Current                         Proposed
Retrieve                        Retrieve
    ↓                               ↓
Ground claims                   Ground claims
    ↓                               ↓
Return evidence                 Synthesize business intelligence
                                    ↓
                                Present evidence
```

The synthesis layer never invents. It performs bounded transformations only.

**Allowed**

- identify bottlenecks
- identify progress
- identify missing evidence
- summarize operating state
- compare observed state against goals
- surface uncertainty

**Forbidden**

- speculate beyond evidence
- introduce unsupported causes
- create unsupported forecasts

If evidence is insufficient, return **Unknown**. Never inference.

## Intelligence Object

Business Intelligence is a first-class object, not a paragraph of prose. Later Max, Rex, dashboards, daily briefings, and Cal can consume the same synthesized objects instead of each reinventing summaries.

| Field | Meaning |
|---|---|
| `finding` | The operator-facing conclusion |
| `category` | `bottleneck` \| `momentum` \| `risk` \| `readiness` \| `unknown` |
| `confidence` | `high` \| `moderate` \| `low` \| `unknown` |
| `supporting_claims` | Grounded claims the finding maps to |
| `unknowns` | What the evidence does not yet show |
| `operator_impact` | What the finding implies for operator attention |

Every synthesized finding must map to grounded evidence. No finding may exist without supporting claims.

Example:

```text
Finding     Prospect supply is sufficient.
Category    bottleneck
Confidence  Moderate
Evidence    72 prospects · 69 companies · 20 AO leads
Unknown     Conversion rate
Impact      Focus should shift toward execution.
```

## Categories (Pilot 0)

| Category | Pattern | Finding |
|---|---|---|
| **Bottleneck** | Prospect inventory exists. No conversions exist. | Execution is the current bottleneck. |
| **Momentum** | Jobs completed. Reviews increasing. | Market validation is improving. |
| **Readiness** | Blueprint approved. Campaign created. Playbook approved. | Ready to begin pilot. |
| **Risk** | No recent outreach evidence. No pipeline activity. | Pipeline freshness cannot be confirmed. |
| **Unknown** | No conversion or channel-effectiveness evidence. | Cannot determine acquisition effectiveness. |

## Response Integration

SPEC-109 still selects the contract. SPEC-110 fills a required **Business Intelligence** section first.

| Contract | Structure |
|---|---|
| **Retrieval** | Business Intelligence → verified state / unknowns → Evidence |
| **Summary** | Business Intelligence → Observed State → Goals → Unknowns → Recommendation (optional) → Evidence |
| **Recommendation** | Business Intelligence → Recommendation → Supporting Evidence |

Challenge and investigation contracts are unchanged. They already present a specific reasoning form.

## Scope

1. First-class `BusinessIntelligence` object and bounded synthesis (`BusinessIntelligence.js`)
2. Required `business_intelligence` section on Retrieval, Summary, and Recommendation contracts
3. Composition: intelligence before evidence
4. Channel-effectiveness questions (`Are Yelp Ads working?`) fail closed as Unknown
5. Competency Registry — `business_intelligence_synthesis`

## Out of Scope

- Persisting intelligence objects as operating fact
- Speculating about unrecorded channels
- Changing SPEC-105 inventory semantics
- Durable assimilation of operator corrections (still after SPEC-108)
- Autonomous execution
- Replacing SPEC-053 prospect `BusinessIntelligenceProfile` (different consumer: ranked prospects, not operator operating state)

## Relationship to Prior Specs

- SPEC-107A ensures challenged reasoning is corrected.
- SPEC-108 ensures recommendations rely only on grounded claims.
- SPEC-109 ensures the response structure matches the operator's intent.
- SPEC-110 ensures the response communicates business intelligence before evidence, while preserving traceability.

## Testing

- `packages/max/workspace/tests/businessIntelligenceSynthesis.test.js`
- `test/businessIntelligenceSynthesis.test.js`

## Acceptance Criteria

- [x] Retrieval (`What outreach has already been sent?`) — business intelligence summarizes verified outreach before listing evidence
- [x] Summary (`How is Anchor Cleaning doing?`) — operator receives understanding before inventory
- [x] Recommendation (`What should we do next?`) — recommendation references synthesized findings rather than isolated facts
- [x] Unknown (`Are Yelp Ads working?`) — returns insufficient evidence to determine effectiveness; not speculation
- [x] Bottleneck (`Where should we focus next?`) — identifies current bottleneck from grounded evidence
- [x] Every finding maps to supporting claims; empty supporting claims cannot produce a finding
- [x] `business_intelligence_synthesis` registered and graduated in the Competency Registry

## Future Work

- Workspace UI rendering of intelligence objects (finding, category, confidence)
- Rex weekly summaries and Max briefings consuming the same object
- Cal coaching prompts consuming `operator_impact`
- Additional categories only when Pilot 0 patterns prove insufficient
