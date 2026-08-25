# SPEC-164 — Opportunity Intelligence Engine

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Priority** | Critical |
| **Owner** | Max |
| **Created** | 2026-08-25 |
| **Depends on** | [SPEC-157](SPEC-157_Autonomous_Discovery_Approval_Policy.md)–[SPEC-163](SPEC-163_Investigative_Strategy_Engine.md) |
| **ADR** | [ADR-084](../architecture/ADR-084_Opportunity_Intelligence.md) |

## Problem

Scout produces market understanding, business understanding, business judgment, investigation strategy, and market memory. Max still reasons primarily around missions and does not yet explicitly reason about opportunity. Questions like "Why this company?", "Why now?", and "Why before the others?" remain largely implicit.

## Objective

Introduce an Opportunity Intelligence layer that continuously evaluates every business opportunity relative to mission objectives, market understanding, business heuristics, operator capacity, expected outcomes, and business impact.

## Pipeline

```
Scout → Business Judgment → Opportunity Intelligence → Strategic Decision → Operator
```

Strategic Decision is [SPEC-165](SPEC-165_Strategic_Decision_Engine.md).

## Core Module

`packages/scout/opportunity/OpportunityIntelligenceEngine.js`

Max integration: `packages/max/opportunity/OpportunityReasoning.js`

## Opportunity Model

Every opportunity is evaluated on independent dimensions:

- **Business Value** — recurring revenue, expansion, strategic/reference customer, LTV
- **Timing** — hiring, growth, leadership change, funding, complaints, seasonality
- **Strategic Fit** — mission objective, beachhead, ideal customer, case study potential
- **Reachability** — decision maker, phone, email, warm introduction
- **Probability** — likelihood of producing the desired outcome (not "can we sell?")
- **Learning Value** — strategic learning even when revenue is smaller

No lead score. Priority is rank (1, 2, 3…) with explicit reasoning.

## Invariant

Every recommendation made by Max must include explicit opportunity reasoning. No opportunity may be prioritized solely by numeric score.

## Acceptance Criteria

| Scenario | Behavior |
|---|---|
| Ten qualified businesses | Max ranks opportunities using multidimensional reasoning |
| Why this one first? | Explains business value, timing, strategic fit, probability, learning value |
| Business expands | Opportunity priority increases; reason recorded |
| Mission objective changes | Opportunity rankings automatically recalculate |
| Mission Intelligence Report | Displays top opportunities, reasoning, recommended action, expected outcome, confidence |
| What changed overnight? | Explains opportunity movement, not just new evidence |

## Tests

`test/scoutOpportunityIntelligenceEngine.test.js`
