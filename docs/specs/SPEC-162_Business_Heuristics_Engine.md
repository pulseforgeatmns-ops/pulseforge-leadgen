# SPEC-162 — Business Heuristics Engine

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Priority** | Critical |
| **Owner** | Scout |
| **Created** | 2026-08-25 |
| **Depends on** | [SPEC-157](SPEC-157_Autonomous_Discovery_Approval_Policy.md)–[SPEC-161](SPEC-161_Market_Memory.md) |
| **ADR** | [ADR-082](../architecture/ADR-082_Business_Judgment_Through_Reusable_Heuristics.md) |

## Problem

Evidence synthesis produces business understanding, but understanding alone does not explain *why* an operator should act. Scout needs reusable business judgment patterns that transform understanding into actionable implications.

## Objective

Insert a Business Heuristics layer between Understanding and Recommendation. Recommendations originate from activated heuristics, not directly from evidence or raw understanding.

## Pipeline

```
Evidence → Synthesis → Understanding → Business Heuristics → Recommendation
```

## Core Module

`packages/scout/heuristics/BusinessHeuristicsEngine.js`

## Acceptance Criteria

| Scenario | Behavior |
|---|---|
| Market Growth | Three growth indicators activate Growth Market heuristic |
| Vendor Replacement | Negative reviews + leadership change + facilities hiring activate Vendor Instability |
| Contradictory Heuristics | Growth Market + Vendor Stability both preserved; confidence reduced; tension explained |
| Explainability | `explainJudgment()` returns activated heuristics, evidence, contradictions, implications |
| Learning | Won outcomes strengthen contributing heuristics; lost outcomes weaken without deletion |
| Mission Intelligence Report | `businessJudgment` section separate from `businessUnderstanding` |

## Tests

`test/scoutBusinessHeuristicsEngine.test.js`
