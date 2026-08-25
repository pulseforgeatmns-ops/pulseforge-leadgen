# SPEC-163 — Investigative Strategy Engine

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Priority** | Critical |
| **Owner** | Scout |
| **Created** | 2026-08-25 |
| **Depends on** | [SPEC-157](SPEC-157_Autonomous_Discovery_Approval_Policy.md)–[SPEC-162](SPEC-162_Business_Heuristics_Engine.md) |
| **ADR** | [ADR-083](../architecture/ADR-083_Investigate_What_Reduces_Uncertainty_Most.md) |

## Philosophy

Scout should not investigate because a source is next in a checklist. Scout should investigate because the next investigation is expected to produce the greatest reduction in uncertainty.

**Goal:** Learn the most important thing next.

## Pipeline

```
Evidence → Understanding → Business Judgment → Investigative Strategy → Next Investigation → Recommendation
```

## Core Module

`packages/scout/investigation/InvestigativeStrategyEngine.js`

## Invariant

Every investigation performed by Scout must have a documented expected information gain. Scout never investigates without a reason.

## Acceptance Criteria

| Scenario | Behavior |
|---|---|
| Five unknowns | Scout ranks investigations by expected information gain |
| Unknown resolved | Strategy automatically recalculates |
| Heuristic activates | Investigation priorities shift to test that heuristic |
| Explainability | Operator can ask why a source was chosen; answer uses expected information gain |
| Coverage threshold | Scout explains why investigation stopped when remaining gain is negligible |
| Mission Intelligence Report | Includes knowns, remaining unknowns, strategy, gain history, next investigation |

## Tests

`test/scoutInvestigativeStrategyEngine.test.js`
