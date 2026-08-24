# SPEC-145 — Adaptive Investigation Planning

**Status:** Implemented  
**Depends on:** [SPEC-141](SPEC-141_Scout_Intelligence_Pipeline.md), [SPEC-142](SPEC-142_Evidence_Driven_Investigation_Engine.md), [SPEC-143](SPEC-143_Scout_Intelligence_Memory.md), [SPEC-144](SPEC-144_Scout_Intelligence_Credibility_Framework.md)  
**Related ADR:** ADR-064 — Investigation Before Execution

## Objective

Before collecting evidence, Scout constructs an explicit **Investigation Plan**. Providers execute the plan; they do not define it.

## Design Principle

Every investigation begins with a hypothesis — not with a search.

## Pipeline

```
Mission
  → Market Understanding
  → Investigation Planning          (SPEC-145 — new)
  → Evidence Planning
  → Provider Strategy
  → Candidate Discovery
  → Evidence Collection
  → Qualification
  → Ranking
  → Coverage Review
```

## InvestigationPlan

```javascript
{
  version: 'SPEC-145',
  mission,
  objective,
  hypotheses,
  evidenceRequired,
  providerSequence,      // ProviderPlan[]
  stoppingConditions,
  estimatedCoverage,
  estimatedConfidence,
  estimatedCost,
  revisions,
}
```

### ProviderPlan

```javascript
{
  provider,
  capabilities,
  evidenceExpected,
  estimatedCost,
  confidenceGain,
  gap,
  order,
  status,               // pending | completed | skipped | failed | unavailable
}
```

### InvestigationStatus

```javascript
{
  completedSteps,
  remainingSteps,
  confidence,
  coverage,
  cost,
  blockers,
  remainingUnknowns,
  recommendedNextProvider,
  recommendedNextInvestigation,
}
```

## Components

| Module | Purpose |
|--------|---------|
| `InvestigationPlanBuilder.js` | Explicit plan before provider execution (ADR-064) |
| `InvestigationBoard.js` | Live Known / Unknown / Persistent board with value-of-information scores |
| `InvestigationJournal.js` | Reasoning trail for every step |
| `ProviderLearning.js` | Second Brain learns provider × gap effectiveness |
| `InvestigationPlanner.js` | Adaptive step selection — question chooses provider |
| `InvestigationLoop.js` | Wires plan, board, journal, stop conditions, learning feedback |

## Cost Optimization

Scout maximizes **confidence gained / cost incurred**, not providers queried.

## Stopping Conditions

Investigation stops when one of:

1. Confidence target achieved
2. Coverage target achieved
3. Budget exhausted
4. Evidence exhausted (diminishing returns)
5. Persistent unknowns only
6. Operator interruption

## Plan Revision

When a provider is unavailable or fails, Scout revises the plan (switches providers, decreases estimated confidence) and continues — no investigation failure.

## Second Brain Integration

Investigation plans and provider learning persist to SPEC-143 memory via `extractInvestigationMemory()`.

## Mission Intelligence Report

Reports now include:

- Investigation Strategy (objective, hypotheses, provider sequence)
- Evidence Summary
- Coverage / Confidence
- Remaining Unknowns
- Recommended Next Investigation

## API

Adaptive planning is enabled by default in `Scout.investigate()` / `runInvestigationEngine()`.

| Option | Default | Purpose |
|--------|---------|---------|
| `coverageThreshold` | 0.91 | Stop when key gap coverage reached |
| `minExpectedGain` | 0.02 | Diminishing returns threshold |
| `adaptivePlanning` | true | Enable value-of-information planning |
| `enforceInvestigationPlan` | true | No provider executes outside the plan |
| `replanOnProviderFailure` | true | Revise plan when a provider fails |

## Acceptance Tests

| Test | Scenario | Expected |
|------|----------|----------|
| 1 | Find STR operators | Complete Investigation Plan before any provider call |
| 2 | Provider unavailable | Replan; investigation continues |
| 3 | Confidence threshold early | Remaining providers skipped |
| 4 | Budget exhausted | Report remaining unknowns, confidence, recommended next provider |
| 5 | Repeat investigation | Plan differs based on prior provider learning |

## Tests

`test/scoutAdaptiveInvestigation.test.js`
