# SPEC-159 — Investigative Reasoning Loop

**Status:** Implemented  
**Priority:** Critical  
**Owner:** Scout  
**Companion:** ADR-079 (Understanding Before Recommendation)

> Note: SPEC-157 in the repository refers to Autonomous Discovery Approval Policy. This spec is numbered SPEC-159 to avoid collision.

## Problem

Scout currently executes an investigation plan. Even with hypotheses and market definitions, investigation is still largely linear:

```
Market Definition → Coverage Plan → Search → Report
```

Real investigators continuously update their mental model as evidence arrives.

## Objective

Scout shall reason continuously during investigation. Every new piece of evidence is allowed to change:

- the market definition
- the estimated universe
- investigation priorities
- future search hypotheses
- confidence
- stopping conditions

## Core Principle

Replace:

```
Plan → Execute → Finish
```

With:

```
Observe → Think → Update Understanding → Decide → Investigate → Repeat
```

Scout is no longer executing a plan. Scout is reducing uncertainty.

## Investigation State

`packages/scout/investigation/InvestigationState.js` introduces:

```javascript
{
  marketDefinition,
  universeEstimate,
  activeHypotheses,
  rejectedHypotheses,
  evidenceGraph,
  coverage,
  uncertainty,
  confidence,
  nextQuestions,
}
```

There are no search terms in this object — only understanding.

## Investigation Loop

Every workload becomes:

1. Collect Evidence
2. Evidence Fusion
3. Did understanding change?
4. YES → Update Investigation State → Generate New Questions → Continue Investigation

Implemented in `packages/scout/investigation/InvestigativeReasoningLoop.js`.

Wired into production via `DiscoveryPipeline.js` after coverage execution.

## Hypothesis Lifecycle

`packages/scout/investigation/HypothesisLifecycle.js`:

```
Generated → Testing → Supported → Rejected → Archived
```

Nothing disappears. Scout remembers why it stopped believing something.

## Mission Intelligence Report

`packages/scout/investigation/MissionIntelligenceReport.js` includes:

- Final Market Definition
- Universe Estimate
- Hypothesis History
- Evidence Graph Summary
- Remaining Unknowns
- Confidence Evolution
- Recommendation (from understanding, not raw evidence)
- Suggested Next Investigation

## New Invariant

Every investigation updates Scout's understanding before deciding what to do next.

Search results never flow directly into conclusions. They flow into understanding first.

## Acceptance Criteria

| Scenario | Result |
|---|---|
| Evidence contradicts market definition | Scout revises understanding |
| Coverage increases | Universe estimate changes with reason recorded |
| Hypothesis fails | Archived; replacement generated |
| Investigation ends | Report includes remaining unknowns |
| Repeat investigation months later | Begins from prior memory, not empty state |

## Implementation Map

| Module | Role |
|---|---|
| `InvestigationState.js` | Canonical understanding container |
| `HypothesisLifecycle.js` | Hypothesis lifecycle transitions |
| `InvestigativeReasoningLoop.js` | Observe-think-update loop |
| `MissionIntelligenceReport.js` | Operator-facing intelligence deliverable |
| `DiscoveryPipeline.js` | Production integration after coverage execution |
| `test/scoutInvestigativeReasoningLoop.test.js` | Acceptance tests |
