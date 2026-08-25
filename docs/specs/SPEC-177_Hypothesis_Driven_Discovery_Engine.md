# SPEC-177 — Hypothesis-Driven Discovery Engine

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Priority** | Critical |
| **Owner** | Scout |
| **Created** | 2026-08-25 |
| **Depends on** | [ADR-092](../adr/ADR-092_Identity_Before_Enrichment.md), [SPEC-158](SPEC-158_Market_Definition_Hypothesis_Engine.md), [SPEC-159](SPEC-159_Investigative_Reasoning_Loop.md), [SPEC-160](SPEC-160_Evidence_Synthesis_Engine.md), [SPEC-162](SPEC-162_Business_Heuristics_Engine.md) |

## Objective

Replace provider-first discovery with hypothesis-driven investigation. Scout shall no longer execute provider-specific searches directly. Instead, Scout generates an Investigation Plan from business hypotheses, then assigns providers capable of answering each investigative question.

## Problem

Current architecture still contains remnants of provider-first thinking:

```
Property Manager → Google Places query → Results
```

Although hypotheses now exist, they still collapse into provider-specific search strings too early. This tightly couples business reasoning to individual data sources.

## Decision

Business hypotheses become the primary planning abstraction. Providers become interchangeable evidence collectors.

## New Discovery Pipeline

```
Mission
  ↓
Market Definition
  ↓
Business Hypotheses
  ↓
Investigation Planning
  ↓
Evidence Requirements
  ↓
Provider Assignment
  ↓
Evidence Collection
  ↓
Identity Resolution
  ↓
Evidence Synthesis
  ↓
Business Understanding
  ↓
Business Judgment
  ↓
Recommendation
```

## Modules

| Module | Role |
|---|---|
| `packages/scout/coverage/EvidenceRequirements.js` | Investigative questions → evidence requirements |
| `packages/scout/coverage/EvidenceProviderAssignment.js` | Evidence → provider assignment with explainability |
| `packages/scout/coverage/ProviderEvidenceContract.js` | Provider reports: evidence produced, confidence, coverage, limitations |
| `packages/scout/coverage/HypothesisInvestigationPlanner.js` | Hypothesis → Investigation Plan builder |
| `packages/scout/coverage/HypothesisDrivenDiscoveryEngine.js` | Unified orchestrator (SPEC-177 entry point) |
| `packages/scout/investigation/InvestigationState.js` | Extended with plan, questions, evidence tracking |

## Investigation Plan

Each hypothesis produces an explicit investigation plan with:

- **Questions** — e.g. Does this business exist? Who makes buying decisions?
- **Evidence Requirements** — identity, portfolio evidence, decision makers, etc.
- **Assigned Providers** — chosen by evidence type, not search keywords
- **Satisfied / Outstanding Evidence** — live tracking in InvestigationState

## Provider Independence

The planner never says "Search Google Places." Instead:

```
Need business identities.
Available providers: ✓ Places ✓ Registry
Assign both.
```

## Planner Invariants

The planner must:

1. Maximize uncertainty reduction
2. Avoid duplicate evidence
3. Retry with alternative providers when evidence is insufficient
4. Explain why each provider was selected

## Operator Explainability

Max can answer "Why are we searching LinkedIn?" with:

> Because the current hypothesis requires identifying decision makers, and LinkedIn is our highest-confidence source for organizational roles.

## Acceptance Scenarios

| # | Scenario | Expected behavior |
|---|---|---|
| 1 | Property Manager hypothesis | Planner assigns Google Places + Registry; no LinkedIn until identity complete |
| 2 | Identity established | Planner requests LinkedIn + Website for decision-maker investigation |
| 3 | Website unavailable | Investigation continues via Registry, Reviews, Social |
| 4 | Google Places unavailable | Planner substitutes Registry, Chamber, Associations |
| 5 | Overlapping provider results | Identity resolution merges into one canonical business |
| 6 | All evidence satisfied | Planner marks hypothesis sufficiently investigated; no further provider work |
| 7 | Anchor Cleaning STR mission | Full pipeline: Hypothesis → Identity → Decision Makers → Growth → Cleaning → Prospects; no provider-specific logic in planning layer |

## Architectural Invariants

1. Business hypotheses own investigations.
2. Providers collect evidence — they do not define search strategy.
3. Investigations are planned from uncertainty, not from keywords.
4. Scout reasons about businesses; providers merely observe them.

## Integration

- `CandidateUniverse.constructCandidateUniverse()` uses `runHypothesisDrivenDiscovery()` when `useHypothesisDiscoveryEngine !== false` (default).
- SPEC-158 terminology engine remains available via `useHypothesisDiscoveryEngine: false`.
- `InvestigationState` is updated live during discovery and passed to SPEC-159 reasoning loop.
