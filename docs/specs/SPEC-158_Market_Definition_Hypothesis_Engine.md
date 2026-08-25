# SPEC-158 — Market Definition & Hypothesis Engine (Scout Brain)

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Priority** | Critical (P0) |
| **Owner** | Scout |
| **Created** | 2026-08-25 |
| **Depends on** | [SPEC-141](SPEC-141_Scout_Intelligence_Pipeline.md), [SPEC-153](SPEC-153_Discovery_Coverage_Engine.md), [SPEC-142](SPEC-142_Evidence_Driven_Investigation_Engine.md) |

> **Note:** SPEC-156 is reserved for the Max Reasoning Operator Engine. This Scout brain spec is numbered **SPEC-158** to avoid collision.

## Objective

Convert operator intent into a structured **Market Definition** before investigation begins. Scout must understand what the operator means — not just repeat their words.

## Problem

Mission language is operator language. Markets do not necessarily describe themselves using the same language.

| Operator says | Market says |
|---|---|
| Short-term rental operator | Airbnb Host, Vacation Rental, Property Management, Corporate Housing, Executive Stays |

Scout previously expanded terminology from a static concept library. It did not build a semantic model of the market.

## Architecture

Replace:

```
Mission → Search Terms
```

With:

```
Mission → Market Definition → Hypothesis Engine → Investigation Plan
```

### Market Definition

```js
interface MarketDefinition {
  market
  geography
  customerTypes
  decisionMakers
  businessModels
  terminology
  adjacentMarkets
  exclusions
  buyingSignals
  expectedEvidence
}
```

### Hypothesis Engine

When results are insufficient, Scout generates terminology hypotheses and spawns investigation branches:

1. Maybe they call themselves **Vacation Rental Management** → search again
2. Maybe they advertise as **Property Managers** → search again
3. Maybe they're known through **Airbnb listings** → search again

Failed branches spawn follow-up hypotheses. Every hypothesis is visible in the mission report.

### Investigation Tree

```
Mission → Market Definition → Hypotheses → Investigation Branches → Evidence
```

## Modules

| Module | Role |
|---|---|
| `packages/scout/intelligence/MarketDefinition.js` | Semantic market models + builder + reviser |
| `packages/scout/investigation/SearchHypothesisEngine.js` | Terminology hypothesis generation + evaluation |
| `packages/scout/investigation/InvestigationTree.js` | Branch lineage + evidence recording |
| `packages/scout/coverage/HypothesisDrivenDiscovery.js` | Hypothesis-driven coverage orchestrator |
| `packages/scout/memory/TerminologyLearning.js` | Cross-mission terminology performance memory |

## Integration

- `buildMarketDefinition()` now produces the full semantic model (SPEC-158 fields)
- `expandConcepts()` prefers `marketDefinition.terminology` over static `SEGMENT_CONCEPTS`
- `constructCandidateUniverse()` uses hypothesis-driven discovery when `marketDefinition` is present
- `buildDiscoveryReport()` includes market definition, investigation hypotheses, and final understanding

## Invariant

**Every investigation begins from a Market Definition, not directly from operator wording.**

## Acceptance Criteria

| Scenario | Requirement |
|---|---|
| 1 | Operator says "short-term rental" → Scout expands into complete Market Definition |
| 2 | No results → Scout generates new hypothesis; investigation continues |
| 3 | Evidence contradicts original terminology → Scout revises Market Definition |
| 4 | Mission Report contains Market Definition, investigation hypotheses, evidence per hypothesis, final understanding |

## Tests

`test/scoutMarketDefinitionHypothesis.test.js`
