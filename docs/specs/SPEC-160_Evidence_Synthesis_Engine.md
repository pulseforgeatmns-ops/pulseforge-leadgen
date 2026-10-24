# SPEC-160 — Evidence Synthesis Engine

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Priority** | Critical |
| **Owner** | Scout |
| **Created** | 2026-08-25 |
| **Depends on** | [SPEC-141](SPEC-141_Scout_Intelligence_Pipeline.md), [SPEC-159](SPEC-159_Investigative_Reasoning_Loop.md), [SPEC-100A](SPEC-100A_Scout_Acquisition_Discovery_Foundation.md) |
| **ADR** | [ADR-080](../architecture/ADR-080_Understanding_Emerges_From_Evidence.md) |

> **Note:** This spec implements the Evidence Synthesis layer described in the Scout brain architecture. It follows SPEC-158 (Market Definition) and SPEC-159 (Investigative Reasoning Loop) in the pipeline.

## Problem

Scout accumulates evidence as independent observations. Google Places, website copy, and Facebook hiring posts remain disconnected. Scout does not synthesize multiple weak signals into coherent business understanding.

## Objective

Scout continuously synthesizes evidence into business understanding. The output of investigation is **understanding**, not raw evidence.

## Architectural Principle

Replace:

```
Evidence → Confidence
```

With:

```
Evidence → Synthesis → Understanding → Confidence
```

Confidence attaches to synthesized understanding, not individual facts.

## Evidence Model

Atomic evidence:

```javascript
{
  id, source, observation, timestamp, confidence, provenance
}
```

## Understanding Model

```javascript
{
  entity, assertions, supportingEvidence, contradictoryEvidence,
  confidence, reasoning
}
```

## Synthesis Rules

1. **Same thing?** — Entity resolution merges aliases (ABC Management / ABC Property Management / ABC Vacation Rentals LLC).
2. **Multiple weak signals?** — Fuse into one assertion (Property Management + Vacation Rentals + Hiring cleaners → STR management company).
3. **Contradiction?** — Revise understanding, retain contradictory evidence, reduce confidence.

## Invariant (ADR-080)

Every recommendation must be supported by synthesized understanding rather than isolated evidence.

Evidence informs understanding. Understanding informs recommendations.

## Implementation

| Module | Role |
|---|---|
| `packages/scout/synthesis/types.js` | Evidence + Understanding models |
| `packages/scout/synthesis/EvidenceSynthesisEngine.js` | Synthesis, entity merge, contradiction handling, explainability |
| `packages/scout/investigation/InvestigationState.js` | `businessUnderstandings`, `synthesisSummary` |
| `packages/scout/investigation/InvestigativeReasoningLoop.js` | Runs synthesis after evidence collection |
| `packages/scout/investigation/MissionIntelligenceReport.js` | Business understanding section in report |

## Acceptance Criteria

| Scenario | Result |
|---|---|
| Three observations support same conclusion | Single synthesized understanding |
| Contradictory evidence appears | Understanding revised; contradiction retained |
| Operator asks "Why do you believe this?" | Supporting + contradictory evidence returned |
| Business under three names | Entity resolution merges to one understanding |
| Mission Intelligence Report | Shows synthesized understanding, not raw search results |

## Pipeline Position

```
Mission → Market Definition → Universe Estimate → Hypothesis Generation →
Investigation → Evidence Collection → **Evidence Synthesis** →
Business Understanding → Market Understanding → Mission Intelligence Report → Max
```
