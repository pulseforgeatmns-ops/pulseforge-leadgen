# SPEC-099A — Scout Investigation Provenance & Coverage Intelligence

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Pulseforge |
| **Created** | 2026-08-16 |
| **Parent** | Product brief SPEC-099 — Max ↔ Scout Acquisition Intelligence Loop |
| **Depends on** | [SPEC-098](SPEC-098_Max_Specialist_Delegation_Contract.md), [SPEC-100](SPEC-100_Max_Scout_Acquisition_Intelligence_Loop.md) |

> **Numbering note:** The product brief called the parent loop SPEC-099. Repository SPEC-099 is [Client Experience Convergence](SPEC-099_Client_Experience_Convergence.md). The implemented parent loop is [SPEC-100](SPEC-100_Max_Scout_Acquisition_Intelligence_Loop.md). This refinement is **SPEC-099A**.

## Objective

Give Max enough visibility into how Scout investigated an acquisition question to evaluate the reliability of Scout's conclusion, especially zero-result and weak-result investigations.

A specialist conclusion and the quality of the investigation that produced it are separate things.

## Problem

SPEC-100 demonstrated the first production cognitive loop. Anchor validation correctly treated a zero as immaterial. Inspection then showed that Max had evidence about the result pipeline (`scout_acquisition`, `spec_100`) but not about the investigation itself.

`0 supported opportunities` can mean thorough negative intelligence or weak coverage. Those must not have equivalent epistemic weight.

## Scope

1. Durable `investigation` object on Scout's SPEC-100 / SPEC-098 result payload
2. Actual search scope (not merely requested scope)
3. Candidate funnel and rejection summary
4. Source classes actually checked vs unavailable
5. Material limitations
6. Deterministic `coverageConfidence`, distinct from result `confidence`
7. Max evaluation of conclusion quality **and** coverage quality
8. Distinct zero-result judgments for strong vs weak coverage
9. Semantic Evidence / Investigation / Provenance presentation
10. Conversational inspection from durable investigation data
11. Tenant isolation

## Out of Scope

- New discovery providers
- Faye / Link / Ivy wiring
- Autonomous outreach
- Broader Scout redesign
- New Command Deck visuals
- New Max orchestration infrastructure
- Aji onboarding changes
- Universal specialist coverage scoring
- Specialist #2

## Data

`SpecialistResult.payload.investigation` reuses the existing result payload. It is not a second evidence architecture.

```text
investigation
  scope            actual geography / segments / criteria / desiredSignals
  coverage         discovered / evaluated / basicFit / signalBearing / supported / unresolved
  sources          sourceTypesChecked[] / sourceTypesUnavailable[] / perception
  rejectionSummary aggregate reasons
  nearThreshold    entity-level near-misses
  freshness        startedAt / completedAt / evidenceWindow
  limitations[]
  coverageConfidence
  coverageBand     weak | moderate | strong
```

Perception slots (`linkedin` / `facebook` / `instagram`) are reserved for future Faye / Link / Ivy coverage. This spec records them as unavailable unless a source was actually used.

## Max judgment

```text
Scout Result
      ↓
Conclusion Quality  +  Coverage Quality
      ↓
   Max Judgment
```

- Zero + strong coverage → meaningful negative intelligence; still not an Acquisition elevation
- Zero + weak coverage → incomplete investigation, not market absence
- Positive + weak coverage → may recommend found companies; must not claim they are the complete/best market

Scout supplies intelligence. Max determines significance. Command Deck visuals are unchanged.

## Presentation

- **Evidence** — business observations (expansion, portfolio, hiring, decision-maker, website)
- **Investigation** — how Scout searched and evaluated
- **Provenance** — capability, delegation, result, spec, evaluation

System contributors such as `scout_acquisition` and `spec_100` must not be labeled as business evidence.

## Testing

`test/scoutInvestigationProvenance.test.js` plus SPEC-100 loop regressions in `test/scoutAcquisitionIntelligence.test.js`.
