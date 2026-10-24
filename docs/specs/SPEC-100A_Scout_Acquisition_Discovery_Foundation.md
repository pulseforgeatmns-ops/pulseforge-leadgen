# SPEC-100A — Scout Acquisition Discovery Foundation

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Pulseforge |
| **Created** | 2026-08-16 |
| **Depends on** | [SPEC-098](SPEC-098_Max_Specialist_Delegation_Contract.md), [SPEC-100](SPEC-100_Max_Scout_Acquisition_Intelligence_Loop.md), [SPEC-099A](SPEC-099A_Scout_Investigation_Provenance.md) |
| **Primary capability** | `scout.acquisition_intelligence` |
| **Initial validation tenant** | Anchor Cleaning (`client_id=10`) |

> **Numbering note:** The product brief called this SPEC-100 (Scout Acquisition Discovery Foundation). Repository SPEC-100 is the Max ↔ Scout loop. This discovery foundation is numbered **100A**.

## Objective

Give Scout a real market-perception layer capable of constructing and evaluating a candidate universe before higher-order acquisition signals are considered.

Scout must take a bounded acquisition objective from Max and independently construct a defensible candidate universe without requiring social specialists.

## Problem

SPEC-100 and SPEC-099A exposed the actual limitation in Scout. For the Anchor prompt, Scout reported `companies evaluated: 0` / `coverage confidence: 0.3`. Max correctly treated that as an incomplete investigation. The missing sequence was:

Define market → discover candidate universe → establish basic fit → gather evidence → detect signals → evaluate opportunities

## Scope

1. `AcquisitionSearchDefinition` from Max's bounded delegation
2. Explicit, inspectable discovery population
3. Retrieve-before-discover against existing tenant intelligence
4. Adapter-based candidate discovery (existing PF + public/business sources)
5. Entity resolution / deduplication against PF companies
6. Explainable basic fit, separate from timing/intent
7. Basic evidence + timing-signal layers with source and timestamp
8. Opportunity classes: supported / fit / watch / rejected
9. Preserve strong-fit candidates when timing is unknown
10. Optional person enrichment that cannot discard a valid company
11. Partial source failure does not collapse useful discovery
12. Zero evaluated candidates → `blocked` / `partial`, not a market-negative conclusion
13. No arbitrary lead quotas and no silent scope broadening
14. Persist discovery intelligence + freshness timestamps
15. SPEC-099A funnel populated from actual execution

## Out of Scope

- Link / Faye / Ivy / Penny / Emmett integration
- Outbound email, SMS, calling, social automation
- New Command Deck visuals or universal orchestration
- Autonomous campaign creation
- Arbitrary lead quotas
- Aji onboarding changes
- A new ICP subsystem
- Specialist #2

## Architecture

```text
                    MAX
                     │
              acquisition need
                     │
                     ▼
                   SCOUT
                     │
        ┌────────────┴────────────┐
        │                         │
 Candidate Discovery        Existing PF Intel
        │                         │
        └────────────┬────────────┘
                     ▼
                Candidate Set
                     │
                Fit Evaluation
                     │
                     ▼
             Evidence Gathering
                     │
          ┌──────────┼──────────┐
          │          │          │
        Link        Faye       Ivy   (optional later)
          └──────────┼──────────┘
                     ▼
              Signal Synthesis
                     │
                     ▼
             SpecialistResult → Max
```

Social specialists deepen signal. They do not create Scout's ability to perceive businesses.

## Data Model

`AcquisitionSearchDefinition`: `tenantId`, `businessNeed`, `geography` (label, cities, permittedNearby), `segments`, `companyCriteria`, `exclusions`, `desiredSignals`, `createdFromDelegationId`, `populationStatement`.

Candidate funnel (SPEC-099A, now real): `candidatesDiscovered`, `candidatesResolved`, `candidatesEvaluated`, `basicFitCount`, `signalBearingCount`, `supportedOpportunityCount`.

Opportunity classes reuse prospect language: `supported`, `fit`, `watch`, `rejected`. Fit is never treated as buying intent.

Freshness: `discoveredAt`, `lastEvaluatedAt`, `evidenceObservedAt`.

## Implementation

| File | Role |
|---|---|
| `packages/max/scoutAcquisition/SearchDefinition.js` | Search definition + population statement |
| `packages/max/scoutAcquisition/DiscoveryAdapters.js` | Adapter interface; Places / injected / social stubs |
| `packages/max/scoutAcquisition/EntityResolution.js` | Name/domain/address resolve + dedupe |
| `packages/max/scoutAcquisition/FitEvaluation.js` | Basic fit ≠ intent; evidence; classification |
| `packages/max/scoutAcquisition/CandidateUniverse.js` | Retrieve → discover gap → resolve → persist |
| `packages/max/scoutAcquisition/ScoutAdapter.js` | Wired into SPEC-098 `acquisition_intelligence` |

## Testing

- `test/scoutAcquisitionDiscovery.test.js` — discovery foundation
- Existing SPEC-100 / SPEC-099A loop regressions remain green
- [SPEC-101](SPEC-101_Max_Specialist_Result_Interrogation.md) inspects the discovery/search-definition failure without rerunning Scout

## Acceptance Criteria

- [x] A valid acquisition definition produces an actual candidate universe
- [x] Existing relevant companies are reused before open-market discovery
- [x] Duplicate businesses resolve to one entity
- [x] Fit decisions retain reasons; fit ≠ intent
- [x] Strong-fit / no-timing candidates remain visible
- [x] Person enrichment failure does not discard valid companies
- [x] Partial source failure preserves useful discovery
- [x] Zero candidates + weak discovery returns partial/blocked
- [x] Zero opportunities + strong discovery returns a valid complete result
- [x] No quota filling; no silent scope broadening
- [x] Discovery sources/evidence remain traceable
- [x] SPEC-099A funnel reflects actual execution
- [x] Tenant isolation; no outbound execution

## Future Work

- Optional live Places discovery in production when `GOOGLE_PLACES_KEY` is present
- Link / Faye / Ivy as additional wavelengths on the same candidate set
- Operator-authorized search expansion
