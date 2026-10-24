# SPEC-100 — Max ↔ Scout Acquisition Intelligence Loop

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Pulseforge |
| **Created** | 2026-08-16 |
| **Depends on** | [SPEC-098](SPEC-098_Max_Specialist_Delegation_Contract.md) |
| **Primary specialist** | Scout |
| **Initial authority** | observe + recommend only |
| **Initial validation tenant** | Anchor Cleaning (`client_id=10`) |

> **Numbering note:** The product brief called this SPEC-099. Repository SPEC-099 is [Client Experience Convergence](SPEC-099_Client_Experience_Convergence.md). This spec is numbered **100**.

## Objective

Prove the first complete production specialist loop beneath Max.

Max identifies an acquisition intelligence need → delegates a bounded investigation to Scout → Scout investigates and returns structured evidence → Max evaluates the result → Acquisition intelligence changes if warranted → Command Deck reflects Max's judgment → the operator can ask why.

Scout does not conduct outreach. Scout investigates acquisition opportunities for Max.

## Vision References

- [Intelligence Architecture](../vision/Intelligence_Architecture.md)
- [SPEC-098 Max Specialist Delegation Contract](SPEC-098_Max_Specialist_Delegation_Contract.md)
- [SPEC-097 Living Command Deck](SPEC-097_Living_Command_Deck.md)
- [SPEC-096 Max Specialist Direction](SPEC-096_Max_Specialist_Direction_and_Operator_Rationale.md)
- [SPEC-060 Prospect Acquisition Framework](SPEC-060_Prospect_Acquisition_Framework.md)
- [SPEC-024 Prospect Discovery Capability](SPEC-024_Prospect_Discovery_Capability.md)

## Problem

SPEC-098 established the contract language. Scout was registered but not callable. Max could not yet run a real specialist loop that produces evidence-backed acquisition intelligence, independent evaluation, AO state, and explainable Command Deck priority.

## Scope

1. Callable Scout `acquisition_intelligence` adapter over SPEC-098
2. Max need-assessment before unnecessary Scout runs
3. Bounded tenant-specific business + target context
4. Observation / inference / unknown separation
5. Structured `AcquisitionOpportunity` results with provenance
6. Max evaluation that does not treat Scout as ground truth
7. Acquisition / AO state updates from accepted material intelligence
8. Max-owned Command Deck priority changes (never Scout-owned)
9. Explainability chain: priority → evaluation → result → delegation → evidence
10. Discuss-with-Max acquisition context + follow-up delegations
11. Duplicate-work / freshness protection
12. Failure, zero-result, and no-broadening behavior
13. Tenant isolation and intelligence-only safety
14. Anchor validation tests

## Out of Scope

- Wiring Faye, Link, Ivy, Emmett, Sam, Cal, or Penny
- Redesigning Scout, AO, CIE, or the Command Deck visual system
- Outreach sequences or external mutations
- Autonomous specialist recursion
- A new prospect database or orchestration framework
- Aji onboarding

## Dependencies

- SPEC-098 specialist delegation contract, registry, evaluation, provenance
- SPEC-097 Command Deck priority (`commandDeckPriority`)
- Existing discovery / prospect / company / evidence structures
- Tenant-scoped `client_id` on companies and prospects

## Architecture

```text
              MAX
               │
        acquisition need?
               │
        /              \
      yes               no
       │                 │
SpecialistDelegation   answer from
       │               durable intel
       ▼
     SCOUT (observe/recommend)
       │
SpecialistResult
  observations ≠ inferences
  unknowns preserved
       │
       ▼
   MAX EVALUATES
    /           \
immaterial     material
    │              │
 retain         update AO
                   │
            priority warranted?
              /           \
            no            yes
            │              │
         retain      Acquisition elevates
                           │
                           ▼
                     COMMAND DECK
```

Scout returns intelligence. Max interprets it. Scout must not mutate Command Deck priority, Max priorities, operator objectives, or campaign priority.

## Data Model

### Scout capability

| Specialist | Capability | Authority | Callable |
|---|---|---|---|
| `scout` | `acquisition_intelligence` | observe, recommend | yes |
| `scout` | `prospect_intelligence` | observe, recommend | no (legacy registry entry) |

### `AcquisitionOpportunity` (artifact on SpecialistResult)

`companyId`, `personIds[]`, `fit`, `timing`, `signals[]`, `observations[]`, `inferences[]`, `unknowns[]`, `evidenceRefs[]`, `confidence`

Reuses company / person / evidence identities. Not a second prospect database.

### `acquisition_intelligence_state`

Tenant-scoped AO snapshot written only by Max after evaluation: summary, opportunity counts, accepted/rejected/unresolved claims, priority impact, evaluation/result/delegation ids.

### Signal taxonomy (reuse, do not invent)

`expansion` · `new_location` · `portfolio_growth` · `hiring` · `leadership_change` · `operational_change` · `vendor_dissatisfaction` · `contract_timing` · `facility_growth` · `service_gap` · `decision_maker`

Unknown remains valid. Do not fabricate vendor timing.

## Implementation

| File | Role |
|---|---|
| `packages/max/scoutAcquisition/` | Need assessment, bounded context, Scout adapter, Max evaluation, AO state, explainability |
| `services/scoutAcquisitionIntelligence.js` | App-level facade |
| `packages/max/workspace/ScoutAcquisitionContext.js` | Max workspace turn handler |
| `migrations/2026-08-16-scout-acquisition-intelligence.sql` | AO state table |

## Testing

- `test/scoutAcquisitionIntelligence.test.js` — contract + critical loop tests
- `packages/max/workspace/tests/scoutAcquisition.test.js` — Max workspace interfaces

## Acceptance Criteria

- [x] Max can create a valid `acquisition_intelligence` Scout delegation
- [x] Scout rejects `draft`, `execute_after_approval`, and `execute`
- [x] Scout receives bounded tenant-specific business context
- [x] Relevant durable intelligence is checked before unnecessary investigation
- [x] Material opportunities contain provenance
- [x] Observed evidence and inferred opportunity are distinguishable
- [x] Missing vendor timing does not become fabricated timing
- [x] Scout returns a valid SPEC-098 result
- [x] Scout result does not automatically become Max belief
- [x] Scout cannot directly mutate Acquisition priority
- [x] Accepted material intelligence can influence Command Deck priority through Max
- [x] Successful Scout execution does not necessarily elevate Acquisition
- [x] Partial / provider failure preserves collected intelligence
- [x] Zero supported opportunities is valid
- [x] Scout does not autonomously change target criteria
- [x] Cross-tenant private context/result access fails
- [x] No outbound capability is invoked
- [x] Priority change traces through Max evaluation → Scout result → delegation → evidence

## Future Work

- [SPEC-099A](SPEC-099A_Scout_Investigation_Provenance.md) — investigation provenance and coverage confidence (implemented)
- [SPEC-100A](SPEC-100A_Scout_Acquisition_Discovery_Foundation.md) — Scout candidate-universe discovery (implemented)
- [SPEC-101](SPEC-101_Max_Specialist_Result_Interrogation.md) — specialist result interrogation and cognitive trace (implemented)
- Run the complete Anchor loop several times before wiring specialist #2 (stop-and-learn gate)
- Incremental adapters for Paige, Penny, Emmett, Sam, Cal
- Optional live Places discovery behind the same intelligence-only adapter
