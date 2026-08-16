# SPEC-098 — Max Specialist Delegation Contract

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Pulseforge |
| **Created** | 2026-08-16 |

> **Numbering note:** Client Intelligence Continuity is an earlier workspace seam that appears as “SPEC-098” in `ClientIntelligenceContext.js`. This spec is the Specialist Delegation Contract. Scout wiring is deferred (product brief called that SPEC-099; repository SPEC-099 is [Client Experience Convergence](SPEC-099_Client_Experience_Convergence.md)).

## Objective

Establish the canonical contract through which Max delegates bounded work to specialist capabilities and consumes their results.

Max understands → decides → delegates → specialist works → specialist returns evidence/result → Max evaluates → intelligence state may change → operator can inspect why.

This spec standardizes the **meaning** of delegation and return. It does not replace existing execution mechanisms and does not wire every specialist.

## Vision References

- [Intelligence Architecture](../vision/Intelligence_Architecture.md)
- [SPEC-005 Policy & Decision Engine](SPEC-005_Policy_Decision_Engine.md)
- [SPEC-094 Max to Paige Campaign Content Delegation](SPEC-094_Max_to_Paige_Campaign_Content_Delegation.md)
- [SPEC-095 Max Durable Operator Objectives](SPEC-095_Max_Durable_Operator_Objectives.md)
- [SPEC-096 Max Specialist Direction & Operator Rationale](SPEC-096_Max_Specialist_Direction_and_Operator_Rationale.md)
- [SPEC-097 Living Command Deck](SPEC-097_Living_Command_Deck.md)

## Problem

Specialist integrations evolved independently. Max lacked one tenant-safe language for:

- what work is being delegated
- why it matters now
- under what authority
- what must come back
- how results become evidence rather than ground truth

## Scope

1. Canonical `SpecialistDelegation` schema
2. Canonical `SpecialistResult` schema
3. Durable persistence (delegations, results, Max evaluations)
4. Tenant isolation
5. Authority validation + policy supremacy
6. Lightweight capability registry
7. Max-side creation / consumption interfaces
8. Explainability / provenance chain
9. Deterministic `test_intelligence` fixture
10. Tests for the critical contract

## Out of Scope

- Full Scout integration
- Faye / Link / Ivy / Penny / Emmett / Sam / Cal wiring
- New autonomous outreach or ad spending
- Specialist-to-specialist orchestration
- New Mission Engine or queue infrastructure
- Command Deck visual redesign
- Aji onboarding or CIE changes
- Autonomous recursive planning
- A new universal agent runtime

## Dependencies

- Existing Max workspace (`packages/max/workspace`)
- SPEC-005 tenant policy primitives
- SPEC-095 / SPEC-096 operator direction (authoritative for intent)
- SPEC-097 Command Deck priority (Max-owned; specialists cannot mutate it)
- Existing specialist transports remain underneath adapters

## Architecture

```text
Max
 ↓
SpecialistDelegation  (objective + reason + bounded context + authority)
 ↓
Specialist Adapter    (test_intelligence in v1; Scout later)
 ↓
Existing specialist or fixture
 ↓
SpecialistResult      (observations ≠ actions; evidence; confidence; uncertainty)
 ↓
Max evaluation        (evidence, not ground truth)
 ↓
Optional Max-owned Command Deck priority change
```

Max owns understanding, objectives, prioritization, orchestration, operator interaction, and final interpretation.

Specialists own bounded domains. They receive a bounded envelope, not the entire Max context.

Effective authority:

`Delegation authority ∩ tenant policy ∩ capability policy ∩ platform safety`

Fail closed. Do not silently downgrade an execution request into another path.

## Data Model

### `specialist_delegations`

id, tenant_id, specialist, capability, objective, reason, business_context, target_context, evidence_refs, constraints, authority, expected_return, requested_by, status, policy_events, timestamps

### `specialist_results`

id, delegation_id, tenant_id, specialist, capability, status (`completed` | `partial` | `blocked` | `failed` | `declined_policy`), summary, observations, actions_taken, evidence_refs, artifact_refs, confidence, uncertainties, recommended_next_action, policy_events, errors, timestamps

### `specialist_evaluations`

id, tenant_id, delegation_id, result_id, objective_satisfied, material_change, warrants_operator_attention, warrants_another_delegation, suggested_priority_change, priority_applied, operator_direction_honored, accepted_as_ground_truth (always false), explanation, provenance, payload, created_at

All reads are scoped by server-side `authorizedTenantId`. Client-supplied tenant ids are never trusted for authorization.

## Authority

Canonical levels: `observe` · `recommend` · `draft` · `execute_after_approval` · `execute`

`execute` is never inferred because a specialist technically can execute.

## Capability Registry (v1)

| Specialist | Capability | Authority | Callable |
|---|---|---|---|
| `test_intelligence` | `acquisition_assessment` | observe | yes |
| `scout` | `acquisition_intelligence` | observe, recommend | yes (SPEC-100) |
| `scout` | `prospect_intelligence` | observe, recommend | no (legacy) |
| `paige` | `content_strategy` | observe, recommend, draft | no (SPEC-094 remains) |

Penny, Emmett, Sam, and Cal are not registered until they are callable.

## Implementation

| File | Role |
|---|---|
| `packages/max/specialistDelegation/` | Contract, registry, authority, store, fixture, evaluation, provenance |
| `services/specialistDelegation.js` | App-level facade |
| `packages/max/workspace/SpecialistDelegationContext.js` | Max-side create / consume / explain |
| `migrations/2026-08-16-specialist-delegation.sql` | Durable schema |

## Testing

- `test/specialistDelegation.test.js` — contract + critical tests
- `packages/max/workspace/tests/specialistDelegation.test.js` — Max interfaces

## Acceptance Criteria

- [x] Max can create a bounded delegation with objective, reason, evidence, and explicit authority
- [x] `test_intelligence` returns a structured result with evidence, confidence, and uncertainty
- [x] Max evaluates the result without accepting it as ground truth
- [x] Provenance traces evaluation → result → delegation → evidence
- [x] Tenant isolation
- [x] Missing / unsupported authority fails closed
- [x] Tenant policy conflict fails closed (no silent downgrade)
- [x] Partial evidence survives later failure
- [x] Specialists cannot mutate Command Deck priority
- [x] Operator direction remains authoritative
- [x] Specialist recommendations do not auto-spawn another specialist
- [x] No new universal runtime; no legacy specialist rewrite

## Future Work

- Incremental adapters for Paige, Penny, Emmett, Sam, Cal
- Optional Max apply of suggested Command Deck priority via existing `commandDeckPriority`
