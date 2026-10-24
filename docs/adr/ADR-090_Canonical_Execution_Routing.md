# ADR-090 — Canonical Execution Routing

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Priority** | Critical |
| **Spec** | [SPEC-171](../specs/SPEC-171_Canonical_Execution_Router.md) |
| **Related** | [ADR-069](ADR-069_Classify_Communication_Before_Cognition.md), [ADR-087](ADR-087_Operator_Objective_Takes_Precedence.md), [ADR-089](ADR-089_Mission_Ownership_Shall_Not_Cross_Runtime_Boundaries.md), [ADR-057](ADR-057_Transactional_Mission_Execution.md) |
| **Audits** | AUDIT-046, AUDIT-047, AUDIT-048, [AUDIT-049](../audits/AUDIT-049_Mission_Runtime_Ownership_Crossover.md), AUDIT-050 |

## Context

Over the past several audits, multiple execution failures shared the same root cause despite appearing unrelated.

| Audit | Apparent failure | Shared cause |
|---|---|---|
| AUDIT-046 | Executive intent classified as reflection instead of execution | Surface-local routing |
| AUDIT-047 | Objective resolution diverged from mission planning | Surface-local routing |
| AUDIT-048 | Execution verified a different object than persistence stored | Surface-local routing |
| AUDIT-049 | Discovery crossed runtime ownership boundaries | Surface-local routing |
| AUDIT-050 | Discovery approval never entered the execution pipeline | Surface-local routing |

Each failure originated **before** specialist execution.

The problem was not Scout, Paige, or Emmett.

The problem was inconsistent routing.

## Problem

Today there are multiple paths capable of initiating execution.

Examples include:

- Workspace chat
- Mission Workspace
- Operator approvals
- Command Deck
- REST endpoints
- Future voice interface
- Future API integrations

Each surface contains some combination of:

- intent detection
- routing
- permission checks
- execution decisions
- specialist dispatch

This duplicates business logic across the system.

The result is **execution drift**.

The same operator intent can execute differently depending on where it originated.

## Decision

Execution routing becomes a first-class architectural service.

Every execution-capable surface shall produce a **Canonical Execution Request (CER)**.

Only the **Execution Router** may dispatch specialists.

No UI surface may directly execute business logic.

## New Architecture

```text
Operator
    ↓
Surface
    (Chat · Workspace · Voice · API · Approval Button · Command Deck)
    ↓
Canonical Execution Request
    ↓
Execution Router
    ↓
Mission Runtime
    ↓
Specialist
    ↓
Transactional Commit
    ↓
Presentation
```

Surfaces become producers.

Execution Router becomes executor.

## Canonical Execution Request

Every executable action becomes:

```text
ExecutionRequest {
    id
    source
    missionId
    operatorId
    intent
    stage
    executionMode
    approval
    permissions
    runtimeOwner
    objective
    payload
    metadata
}
```

This object becomes immutable once created.

## Execution Router Responsibilities

The router owns:

- permission validation
- execution eligibility
- runtime ownership
- TME dispatch
- specialist selection
- rollback behavior
- execution auditing

No specialist receives work outside the router.

## Surface Responsibilities

Every surface performs only:

```text
Input
    ↓
Resolve intent
    ↓
Create CER
    ↓
Submit to router
    ↓
Render result
```

Nothing more.

### Approval Buttons

Buttons no longer call `approveDiscovery()`, `approvePlan()`, or `approvePrioritization()`.

Instead they create an `ExecutionRequest` with intent `APPROVE_DISCOVERY` (or the matching approval intent). The router decides what happens.

### Chat

Instead of `approve` causing dozens of routing branches, chat creates the same `ExecutionRequest` with intent `APPROVE_DISCOVERY` as the UI button.

### Voice

Future voice becomes free. Voice simply creates `ExecutionRequest` with intent `START_DISCOVERY`. No additional execution logic exists.

### REST API

Instead of `POST /approve` calling business code, the API creates an `ExecutionRequest` and submits it.

## Runtime Ownership

Router validates `MissionRuntimeOwnership` before dispatch.

[SPEC-170](../specs/SPEC-170_Mission_Runtime_Ownership_Boundaries.md) becomes mandatory.

## Transaction Ownership

Router owns `executeMissionStage()`.

Surfaces cannot call TME directly.

## Specialist Contract

Scout, Paige, Emmett, and Vera never receive UI requests.

They receive only `ExecutionRequest` plus mission context.

## Invariants

1. Every execution enters through one router.
2. Every specialist receives the same execution contract.
3. UI cannot execute business logic.
4. Routing decisions exist in exactly one location.
5. Execution auditing occurs once.
6. Transaction ownership exists once.
7. Runtime ownership is validated before execution.
8. Every execution request receives a unique ID.
9. Every execution request is replayable.
10. Every execution request is auditable.

## Consequences

### Advantages

- Removes duplicate routing logic.
- Makes every execution surface identical.
- Simplifies audits.
- Enables replay of executions.
- Greatly reduces routing drift.
- Makes future interfaces (voice, mobile, API, CLI) almost free.
- Eliminates an entire class of "works here but not there" bugs.

### Trade-offs

- All execution-capable surfaces must migrate to CER.
- Existing handlers become thin adapters.
- Initial refactor is significant, but future development becomes substantially simpler.

## Relationship to Existing ADRs

| ADR | Role |
|---|---|
| [ADR-069](ADR-069_Classify_Communication_Before_Cognition.md) | Communication is classified before cognition |
| ADR-077 | Decisions must be executable |
| [ADR-087](ADR-087_Operator_Objective_Takes_Precedence.md) | Objective Resolution establishes canonical business intent |
| [ADR-089](ADR-089_Mission_Ownership_Shall_Not_Cross_Runtime_Boundaries.md) | Runtime ownership determines where execution occurs |
| **ADR-090 (this ADR)** | Every executable intent is dispatched through one canonical execution router |

## Follow-on Specification

Implemented by [SPEC-171](../specs/SPEC-171_Canonical_Execution_Router.md) — Canonical Execution Router.
