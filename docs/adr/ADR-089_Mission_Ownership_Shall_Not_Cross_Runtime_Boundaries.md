# ADR-089 — Mission Ownership Shall Not Cross Runtime Boundaries

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Priority** | Critical |
| **Spec** | [SPEC-170](../specs/SPEC-170_Mission_Runtime_Ownership_Boundaries.md) |
| **Related** | [ADR-075](ADR-075_Transactional_Persistence_Exclusivity.md), [ADR-087](ADR-087_Operator_Objective_Takes_Precedence.md), [AUDIT-049](../audits/AUDIT-049_Mission_Runtime_Ownership_Crossover.md) |

## Context

AUDIT-049 demonstrated that Acquisition Mission execution correctly creates, persists, approves, and manages mission state inside the Acquisition Mission Runtime (SPEC-118).

Discovery execution later fails with:

```text
Unknown mission: mission_<uuid>
```

The mission identifier itself remains unchanged throughout execution.

The failure occurs because Scout crosses into a different mission runtime and attempts to interpret an Acquisition Mission identifier as a SPEC-022 Mission Engine identifier.

The architectural failure is not identity loss. It is ownership loss.

## Problem

Current execution:

```text
Acquisition Mission Runtime
        │
        ▼
Scout
        │
        ▼
SPEC-022 Mission Engine
        │
Unknown Mission
```

The Acquisition Mission Runtime owns the mission. The Mission Engine does not. Scout crosses ownership boundaries.

## Decision

Mission ownership shall never cross runtime boundaries.

A specialist executes inside the runtime that owns the mission.

Mission identifiers shall never be translated between independent mission systems.

### Runtime Authority

The owner of the mission determines:

- mission lifecycle
- mission state
- mission persistence
- transaction boundaries
- specialist execution
- mission identity

Specialists execute within that authority. They do not establish their own.

### Identity Rule

Mission identity is contextual.

The identifier `mission_c1b6003f...` is meaningful only inside the Acquisition Mission Runtime. It shall never be interpreted as a SPEC-022 mission identifier.

### Synchronization

Mission synchronization between runtime implementations is prohibited.

Execution authority shall not depend on mirrored mission state.

### New Invariant

A mission shall be executed only by the runtime that owns it.

## Consequences

- Scout becomes runtime-agnostic at the contract layer but runtime-bound at execution
- Mission ownership becomes explicit (`runtimeOwner`, `resolveMissionRuntimeOwner`)
- Identity translation disappears
- Unknown mission errors caused by runtime crossover become impossible
- AMO discovery commits exclusively through TME (`advanceDiscoveryAfterApproval` → `executeMissionStage`)

## Acceptance Criteria

- Acquisition Missions execute entirely inside the Acquisition Mission Runtime
- Specialists never resolve Acquisition Mission IDs through the SPEC-022 Mission Engine
- Mission ownership remains constant throughout execution
- Runtime boundaries are explicit and enforced
