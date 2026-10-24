# AUDIT-049 — Mission Runtime Ownership Crossover

| Field | Value |
|---|---|
| **Date** | 2026-08-25 |
| **Severity** | Critical |
| **ADR** | [ADR-089](../adr/ADR-089_Mission_Ownership_Shall_Not_Cross_Runtime_Boundaries.md) |
| **Spec** | [SPEC-170](../specs/SPEC-170_Mission_Runtime_Ownership_Boundaries.md) |
| **Related** | [ADR-075](../adr/ADR-075_Transactional_Persistence_Exclusivity.md), [ADR-087](../adr/ADR-087_Operator_Objective_Takes_Precedence.md) |

## Symptom

After operator approval, AMO discovery execution fails with:

```text
Unknown mission: mission_<uuid>
```

The mission id is stable across planning, approval, and execution.

## Root Cause

Scout crossed runtime boundaries:

1. AMO created and owned `mission_<uuid>` through TME
2. Workspace passed the AMO mission and a SPEC-022 `missionEngine` into `Scout.discover`
3. Scout called `missionEngine.store.update({ id: mission_<uuid> })`
4. Mission Engine store had no such mission → `Unknown mission`

Identity was preserved. Ownership was not.

## Fix

SPEC-170 enforces runtime ownership at Scout dispatch:

- AMO missions execute Scout without Mission Engine sync
- TME commits discovery contributions inside AMO
- Cross-runtime sync attempts fail with `MISSION_RUNTIME_BOUNDARY_VIOLATION`

## Verification

`spec170MissionRuntimeOwnership.test.js` — AMO discovery completes; crossover rejected.
