# SPEC-169 — Canonical Mission Verification

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | Critical |
| **Owner** | Max |
| **Created** | 2026-08-25 |
| **Depends on** | [ADR-075](../adr/ADR-075_Transactional_Persistence_Exclusivity.md) Completion, [ADR-087](../adr/ADR-087_Operator_Objective_Takes_Precedence.md), SPEC-168 Canonical Objective Resolution |
| **ADR** | [ADR-088](../adr/ADR-088_Canonical_Mission_Projection_Is_The_Verification_Contract.md) |

## Objective

Replace subset-based persistence verification with canonical mission verification.

Verification shall validate the complete canonical mission contract.

## Problem

Current architecture:

```text
Persist
        ↓
Entire Mission Bundle
Verification
ComparableMissionState()
        ↓
10 manually selected fields
```

AUDIT-048 demonstrated that many persisted fields are excluded from verification:

- `resolvedObjective`
- `executionPolicy`
- `communicationPolicy`
- `evaluationPolicy`
- `objective`
- `events`

Persistence and verification no longer represent the same contract.

## Philosophy

Persist what you verify.

Verify what you persist.

There shall be exactly one canonical mission representation used for transaction verification.

## Architecture

Introduce:

`CanonicalMissionProjection`

Every mission produces a canonical verification projection.

## Canonical Projection

```text
interface CanonicalMissionProjection {
    mission
    structuredMission
    resolvedObjective
    executionPolicy
    communicationPolicy
    evaluationPolicy
    pendingOperatorDecision
    contributions
    observations
    outcomes
    events
}
```

The projection excludes only derived or ephemeral runtime state.

Examples:

- timeline
- workspace presentation
- computed health
- cached inspection data

## Verification Flow

### Current

```text
Persist
        ↓
Reload
        ↓
ComparableMissionState()
        ↓
JSON equality
```

### New

```text
Persist
        ↓
Reload
        ↓
CanonicalMissionProjection(memory)
        ↓
CanonicalMissionProjection(database)
        ↓
Structural Equality
```

## Projection Builder

Introduce a single projection builder.

`buildCanonicalMissionProjection()`

Persistence verification shall consume only this projection.

No duplicate comparator logic.

`assertPersistedMatchesEngine()` builds the memory snapshot from the engine store (`snapshotFromEngine`), not from `inspect()`. Presentation is not the verification contract.

## Drift Prevention

Future mission fields automatically participate in verification when added to the canonical mission model.

Verification shall no longer require manual comparator maintenance.

A field such as `organizationalPlan` on the mission record appears in `projection.mission` without changes to `assertPersistedMatchesEngine`.

## Diagnostics

When verification fails, return:

```text
Canonical Projection Diff
        ↓
Field
Memory
Persisted
Reason
First Divergence
```

instead of a generic bundle mismatch.

Error code remains `tme_persistence_verify`. `err.details` includes `firstDivergence`, `field`, `memory`, `persisted`, `reason`, and the full leaf `diff`.

## New Invariant

Transaction verification shall compare canonical mission projections, not manually curated subsets.

## Implementation

| Component | Change |
|---|---|
| `packages/acquisition-mission/CanonicalMissionProjection.js` | Projection builder, engine snapshot, structural diff |
| `services/acquisitionMissionPersistence.js` | `assertPersistedMatchesEngine` consumes only the canonical projection |
| `packages/acquisition-mission/tests/spec169.test.js` | Acceptance scenarios 1–5 |

## Acceptance Criteria

- [x] Scenario 1 — Mission Plan approval: canonical projections are identical.
- [x] Scenario 2 — Discovery approval: canonical projections are identical.
- [x] Scenario 3 — Prioritization approval: canonical projections are identical.
- [x] Scenario 4 — A future mission field (for example `organizationalPlan`) automatically participates in verification through the canonical projection. No comparator modifications are required elsewhere.
- [x] Scenario 5 — A persistence mismatch produces a structured projection diff showing first divergent field, memory value, persisted value, and divergence reason.

## Testing

`node --test packages/acquisition-mission/tests/spec169.test.js`

Also `npm run test:amo` to confirm SPEC-139 / ADR-075 persistence tests still pass.
