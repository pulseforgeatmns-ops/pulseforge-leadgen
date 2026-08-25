# ADR-075 — Transactional Persistence Exclusivity

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Spec** | [SPEC-131](../specs/SPEC-131_Transactional_Mission_Execution.md), [SPEC-139](../specs/SPEC-139_Transactional_Durable_Mission_Persistence.md) |
| **Related** | [ADR-057](ADR-057_Transactional_Mission_Execution.md) |

## Problem

Mission state is persisted through two independent durability paths:

**Transactional**

```
executeMissionStage() → commit() → persistStageCommit() → verify() → COMMIT
```

**Legacy**

```
runtime.persistSideEffects() → rememberMission() → Postgres
```

Both mutate durable mission state. This violates the transactional execution model.

When verification compares in-memory → persist → reload → compare, a competing writer produces mismatches on fields like `contributionIds` even when stage, status, version, and transaction id all match. That is evidence of an external writer — not a verifier bug.

## Decision

Mission state shall have exactly **one** durable writer: `persistStageCommit()`.

All other persistence paths become read-only or are removed during transactional mission stages.

### Runtime rule

The following shall **never** write durable mission state during an active TME transaction:

- `persistSideEffects()`
- `rememberMission()` (durable leg)
- Runtime persistence helpers
- Legacy mission persistence

If side effects are required during TME, queue them in the in-memory store. Do not persist them until `persistStageCommit()`.

### Architectural invariant

```
One transaction → One commit → One durable representation
```

Never:

```
Transaction → Legacy writer → Verifier
```

## Implementation

| Component | Change |
|---|---|
| `TransactionalPersistence.js` | TME guard — `beginTmeTransaction` / `endTmeTransaction` / `shouldSuppressLegacyDurableWrite` |
| `TransactionalExecution.js` | Wraps `executeMissionStage` with guard lifecycle |
| `acquisitionMissionRuntime.js` | Legacy writers no-op during TME; `persistMissionState()` routes non-TME writes through `persistStageCommit()` |

## Verification

Verification compares in-memory state to reloaded durable state. This comparison is only valid if no external writer exists. Do not weaken the verifier (e.g. ignore extra contribution IDs). Fix the competing writer instead.

## Consequences

### Positive

- `contributionIds` and other durable fields stay consistent with TME commits.
- Failed in-memory rollbacks are not contradicted by orphan legacy writes.
- Hydration reloads match what the transaction believed it committed.

### Negative / tradeoffs

- Non-TME paths (`contribute`, `progress`, mission create) now use `persistStageCommit` instead of row-by-row legacy upserts.
- Learning rows remain a separate persistence boundary (not part of stage commit bundle).

## Migration

1. Deprecate `runtime.persistSideEffects()` for durable mission writes.
2. Replace with `persistStageCommit()` via `persistMissionState()`.
3. Defer autonomous mission create durable writes until TME stages commit.
