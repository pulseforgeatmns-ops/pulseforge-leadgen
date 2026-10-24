# ADR-075 — Transactional Persistence Exclusivity (Completion)

| Field | Value |
|---|---|
| **Status** | Accepted (Completion) |
| **Date** | 2026-08-25 |
| **Priority** | Critical |
| **Spec** | [SPEC-131](../specs/SPEC-131_Transactional_Mission_Execution.md), [SPEC-139](../specs/SPEC-139_Transactional_Durable_Mission_Persistence.md) |
| **Related** | [ADR-057](ADR-057_Transactional_Mission_Execution.md) |
| **Supersedes** | Remaining legacy persistence behavior |

## Context

AUDIT-042, AUDIT-043, and AUDIT-048 demonstrate that mission persistence failures continue to originate from durable contribution drift.

The transactional model is architecturally correct. However, competing durable writers still existed outside the Transactional Mission Execution (TME) boundary. The resulting divergence produced `tme_persistence_verify` — persisted mission does not match committed in-memory mission.

The failure is not caused by mission planning, Objective Resolution, or canonical mission state. It is caused by violations of transactional persistence exclusivity.

## Decision

TME becomes the **exclusive authority** for durable mission state.

No component outside an active transaction may directly mutate:

- `acquisition_missions`
- `acquisition_mission_contributions`
- `acquisition_mission_events`
- `acquisition_mission_observations`
- `acquisition_mission_outcomes`

### Transaction boundary

Only:

```
executeMissionStage() → persistStageCommit()
```

may commit mission state.

Everything else becomes read-only, in-memory, deferred, or rejected.

### Legacy APIs

The following APIs shall **never** produce durable writes:

- `rememberMission()`
- `persistMission()`
- `persistContribution()`
- `persistObservation()`
- `persistOutcome()`
- `runtime.persistSideEffects()`

They remain as compatibility facades. Every durable mutation routes through the transactional persistence contract (`persistStageCommit()`).

Non-TME callers (`create`, `contribute`, `progress`) use `persistMissionState()` which bundles state and commits via `persistStageCommit()` — not row-by-row legacy upserts.

### Cross-process invariant

Transactional ownership is **global**, not process-local.

Multiple Railway instances cannot independently mutate the same mission during an active transaction. PostgreSQL advisory locks (`pg_try_advisory_lock`) enforce exactly one transactional authority per mission.

### New invariant

Exactly one transactional authority may mutate durable mission state.

## Implementation

| Component | Change |
|---|---|
| `TransactionalPersistence.js` | In-process TME guard + cross-process advisory lock acquire/release |
| `TransactionalExecution.js` | Acquire global lock at stage start when `pool` is provided; release in `finally` |
| `acquisitionMissionPersistence.js` | Core table writers reject calls outside `persistStageCommit`; stage commit holds advisory lock |
| `acquisitionMissionRuntime.js` | `rememberMission()` in-memory only; `persistMissionState()` → `persistStageCommit()` |
| `AmoOperatorApproval.js` | Pass `pool` into `executeMissionStage` for global lock lifecycle |

## Verification

Verification compares in-memory state to reloaded durable state. This comparison is only valid if no external writer exists. Do not weaken the verifier (e.g. ignore extra contribution IDs). Fix the competing writer instead.

## Acceptance criteria

- Every durable mission write originates from `persistStageCommit()`.
- No competing writer exists.
- Concurrent execution cannot create orphan contributions.
- `contributionIds` remain identical before and after verification.
- `tme_persistence_verify` cannot be reproduced under concurrent execution.

## Consequences

### Positive

- Mission execution becomes deterministic.
- Transaction rollback represents genuine execution failure rather than persistence drift.
- Mission state becomes globally consistent across all runtimes.
- `contributionIds` and other durable fields stay consistent with TME commits.

### Negative / tradeoffs

- Learning/prediction/evaluation rows remain a separate persistence boundary (not part of stage commit bundle).
- Non-TME paths must acquire the global advisory lock before `persistStageCommit`.

## Migration (completed)

1. Deprecate `runtime.persistSideEffects()` for durable mission writes — suppressed during TME.
2. Replace legacy row upserts with `persistStageCommit()` via `persistMissionState()`.
3. Remove `rememberMission()` durable leg — in-memory only.
4. Guard `persistMission` / `persistContribution` / `persistObservation` / `persistOutcome` — throw `tme_persistence_exclusivity` outside stage commit.
5. Add cross-process advisory lock in `executeMissionStage` and `persistMissionState`.
