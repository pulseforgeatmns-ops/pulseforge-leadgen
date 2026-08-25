# ADR-088 — Canonical Mission Projection Is The Verification Contract

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Spec** | [SPEC-169](../specs/SPEC-169_Canonical_Mission_Verification.md) |
| **Related** | [ADR-075](ADR-075_Transactional_Persistence_Exclusivity.md), SPEC-139 Transactional Durable Mission Persistence, SPEC-168 Canonical Objective Resolution, [ADR-087](ADR-087_Operator_Objective_Takes_Precedence.md) |
| **Supersedes** | Subset comparator `comparableMissionState()` (ten manually selected fields) |

## Context

ADR-075 made `persistStageCommit()` the exclusive durable writer. Verification still compared a hand-picked subset of mission fields (`id`, `version`, `stage`, `status`, `structuredMissionApproved`, `structuredMission`, `missionPlanDraft`, `pendingOperatorDecision`, `lastTransactionId`, `contributionIds`).

AUDIT-048 showed that persisted canonical fields — `resolvedObjective`, `executionPolicy`, `communicationPolicy`, `evaluationPolicy`, `objective`, `events` — were written to the bundle and never verified. Persistence and verification had drifted into different contracts.

## Decision

There is exactly one canonical mission representation for transaction verification: `CanonicalMissionProjection`.

```text
Persist → Reload → CanonicalMissionProjection(memory)
                 → CanonicalMissionProjection(database)
                 → Structural equality
```

`buildCanonicalMissionProjection()` is the only builder. Persistence verification consumes only this projection. Derived presentation (timeline, workspace, health, cached inspection) is excluded. Future durable mission fields participate automatically via the mission record.

When projections differ, the verifier returns a structured diff: field path, memory value, persisted value, reason, and first divergence.

## Consequences

### Positive

- Persist what you verify; verify what you persist.
- SPEC-168 canonical objective fields cannot silently fail to round-trip.
- Comparator maintenance is no longer a manual field list.

### Negative / tradeoffs

- Verification is stricter. Shape mismatches that the subset comparator ignored (event order, observation presentation fields) must be canonicalized, not ignored.

### Follow-ups

- [SPEC-169](../specs/SPEC-169_Canonical_Mission_Verification.md)
