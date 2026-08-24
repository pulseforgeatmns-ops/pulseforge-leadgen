# ADR-067 — Stage Contracts Are Authoritative

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-24 |
| **Spec** | [SPEC-147](../specs/SPEC-147_Conversational_Intelligence_Layer.md) (autonomous progression), `packages/acquisition-mission/MissionProgression.js` |
| **Related** | SPEC-147 autonomous progression (`MissionProgression.js`), [ADR-058](ADR-058_Pending_Operator_Decision_Matches_Execution.md), [ADR-057](ADR-057_Transactional_Mission_Execution.md), [ADR-016](ADR-016_Execution_Does_Not_Decide.md) |

## Context

Mission progression determines whether execution should continue automatically or pause for operator judgment.

Previous implementations relied on the successful creation of presentation-oriented objects (for example, `MissionPause`) to determine whether execution should stop. This creates an undesirable dependency between execution control and presentation.

If a pause object cannot be constructed due to inconsistent runtime state, execution may continue despite the Stage Contract requiring operator approval. This violates ADR-066 and weakens the safety guarantees of the Mission Runtime.

## Decision

1. Mission progression shall be governed exclusively by Stage Contracts (`MISSION_STAGE_CONTRACTS`).
2. The Stage Contract is the authoritative source for:
   - whether a stage executes automatically,
   - whether human judgment is required,
   - whether execution may continue,
   - when a transition is permitted.
3. Presentation objects (such as `MissionPause`) explain runtime decisions but never determine them.
4. Progression performs contract validation before every automatic stage transition.

### Runtime Order

```text
Current Stage
  ↓
Stage Contract
  ↓
Requires Human Decision?
  ↓ YES
Execution pauses
  ↓
Build MissionPause
  ↓
MissionPause available?
  ↓ YES → Present explanation
  ↓ NO  → Create ExecutionBlock (pauseFallback)
  ↓
Return paused
```

Execution must never continue solely because a presentation object could not be created.

## Implementation

- `resolveHumanDecisionGate(snapshot)` — contract-first gate; returns `shouldPause`, `pause`, and optional `block` fallback.
- `resolveProgressionState(snapshot)` — authoritative progression outcome for inspect and presentation.
- `validateStageTransition(fromStage, toStage)` — contract validation before automatic transitions.
- `ExecutionBlock.pauseFallback = true` — marks blocks created when pause metadata is unavailable; presentation renders as **Mission Paused**, not **Mission Blocked**.

## Consequences

### Positive

- Execution control becomes deterministic.
- Runtime safety no longer depends on presentation state.
- Stage Contracts become the single source of truth.
- Missing pause metadata results in an explicit execution block rather than accidental progression.
- Human approval boundaries cannot be bypassed by presentation failures.

### Negative / tradeoffs

- `ExecutionBlock` becomes a required fallback whenever pause construction fails.
- Progression performs one additional contract validation before every transition.

## Architectural Principle

**Stage Contracts govern behavior. Presentation communicates behavior. Presentation objects must never determine runtime execution.**
