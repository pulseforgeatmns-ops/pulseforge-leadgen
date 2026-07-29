# SPEC-058 — Diagnostic Capability Behavior

| Field | Value |
|---|---|
| **Status** | In Progress |
| **Target Version** | v1.3.0 |
| **Priority** | High |
| **Owner** | Max / Mission Engine |
| **Created** | 2026-07-28 |

## Objective

Ensure diagnostic capabilities explain why execution cannot proceed instead of failing with opaque boolean precondition checks. Operators asking “Why can’t this run?” receive structured blocked-precondition diagnostics in Mission Workspace.

## Vision References

- [`docs/adr/ADR-042_Diagnostic_Capabilities_Explain_Blocked_Execution.md`](../adr/ADR-042_Diagnostic_Capabilities_Explain_Blocked_Execution.md)
- [`docs/adr/ADR-040_Separate_Evidence_Acquisition_from_Capability_Selection.md`](../adr/ADR-040_Separate_Evidence_Acquisition_from_Capability_Selection.md)
- [`docs/adr/ADR-038_Explain_Planning_Decisions.md`](../adr/ADR-038_Explain_Planning_Decisions.md)
- [`docs/specs/SPEC-056_Evidence_Driven_Capability_Planning.md`](SPEC-056_Evidence_Driven_Capability_Planning.md)
- [`docs/specs/SPEC-023_Capability_Framework.md`](SPEC-023_Capability_Framework.md)

## Problem

Production capabilities correctly prevent invalid execution:

```text
canRun() → false → Capability failed
```

That is correct for **execution**. It is insufficient for **diagnostics**. Returning `false` / `"canRun returned false"` does not answer “Why can’t this run?”

Additionally, empty capability outputs were falling through Artifact Bus payload extraction and fabricating quarantined `ReviewDecision` placeholders.

## Design Principle

| Mode | Responsibility |
|---|---|
| **Execution** | Decide whether work may execute |
| **Diagnostic** | Explain why work cannot execute |

Diagnostics never terminate with opaque boolean failures.

## Scope

- Capability execution modes: `execution` \| `diagnostic`
- Optional `diagnoseCanRun(context)` contract on capabilities
- CapabilityRunner dual-mode precondition handling
- `CAPABILITY_RESULT_STATUS.BLOCKED` for diagnostic blocked preconditions
- MissionExecutor passes `executionMode` from mission intent / diagnostic stages (planning unchanged)
- Mission Workspace **Blocked Preconditions** panel
- No fabricated empty `ReviewDecision` / business artifacts from blocked preconditions
- Campaign Review implements `diagnoseCanRun` as the first capability

## Out of Scope

- Changing Mission Planning / Evidence Planning / Intent Understanding
- Changing execution-mode `canRun` semantics for production runs
- Requiring every capability to implement `diagnoseCanRun` in v1 (optional; Campaign Review required)
- Interactive remediation UI beyond the Blocked Preconditions panel

## Dependencies

- SPEC-023 Capability Framework
- SPEC-034 Campaign Review Workspace
- SPEC-040 / SPEC-042 Artifact validation + bus
- SPEC-055 / SPEC-056 Intent + Evidence Planning (consumes intent/mode; does not modify planners)

## Architecture

```text
Execution mode
  canRun() → false → FAILED (existing gate; structured if diagnoseCanRun present)

Diagnostic mode
  diagnoseCanRun() → runnable:false → BLOCKED + structured diagnostic → Mission Workspace
```

CapabilityRunner resolves mode from:

1. Explicit `context.executionMode`
2. MissionIntent (`diagnostics`, diagnostic categories, investigation mode)
3. Stage / capability `diagnostic: true`

Mission Planning is unchanged — only reporting of blocked preconditions changes.

## Diagnostic Contract

```ts
diagnoseCanRun(context): {
  runnable: boolean
  reason?: string | null
  failedPrecondition?: string | null
  expectedArtifact?: string | null
  actualState?: string | null
  producer?: string | null
  recommendedNextAction?: string | null
}
```

When `runnable: true`, execution proceeds normally.

### Example (Campaign Review blocked)

| Field | Value |
|---|---|
| Status | Blocked |
| Failed Precondition | Campaign artifact required |
| Expected Artifact | Campaign |
| Actual State | Not Present |
| Producer | Campaign Builder |
| Recommended Next Action | Execute Campaign Builder after Discovery succeeds. |

## Artifact Behavior

Diagnostic / blocked precondition results:

- Set `reviewDecision = null` and `reviewPackage = null`
- Publish **no** empty stage produces
- Missing artifacts remain missing
- Empty placeholder artifacts never exist

## Mission Workspace

New **Blocked Preconditions** panel shows:

- Failed precondition
- Expected artifact
- Actual state
- Producer
- Recommended next action

## Implementation Plan

1. Add `CAPABILITY_EXECUTION_MODES` + `CAPABILITY_RESULT_STATUS.BLOCKED`
2. Add `executionMode.js` helpers + CapabilityRunner dual-mode path
3. Campaign Review `diagnoseCanRun` + restored boolean `canRun`
4. MissionExecutor mode derivation; PipelineGate `blocked` outcome
5. Fix ReviewDecision extractPayload fall-through
6. Command Deck Blocked Preconditions panel
7. Tests + SPEC/ADR docs

## Migration Strategy

No schema migration. Backward compatible: capabilities without `diagnoseCanRun` still gate via `canRun`; diagnostic mode falls back to a structured explanation of boolean false.

## Testing

- `packages/capabilities/tests/capabilities.test.js` — Campaign Review diagnoseCanRun / diagnostic mode
- Artifact drafts: empty outputs → no ReviewDecision
- Existing campaign review + evidence planning suites remain green

## Acceptance Criteria

- [x] When required artifacts are missing in diagnostic mode, operator receives failed precondition, expected artifact, actual state, producer, recommended next action — not `canRun returned false`
- [x] No empty ReviewDecision or placeholder artifacts published
- [x] Mission Planning unchanged
- [x] Execution-mode `canRun` semantics unchanged
- [x] Diagnostic mode only affects explanation / blocked reporting

## Future Work

- `diagnoseCanRun` on Mail Package Generator, Direct Mail Execution, Outcome Intelligence
- Dedicated Campaign Diagnostics capability (ADR-040 follow-up)
- Interactive blocked-evidence remediation in Command Deck
