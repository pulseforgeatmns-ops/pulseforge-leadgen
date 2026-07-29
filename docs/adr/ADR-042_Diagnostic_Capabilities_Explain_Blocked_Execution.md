# ADR-042 — Diagnostic Capabilities Explain Blocked Execution

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-28 |
| **Spec** | [SPEC-058](../specs/SPEC-058_Diagnostic_Capability_Behavior.md) |
| **Related** | [ADR-040](ADR-040_Separate_Evidence_Acquisition_from_Capability_Selection.md), [ADR-038](ADR-038_Explain_Planning_Decisions.md), [ADR-011](ADR-011_Capability_Framework.md), [ADR-026](ADR-026_Business_Success_Determines_Pipeline_Progress.md) |

## Context

Production capabilities correctly refuse invalid work via `canRun() → false`. Diagnostic missions (e.g. Campaign Diagnostics after Discovery Diagnostics) still need to answer “Why can’t this run?” Opaque `"canRun returned false"` failures do not. Empty outputs also fabricated quarantined `ReviewDecision` placeholders on the Artifact Bus.

## Decision

1. **Diagnostic execution converts blocked capability preconditions into structured explanations rather than opaque boolean failures.**
2. **Production execution determines whether work is permitted** (`canRun` boolean semantics unchanged).
3. **Diagnostic execution explains why work is not permitted** via optional `diagnoseCanRun(context)`.
4. The two behaviors are intentionally distinct (`execution` vs `diagnostic` mode).
5. Blocked diagnostic results use status `blocked`, never fabricate business artifacts, and surface Failed Precondition / Expected Artifact / Actual State / Producer / Recommended Next Action in Mission Workspace.
6. Implementing contract: [SPEC-058 Diagnostic Capability Behavior](../specs/SPEC-058_Diagnostic_Capability_Behavior.md).

## Consequences

### Positive

- Operators get actionable answers when review/execution cannot proceed
- Execution semantics stay fail-closed and unchanged
- No empty ReviewDecision quarantine noise from diagnostic blocks
- Clear boundary between deciding and explaining

### Negative / tradeoffs

- Capabilities must optionally implement `diagnoseCanRun` for best explanations
- MissionExecutor must pass `executionMode` derived from intent/stage without altering planners

### Follow-ups

- [x] CapabilityRunner dual-mode + Campaign Review `diagnoseCanRun`
- [x] Mission Workspace Blocked Preconditions panel
- [ ] Extend `diagnoseCanRun` to remaining pipeline capabilities
- [ ] Campaign Diagnostics capability distinct from Discovery Diagnostics
