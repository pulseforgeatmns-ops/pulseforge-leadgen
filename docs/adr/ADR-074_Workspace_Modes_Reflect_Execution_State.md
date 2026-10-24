# ADR-074 — Workspace Modes Shall Reflect Execution State

## Status

Accepted — August 2026

## Context

Workspace mode previously mapped coarse lifecycle stages to UI render groups. After plan approval, the runtime set `pendingOperatorDecision = discovery_approval` but workspace mode showed **Discovery**, implying Scout was executing. Execution had not entered Discovery; the runtime was waiting for operator approval.

## Decision

Workspace mode is derived from **execution state**, not the next lifecycle stage.

| Execution state | Workspace mode |
|---|---|
| Plan clarification or plan approval pending | `mission_planning` |
| Discovery approval pending | `discovery_approval` |
| Scout dispatched, no artifact yet | `discovery_running` |
| Discovery artifact present, prioritization pending | `discovery_review` |
| Outreach planning | `outreach` |
| Execute / Ready | `execution` |

Implementation lives in `packages/acquisition-mission/MissionProgression.js` (progression stage) and `packages/acquisition-mission/WorkspaceMode.js` (UI mode mapping).

## Consequences

- Operator decision panel is visible in `discovery_approval` mode.
- Discovery panel spans approval, running, and review modes.
- Legacy aliases `understanding` → `mission_planning` and `discovery` → `discovery_approval` remain on exported constants for backward compatibility.

## Related

- SPEC-153 — Mission Workspace Modes
- SPEC-157 — Autonomous Discovery Approval Policy
