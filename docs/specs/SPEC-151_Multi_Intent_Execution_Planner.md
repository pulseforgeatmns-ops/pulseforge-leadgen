# SPEC-151 — Multi-Intent Execution Planner (MIEP)

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Depends on** | [SPEC-148](SPEC-148_Session_State_Manager.md), [SPEC-149](SPEC-149_Message_Type_Classification.md), [SPEC-150](SPEC-150_Session_State_Inspection.md), [SPEC-147](SPEC-147_Conversational_Intelligence_Layer.md) |
| **ADR** | [ADR-072](../adr/ADR-072_Operator_Messages_May_Contain_Multiple_Intents.md) |

## Objective

Allow a single operator message to contain multiple independent intents. Max constructs an ordered execution plan and executes each intent deterministically. No compatible intent is discarded.

## Modules

| Module | Role |
|---|---|
| `MultiIntentTypes.js` | `DetectedIntent`, `ExecutionPlan`, `ExecutionResult` vocabulary |
| `IntentExtractor.js` | Segment split + per-segment intent classification |
| `ExecutionPlanner.js` | Dependency-aware plan builder |
| `MultiIntentExecutor.js` | Sequential step execution via `WorkspaceEngine` |
| `WorkspaceEngine.js` | Compound-turn detection; defers single-intent early exits |

## Pipeline Position

```text
Operator Message
  → Message Type Classifier (whole message — informational)
  → Session State Manager (whole message — apply directives)
  → Intent Extraction (SPEC-151)
  → Execution Plan (SPEC-151)
  → [compound] Multi-Intent Executor
  → [single] existing ownership pipeline
```

## Runtime Guarantees

- Multiple compatible intents execute from one message.
- Execution order is deterministic (segment order + dependency repair).
- Human approval contracts remain intact.
- Ownership is per step, not per message.
- Session changes affect downstream steps in the same plan.

## Acceptance Tests

See `packages/max/workspace/tests/spec151MultiIntentExecutionPlanner.test.js`.
