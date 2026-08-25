# AUDIT-046 — Operator Objective Routing Failure

| Field | Value |
|---|---|
| **Date** | 2026-08-25 |
| **Status** | Resolved by [ADR-087](../adr/ADR-087_Operator_Objective_Takes_Precedence.md) |
| **Related** | [SPEC-149](../specs/SPEC-149_Message_Type_Classification.md), [ADR-069](../adr/ADR-069_Classify_Communication_Before_Cognition.md) |

## Finding

Compound operator messages that combine a business objective with session modifiers were misrouted. The Message Type Classifier evaluated session configuration signals before resolving the primary business objective.

## Reproduction

```text
Create a production acquisition mission.
Execute autonomously.
Explain your reasoning naturally.
```

**Expected:** `mission_creation` (primary objective) with session modifiers applied  
**Actual:** `session_configuration` — mission creation never reached

## Root causes

1. **Pattern gap** — "Create a production acquisition mission" did not match mission-creation patterns (`create … mission` required adjacent tokens).
2. **Precedence inversion** — whole-message classification returned `session_configuration` when modifier signals were present, even when a primary objective segment existed in the same message.

## Resolution

[ADR-087](../adr/ADR-087_Operator_Objective_Takes_Precedence.md) — resolve primary objective before modifiers; modifiers influence behavior and presentation only.
