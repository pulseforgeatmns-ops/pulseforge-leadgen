# ADR-087 — Operator Objective Takes Precedence

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Related** | [AUDIT-046](../audits/AUDIT-046_Operator_Objective_Routing.md), [ADR-069](ADR-069_Classify_Communication_Before_Cognition.md), [ADR-072](ADR-072_Operator_Messages_May_Contain_Multiple_Intents.md), [SPEC-149](../specs/SPEC-149_Message_Type_Classification.md) |
| **Supersedes** | Implicit routing assumptions in [ADR-069](ADR-069_Classify_Communication_Before_Cognition.md) |

## Context

Operators frequently communicate:

- a business objective,
- execution constraints,
- reasoning preferences,
- conversation preferences,

within the same message.

Historically, PulseForge classified messages using fixed-precedence message types. This allowed execution modifiers (autonomous execution), reasoning preferences ("explain your reasoning"), and session directives to displace the operator's actual objective.

AUDIT-046 demonstrated this failure.

**Example:**

```text
Create a production acquisition mission.
Execute autonomously.
Explain your reasoning naturally.
```

was classified as `session_configuration` instead of `mission_creation`.

## Decision

PulseForge shall resolve the operator's **primary business objective** before considering execution, reasoning, or presentation preferences.

Operator messages are treated as executive directives composed of:

| Layer | Role |
|---|---|
| **Primary Objective** | Determines routing |
| **Supporting Objectives** | May add steps to compound execution plans |
| **Execution Modifiers** | Influence execution behavior (e.g. autonomous, read-only) |
| **Conversation Modifiers** | Influence presentation (e.g. natural reasoning style) |

**Routing shall always be determined by the Primary Objective.**

Modifiers may influence execution behavior or presentation. Modifiers shall never replace routing.

## Precedence

```text
Operator Message
  ↓
Resolve Primary Objective (routing)
  ↓
Collect Execution / Conversation Modifiers (behavior + presentation)
  ↓
Message Type = Primary Objective
  ↓
Session State Manager applies modifiers (if present)
  ↓
Route to Primary Objective pipeline
```

## Invariant

**Routing shall always be determined by the resolved Primary Objective.**

Execution constraints modify execution. Presentation constraints modify presentation. Neither modifies routing.

## Consequences

- Execution routing becomes objective-driven rather than message-type-driven
- Compound executive messages are supported naturally
- Conversation preferences become modifiers instead of routing decisions
- Future capabilities can introduce new modifiers without changing routing precedence
- Mission execution can no longer be displaced by reflection, identity, session configuration, or formatting preferences

## Implementation

| Module | Change |
|---|---|
| `packages/max/workspace/MessageTypeClassifier.js` | Primary objective resolved before session configuration; modifier evidence attached without displacing routing |
| `packages/max/workspace/IntentExtractor.js` | Segment classification checks mission creation before session modifiers |
| `packages/max/workspace/tests/adr087OperatorObjectivePrecedence.test.js` | AUDIT-046 regression and modifier coexistence tests |
