# SPEC-150 — Session State Inspection

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Max |
| **Created** | 2026-08-24 |
| **Depends on** | [SPEC-148](SPEC-148_Session_State_Manager.md), [SPEC-149](SPEC-149_Message_Type_Classification.md), [SPEC-149A](SPEC-149A_Max_Identity_and_Operating_Model.md) |
| **Related ADR** | [ADR-070](../adr/ADR-070_Session_State_Is_Inspectable.md) |

## Objective

Allow Max to inspect and explain the current Session State instead of reconstructing it through reasoning.

Questions about the current operating session shall read Session State directly.

## Problem

SPEC-148 introduced persistent Session State. Session configuration now works correctly.

Example:

```text
Operator
Operate according to your role.
↓
Session Updated
Operating Mode
Business Operation
```

Subsequent inspection questions did not consult Session State.

```text
Operator
What operating mode are you currently using?
Current
Business advisory
↓
Commercial acquisition recommendation
Expected
Current Session
Operating Mode
Business Operation
```

## Design Principle

**Configuration is written once. Inspection reads it directly.**

Reasoning must never reconstruct explicit Session State.

## Runtime Position

```text
Operator Message
  → Message Type
  → Operator Cognition
  → Session Inspection
  → Session State Manager (READ)
  → Response
```

No business reasoning. No ownership. No mission routing.

## Session Inspection Operator

Introduce `SESSION_INSPECTION`.

**Purpose:** Answer questions about the current session.

### Detection examples

- What operating mode are you using?
- What mode are you currently in?
- What are your current session settings?
- How are you operating right now?
- What execution policy are you following?
- What conversation style is active?
- What reasoning mode is active?
- Summarize the current session.

## Session Read API

Expose `SessionStateManager.getCurrentState(session)`.

Returns:

```js
SessionState {
  operatingMode;
  executionPolicy;
  reasoningMode;
  conversationStyle;
  evaluationMode;
  activeObjective;
  sessionStarted;
  lastUpdated;
}
```

Inspection never reconstructs these values.

## Response Contract

```text
Operator
What operating mode are you using?
Response
Current Session
Operating Mode
Business Operation
Execution Policy
Normal
Reasoning Mode
Analytical
Conversation Style
Natural
Evaluation Mode
Max Operating Model
```

## Follow-up Questions

```text
Operator
Why are you using that operating mode?
```

```text
Question → Reasoning → Session State → Explain
```

**First question** — *What are you using?* → Inspection

**Second question** — *Why are you using it?* → Reasoning

Inspection and reasoning remain separate.

## Ownership

`SESSION_INSPECTION` bypasses Mission Runtime, Scout, Business Advisory, and Mission Ownership.

**Owner:** Session State Manager

## Acceptance Tests

`packages/max/workspace/tests/spec150SessionStateInspection.test.js`

1. **Stored state** — "Operate according to your role." then "What operating mode are you using?" returns stored Session State
2. **Execution policy** — "Don't execute anything." then "What execution policy is active?" → Read Only
3. **Reasoning mode** — "Explain your reasoning naturally." then "What reasoning mode is active?" returns stored reasoning mode
4. **Summary** — "Summarize the current session." returns a complete Session State summary
5. **Regression** — inspection must not create a mission, invoke Scout, invoke business advisory, or produce acquisition recommendations
6. **Why follow-up** — "Why are you using that operating mode?" executes reasoning, uses Session State as evidence, and does not infer operating mode

## Runtime Guarantees

- Session inspection reads stored state
- Explicit Session State always overrides inference
- Inspection bypasses business reasoning
- Configuration and inspection remain symmetrical
- Session State becomes observable
- Any runtime component that mutates persistent Session State exposes that state through `getCurrentState`

## Architectural Separation

| Layer | Answers |
|---|---|
| Message Type | What kind of communication is this? |
| Session Configuration | How should Max operate? |
| Session Inspection | How is Max currently operating? |
| Reasoning | Why is Max operating that way? |

## Modules

| Module | Purpose |
|---|---|
| `packages/max/workspace/SessionInspectionOperator.js` | Detect inspection vs why-explanation; compose inspection and explanation responses |
| `packages/max/workspace/SessionStateManager.js` | `getCurrentState(session)` — the shared read interface |
| `packages/max/workspace/MessageType.js` | `SESSION_INSPECTION` type |
| `packages/max/workspace/MessageTypeClassifier.js` | Classify inspection before question/configuration fallback |
| `packages/max/workspace/WorkspaceEngine.js` | Early return for inspection; explanation after cognition |
