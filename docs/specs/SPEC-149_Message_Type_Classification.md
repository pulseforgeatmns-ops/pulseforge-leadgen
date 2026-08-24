# SPEC-149 — Message Type Classification

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Max |
| **Created** | 2026-08-24 |
| **Depends on** | [SPEC-146](SPEC-146_Operator_Cognition_Engine.md), [SPEC-147](SPEC-147_Conversational_Intelligence_Layer.md), [SPEC-148](SPEC-148_Session_State_Manager.md) |
| **Related ADR** | [ADR-069](../adr/ADR-069_Classify_Communication_Before_Cognition.md) |

## Objective

Introduce a **Message Type Classifier (MTC)** that determines the communicative purpose of every operator message before any ownership, reasoning, mission, or conversation pipeline executes.

## Design Principle

**Communication precedes cognition.** Max cannot decide how to think until it understands what kind of communication it received (ADR-069).

## Pipeline Position

```text
Raw Operator Message
  → Message Type Classifier (SPEC-149)
  → Session State Manager (SPEC-148, if SESSION_CONFIGURATION)
  → Session Inspection (SPEC-150, if SESSION_INSPECTION)
  → Conversation Contract Engine (SPEC-155)
  → Operator Intent (SPEC-153)
  → Workspace Ownership
  → Reasoning
  → Presentation
```

## Message Types

| Type | Purpose | Routes to |
|---|---|---|
| `QUESTION` | Operator seeks understanding | Operator Cognition |
| `COMMAND` | Immediate action | Mission Runtime |
| `SESSION_CONFIGURATION` | Persistent operating instructions | Session State Manager (ack only) |
| `SESSION_INSPECTION` | Questions about the current session | Session State Manager (read only) |
| `MISSION_CREATION` | Create new mission | Mission Runtime |
| `MISSION_EXECUTION` | Advance active mission | Mission Runtime |
| `INFORMATION` | Provide facts | Knowledge / Second Brain |
| `FEEDBACK` | Operator feedback | Conversation layer |
| `CORRECTION` | Correct runtime understanding | Conversation repair |
| `APPROVAL` | Approve pending decision | Mission Runtime |
| `REJECTION` | Reject proposal | Reasoning update |
| `SYSTEM_CONFIGURATION` | System-level config | Admin / tenant |
| `UNKNOWN` | Unclassified | Reasoning fallback |

## Modules

| Module | Purpose |
|---|---|
| `packages/max/workspace/MessageType.js` | `MESSAGE_TYPES` enum and classification shape |
| `packages/max/workspace/MessageTypeClassifier.js` | `classifyMessageType()` / `resolveMessageType()` — first step in `WorkspaceEngine.ask` |
| `packages/max/workspace/SessionConfigurationAcknowledgement.js` | Acknowledgement response for `SESSION_CONFIGURATION` turns |
| `packages/max/workspace/SessionStateManager.js` | Session mutations when `mutatesSession` is true |
| `packages/max/workspace/SessionInspectionOperator.js` | Inspection/explanation for `SESSION_INSPECTION` and session-why questions |
| `packages/max/workspace/WorkspaceEngine.js` | Early return for session configuration and inspection; passes classification downstream |
| `packages/max/workspace/SubjectRoutingTrace.js` | Includes `messageType` on every routing trace |

## Acceptance Tests

`packages/max/workspace/tests/spec149MessageTypeClassification.test.js`

1. **Persistent directive** → `SESSION_CONFIGURATION`, no reasoning
2. **Why?** → `QUESTION`, reasoning pipeline executes
3. **Create a mission** → `MISSION_CREATION`
4. **Approved** → `APPROVAL`
5. **Misunderstanding** → `CORRECTION`
6. **Today's session evaluation** → `SESSION_CONFIGURATION`

## Runtime Guarantees

- Every message has exactly one primary Message Type
- Session Configuration bypasses reasoning and ownership
- Session Inspection reads stored Session State and bypasses business reasoning
- Reasoning never classifies communication purpose
- Session mutations occur before downstream pipelines

## Architectural Separation

| Layer | Answers |
|---|---|
| Message Type | What kind of communication is this? |
| Session State | How should Max operate? |
| Session Inspection | How is Max currently operating? |
| Conversation Contract | How should this dialogue behave? |
| Operator Intent | What is requested on this turn? |
| Reasoning | Given all of the above, how should I think? |

## Related Specs

- [SPEC-149A](SPEC-149A_Max_Identity_and_Operating_Model.md) — identity conversation (subject routing)
- [SPEC-149 (Subject Routing)](SPEC-149_Conversation_Subject_Routing.md) — conversation subject detection (complementary layer)
