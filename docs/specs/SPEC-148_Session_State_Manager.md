# SPEC-148 — Session State Manager

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Max |
| **Created** | 2026-08-24 |
| **Depends on** | [SPEC-146](SPEC-146_Operator_Cognition_Engine.md), [SPEC-155](SPEC-155_Conversation_Contract_Engine.md) |
| **Related ADR** | [ADR-068](../adr/ADR-068_Session_State_Is_Explicit.md) |

## Objective

Introduce a **Session State Manager (SSM)** that maintains persistent operating state throughout a Workspace conversation. Session State governs how Max operates, independent of any individual prompt.

## Problem

Long-lived operator instructions such as *"For the rest of this conversation… Don't execute anything. Answer naturally."* were treated as one-turn conversational input. By turn 2, Max forgot and fell back to advisory mode.

## Design Principle

**Conversation is transient. Session State is persistent.**

Session behavior shall never be inferred solely from recent prompts (ADR-068).

## Pipeline Position

```text
Raw Operator Message
  → Session State Manager (SPEC-148)
  → Conversation Contract Engine (SPEC-155)
  → Operator Intent (SPEC-153)
  → Workspace Ownership
  → Reasoning
  → Presentation
```

## Session State

```js
{
  operatingMode: 'business_operation' | 'reasoning_evaluation' | 'mission_execution' | …,
  executionPolicy: 'normal' | 'read_only' | 'autonomous' | 'operator_approval_required' | 'execution_disabled',
  reasoningMode: 'natural' | 'think_aloud' | 'concise' | 'teaching' | 'analytical' | 'reflective',
  conversationStyle: 'natural' | 'technical' | 'executive' | 'specification' | 'brainstorm',
  evaluationMode: 'max' | 'scout' | 'mission_runtime' | 'business' | 'none',
  activeObjective: string | null,
  activeConversation: string | null,
  activeReasoningGoal: string | null,
  operatorPreferences: object,
  expires: string | null
}
```

## Modules

| Module | Purpose |
|---|---|
| `packages/max/workspace/SessionState.js` | Types, constants, session get/set helpers |
| `packages/max/workspace/SessionStateManager.js` | `resolveSessionState()` — first step in `WorkspaceEngine.ask` |
| `packages/max/workspace/ConversationContractEngine.js` | Applies session execution policy to contract |
| `packages/max/workspace/OperatorIntent.js` | Consumes session state; blocks execution when read-only |
| `packages/max/workspace/MissionRuntimeDispatch.js` | Returns read-only when session forbids execution |
| `packages/max/workspace/IdentityConversationContext.js` | Session inspection from stored state |

## Acceptance Tests

`packages/max/workspace/tests/spec148SessionStateManager.test.js`

1. **Execution disabled persists** — five turns later execution still disabled
2. **Reasoning mode persists** — analytical reasoning survives follow-ups
3. **Session inspection** — returns stored state, not inference
4. **Mode change** — immediate update with history record
5. **Mission runtime blocked** — read-only session prevents execution

## Runtime Guarantees

- Persistent operator directives are never forgotten within a session
- Session behavior is explicit and inspectable
- Every downstream subsystem receives identical session context
- Session State outlives individual prompts until changed or reset
