# SPEC-155 — Conversation Contract Engine

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Max |
| **Created** | 2026-08-24 |
| **Depends on** | [SPEC-146](SPEC-146_Operator_Cognition_Engine.md), [SPEC-150](SPEC-150_Conversational_State_Machine.md), [SPEC-153](SPEC-153_Operator_Intent_Single_Source_of_Truth.md), [SPEC-154](SPEC-154_Active_Reasoning_Context.md) |
| **Related ADR** | [ADR-062](../adr/ADR-062_Conversation_Contracts_Precede_Ownership.md) |

## Objective

Introduce a **Conversation Contract Engine (CCE)** that establishes the rules of engagement for a conversation **before** ownership is resolved.

Conversation contracts become first-class runtime objects that constrain ownership, reasoning, execution, and presentation.

## Problem

The runtime previously began classifying execution immediately after operator input. Instructions such as *"Don't execute anything during this conversation"* or *"I'm evaluating how you think"* were treated as ordinary text instead of runtime constraints, allowing execution heuristics to override explicit operator intent.

## Design Goal

Conversation rules become runtime state. Ownership reasons from those rules instead of rediscovering them.

## New Pipeline

```text
Raw Operator Message
  → Conversation Contract Engine (SPEC-155)
  → Conversation State
  → Operator Intent (SPEC-153)
  → Active Reasoning Context (SPEC-154)
  → Workspace Ownership
  → Reasoning
  → Presentation
```

Ownership never precedes conversation.

## Conversation Contract

```js
{
  executionAllowed: boolean,
  reasoningMode: 'reflection' | 'explanation' | 'execution' | 'inspection' | 'exploration',
  maintainContext: boolean,
  naturalConversation: boolean,
  explanationDepth: 'brief' | 'standard' | 'deep',
  conversationGoal: string | null,
  locked: boolean,
  confidence: number,
  createdAt: string
}
```

## Modules

| Module | Purpose |
|---|---|
| `packages/max/workspace/ConversationContract.js` | Contract types, detection patterns, session storage |
| `packages/max/workspace/ConversationContractEngine.js` | `resolveConversationContract()` — first step in `WorkspaceEngine.ask` |
| `packages/max/workspace/OperatorIntent.js` | Consumes contract; no longer infers execution/continuity rules |
| `packages/max/workspace/WorkspaceOwnershipResolver.js` | Skips mission ownership when `executionAllowed === false` |
| `packages/max/workspace/MissionRuntimeDispatch.js` | Returns read-only when contract forbids execution |
| `packages/max/workspace/PresentationEngine.js` | Suppresses mission presentation under read-only contract |
| `packages/max/workspace/ConversationalStateMachine.js` | Persists `contract` and `goal` in conversational state |

## Acceptance Tests

`packages/max/workspace/tests/spec155ConversationContract.test.js`

1. **Execution forbidden** — `"Don't execute anything. Let's discuss your reasoning."` → conversation owner, no mission update
2. **Active mission + Why?** — stays conversational under established contract
3. **Contract update** — `"Actually approve discovery."` → `executionAllowed = true`, mission execution allowed
4. **Stay conversational** — follow-ups (`Why?`, `How?`, `What assumption?`, `What if?`) maintain thread
5. **Execute release** — `"Stop theorizing. Execute."` → contract changes, mission runtime allowed

## Runtime Guarantees

- Conversation Contract is created **before** ownership
- Conversation Contract is immutable during a turn
- Ownership consumes Conversation Contract
- No downstream subsystem reinterprets operator conversation rules

## Architectural Principles

Every workspace turn has four independent runtime objects:

| Object | Responsibility |
|---|---|
| Mission State | What is happening in the business? |
| Conversation Contract | What are the rules of this conversation? |
| Operator Intent | What is the operator trying to accomplish? |
| Active Reasoning Context | What are we currently reasoning about? |

Each object has one responsibility and one source of truth. No subsystem recreates another's state.
