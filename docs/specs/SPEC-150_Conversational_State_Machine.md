# SPEC-150 — Conversational State Machine

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Max |
| **Created** | 2026-08-23 |
| **Depends on** | [SPEC-146](SPEC-146_Operator_Cognition_Engine.md) (operator intent), [SPEC-149](SPEC-149_Conversation_Subject_Routing.md) (subject before intent), [SPEC-147](SPEC-147_Conversational_Intelligence_Layer.md) (natural mission prose) |

## Objective

Add **conversation continuity** — the missing pillar alongside mission, business, knowledge, and specialist continuity.

Without it, every turn is a stateless REST API. With it, Max behaves like a person in a persistent conversation.

## Philosophy

| Continuity type | Answers |
|---|---|
| Mission continuity | Where are we in the mission? |
| Business continuity | What do we know about the business? |
| Knowledge continuity | What evidence and beliefs persist? |
| Specialist continuity | What did Scout/Paige last produce? |
| **Conversation continuity** | **What are we talking about?** |

This is **not** a memory system. It is a **conversation system** — session-scoped state that carries subject, owner, active object, mode, and depth across turns.

## Problem

Today each turn runs fresh classification:

1. `detectConversationSubject(question)` — from scratch
2. `classifyOperatorCognition(question)` — from scratch
3. Owner resolution — from scratch

A bare follow-up like `Why?` after an identity discussion classifies as default business subject and generic explain — losing the thread. `How is that different from Scout?` may classify as explain instead of compare.

## Scope

Session-scoped conversational state on every turn:

```js
{
  subject: 'identity',
  owner: 'conversation_identity',
  activeObject: 'max',
  mode: 'explanation',
  depth: 2,
  objects: ['max'],
  lastQuestion: 'What is your role?',
  lastIntent: 'explain',
  lastResolvedQuestion: 'What is your role?'
}
```

Follow-up resolution (not re-classification from scratch):

| Prior context | Operator says | Resolved |
|---|---|---|
| subject=identity, activeObject=max | `Why?` | subject=identity, intent=explain, question=`why(identity)` |
| subject=identity, activeObject=max | `How is that different from Scout?` | subject=identity, intent=**compare**, objects=[max, scout] — **not** explain |

Continuity rules:

- Bare follow-ups (`Why?`, `How?`, pronoun references) inherit subject and owner from prior state
- Compare patterns always resolve to `compare` intent, never explain
- Explicit subject changes (`What is our ICP?`, `Who are you?`) reset continuity
- Depth increments on each turn within the same subject; resets on subject change

## Out of Scope

- Durable cross-session conversation memory (SPEC-032 vision)
- LLM-based coreference resolution
- Replacing mission runtime state or business understanding retrieval

## Architecture

```text
Operator message
  → detectConversationSubject (raw)
  → classifyOperatorCognition (raw)
  → applyConversationalContinuity (SPEC-150)
       inherit subject / intent / resolvedQuestion when follow-up
  → resolveWorkspaceOwner (uses continuity-adjusted subject)
  → pipeline handler
  → advanceConversationalState (persist for next turn)
```

Module: `packages/max/workspace/ConversationalStateMachine.js`

Integration: `WorkspaceEngine.ask` — continuity applied before ownership; state advanced on every return via `traceAskReturn`.

Routing trace extended with: `activeObject`, `mode`, `depth`, `resolvedQuestion`, `continuity`.

## Data Model

No new tables. Session JSONB field:

```js
session.context.conversationalState = {
  subject, owner, activeObject, mode, depth, objects,
  lastQuestion, lastIntent, lastResolvedQuestion, confidence, updatedAt
}
```

Ephemeral per-turn fields:

```js
session.context.resolvedQuestion   // e.g. why(identity), compare(max,scout)
session.context.conversationContinuityApplied
```

## Testing

- `packages/max/workspace/tests/spec150ConversationalStateMachine.test.js`

## Acceptance Criteria

- [x] Multi-turn identity conversation maintains subject=identity across bare follow-ups
- [x] `Why?` after identity resolves to intent=explain, question=`why(identity)` — not re-classified from scratch
- [x] `How is that different from Scout?` resolves to intent=compare with objects=[max, scout] — not explain
- [x] Explicit topic changes reset depth and adopt new subject
- [x] Continuity follow-ups route to inherited owner (identity stays on IdentityConversation)
- [x] Routing trace exposes activeObject, mode, depth, resolvedQuestion, continuity flag

## Related Specs

- SPEC-103C — Active conversational reasoning (recommendation/plan follow-ups within business context)
- SPEC-147 — Conversational Intelligence Layer (mission prose, not state machine)
- SPEC-149 — Subject routing (subject detected first; SPEC-150 inherits on follow-ups)
