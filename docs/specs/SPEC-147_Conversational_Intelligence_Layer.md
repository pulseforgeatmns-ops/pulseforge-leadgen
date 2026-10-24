# SPEC-147 — Conversational Intelligence Layer

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Max |
| **Created** | 2026-08-23 |
| **Depends on** | [SPEC-118](SPEC-118_Acquisition_Mission_Orchestration.md), [SPEC-146](SPEC-146_Operator_Cognition_Engine.md) (operator cognition + execution guard), [SPEC-121](SPEC-121_Mission_Oriented_Communication.md) (structured mission cards for execution turns) |

## Objective

Max should communicate like an experienced operator, not a workflow engine. The mission engine owns truth; the Conversation Layer owns communication.

## Philosophy

| Layer | Owns |
|---|---|
| Mission engine | Truth — stage, evidence, blockers, pending decisions |
| Conversation Layer | Communication — natural prose, memory, progressive disclosure |

## Problem

Today, read-only mission turns still render as SPEC-121 status cards (`Mission Updated`, `Status`, `Stage`, …). Operators discussing a mission for twenty minutes see the same card structure repeated. Casual phrases like `Continue.` can classify as execution and advance the mission unintentionally.

## Scope

- `ConversationLayer.js` — compose natural-language mission responses from verified mission state
- `ConversationMemory.js` — session-scoped memory of explained topics (avoid repetition)
- Read-only cognition modes (`inspect`, `explain`, `challenge`, `compare`, `strategy`, `brainstorm`, `teach`, `resume`) route through the conversation layer
- `PresentationEngine` presents conversational responses without card rewrite
- Context-aware `Continue.` — read-only when no consumable pending decision; execute when approval is pending
- Self-reflection — Max may disagree with low Scout confidence when evidence exists, or recommend coverage before prioritization

## Out of Scope

- Replacing SPEC-121 status cards on **execution** turns (approve, begin discovery, plan clarification commits)
- Durable mission conversation store (SPEC-032 vision) — session memory only in v1
- LLM paraphrase of mission turns — deterministic composition from mission facts

## Architecture

```text
Operator message
  → Operator Cognition (SPEC-146)
  → Mission inspection / snapshot (read-only)
  → ConversationLayer
       Operator intent
         ↓
       Mission context
         ↓
       Specialist knowledge
         ↓
       Reasoning (internal)
         ↓
       Conversation memory
         ↓
       Natural-language response
  → PresentationEngine (conversational_intelligence)
```

## Conversation Principles

1. **Natural** — no unnecessary status cards on read-only turns
2. **Contextual** — conversation memory avoids repeating the same explanation
3. **Adaptive** — Why → explanation; Continue (no gate) → inspect; Teach → education; Challenge → judgment
4. **Progressive disclosure** — lead with the important part; expand on explicit reasoning requests
5. **Self-reflection** — Max may disagree with specialist confidence when mission evidence supports a different read

## Testing

- `packages/max/workspace/tests/spec147ConversationalIntelligence.test.js`
- Existing SPEC-146 tests continue to pass

## Acceptance Criteria

- [x] Operator can discuss a mission across multiple turns without accidental mutation
- [x] Operator can challenge Scout, ask why, debate strategy, explore alternatives, teach concepts, and inspect state — all read-only
- [x] Read-only responses do not contain `Mission Updated` or status-card section headers
- [x] Bare `Continue.` does not mutate — explicit approval language is required to advance
