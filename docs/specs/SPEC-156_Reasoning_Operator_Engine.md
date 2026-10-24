# SPEC-156 — Reasoning Operator Engine

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Max |
| **Created** | 2026-08-24 |
| **Depends on** | [SPEC-153](SPEC-153_Operator_Intent_Single_Source_of_Truth.md), [SPEC-154](SPEC-154_Active_Reasoning_Context.md), [SPEC-155](SPEC-155_Conversation_Contract_Engine.md) |
| **Related ADR** | [ADR-063](../adr/ADR-063_Reasoning_Is_Explicit.md) |

## Objective

Introduce a dedicated **Reasoning Operator Engine (ROE)** responsible for selecting and executing cognitive operations over the Active Reasoning Context.

The ROE separates **what Max knows** from **how Max thinks with what it knows**.

## Problem

ARC stores the current proposition. Without transformation, follow-up questions (`Why?`, `What assumption?`) retrieve the same claim instead of deepening reasoning. Conversation becomes retrieval, not reasoning.

## Design Principle

**Reasoning is a transformation, not retrieval** (ADR-063). Every operator utterance requests a cognitive operation. The runtime must explicitly identify that operation before language generation. No response may be generated directly from ARC.

## Pipeline

```text
Conversation Contract
  → Conversation State
  → Operator Intent
  → ARC
  → Reasoning Operator Engine
  → Reasoning Result
  → Response Composer
```

ARC is never sent directly to presentation.

## Module

| Module | Purpose |
|---|---|
| `packages/max/workspace/ReasoningOperatorEngine.js` | Operator dispatch, execution, depth tracking, ARC delta |

## Types

### ReasoningOperator

```js
{
  id: string,
  category: 'elaboration' | 'critical' | 'structural' | 'meta',
  dispatchSource: string,
  dispatchConfidence: number,
}
```

### ReasoningResult

```js
{
  operator: ReasoningOperator,
  transformedClaim: string,
  evidenceUsed: string[],
  assumptions: string[],
  confidence: number,
  depth: number,
  arcDelta: object,
}
```

## Core Operators

| Operator | Transforms |
|---|---|
| `explain` | Claim → Explanation |
| `justify` | Claim → Reasons supporting the claim |
| `surface_assumptions` | Claim → Underlying assumptions |
| `challenge` | Claim → Weaknesses, counterarguments |
| `counterfactual` | Claim → What if assumption changes? |
| `compare` | Claim A + Claim B → Comparison |
| `contrast` | Claim pair → Differences |
| `generalize` | Claim → Higher abstraction |
| `specialize` | Claim → Concrete examples |
| `evaluate` | Claim → Strengths and weaknesses |
| `summarize` | Reasoning chain → Compressed summary |
| `synthesize` | Multiple claims → Merged view |
| `revise` | Claim + new evidence → Modified conclusion |
| `reflect` | History → Evolution of reasoning |

## Dispatch Priority

1. Explicit operator (question pattern)
2. Conversation continuity (previous operator chain)
3. ARC follow-up type
4. Default `explain`

`What assumption is that based on?` never falls back to `explain`.

## Reasoning Depth

Every operator knows how many layers deep it operates:

| Layer | Example |
|---|---|
| 0 | Claim |
| 1 | Justification |
| 2 | Justification of justification |
| 3 | Underlying assumption |
| 4 | Counterfactual |
| 5 | Revised conclusion |

Depth is tracked in `session.context.reasoningHistory`.

## ARC Interaction

ARC is immutable input. Operators return an **ARC Delta**; workspace applies the delta separately via `advanceActiveReasoningContext()`.

## Integration

| Layer | Change |
|---|---|
| `IdentityReasoning.js` | ARC-bound follow-ups call `executeReasoning()` instead of `synthesizeFromArc()` |
| `IdentityConversationContext.js` | Passes operator metadata; records ROE in structured response |
| `WorkspaceEngine.js` | Passes `operatorIntent` and `conversationContract` to identity pipeline |

## Runtime Guarantees

- Every ARC-bound response passes through exactly one reasoning operator
- Every operator is explicit and logged in dispatch metadata
- Every operator produces a `ReasoningResult`
- Presentation verbalizes `ReasoningResult` only — it never reasons

## Acceptance Tests

`packages/max/workspace/tests/spec156ReasoningOperatorEngine.test.js`

1. **Deepening Why chain** — Role → Why → Why → Why; each answer deepens, no repetition
2. **Assumption flow** — SurfaceAssumptions → Counterfactual → Revise
3. **Compare chain** — Compare → Generalize → Summarize
4. **Reflect flow** — Reflect → Revise on belief check
5. **Full chain** — Explain → Challenge → Counterfactual → Revise → Summarize

## Architectural Principles

| Object | Answers |
|---|---|
| Knowledge | What do I know? |
| ARC | What are we discussing? |
| Conversation Contract | How should this conversation proceed? |
| Operator Intent | What is the operator trying to accomplish? |
| Reasoning Operator Engine | What cognitive transformation should be applied? |
