# ADR-063 — Reasoning Is Explicit

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-24 |
| **Spec** | [SPEC-156](../specs/SPEC-156_Reasoning_Operator_Engine.md) |

## Context

Active Reasoning Context (SPEC-154) tracks the current proposition chain. Without an explicit transformation layer, follow-up questions retrieve the same claim — `Why?`, `Why is that necessary?`, and `What assumption is that based on?` all restate the primary claim. Conversation becomes retrieval, not reasoning.

## Decision

Every operator utterance requests a **cognitive operation**. The runtime must explicitly identify that operation via the Reasoning Operator Engine before language generation.

No response may be generated directly from ARC. Every response must first pass through a Reasoning Operator that returns a `ReasoningResult`.

Presentation verbalizes `ReasoningResult` only — it never reasons.

## Consequences

- Follow-up chains deepen progressively (justification → assumption → counterfactual → revision)
- Reasoning depth is tracked per conversation in `session.context.reasoningHistory`
- Operator dispatch is auditable (`dispatchSource`, `operator.id`, `depth`)
- ARC remains immutable input; operators emit deltas for workspace application

## Alternatives Considered

**Embed transformation in ARC synthesis** — rejected; mixes knowledge state with cognitive operations (violates SPEC-156 separation).

**Let presentation infer reasoning** — rejected; presentation must not reason; only verbalize structured results.

**Re-parse question in each handler** — rejected; violates SPEC-153 single-parse guarantee; dispatch consumes sealed operator intent and ARC follow-up bindings.
