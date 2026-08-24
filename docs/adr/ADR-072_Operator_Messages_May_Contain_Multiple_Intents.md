# ADR-072 — Operator Messages May Contain Multiple Intents

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-24 |
| **Related** | [SPEC-151](../specs/SPEC-151_Multi_Intent_Execution_Planner.md), [SPEC-148](../specs/SPEC-148_Session_State_Manager.md), [SPEC-149](../specs/SPEC-149_Message_Type_Classification.md), [SPEC-150](../specs/SPEC-150_Session_State_Inspection.md), [SPEC-147](../specs/SPEC-147_Conversational_Intelligence_Layer.md), [ADR-069](ADR-069_Classify_Communication_Before_Cognition.md) |

## Context

The workspace runtime previously assumed:

```text
One Message → One Intent → One Owner → One Pipeline
```

Real operator messages combine independent intentions in a single utterance — for example, configuring session behavior, stating a business objective, and asking what should happen next. Selecting one intent discarded the remainder. Session configuration in particular stopped the pipeline after acknowledgement even when the operator also requested business operation or reasoning.

## Decision

Operator messages are **plans**, not single classifications. The runtime shall:

1. **Extract** all compatible intents from one message.
2. **Construct** a deterministic ordered execution plan.
3. **Execute** each step with exactly one owner.
4. **Pause** only when blocked by policy, approval gates, or fatal errors.

### Architecture

```text
Operator Message
  ↓
Intent Extraction
  ↓
Execution Plan
  ↓
Execute Step 1 → Step 2 → … → Step N
  ↓
Return (blocked | complete)
```

### Ordering

- Message segment order is the default ordering.
- Declared `requires` / `produces` dependencies reorder steps when needed (e.g. session configuration before business operation).
- Inspection reflects the latest runtime state when it appears after mutating steps; explicit operator ordering ("Summarize… Then continue…") is preserved.

### Ownership

Ownership is **step-level**, not message-level. Each execution step assigns one pipeline owner (Session State Manager, Mission Runtime, Max Reasoning, etc.).

### Constraints

- Human approval contracts remain intact — the planner never bypasses discovery review, plan approval, or operator decision gates.
- Session execution policy (`read_only`, `execution_disabled`) blocks mutating mission steps; the planner explains why instead of silently dropping the intent.

## Consequences

- Compound operator messages execute fully in one turn when compatible.
- Session configuration no longer short-circuits unrelated intents in the same message.
- Audit traces may record an `executionPlan` with per-step completion status.
- Single-intent messages retain existing behavior (no planner overhead when not compound).
