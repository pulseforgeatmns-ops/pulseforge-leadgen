# ADR-005 — LLM Presentation Engine

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-26 |
| **Spec** | [SPEC-009](../specs/SPEC-009_Max_Intelligence_Workspace.md) |
| **Supersedes** | — |
| **Related** | [ADR-001](ADR-001_Conversation_First.md), [ADR-002](ADR-002_Explainable_AI.md) |

## Context

Conversational Max (Ask Max / Intelligence Workspace) must feel like a knowledgeable advisor. LLMs excel at natural language. They are also capable of inventing entities, scores, evidence, and policy outcomes — which would violate explainability (ADR-002) and turn conversation into a second, unverifiable brain.

The intelligence stack already produces verified recommendations, briefings, memory diffs, and policy decisions. Conversation needs a presentation layer, not a parallel reasoning engine.

## Decision

**LLMs are presentation engines.**

They never create business intelligence. They only communicate verified intelligence produced by the deterministic stack:

```text
User Question
  → Context Envelope
  → Knowledge → Reasoning → Memory → Briefing → Policy
  → Structured Response Object
  → LLM (Claude)
  → Natural Language Response
```

The LLM must never:

- query repositories
- calculate scores
- rank recommendations
- infer business intelligence
- override policy
- invent evidence
- fabricate confidence

If information is unavailable, the Structured Response Object says so explicitly. The LLM may only rephrase what is present.

## Consequences

### Positive

- Conversational UX without chatbot theater
- Every Max answer remains explainable, auditable, and reproducible
- Aligns with ADR-001 (conversation over shared truth) and ADR-002 (explainable AI)

### Negative / tradeoffs

- Requires a complete Structured Response Object before phrasing
- Thin or empty graph yields honest “unavailable” answers rather than fluent invention
- Presentation quality depends on Claude (or a deterministic fallback formatter)

### Follow-ups

- SPEC-009 WorkspaceEngine + PresentationEngine
- Keep dashboard legacy `/api/max/ask` isolated until migrated to the same contract
