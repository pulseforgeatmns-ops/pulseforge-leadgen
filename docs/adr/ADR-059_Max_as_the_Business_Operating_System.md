# ADR-059 — Max as the Business Operating System

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-23 |
| **Spec** | [SPEC-149A](../specs/SPEC-149A_Max_Identity_and_Operating_Model.md) |
| **Related** | [ADR-055](ADR-055_Max_Manages_Missions.md), [ADR-005](ADR-005_LLM_Presentation_Engine.md), [ADR-016](ADR-016_Execution_Does_Not_Decide.md), [ADR-003](ADR-003_Human_Approval.md), [ADR-002](ADR-002_Explainable_AI.md) |

## Context

As PulseForge evolved, Max accumulated capabilities across reasoning, orchestration, mission planning, execution governance, learning, and business intelligence. Existing identity responses described implementation ("mission manager", "manager agent", "intelligence advisor", "workspace reasoning") rather than organizational purpose, creating an inconsistent mental model for operators.

Different entry points — workspace ask, legacy dashboard chat, daily digest, UI agent roster — each used different persona labels.

## Decision

**Max is the Business Operating System — not another AI assistant, not another specialist.**

1. **Canonical identity lives in `packages/max/identity/MaxIdentity.js`.** All conversational, presentation, chat, digest, and UI surfaces import from this module.
2. **Identity describes organizational responsibility.** Max coordinates specialists and operators toward measurable outcomes. It does not cold call, send email, publish content, or mutate CRM state without authorization.
3. **Forbidden implementation labels** — AI assistant, chatbot, mission manager, agent, LLM, prompt — must not appear in identity responses.
4. **Workspace context varies; identity does not.** Tenant name changes the opening line; the operating-system role stays constant.
5. **PresentationEngine bypasses LLM for identity turns** so Claude cannot reframe Max as an "intelligence advisor."
6. **Legacy `/api/max/ask` routes identity questions** through the same handler as workspace ask before falling back to pipeline context.
7. **Operator authority is explicit.** Only the operator owns objectives, risk acceptance, approvals, external relationships, final decisions, and strategic direction.

## Consequences

### Positive

- Operators get a consistent mental model regardless of entry point
- Identity aligns with ADR-055 (Max manages missions) without collapsing into "mission manager" implementation language
- Specialist boundaries remain clear — they own domain expertise, Max owns coordination

### Negative / tradeoffs

- UI copy on legacy dashboard still describes Max alongside "agents" in the roster visual — architectural distinction vs. visual metaphor
- Existing ADR-055 language ("Max manages missions") remains valid doctrine; SPEC-149A adds the operator-facing identity layer

### Follow-ups

- [ ] Command Deck agent roster copy alignment
- [ ] Onboarding greeting identity consistency review
