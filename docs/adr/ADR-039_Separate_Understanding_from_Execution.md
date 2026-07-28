# ADR-039 — Separate Understanding from Execution

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-28 |
| **Spec** | [SPEC-055](../specs/SPEC-055_Intent_Understanding.md) |
| **Related** | [ADR-034](ADR-034_Intent_Before_Execution.md), [ADR-038](ADR-038_Explain_Planning_Decisions.md), [ADR-027](ADR-027_Mission_Planning_Is_Objective_Driven.md), [ADR-010](ADR-010_Mission_Engine.md) |

## Context

SPEC-050 compiled operator language into a Mission Plan IR so Notes never become executable nodes. SPEC-054 made alias misses diagnosable. Operators still failed when they expressed valid goals in natural language that did not contain registered capability aliases — e.g. “Run an end-to-end execution audit for Campaign 001” surfaced as “Unknown mission alias” instead of Campaign Diagnostics.

Language is probabilistic. Execution must remain deterministic. A single alias-matching step conflates those concerns.

## Decision

1. **Mission Planning consists of two distinct phases:**
   - **Intent Understanding** — semantic interpretation of operator language into a structured **MissionIntent**.
   - **Capability Planning** — deterministic translation of MissionIntent into an executable **MissionPlan**.
2. **MissionIntent is the boundary** between probabilistic language and deterministic execution.
3. **Capabilities never parse natural language.** They consume MissionPlan only.
4. **Intent Understanding owns language.** The planner owns execution selection. Capabilities own work.
5. **Confidence is first-class.** High confidence proceeds; low confidence returns clarification with suggested interpretations — never invents capability nodes from unmatched aliases.
6. Implementing contract: [SPEC-055 Intent Understanding](../specs/SPEC-055_Intent_Understanding.md).

## Consequences

### Positive

- Operators express goals without memorizing capability names
- Semantic requests (audit, investigate, “what’s wrong”) resolve to intents
- Deterministic execution graph path (MissionPlan → Execution Graph) unchanged
- Review Workspace shows Understood Intent before Execution Plan
- Clarification replaces opaque “Unknown mission alias” for ambiguous free-form text

### Negative / tradeoffs

- Deterministic intent patterns will miss some phrasings (fail to clarification, not silent wrong graph)
- Intent taxonomy must be maintained alongside Capability Registry aliases
- Multi-sentence campaign builds still lean on IntentParser for Notes/Options (compat bridge)

### Follow-ups

- [x] MissionIntent + Intent Understanding + Capability Planner (SPEC-055 v1)
- [x] MissionPlanner two-stage wiring + clarification draft
- [x] Review Workspace Understood Intent panel
- [ ] Interactive clarification picker in Command Deck
- [ ] Optional LLM-assisted intent ranking behind the same MissionIntent contract
- [ ] Retire IntentParser as primary path once Notes/Options extraction lives fully in Understanding
