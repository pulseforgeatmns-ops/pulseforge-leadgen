# ADR-060 — Knowledge as Relationships

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-24 |
| **Spec** | [SPEC-152](../specs/SPEC-152_Concept_Graph_Reasoning.md) |
| **Related** | [ADR-059](ADR-059_Max_as_the_Business_Operating_System.md), [ADR-004](ADR-004_Knowledge_Graph.md), [ADR-005](ADR-005_LLM_Presentation_Engine.md), [ADR-045](ADR-045_Evidence_Before_Reasoning.md) |

## Context

Max's identity and operating-model reasoning (SPEC-151) stored structured concepts but still routed questions to single knowledge sections. Multi-concept questions — authority + Scout + specialization, or Scout + Paige + conflict + governance — could not be answered from one document slice.

PulseForge has repeatedly evolved from stored answers toward stored structure:

- Client Intelligence: interview transcripts → compiled Blueprint
- Mission Engine: free-text intent → durable Mission Plan
- Scout: search results → intelligence pipeline artifacts
- Conversation: stateless turns → session Conversation State
- Identity: static prose → Operating Model concepts

Reasoning was the remaining layer still resembling "pick a paragraph, generate an answer."

## Decision

**PulseForge does not primarily store answers. It stores relationships between concepts, and answers are synthesized by traversing those relationships.**

1. **ConceptGraph is the reasoning substrate.** Concepts (operator, max, scout, authority, mission, …) connect via typed relationships (`delegates_to`, `coordinates`, `cannot_override`, …). The graph stores no canned answers.
2. **ConceptPlanner maps questions to concept sets and goals.** Example: "Who ultimately decides?" → `{ concepts: [authority, operator, max], goal: explain_authority }`.
3. **ConceptReasoner traverses the graph and composes prose.** Multi-hop paths (Operator → Max → Scout → Market Discovery) produce explanations that no single section could hold.
4. **Conversation state carries active concepts.** SPEC-150 `activeConcepts` merges across follow-ups so subject stays intact while the active graph evolves.
5. **LLM remains presentation-only.** Synthesis is deterministic from graph traversal; PresentationEngine does not invent authority or specialist boundaries.

## Consequences

### Positive

- Multi-concept questions resolve through relationship composition instead of handler fallbacks
- The same graph substrate can later explain blocked missions, specialist disagreements, and prioritization tradeoffs
- Aligns Max reasoning with ADR-004 (Knowledge Graph) at the operating-system layer — relationships, not paragraphs

### Negative / tradeoffs

- Initial graph is seeded from SPEC-151 OperatingModel — tenant-specific business graphs are a follow-up
- Traversal depth is capped to prevent runaway paths; very novel questions may still need graph expansion

### Follow-ups

- [ ] Extend ConceptGraph with tenant AIM / mission concepts
- [ ] Wire blocked-mission diagnostics through graph traversal
- [ ] Surface traversed relationships in operator-facing reasoning trace
