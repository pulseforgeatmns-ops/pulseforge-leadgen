# ADR-014 — Personalized by Default

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-027B](../specs/SPEC-027B_Proposal_Generator_Capability.md) |
| **Supersedes** | — |
| **Related** | [ADR-002](ADR-002_Explainable_AI.md), [ADR-003](ADR-003_Human_Approval.md), [ADR-005](ADR-005_LLM_Presentation_Engine.md), [ADR-010](ADR-010_Mission_Engine.md), [ADR-011](ADR-011_Capability_Framework.md) |

## Context

The Proposal Generator is the first client-facing deliverable after discovery. Template engines and name-swap decks produce proposals that feel interchangeable across cleaning companies. That undermines trust and fails the Mission bar: Pulseforge must demonstrate the quality of future work before asking for a commitment.

Speed-first generation (generic scaffolding + token substitution) conflicts with explainability ([ADR-002](ADR-002_Explainable_AI.md)) and with the rule that presentation layers must not invent intelligence ([ADR-005](ADR-005_LLM_Presentation_Engine.md)).

## Decision

1. **Proposal generation optimizes for relevance, not speed.**
2. A proposal should make the client feel understood **before** it attempts to sell.
3. The success test is: the prospect thinks *"This was clearly written for my business."*
4. **Interchangeability failure:** if another cleaning company could receive the same proposal with only the name changed, the Proposal Generator has failed.
5. Implementation is a **Mission capability personalization engine**, not a template engine: sections are composed from Discovery Summary, Discovery Profile, and optional ranking / campaign evidence. Shared structure is allowed; interchangeable body copy is not.
6. Recommendations must carry evidence refs. When evidence is thin, state uncertainty — never invent markets, goals, or challenges.
7. Operator review remains mandatory before client delivery ([ADR-003](ADR-003_Human_Approval.md)).

## Consequences

### Positive

- Proposal quality becomes a durable differentiator
- Acceptance tests can reject name-swap / generic output
- Aligns Mission UX with consultative sales reality
- Learning loop (versions, win/loss, feedback) has personalized baselines to improve

### Negative / tradeoffs

- Generation is more expensive to build and test than Mad Libs templates
- Incomplete discovery summaries produce shorter, more uncertain proposals (correct — not a bug)
- PDF / portal polish may lag the personalization core

### Follow-ups

- [x] SPEC-027B Proposal Generator Capability (v1 personalization engine + review + version store)
- [ ] Optional LLM polish that rephrases verified facts only (must not invent claims)
- [ ] Puppeteer PDF + shareable client link
- [ ] Win/loss feedback into messaging calibration (SPEC-021)
