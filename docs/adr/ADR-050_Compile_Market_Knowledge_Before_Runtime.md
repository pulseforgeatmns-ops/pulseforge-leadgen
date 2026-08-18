# ADR-050 — Compile Market Knowledge Before Runtime

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-18 |
| **Spec** | [SPEC-113](../specs/SPEC-113_Acquisition_Intelligence_Compiler.md) |
| **Related** | [ADR-049](ADR-049_Understand_Market_Before_Selling.md), [ADR-003](ADR-003_Human_Approval.md), [ADR-021](ADR-021_Human_Approval_Before_Execution.md), [ADR-039](ADR-039_Separate_Understanding_from_Execution.md), [ADR-045](ADR-045_Evidence_Before_Reasoning.md) |

## Context

SPEC-112 / ADR-049 established the Acquisition Intelligence Model as the artifact Scout reasons over before selling into a market. The Fedir AIM is a hand-authored seed. Operators still hold market knowledge as PDFs, interviews, playbooks, and landing pages.

If Scout (or an LLM) reads those documents at search time, understanding is ephemeral, ungoverned, and unexplainable. That is the current-AI pattern: prompt → model reads everything → answer.

PulseForge already refused that pattern for client understanding: CIE compiles interviews into an approved Business Blueprint. Market understanding needs the same gate.

The product brief called this SPEC-111. Repository SPEC-111 is Operator Intent Taxonomy. This decision is numbered **SPEC-113 / ADR-050**.

## Decision

1. **Market knowledge is compiled, not reread.** Unstructured documents become an Acquisition Intelligence Workspace of concepts and relationships.
2. **The compiler never executes outreach.** Ingestion, extraction, ontology, review, and publication are the entire authority boundary.
3. **Nothing publishes automatically.** Operator review (accept / edit / merge / remove) is required, matching CIE. Every concept traces to source evidence.
4. **Only published AIMs are runtime knowledge.** Scout loads a published (or grandfathered `complete` seed) AIM. Draft compiler output and document bodies are not search inputs.
5. **Missing knowledge stays unknown.** The compiler does not invent mission, geography, case studies, or confidence.
6. **Compiled AIM is not operating fact.** SPEC-108 claim grounding and SPEC-110 operator intelligence objects remain separate.

## Consequences

### Positive

- A market can be learned once and reasoned over repeatedly
- Scout recommendations stay explainable against a reviewed ontology
- Operators can reject or reshape extracted concepts before they become runtime knowledge

### Negative / tradeoffs

- v1 extraction is deterministic (headings/labels), not LLM-adaptive, so poorly labeled documents yield unknowns instead of guesses
- Hand-authored SPEC-112 seeds (`complete`) remain runtime-valid so Fedir does not require a recompile

### Follow-ups

- [x] SPEC-113 v1 compiler + fixtures
- [x] Scout loads only runtime AIMs
- [ ] Operator compiler UI
- [ ] LLM-adaptive extraction behind the same approval gate
