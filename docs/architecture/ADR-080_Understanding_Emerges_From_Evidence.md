# ADR-080 — Understanding Emerges From Evidence

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Spec** | [SPEC-160](../specs/SPEC-160_Evidence_Synthesis_Engine.md) |

## Context

Scout collects evidence from Google Places, websites, social signals, and prior missions. Each observation carries source weight, but isolated facts do not constitute intelligence. Recommendations grounded only in single-source observations produce false precision.

## Decision

Evidence is not intelligence. Intelligence is the **understanding** that emerges when multiple pieces of evidence are synthesized into a coherent explanation.

Scout shall never recommend actions directly from isolated observations. Recommendations flow from synthesized business understanding.

## Consequences

- Confidence attaches to **understanding**, not individual evidence items.
- Contradictions are retained and reduce understanding confidence — never discarded.
- Entity resolution runs before synthesis so aliases do not produce duplicate understandings.
- Mission Intelligence Reports surface business understanding, supporting evidence, contradictory evidence, confidence, and reasoning — not raw search results.
- The Investigative Reasoning Loop (SPEC-159) invokes the Evidence Synthesis Engine (SPEC-160) before emitting recommendations.

## Alternatives Considered

1. **Per-evidence confidence only** — Rejected. Produces overconfident single-source conclusions.
2. **Raw evidence in mission reports** — Rejected. Operators cannot act on disconnected observations.
3. **Max-side synthesis only (SPEC-110)** — Insufficient. Scout must synthesize at investigation time, before handoff.
