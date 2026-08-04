# ADR-045 — Evidence Before Reasoning

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-03 |
| **Spec** | [SPEC-065](../specs/SPEC-065_Market_Intelligence_Foundation.md), [SPEC-066](../specs/SPEC-066_Max_Market_Intelligence_Integration.md) (consumer) |
| **Related** | [ADR-009](ADR-009_Evidence_Platform_Architecture.md), [ADR-017](ADR-017_Intelligence_Before_Execution.md), [ADR-031](ADR-031_Review_Must_Be_Evidence_First.md), [ADR-040](ADR-040_Separate_Evidence_Acquisition_from_Capability_Selection.md) |

## Context

Intelligence domains are tempted to collapse observation and advice into one write path: store a recommendation, score a campaign, or let an agent invent positioning from thin context. That mixes durable facts with ephemeral judgment, makes audits impossible, and couples collectors (ingestion, extraction, market corpus) to consumers (Max, Composer, reporting).

We need a system-wide rule: domains accumulate evidence; reasoning engines consume it later. Market Intelligence (SPEC-065 / SPEC-066) is the first large application of this rule, but the decision is not MI-specific.

## Decision

1. **Domains collect evidence** — A domain’s durable store is observational: raw sources, structured extractions fields, timelines, and profiles that cite sources.
2. **Domains do not make recommendations** — No scoring, winner declarations, “should do X,” or auto-strategy writes inside an evidence domain.
3. **Reasoning engines consume evidence** — Max and similar engines read evidence at reasoning time; they do not own the evidence schema.
4. **Facts are persisted; conclusions are ephemeral** — Stored rows must remain re-derivable from sources. Conclusions live in responses, briefings, or transient artifacts unless a later ADR explicitly promotes a conclusion type with its own evidence links.
5. **Every conclusion must be traceable to evidence** — If a system asserts a market or business observation, it must cite supporting evidence IDs or verbatim quotes. Unsupported claims are omitted, not invented.

## Consequences

### Positive

- Market Intelligence (and future domains) can be queried, tested, and trusted before Max reads them
- Clear seam for SPEC-066: Max is a consumer, not part of MI implementation
- Aligns with Evidence Core (ADR-009) and evidence-first review (ADR-031)

### Negative / tradeoffs

- Extractors must prefer omit-over-invent (fewer filled fields, more honest gaps)
- Cross-market “patterns” are frequency counts with sample refs — not ranked advice

### Follow-ups

- [x] SPEC-065: observational MI foundation (Phases 2–5)
- [ ] SPEC-066: Max consumes MI without embedding recommendations in the MI store
- [ ] Future domains apply the same evidence / reasoning split
