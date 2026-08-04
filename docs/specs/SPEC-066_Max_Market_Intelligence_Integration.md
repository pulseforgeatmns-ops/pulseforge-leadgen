# SPEC-066 — Max Market Intelligence Integration

| Field | Value |
|---|---|
| **Status** | In Progress — thin read-only context adapter implemented |
| **Target Version** | TBD |
| **Priority** | High |
| **Owner** | Max Platform |
| **Created** | 2026-08-03 |
| **Depends** | [SPEC-065](SPEC-065_Market_Intelligence_Foundation.md) (must be accepted first), [ADR-045](../adr/ADR-045_Evidence_Before_Reasoning.md) |
| **Blocked by** | Full SPEC-065 corpus acceptance before richer reasoning / Composer usage |

## Objective

Make Max a **consumer** of the Market Intelligence layer rather than part of its implementation. When preparing outbound strategy, Max combines Company Intelligence (Scout / knowledge) with Market Intelligence observations, then reasons ephemerally; Composer writes channel copy from that reasoning.

## Vision References

- [ADR-045](../adr/ADR-045_Evidence_Before_Reasoning.md)
- [SPEC-065](SPEC-065_Market_Intelligence_Foundation.md)
- [SPEC-057](SPEC-057_Execution_Domain_Routing.md) — `market_intelligence` execution domain socket
- [SPEC-009](SPEC-009_Max_Intelligence_Workspace.md)

## Problem

Today the Max `market_intelligence` domain routes to an empty corpus stub. SPEC-065 will expose queryable profiles, timelines, and cross-market patterns. Without a consumer contract, Max cannot cite market evidence when planning outbound — and implementers may be tempted to embed recommendations inside the MI store.

## Scope (future implementation)

- Read-only adapters from Max → `marketIntelligenceQuery` (profiles, timelines, patterns)
- Cite `evidenceRefs` / observation IDs in Max responses
- Wire `market_intelligence` execution domain to real corpus answers (descriptive + ephemeral reasoning only)

## Out of Scope (always for MI store)

- Writing recommendations into `market_*` tables
- Scoring campaigns inside MI
- Coupling Scout/Composer into MI extraction

## Architecture (target)

```
User: Prepare an outbound strategy for Acme
        ↓
Scout / Company Intelligence  →  company facts
        ↓
Market Intelligence APIs      →  market observations (SPEC-065)
        ↓
Max                           →  ephemeral reasoning over both
        ↓
Composer                      →  email / channel copy
```

Market Intelligence remains valuable on its own: queryable, inspectable, and testable before Max ever reads from it.

## Implementation Plan

1. ✅ Thin read-only context adapter: hydrate Max market turns from SPEC-065 query services.
2. ✅ Evidence plumbing: expose market profile / pattern / email refs as supporting evidence.
3. ✅ Descriptive response path: answer market turns from corpus observations only; no mission creation and no recommended actions.
4. 🔜 Richer company / campaign matching after the corpus has enough accepted data.
5. 🔜 Composer strategy handoff after market evidence quality is accepted.

## Acceptance Criteria (draft)

- [x] Max can fetch company market profile + timeline for a named vendor/domain
- [x] Every market claim in a Max response cites SPEC-065 evidence
- [ ] MI tables remain free of recommendation / score columns
- [x] Composer does not call MI extractors directly
- [x] Market Intelligence turns do not create/resume Mission Engine work

## Future Work

- Benchmarking and reporting consumers
- Atlas / Scout optional hooks (still read-only)
