# SPEC-001C — Knowledge Query Engine

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v0.7.4 |
| **Priority** | High |
| **Owner** | TBD |
| **Created** | 2026-07-26 |

> Numbered **SPEC-001C** (knowledge-layer slice) to avoid colliding with [SPEC-002 Max Reasoning Engine](SPEC-002_Max_Reasoning_Engine.md) (v0.8.0). Product intent matches the approved “Knowledge Query Engine” brief for v0.7.4.

## Objective

Transform the Knowledge Graph from a persistent data store into a queryable intelligence layer. The Query Engine answers structured questions efficiently. It does **not** perform AI reasoning or generate recommendations—that comes later (SPEC-002).

## Vision References

- `docs/architecture/Knowledge_Graph_Architecture.md`
- `docs/vision/Intelligence_Architecture.md`
- [ADR-002](../adr/ADR-002_Explainable_AI.md)
- [ADR-004](../adr/ADR-004_Knowledge_Graph.md)

## Problem

After SPEC-001A–001 the graph can store and sync nodes/edges, but callers lack a stable, typed, tenant-safe query surface for filtering, traversal, timeline, path finding, and instrumented explainability.

## Scope

- `packages/knowledge/query/` — QueryEngine, QueryTypes, Filters, Traversal, Timeline, Metrics
- Expand `KnowledgeService` with query operations that return domain objects only
- Deterministic traversals with depth limits
- Enhanced `explain()` (Claim → Evidence → Source → Confidence → Timeline position → Reason)
- Structured per-query metrics
- Repository parity tests (InMemory + Persistent)

## Out of Scope

- LLM / recommendations / formatting / summaries
- Dashboards for metrics
- Runtime agent/server wiring
- GraphRepository interface changes (query uses existing `find` / `neighbors`)

## Dependencies

- v0.7.3 — SPEC-001 Persistent Knowledge Store
- SPEC-001A `KnowledgeService` + node/edge model
- SPEC-001B sync (fixtures for realistic graphs)

## Architecture

```text
Caller
  ↓
KnowledgeService  (public API)
  ↓
QueryEngine       (filters, traversal, timeline, path, metrics)
  ↓
GraphRepository   (InMemory | Persistent) — unchanged contract
```

Design principles:

- All queries flow through `KnowledgeService`
- Repository implementations remain interchangeable
- Results are strongly typed (JSDoc + stable shapes)
- Queries are tenant-safe by default (`tenantId` required on every query object)
- Traversals are deterministic (stable sort by edge.id, node.id)
- No LLM dependencies

## Data Model

Query objects (not long parameter lists):

| Query | Key fields |
|---|---|
| `CompanyQuery` | tenantId, industry, technology, location, confidenceMin, createdAfter, limit |
| `PersonQuery` | tenantId, companyId, email, title, name, confidenceMin, createdAfter, limit |
| `InteractionQuery` | tenantId, channel, actionType, relatedNodeId, createdAfter, occurredAfter, limit |
| `EvidenceQuery` | tenantId, sourceType, sourceId, aboutNodeId, confidenceMin, createdAfter, limit |
| `ClaimQuery` | tenantId, subjectId, status, confidenceMin, createdAfter, limit |
| `NeighborQuery` / `RelatedQuery` / `PathQuery` / `TimelineQuery` | tenantId + node ids + depth/edgeTypes |

Metrics payload: queryName, executionTimeMs, nodesVisited, edgesTraversed, resultsReturned, repositoryType.

## Implementation Plan

1. Add `query/` modules
2. Expand `KnowledgeService` (backward-compatible overloads for `findEvidence` / `findClaims` / `explain`)
3. Enhance explainability with timeline position
4. Tests: filters, traversal, timeline, path, confidence, tenant isolation, repo parity, metrics
5. Docs: release v0.7.4, CHANGELOG, CURRENT_STATE, package README

## Migration Strategy

- Additive API only; existing `(tenantId, …)` call shapes still work
- No schema migration
- Default runtime remains in-memory; agents stay unwired

## Testing

- Unit tests against `InMemoryGraphRepository`
- Parity suite also run against `PersistentGraphRepository` when Postgres harness available
- SPEC-001 hash guard replaced: KnowledgeService expansion is explicitly allowed by this spec

## Acceptance Criteria

- [x] All query APIs implemented on KnowledgeService
- [x] Traversals deterministic
- [x] Path finding operational
- [x] Timeline queries operational
- [x] Explainability enhanced
- [x] Metrics emitted
- [x] Repository parity achieved
- [x] Runtime agents remain unwired

## Future Work

- Max Reasoning Engine (SPEC-002) consumes this query API
- Shadow dual-write / production ingest
- Metrics dashboard
