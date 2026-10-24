# SPEC-001 — Persistent Knowledge Store

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v0.7.3 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-26 |
| **Depends on** | [SPEC-001A](SPEC-001A_Knowledge_Layer_Foundation.md), [SPEC-001B](SPEC-001B_Graph_Synchronization_Engine.md) |

## Objective

Replace the in-memory repository with a persistent Postgres implementation while keeping the public API completely unchanged.

> **If `KnowledgeService` changes, this spec has failed.**  
> Only the repository implementation changes.

## Vision References

- `docs/architecture/Knowledge_Graph_Architecture.md`
- [ADR-004](../adr/ADR-004_Knowledge_Graph.md)
- [SPEC-001A](SPEC-001A_Knowledge_Layer_Foundation.md)

## Problem

`InMemoryGraphRepository` proves the domain model but does not survive restart. Introducing Neo4j (or similar) now would add operational burden while the model is still settling. Postgres is already the operational database with tenancy, backups, and migrations.

## Scope

- `PersistentGraphRepository` implementing the existing `GraphRepository` interface (no new methods, no interface changes)
- Graph-owned Postgres schema: `knowledge_nodes`, `knowledge_edges`, `knowledge_evidence`, `knowledge_claims`
- Migration + `ensureKnowledgeSchema` helper for tests
- Tests: restart survival, rebuild equivalence, `explain()` after restart, contract compliance, KnowledgeService hash guard
- Docs / CURRENT_STATE / CHANGELOG / v0.7.3

## Out of Scope

- KnowledgeService API changes
- Neo4j / Memgraph / Dgraph
- Wiring Scout/agents/server to dual-write
- Persistent sync ledger
- UI / LLM / recommendations

## Dependencies

- SPEC-001A / SPEC-001B
- Existing `pg` dependency and disposable Postgres test harness

## Architecture

```text
KnowledgeService  (UNCHANGED)
        ↓
GraphRepository interface  (UNCHANGED)
   ├── InMemoryGraphRepository   (dev/tests)
   └── PersistentGraphRepository (Postgres)
```

## Data Model

| Table | Holds |
|---|---|
| `knowledge_nodes` | company, person, interaction |
| `knowledge_evidence` | evidence nodes |
| `knowledge_claims` | claim nodes |
| `knowledge_edges` | typed edges between any graph node ids |

Nodes store type-specific fields in `body` JSONB plus `metadata` JSONB. The graph schema is not a mirror of CRM tables.

## Implementation Plan

1. Migration SQL
2. `PersistentGraphRepository`
3. Postgres integration tests
4. Export from package; leave default runtime on in-memory unless a pool is injected
5. Docs

## Migration Strategy

- Additive migration `2026-07-26-knowledge-graph-persistent.sql`
- Rollback drops the four graph tables
- Default `createKnowledgeRuntime()` remains in-memory; callers opt into Postgres via `new PersistentGraphRepository(pool)`

## Testing

```bash
npm run test:knowledge
npm run test:knowledge:postgres
```

## Acceptance Criteria

- [x] Graph survives application restart
- [x] Full rebuild produces an equivalent graph
- [x] `explain()` works after restart
- [x] Existing in-memory tests still pass
- [x] KnowledgeService public API unchanged (source hash guard)
- [x] Runtime agents remain unwired

## Future Work

- Shadow dual-write from Scout/CRM via SPEC-001B sync
- Persistent sync ledger
- Optional dedicated graph DB only if Postgres proves insufficient (swap repository only)
