# SPEC-001B — Graph Synchronization Engine

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v0.7.2 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-26 |
| **Depends on** | [SPEC-001A](SPEC-001A_Knowledge_Layer_Foundation.md) |

## Objective

Build an event-driven synchronization service that translates production entities (companies, prospects, interactions, imports, and future mutations) into KnowledgeService operations. The engine is replayable, idempotent, tenant-aware, and can rebuild the graph from existing relational data **without direct repository access**. All writes flow exclusively through `KnowledgeService`, preserving SPEC-001A.

## Vision References

- `docs/architecture/Knowledge_Graph_Architecture.md`
- `docs/architecture/Memory_Architecture.md`
- [ADR-004](../adr/ADR-004_Knowledge_Graph.md)
- [SPEC-001A](SPEC-001A_Knowledge_Layer_Foundation.md)

## Problem

SPEC-001A defined the graph API, but production CRM rows still live only in Postgres. Without a sync layer, Scout/CRM would either skip the graph or (worse) write storage-specific code. We need a single, auditable translation path that can also rebuild from historical tables.

## Scope

- `GraphSyncEngine` with `apply` / `applyMany` / `rebuildFromRelational`
- Source event types: company, prospect, touchpoint, import batch item, generic entity mutation
- Deterministic node IDs + sync idempotency ledger
- Idempotent ingest (`ensureNode` / `ensureEdge` / `ensureEvidence`)
- `MemoryRelationalSource` + read-only `PostgresRelationalSource`
- Unit tests for replay, tenancy, rebuild, imports
- Docs / CURRENT_STATE / CHANGELOG / v0.7.2

## Out of Scope

- Wiring Scout/agents/server boot to emit sync events in production
- Persistent ledger or persistent graph repository
- UI
- LLM / recommendations
- Dual-write from `dbClient.addCompany` (deferred to a later wiring slice)

## Dependencies

- SPEC-001A `KnowledgeService`, event bus, in-memory repository

## Architecture

```text
CRM / Scout / Import / Rebuild reader
        ↓
  sync event (idempotency key)
        ↓
  GraphSyncEngine.apply
        ↓
  KnowledgeEventBus → KnowledgeIngestor
        ↓
  KnowledgeService.ensure*
        ↓
  GraphRepository (still hidden)
```

## Data Model

Stable IDs:

- `company:{tenantId}:{companyId}`
- `person:{tenantId}:{prospectId}`
- `interaction:{tenantId}:{touchpointId}`
- `evidence:{tenantId}:{sourceType}:{sourceId}`

Ledger key: `sync:{tenantId}:{entityKind}:{entityId}:{revision}`

## Implementation Plan

1. Add ensure* APIs on KnowledgeService / EvidenceEngine
2. Make KnowledgeIngestor idempotent
3. Implement sync mappers, ledger, engine, relational sources
4. Tests + docs

## Migration Strategy

Additive library only. No schema migrations. No production dual-write.

## Testing

```bash
npm run test:knowledge
```

## Acceptance Criteria

- [x] Event-driven sync translates companies, prospects, interactions, imports
- [x] Replayable and idempotent
- [x] Tenant-aware
- [x] Rebuild from relational source without repository access
- [x] All writes through KnowledgeService
- [x] Runtime agents/server remain unwired (behavior unchanged)

## Future Work

- Shadow dual-write hooks from Scout / `dbClient`
- Persistent sync ledger
- Persistent `GraphRepository` (SPEC-001)
