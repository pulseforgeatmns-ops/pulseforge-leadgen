# @pulseforge/knowledge

Storage-agnostic knowledge layer for Pulseforge intelligence features.

**SPEC-001A** · Target version v0.7.1

## Rule

Nothing outside this package should know whether knowledge lives in memory, Postgres, Neo4j, Memgraph, or anything else. All graph operations go through `KnowledgeService`.

```text
Scout / CRM / Max
        ↓
   Knowledge events
        ↓
   KnowledgeService   ← only public write/read API
        ↓
   GraphRepository    ← swappable storage
```

## Install / use (in-repo)

```js
const {
  createKnowledgeRuntime,
  EDGE_TYPES,
  NODE_TYPES,
  KNOWLEDGE_EVENTS,
} = require('@pulseforge/knowledge');
// or: require('../../packages/knowledge')

const runtime = createKnowledgeRuntime(); // InMemoryGraphRepository by default
const { knowledge, bus } = runtime;

await bus.publish({
  type: KNOWLEDGE_EVENTS.COMPANY_OBSERVED,
  tenantId: '10',
  payload: { name: 'Lodgism', metadata: { source: 'scout' } },
});

const explanation = await knowledge.explain(tenantId, nodeId);
```

## Sync (SPEC-001B)

```js
const {
  createKnowledgeRuntime,
  mapCompanyRow,
  MemoryRelationalSource,
  PersistentGraphRepository,
  ensureKnowledgeSchema,
} = require('@pulseforge/knowledge');

// Default: in-memory
const { knowledge, sync } = createKnowledgeRuntime();

// Opt-in Postgres (SPEC-001) — same KnowledgeService API
// await ensureKnowledgeSchema(pool);
// const { knowledge, sync } = createKnowledgeRuntime({
//   repository: new PersistentGraphRepository(pool),
// });
```

All sync writes go through `KnowledgeService` (via the event bus). Callers never touch `GraphRepository` directly for business logic.

## Public API

| Surface | Role |
|---|---|
| `KnowledgeService` | create/update/ensure nodes & edges, find, neighbors, evidence, claims, explain, search |
| `GraphSyncEngine` | CRM/import/rebuild → knowledge events (idempotent, tenant-aware) |
| `GraphRepository` | storage contract — `InMemoryGraphRepository` or `PersistentGraphRepository` |
| `EvidenceEngine` | create / ensure / attach / merge evidence, confidence |
| `ClaimEngine` | create / evaluate / invalidate / merge claims |
| `KnowledgeEventBus` | ingest path — producers emit events, never touch the repository |

## Node types

`Company` · `Person` · `Interaction` · `Evidence` · `Claim`

Each: `id`, `tenantId`, `createdAt`, `updatedAt`, `metadata` (+ type-specific fields).

## Edge types

`HAS_CONTACT` · `PARTICIPATED_IN` · `GENERATED` · `SUPPORTS` · `ABOUT` · `USES` · `LOCATED_IN` · `KNOWS` · `WORKS_FOR`

## Out of scope (this package version)

UI, visual explorer, LLM, recommendations, embeddings, production sync, persistent repositories.
