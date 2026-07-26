# @pulseforge/knowledge

Storage-agnostic knowledge layer for Pulseforge intelligence features.

**SPEC-001A** · v0.7.1 · **SPEC-001B** · v0.7.2 · **SPEC-001** · v0.7.3 · **SPEC-001C** · v0.7.4

## Rule

Nothing outside this package should know whether knowledge lives in memory, Postgres, Neo4j, Memgraph, or anything else. All graph operations go through `KnowledgeService`.

```text
Scout / CRM / Max
        ↓
   Knowledge events
        ↓
   KnowledgeService   ← only public write/read/query API
        ↓
   QueryEngine        ← filters, traversal, timeline, path, metrics
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

const companies = await knowledge.findCompanies({
  tenantId: '10',
  industry: 'property',
  limit: 20,
});

const explanation = await knowledge.explain({ tenantId: '10', nodeId });
```

## Query Engine (SPEC-001C)

```js
await knowledge.findCompanies({ tenantId, industry, technology, location, confidenceMin });
await knowledge.findPeople({ tenantId, companyId, email, title });
await knowledge.findInteractions({ tenantId, channel, relatedNodeId });
await knowledge.neighbors({ tenantId, nodeId, edgeTypes: ['WORKS_FOR'], direction: 'out' });
await knowledge.related({ tenantId, nodeId, depth: 2 });
await knowledge.timeline({ tenantId, nodeId });
await knowledge.path({ tenantId, fromId, toId });
await knowledge.explain({ tenantId, nodeId }); // includes timelinePosition

knowledge.getLastQueryMetrics(); // structured instrumentation
```

Results are domain objects only — no formatting, summaries, or AI text.

Max Reasoning Engine (SPEC-002) consumes this query API via `packages/max` — it never touches repositories directly.

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
| `KnowledgeService` | create/update/ensure nodes & edges, find, query, neighbors, related, timeline, path, evidence, claims, explain, search |
| `QueryEngine` | structured interrogation (used by KnowledgeService) |
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

## Out of Scope (this package version)

UI, visual explorer, LLM, recommendations, embeddings, production agent wiring, metrics dashboards.
