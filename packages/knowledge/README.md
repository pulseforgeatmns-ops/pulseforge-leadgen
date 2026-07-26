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

## Public API

| Surface | Role |
|---|---|
| `KnowledgeService` | create/update nodes & edges, find, neighbors, evidence, claims, explain, search |
| `GraphRepository` | storage contract only (`InMemoryGraphRepository` ships in this spec) |
| `EvidenceEngine` | create / attach / merge evidence, confidence |
| `ClaimEngine` | create / evaluate / invalidate / merge claims |
| `KnowledgeEventBus` | ingest path — producers emit events, never touch the repository |

## Node types

`Company` · `Person` · `Interaction` · `Evidence` · `Claim`

Each: `id`, `tenantId`, `createdAt`, `updatedAt`, `metadata` (+ type-specific fields).

## Edge types

`HAS_CONTACT` · `PARTICIPATED_IN` · `GENERATED` · `SUPPORTS` · `ABOUT` · `USES` · `LOCATED_IN` · `KNOWS` · `WORKS_FOR`

## Out of scope (this package version)

UI, visual explorer, LLM, recommendations, embeddings, production sync, persistent repositories.
