# @pulseforge/max

Max Reasoning Engine + Temporal Memory — deterministic, evidence-backed recommendations over the Knowledge Graph, with transition tracking over time.

**SPEC-002** · v0.8.0 · **SPEC-003** · v0.8.1

## Philosophy

Max does not make decisions. Max constructs arguments — and remembers how those arguments change.

Every recommendation answers: why this, why now, why not, how confident, what supports, what contradicts — using only facts present in the Knowledge Graph. Memory adds: what changed, why it changed, and which way the trend is moving. No LLM. No invented prose.

## Architecture

```text
Operator → Max → ReasoningEngine → KnowledgeService (Query Engine) → Graph
                 MemoryEngine   → SnapshotStore (append-only)
```

Strategies never query the graph and never mutate context. Memory never mutates snapshots.

## Use (in-repo)

```js
const { createMaxReasoningRuntime } = require('@pulseforge/max');
// or: require('../../packages/max')

const max = createMaxReasoningRuntime();

const { recommendation } = await max.evaluate({ tenantId: '10', companyId, asOf });

const { snapshot, diff, changes, evolution, temporalExplanation } = await max.remember({
  tenantId: '10',
  companyId,
  asOf,
  timestamp: '2026-07-22T12:00:00.000Z',
});

await max.memory.whatChanged({ tenantId: '10', companyId });
await max.memory.whyChanged({ tenantId: '10', companyId });
await max.memory.scoreHistory('10', companyId);
await max.memory.trend('10', companyId);

max.memory.watch({
  tenantId: '10',
  targetType: 'company',
  targetId: companyId,
  condition: { op: 'delta_abs_gt', field: 'score', value: 10 },
});
```

## Layers

| Layer | Package | Role |
|---|---|---|
| Knowledge | `@pulseforge/knowledge` | What we know |
| Reasoning | `packages/max` strategies + engine | What it means |
| Memory | `packages/max/memory` | How it changed |

## Tests

```bash
npm run test:max
# or: npm test --prefix packages/max
```

## Out of scope (this package)

Runtime agent wiring, dashboards, LLM summaries, autonomous outbound, push notifications.
