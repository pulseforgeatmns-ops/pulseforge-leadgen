# @pulseforge/reasoning-runtime

Domain-neutral reasoning orchestration (SPEC-015A).

The runtime understands **evidence · claims · confidence · memory · analogs**.
Domain meaning lives in injectable **strategy packs**.

See [Reasoning_Runtime_Architecture.md](../../docs/architecture/Reasoning_Runtime_Architecture.md).

## Install (in-repo)

```js
const {
  createReasoningRuntime,
  CRMStrategyPack,
  assertStrategyPack,
} = require('@pulseforge/reasoning-runtime');
```

## Evaluate

```js
const runtime = createReasoningRuntime({
  strategyPack: crmPack,
  contextProvider: crmContextProvider,
  recommendationProvider: nextBestActionProvider,
  memory, // optional
});

const out = await runtime.evaluate(input);
// out.recommendation | explanation | report | analogs | trace | meta
```

## Contracts

- `StrategyPack` — `initialize`, `buildEvidence`, `buildClaims`, `findHistoricalAnalogs`, `rankClaims`, `generateRecommendations`, `explain`
- `ContextProvider` — `build(input)`
- `RecommendationProvider` — `generate(input)`

CRM is the default pack. Market packs attach later without modifying this package's orchestration.
