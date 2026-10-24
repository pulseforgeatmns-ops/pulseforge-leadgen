# Reasoning Runtime Architecture

| Field | Value |
|---|---|
| **Status** | Implemented (SPEC-015A) |
| **Package** | `packages/reasoning-runtime` |
| **Related** | [ADR-009](../adr/ADR-009_Evidence_Platform_Architecture.md), [SPEC-015A](../specs/SPEC-015A_Reasoning_Runtime_Decoupling.md), [SPEC-002](../specs/SPEC-002_Max_Reasoning_Engine.md), [SPEC-015](../specs/SPEC-015_Market_Intelligence_Domain.md) |

## Purpose

Separate **how to reason** (runtime) from **what reasoning means** (strategy packs).

```text
Evidence → Memory → Reasoning Runtime → Strategy Pack → Domain Actions
```

## Layering

| Layer | Knows | Does not know |
|---|---|---|
| Evidence / Claim / Confidence | IDs, statements, numeric confidence | Prospects, BTC, outreach |
| Memory math | Snapshots, diffs, trends | Domain vocabulary |
| **Reasoning Runtime** | Interfaces + orchestration order | Any domain noun |
| **Strategy Pack** | Domain meaning | Storage / confidence algorithms |
| Recommendation Provider | Domain action taxonomy | Graph repositories |

## Contracts

### StrategyPack

```text
initialize(sessionInput)
buildEvidence()
buildClaims()
findHistoricalAnalogs()
rankClaims()
generateRecommendations()
explain()
```

The runtime calls these in order. It never `switch`es on `domain`.

### ContextProvider

```text
build(input) → immutable context
```

Runtime receives context; it does not build domain context.

### RecommendationProvider

```text
generate({ context, strategyResults, aggregated, … }) → recommendation
```

Exactly one provider is active per domain pack.

## CRM default

`CRMStrategyPack` injects existing Max modules:

- `CRMContextProvider` → `ReasoningContextBuilder`
- Claim/strategy pass → `StrategyRegistry.evaluateAll`
- Rank → `ScoreAggregator`
- `NextBestActionProvider` → `RecommendationBuilder`
- Explain → `ExplanationEngine`

Public Max API `evaluate({ tenantId, companyId })` remains unchanged.

## Attaching a future Market pack

```js
const runtime = createReasoningRuntime({
  strategyPack: marketStrategyPack,
  contextProvider: marketContextProvider,
  recommendationProvider: researchRecommendationProvider,
  memory: memoryEngine,
});
```

No changes to `ReasoningRuntime.js` are required.

## Forbidden in runtime core

Source under `packages/reasoning-runtime/` (excluding `packs/` / CRM provider wrappers) must not reference:

`prospect`, `company`, `email`, `outreach`, `appointment`, `btc`, `kalshi`, `exchange`, `market`

Domain vocabulary lives only inside strategy packs and their providers.
