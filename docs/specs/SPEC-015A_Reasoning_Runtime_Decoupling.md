# SPEC-015A — Reasoning Runtime Decoupling

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v1.0.0 |
| **Priority** | Highest |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-26 |
| **Completed** | 2026-07-26 |
| **Depends on** | SPEC-001 / 001A–C, SPEC-002, SPEC-003, SPEC-014 |
| **Blocks** | SPEC-015 Market Intelligence Domain; any future non-CRM domain |

## Objective

Transform the CRM-oriented reasoning shell into a fully domain-neutral runtime.

The runtime understands only:

- evidence
- claims
- confidence
- memory
- historical analogs

It never understands prospects, outreach, appointments, BTC, Kalshi, or markets. Those belong to domain strategy packs.

Success: CRM behavior is identical before and after; a Market Strategy Pack could attach later without modifying runtime code.

## Vision References

- `docs/vision/Intelligence_Architecture.md`
- [ADR-002](../adr/ADR-002_Explainable_AI.md)
- [ADR-004](../adr/ADR-004_Knowledge_Graph.md)
- [ADR-009](../adr/ADR-009_Evidence_Platform_Architecture.md)
- [SPEC-002](SPEC-002_Max_Reasoning_Engine.md)
- [SPEC-003](SPEC-003_Temporal_Intelligence_Memory.md)
- [SPEC-014](SPEC-014_Knowledge_Dual_Write.md)
- [SPEC-015](SPEC-015_Market_Intelligence_Domain.md)
- [EVIDENCE_CORE_DOMAIN_AUDIT.md](../architecture/EVIDENCE_CORE_DOMAIN_AUDIT.md)
- [SPEC015_FEASIBILITY.md](../architecture/SPEC015_FEASIBILITY.md)
- [Reasoning_Runtime_Architecture.md](../architecture/Reasoning_Runtime_Architecture.md)

## Problem

Evidence Core, Claim Engine, Memory math, and confidence primitives are generic. The orchestration shell still assumes CRM workflows (`companyId`, people, outreach actions). SPEC-015 cannot land cleanly until that assumption is removed without changing current CRM behavior.

## Scope

- Package `packages/reasoning-runtime/`
  - `StrategyPack` interface
  - `ContextProvider` interface
  - `RecommendationProvider` interface
  - `ReasoningRuntime` dependency injection
  - `CRMStrategyPack` (wraps existing CRM logic; identical outputs)
  - Architecture documentation
- Wire `packages/max` `ReasoningEngine` through the runtime with the CRM pack as default
- Preserve existing `evaluate({ tenantId, companyId })` caller API

## Out of Scope

- Market Strategy Pack implementation
- Market context / recommendation providers
- Schema / migration changes
- Evidence Core / Claim Engine / Confidence algorithm changes
- Behavior changes to CRM recommendations, scores, or explanations
- Moving CRM strategies out of `packages/max/strategies/`

## Dependencies

- ✅ SPEC-001 Persistent Knowledge Store (+ 001A–C)
- ✅ SPEC-002 Max Reasoning Engine
- ✅ SPEC-003 Temporal Intelligence Memory
- SPEC-014 Knowledge Dual-Write (operational path; not required for unit tests)

## Architecture

```text
Evidence
  ↓
Memory
  ↓
Reasoning Runtime          ← domain-neutral; interfaces only
  ↓
Strategy Pack              ← CRM today; Market later
  ↓
Domain Actions
```

The runtime invokes the strategy pack. The runtime never branches on domain type.

```text
interface StrategyPack {
  initialize(context)
  buildEvidence()
  buildClaims()
  findHistoricalAnalogs()
  rankClaims()
  generateRecommendations()
  explain()
}

interface ContextProvider {
  build(input) → ReasoningContext
}

interface RecommendationProvider {
  generate(input) → Recommendation
}
```

### CRM Strategy Pack

Encapsulates existing CRM logic:

- outreach recommendations
- follow-up timing
- appointment / engagement likelihood signals
- decay-aware observations (via strategies)
- pipeline-oriented actions

Wired via DI to existing `ReasoningContextBuilder`, strategy registry, `ScoreAggregator`, `RecommendationBuilder`, `ExplanationEngine`.

### Future Market Strategy Pack

Not implemented. Will eventually provide regime detection, evidence weighting, analog search, market explanations, and research recommendations. Execution remains outside scope (SPEC-015).

### Explainability

Every strategy pack must return:

- Evidence used
- Claims evaluated
- Historical analogs
- Confidence changes
- Reasoning trace

No opaque outputs.

### Claim Engine

No new hypothesis engine. Market hypotheses become Claims. CRM strategy observations surface as Claims. Future domains reuse the same engine. No algorithm changes.

## Data Model

No schema changes. Runtime session state is in-memory only:

```text
ReasoningSession {
  input, context, evidence, claims,
  strategyResults, analogs, ranked,
  recommendation, explanation, trace
}
```

## Implementation Plan

1. Land `packages/reasoning-runtime` interfaces + `ReasoningRuntime`
2. Implement `CRMStrategyPack` + CRM context/recommendation providers (DI wrappers)
3. Point `ReasoningEngine.evaluate` at the runtime (CRM pack default)
4. Parity tests against fixture evaluate outputs
5. Docs: SPEC-015A, architecture note, index updates

## Migration Strategy

- Additive package only
- Default wiring = CRM pack → identical public API
- No feature flags required
- Rollback = revert commit (no DB)

## Testing

- Unit: interface asserts, runtime orchestration order, domain-term absence in runtime core
- Parity: CRM evaluate recommendation/score/confidence/action match pre-refactor fixture expectations
- Regression: `npm run test:max` + `npm run test:reasoning-runtime`

## Acceptance Criteria

- [x] Evidence Runtime can execute without knowing: Prospect, Company, Email, BTC, Exchange, Kalshi, Market
- [x] The runtime interacts only with interfaces
- [x] CRM behavior remains unchanged
- [x] Existing Max tests continue to pass
- [x] A Market Strategy Pack could be attached without modifying runtime code
- [x] No schema changes
- [x] No Evidence / Claim / Confidence algorithm changes

## Future Work

- SPEC-015 Market Strategy Pack + MarketContextProvider + ResearchRecommendationProvider
- Subject alias (`subjectId`) across Memory public API (audit R1)
- Soft-gate CRM change labels when non-CRM packs are active (audit R10)
