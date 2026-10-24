# SPEC-018 — Deterministic Replay & Temporal Reasoning Engine

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v1.0.1 |
| **Priority** | High |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Completed** | 2026-07-27 |
| **Depends on** | ADR-009, SPEC-014, SPEC-015A, SPEC-016, SPEC-017 |
| **Blocks** | Historical comparison tooling; research debugging; future simulation layers |

## Objective

Introduce deterministic replay as a first-class Evidence Platform capability.

Replay reconstructs reasoning from immutable observations. It does **not** restore saved reasoning state.

Success: given a set of observations and specific platform versions, the platform reconstructs the reasoning state exactly as it existed at any point in time — without database snapshots or cached conclusions.

## Vision References

- [ADR-009 Evidence Platform Architecture](../adr/ADR-009_Evidence_Platform_Architecture.md)
- [SPEC-014 Knowledge Dual-Write](SPEC-014_Knowledge_Dual_Write.md)
- [SPEC-015A Reasoning Runtime Decoupling](SPEC-015A_Reasoning_Runtime_Decoupling.md)
- [SPEC-015 Market Intelligence Domain](SPEC-015_Market_Intelligence_Domain.md)
- [SPEC-017 Domain Ontology Framework](SPEC-017_Domain_Ontology_Framework_and_Market_Ontology.md)
- `packages/market-strategy` (SPEC-016 Market Strategy Pack)
- [Reasoning_Runtime_Architecture.md](../architecture/Reasoning_Runtime_Architecture.md)

## Problem

Today the platform can evaluate live reasoning, but cannot answer:

- What did we believe at time T?
- Why did confidence increase?
- Which observation changed the recommendation?
- When did this claim first appear / become dominant?

Historical debugging currently implies restoring snapshots or trusting stored conclusions. That violates the Evidence Platform principle that **history is stored once; reasoning is regenerated**.

## Scope

- Package `packages/replay/`
  - `ReplayEngine.js` — rebuilds reasoning from observations
  - `ReplaySession.js` — disposable execution state
  - `ReplayTimeline.js` — ordered immutable observation cursor
  - `ReplayComparator.js` — compare two replay executions
  - `types.js` — shared shapes / version metadata
  - `index.js` — public API (`createReplayEngine`)
- Temporal query helpers on replay results
- Version awareness (ontology / strategy pack / runtime)
- Deterministic identity for observations (no positional indexing)
- Unit tests covering acceptance criteria and determinism

## Out of Scope

- Paper trading
- Execution / brokerage integrations
- Forecasting
- Machine learning
- Simulation of future markets
- Persisting replay sessions
- Restoring cached claims / recommendations from storage

Replay reconstructs history only.

## Dependencies

- ✅ ADR-009 Evidence Platform
- ✅ SPEC-014 Dual Write (observation durability path)
- ✅ SPEC-015A Reasoning Runtime
- ✅ SPEC-016 Market Strategy Pack (`packages/market-strategy`)
- ✅ SPEC-017 Domain Ontology Framework

## Architecture

```text
Immutable Observations
        │
        ▼
Replay Engine
        │
        ▼
Ontology
        │
        ▼
Strategy Pack
        │
        ▼
Reasoning Runtime
        │
        ▼
Evidence · Claims · Confidence · Recommendations · Explanation
```

### Guiding principle

> History is stored once. Reasoning is regenerated.

### Replay rules

| Rule | Meaning |
|---|---|
| 1 | Replay only consumes immutable observations |
| 2 | Replay never modifies history |
| 3 | Replay regenerates reasoning — never restores conclusions |
| 4 | Replay is deterministic — identical inputs → identical outputs |
| 5 | Replay is explainable — every recommendation exposes evidence, claims, confidence, reasoning trace |

### Components

**ReplayTimeline** — ordered immutable observations with `next()`, `previous()`, `seek(timestamp)`, `seek(observationId)`. Never mutates observations.

**ReplaySession** — tracks current observation, claims, confidence, evidence, recommendation. Disposable. Not persisted.

**ReplayEngine** — rebuilds reasoning for a subject over a time window via injected ontology + strategy pack + reasoning runtime.

**ReplayComparator** — diffs two executions (e.g. runtime v1 vs v2, ontology v1 vs v2): confidence, recommendation, claim, and reasoning differences.

## Data Model

No new durable tables. Replay is ephemeral.

### Run input

```js
{
  subjectId,
  startTime,
  endTime,
  ontology,       // "market" | ontology descriptor
  strategyPack,   // "market" | strategy pack instance / id
  runtimeVersion, // optional pin
  observations,   // optional inline immutable observations
}
```

### Run output

```js
{
  observations,
  evidence,
  claims,
  confidence,
  recommendations,
  explanation,
  reasoningTrace,
  steps,          // per-observation explainability
  versions,       // ontology / strategyPack / runtime
}
```

### Per-step explainability

```js
{
  observation,
  generatedEvidence,
  affectedClaims,
  confidenceChanges,
  recommendation,
  reasoningTrace,
}
```

Observations are referenced by **deterministic IDs only** (never positional indexes).

## Implementation Plan

1. Land `packages/replay/` with timeline, session, engine, comparator
2. Wire default `"market"` resolvers to `@pulseforge/market-strategy` + ontology versions
3. Harden market recommendation / context / analog helpers for deterministic IDs and clocks
4. Add temporal query helpers on replay results
5. Unit tests: acceptance path, double-run identity, comparator, temporal queries
6. Index SPEC-018; update CHANGELOG + CURRENT_STATE

## Migration Strategy

None. Pure additive package. No schema changes. Existing live evaluate paths unchanged aside from small determinism hardening in market-strategy (recommendation id, context `builtAt`, fixed analog timestamps).

## Testing

```bash
npm run test:replay
```

Coverage:

- `createReplayEngine().run({ subjectId: "BTC", … })` returns full surface
- Two identical runs produce identical output
- Timeline seek / next / previous never mutate observations
- Comparator surfaces confidence / recommendation / claim / reasoning diffs
- Temporal queries: belief at T, confidence rise, recommendation-changing observation, claim first appearance

## Acceptance Criteria

- [x] `packages/replay/` delivers ReplayEngine, ReplaySession, ReplayTimeline, ReplayComparator, types, index
- [x] `createReplayEngine().run({ subjectId: "BTC", startTime, endTime, ontology: "market", strategyPack: "market" })` returns observations, evidence, claims, confidence, recommendations, explanation
- [x] Running the same replay twice produces identical output
- [x] Replay never writes observations or persists session state
- [x] Every step exposes evidence, claims, confidence changes, recommendation, reasoning trace
- [x] Versions recorded for ontology, strategy pack, and runtime
- [x] Spec documented at `docs/specs/SPEC-018_Deterministic_Replay_and_Temporal_Reasoning_Engine.md`

## Future Work

- Postgres observation loader via dual-write / knowledge query
- CRM domain replay pack wiring
- UI for historical comparison
- Optional ADR for replay as a core Evidence Platform subsystem (ADR-009 already lists replay infrastructure)
- Future simulation layers built *on top of* replay (not inside it)
