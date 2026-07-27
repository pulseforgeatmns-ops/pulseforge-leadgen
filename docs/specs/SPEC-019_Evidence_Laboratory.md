# SPEC-019 — Evidence Laboratory

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v1.0.1 |
| **Priority** | High |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Completed** | 2026-07-27 |
| **Depends on** | ADR-009, SPEC-018, SPEC-015A, SPEC-017 |
| **Blocks** | Research debugging UI; historical comparison tooling |

## Objective

Provide an environment for asking questions of the Evidence Platform without changing production state.

This is **not** paper trading.

This is where developers, operators, and researchers explore evidence.

Success: given immutable observations and platform versions, the Laboratory can answer exploratory questions (ablations, injections, strategy/ontology comparisons, analogs) in isolated experiments — and nothing produced here affects production.

## Vision References

- [ADR-009 Evidence Platform Architecture](../adr/ADR-009_Evidence_Platform_Architecture.md)
- [SPEC-018 Deterministic Replay & Temporal Reasoning Engine](SPEC-018_Deterministic_Replay_and_Temporal_Reasoning_Engine.md)
- [SPEC-015A Reasoning Runtime Decoupling](SPEC-015A_Reasoning_Runtime_Decoupling.md)
- [SPEC-017 Domain Ontology Framework](SPEC-017_Domain_Ontology_Framework_and_Market_Ontology.md)
- [Reasoning_Runtime_Architecture.md](../architecture/Reasoning_Runtime_Architecture.md)

## Problem

Replay (SPEC-018) regenerates reasoning from history, but researchers still lack a safe workspace to ask:

- Show every observation supporting Claim X
- Compare two strategy packs over the same history
- Compare ontology versions
- Ask "what if Observation Y never happened?"
- Find similar historical situations
- Replay reasoning side-by-side

Without an isolated laboratory layer, exploratory work risks mutating production state or conflating research with paper trading / execution.

## Scope

- Package `packages/laboratory/`
  - `EvidenceLab.js` — primary façade (`compareReplay`, `removeObservation`, `injectObservation`, `findAnalogs`, `compareStrategies`, `compareOntologies`)
  - `ScenarioRunner.js` — executes isolated experiments via ReplayEngine
  - `EvidenceQuery.js` — claim / observation / belief query helpers
  - `ComparisonWorkspace.js` — side-by-side comparison board
  - `Experiment.js` — isolated, copy-on-write scenario container
  - `index.js` — public API (`createEvidenceLab`)
- Unit tests covering acceptance criteria
- Spec index + CHANGELOG / CURRENT_STATE updates

## Out of Scope

- Paper trading
- Execution / brokerage integrations
- Forecasting / simulation of future markets
- Persisting experiments or laboratory results
- Production graph / outbox / database writes
- UI for the laboratory (future)

## Dependencies

- ✅ ADR-009 Evidence Platform
- ✅ SPEC-018 Deterministic Replay (`@pulseforge/replay`)
- ✅ SPEC-015A Reasoning Runtime
- ✅ SPEC-017 Domain Ontology Framework
- ✅ SPEC-016 Market Strategy Pack (`@pulseforge/market-strategy`)

## Architecture

```text
                  Evidence Laboratory
                          │
            asks questions (isolated)
                          │
                          ▼
                 Experiment (copy-on-write)
                          │
                          ▼
                   ScenarioRunner
                          │
                          ▼
                    Replay Engine
                          │
                          ▼
              Evidence Platform answers
         (evidence · claims · confidence ·
          recommendations · explanation)
```

### Guiding principle

> The Laboratory asks questions. The Evidence Platform provides answers.

### Laboratory rules

| Rule | Meaning |
|---|---|
| 1 | Experiments are isolated (copy-on-write observation sets) |
| 2 | Nothing produced here affects production |
| 3 | Laboratory is not paper trading |
| 4 | Reasoning is regenerated via Replay — never restored from caches |
| 5 | Comparisons are ephemeral (ComparisonWorkspace is not persisted) |

### Components

**Experiment** — frozen baseline observations plus local remove/inject mutations. Derives children via `withRemoved` / `withInjected` / `withConfig`. Never writes to production.

**ScenarioRunner** — runs an Experiment through ReplayEngine. Refuses `persist` / `write` / `commit` options.

**EvidenceQuery** — claim evidence, contradicting evidence, belief-at-T, recommendation listing, observation diffs.

**ComparisonWorkspace** — named side-by-side boards over ReplayComparator diffs.

**EvidenceLab** — operator-facing API composing the above.

## Data Model

No new durable tables. Laboratory state is ephemeral and process-local.

### Experiment seed

```js
{
  subjectId,
  startTime,
  endTime,
  ontology,        // "market" | { id, version } | injectable descriptor
  strategyPack,    // "market" | StrategyPack instance
  observations,    // immutable observation list (cloned into experiment)
  hypothesis,      // optional research question
}
```

### Laboratory result

```js
{
  experimentId,
  experiment,      // snapshot
  observations,
  evidence,
  claims,
  confidence,
  recommendations,
  explanation,
  reasoningTrace,
  steps,
  versions,
  isolated: true,
  mutatesProduction: false,
}
```

## Implementation Plan

1. Land `packages/laboratory/` with Experiment, ScenarioRunner, EvidenceQuery, ComparisonWorkspace, EvidenceLab
2. Wire laboratory-aware `resolveBundle` so injectable Strategy Packs / ontology version labels work without production changes
3. Unit tests for isolation, ablations, injections, analogs, strategy/ontology comparison
4. Index SPEC-019; update CHANGELOG + CURRENT_STATE

## Migration Strategy

None. Pure additive package. No schema changes. Production evaluate / dual-write / replay paths unchanged.

## Testing

```bash
npm run test:laboratory
```

Coverage:

- Experiments are isolated; parent observation sets unchanged after remove/inject
- `lab.compareReplay` returns side-by-side board
- `lab.removeObservation` / `lab.injectObservation` counterfactuals
- `lab.findAnalogs` returns historical situations
- `lab.compareStrategies` / `lab.compareOntologies` over shared history
- ScenarioRunner refuses production write options

## Acceptance Criteria

- [x] `packages/laboratory/` delivers EvidenceLab, ScenarioRunner, EvidenceQuery, ComparisonWorkspace, Experiment, index
- [x] `lab.compareReplay(...)` compares two isolated replays side-by-side
- [x] `lab.removeObservation(...)` answers "what if Observation Y never happened?" without mutating the parent
- [x] `lab.injectObservation(...)` injects counterfactual observations into an isolated copy
- [x] `lab.findAnalogs(...)` finds similar historical situations
- [x] `lab.compareStrategies(...)` compares strategy packs over the same history
- [x] `lab.compareOntologies(...)` compares ontology versions over the same history
- [x] Experiments are isolated; nothing produced mutates production
- [x] Spec documented at `docs/specs/SPEC-019_Evidence_Laboratory.md`

## Future Work

- Laboratory UI for operators / researchers
- Postgres observation loader integration (read-only)
- CRM domain laboratory pack wiring
- Persisted experiment notebooks (explicit opt-in; still never write production evidence)
- Optional ADR for Laboratory as a named Evidence Platform subsystem
