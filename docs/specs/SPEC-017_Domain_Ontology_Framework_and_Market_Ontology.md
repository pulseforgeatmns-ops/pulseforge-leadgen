# SPEC-017 — Domain Ontology Framework & Market Ontology

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v1.0.1 |
| **Priority** | High |
| **Owner** | |
| **Created** | 2026-07-26 |

## Objective

Introduce a pluggable domain ontology framework so the Evidence Graph remains universal while domains contribute vocabulary. Ship Market as the first domain implementation, proving that future domains require only Ontology + Strategy Pack + Context Provider — no changes to Graph, Memory, Claim Engine, Confidence Engine, or Runtime.

## Vision References

- `docs/adr/ADR-009_Evidence_Platform_Architecture.md`
- `docs/architecture/EVIDENCE_CORE_DOMAIN_AUDIT.md`
- `docs/specs/SPEC-001_Persistent_Knowledge_Store.md`
- `docs/specs/SPEC-003_Temporal_Intelligence_Memory.md`
- `docs/specs/SPEC-014_Knowledge_Dual_Write.md`
- `docs/specs/SPEC-015A_Reasoning_Runtime_Decoupling.md`
- `packages/market-strategy` (SPEC-016 Market Strategy Pack)

## Problem

The Knowledge Graph shipped with a closed CRM ontology (`company`, `person`, `interaction`). Market and future domains cannot contribute vocabulary without editing core type registries. Observations, outcomes, and universal edges (`CONTRADICTS`, `RESULTED_IN`, …) were not first-class. Domain meaning leaked into the Evidence Core.

## Scope

- Domain Ontology contract: entity types, relationship types, observation types, claim vocabulary, outcome vocabulary
- `OntologyRegistry` with CRM as default domain
- Core graph invariants: `observation`, `evidence`, `claim`, `outcome`
- Universal edge vocabulary: `SUPPORTS`, `CONTRADICTS`, `ABOUT`, `GENERATED`, `RESULTED_IN`, `OBSERVED_ON`, `PART_OF`, `INFLUENCED`, `SIMILAR_TO`
- Provenance and deterministic identity helpers
- Immutable `Observation` node factory (Rule 1)
- `Outcome` node factory
- Generic `OntologyEntity` factory for registered domain entity types
- `@pulseforge/market-ontology` — first domain implementation
- Tests proving new domain registration without core engine changes

## Out of Scope

- Persisting market observations through dual-write adapters (SPEC-015)
- Retroactive migration of CRM sync mappers to ontology modules
- Hypothesis engine beyond existing `ClaimEngine`
- Trading / execution vocabulary

## Dependencies

- ADR-009 Evidence Platform
- SPEC-001 Knowledge Store
- SPEC-003 Memory
- SPEC-014 Dual Write
- SPEC-015A Reasoning Runtime
- SPEC-016 Market Strategy Pack

## Architecture

```
                     Evidence Graph
                 Entities · Relationships
                 Evidence · Claims · Outcomes
                        ▲
                Ontology Extension
        CRM        Market      Manufacturing
```

**Guiding principle:** Evidence Graphs store reality. Ontologies provide meaning.

### Domain Ontology Contract

Every ontology contributes:

| Contribution | Purpose |
|---|---|
| Entity Types | Identifiable things (Company, BTC, Machine) |
| Relationship Types | Named edges (`TRADES_ON`, `WORKS_FOR`) |
| Observation Types | Immutable fact vocabulary |
| Claim Vocabulary | Proposition labels (no algorithms) |
| Outcome Vocabulary | Reality validation labels |

Nothing else.

### Core Graph Invariants (permanent)

| Invariant | Rule |
|---|---|
| Entity | Identifiable thing; domain-specific subtypes register via ontology |
| Subject | Every reasoning session concerns exactly one Subject |
| Observation | Immutable, append-only fact |
| Evidence | Derived interpretation of observations; reproducible |
| Claim | Proposition; accumulates evidence only — no business logic |
| Outcome | Reality that validates claims |

### Graph Rules

1. Observations are immutable — never modify, only append
2. Evidence is reproducible from observations
3. Claims never contain business logic
4. Everything has provenance (`origin`, `adapter`, `observed_at`, `recorded_at`, `version`, `tenant`, `confidence`)
5. Every edge has meaning — no anonymous edges
6. Replay reconstructs from observations, not conclusions

### Package layout

```
packages/knowledge/ontology/     # framework + CRM default
packages/market-ontology/        # Market vocabulary + identities
packages/market-strategy/        # Strategy Pack (unchanged)
```

## Data Model

No new Postgres tables. Ontology vocabulary is runtime registry + node `type` / `metadata` fields. Observations and outcomes use existing JSONB node storage.

### Provenance (minimum metadata)

- `origin`, `adapter`, `rawReference`, `observedAt`, `recordedAt`, `version`, `tenant`, `confidence`

### Market Ontology (first implementation)

**Entity types:** `asset`, `exchange`, `contract`, `market`, `market_session`, `economic_calendar`, `news_source`, `indicator`

**Subject types:** `asset`, `contract`

**Observation types:** `price_tick`, `volume_update`, `volatility_observation`, `funding_update`, `liquidation`, `news_event`, `economic_release`, `session_transition`, `indicator_snapshot`

**Claim vocabulary:** `momentum_continuation`, `momentum_exhaustion`, `elevated_volatility`, `regime_transition`, `mean_reversion`, `liquidity_contraction`, `news_driven_expansion`

**Outcome vocabulary:** `trend_continued`, `trend_failed`, `volatility_expanded`, `volatility_contracted`, `range_held`, `breakout_confirmed`, `breakout_failed`

**Relationships:** `TRADES_ON` (Asset → Exchange), plus universal core edges

## Implementation Plan

1. `packages/knowledge/ontology/` — registry, contract, CRM default, provenance, identity
2. Core node types `observation`, `outcome`; universal edges in registry
3. `packages/market-ontology/` — Market vocabulary + `registerMarketOntology()`
4. Tests: ontology framework + market registration + graph writes
5. Spec doc + README index

## Migration Strategy

Backward compatible. CRM node/edge constants unchanged. `getOntologyRegistry()` auto-registers CRM on first access. Market domains call `registerMarketOntology()` explicitly. No DB migration required.

## Testing

```bash
npm run test:knowledge
npm run test:market-ontology
```

- Domain ontology contract validation
- Registry collision prevention
- Immutable observation enforcement
- Market entity/edge acceptance after registration
- Claim/confidence engines unchanged with market subjects

## Acceptance Criteria

- [x] A new domain can be introduced by supplying only: Ontology, Strategy Pack, Context Provider
- [x] No changes to: Graph storage, Memory, Claim Engine, Confidence Engine, Runtime
- [x] CRM behavior preserved (default ontology)
- [x] Market ontology registers entity, observation, claim, outcome, and relationship vocabulary
- [x] Deterministic identity for market assets and observations
- [x] Observations reject updates (Rule 1)
- [x] Universal edges (`CONTRADICTS`, etc.) accepted by graph

## Future Work

- Extract CRM sync/dual-write into explicit CRM adapter using ontology IDs
- Persist market observations via dual-write (SPEC-015)
- Wire `market-strategy` observation type aliases to canonical ontology ids
- Manufacturing domain ontology as second proof point
