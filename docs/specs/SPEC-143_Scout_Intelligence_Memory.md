# SPEC-143 — Scout Acquisition Intelligence Memory

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Pulseforge |
| **Depends on** | [SPEC-142](SPEC-142_Evidence_Driven_Investigation_Engine.md), [SPEC-100](SPEC-100_Scout_Acquisition_Intelligence_Loop.md), [SPEC-118](SPEC-118_Acquisition_Mission_Orchestration.md) |

## Objective

Scout should never investigate the same thing twice. Every investigation should permanently improve future investigations.

Scout owns **Acquisition Intelligence Memory** — durable knowledge that compounds across missions.

## Philosophy

**Today (pre-SPEC-143):**

```
Mission → Scout investigates → Mission ends → Knowledge disappears
```

**Future (SPEC-143):**

```
Mission → Scout investigates → Knowledge extracted → Knowledge verified
→ Knowledge stored → Future investigations begin smarter
```

## Memory Types

| Type | Example | Stores |
|---|---|---|
| **Market Memory** | Greater Manchester | Known industries, market size, buying behavior, seasonality, competition, coverage |
| **Company Memory** | ABC Property Management | Known offices, decision makers, cleaning vendors, buying signals, evidence |
| **Person Memory** | John Smith | Role, preferred channel, response history, relationship history |
| **Claim Memory** | "ABC manages 41 STRs" | Confidence, verification sources, evidence, contradictions |
| **Investigation Memory** | Manchester PM pass | Attempted steps, resolved/remaining gaps, source chain |

## Knowledge Extraction

Every completed investigation asks: **What did we permanently learn?**

`extractKnowledgeFromInvestigation()` transforms investigation results into durable memory objects. Called automatically at the end of `runInvestigationEngine()` unless `opts.persistMemory === false`.

## Memory Confidence

Memory ages. Confidence decays with a 90-day half-life. More verification sources slow decay.

```javascript
{
  confidence: 0.91,
  freshnessDays: 32,
  sourceCount: 3,
  verificationSources: ['website', 'linkedin', 'county_records']
}
```

Old knowledge naturally decays until refreshed by a new investigation.

## Cross-Investigation Learning

Mission 1 discovers "ABC uses Vendor X." Mission 2 loads claim memory and begins with that knowledge preloaded — no re-investigation required when confidence remains above threshold.

## Contradictions

When new evidence conflicts with stored claims (e.g., 15 employees → 120 employees), memory status becomes `conflict` and the starting point routes the claim to **Need to verify** with action `reinvestigate`.

## Memory Graph

Everything connects: Market → Companies → People → Claims → Evidence → Sources.

Built via `buildMemoryGraphFromKnowledge()` and serialized on persist.

## Investigation Starting Point

Every new mission begins with:

```
Known → Unknown → Need to verify → Need to discover
```

instead of a blank slate. `prepareInvestigationWithMemory()` loads tenant-scoped memory and `buildInvestigationStartingPoint()` classifies each claim.

Prior investigation steps with resolved gaps are added to the `attempted` set so Scout does not repeat completed work.

## Storage

| Backend | Module | Durability |
|---|---|---|
| In-memory | `createMemoryIntelligenceStore()` | Process lifetime (tests, dev) |
| Postgres | `createPostgresIntelligenceStore(pool)` | Production |

Tables: `scout_intelligence_memory`, `scout_intelligence_memory_edges`

Migration: `migrations/2026-08-23-scout-intelligence-memory.sql`

## Integration Points

| Location | Role |
|---|---|
| `packages/scout/memory/` | Core memory package |
| `packages/scout/investigation/InvestigationLoop.js` | Load at start, persist at end |
| `packages/scout/Investigate.js` | Returns `startingPoint`, `memoryLoaded`, `memoryPersist` |
| `packages/scout/index.js` | Exports `memory` namespace |

## API

```javascript
const { memory } = require('@pulseforge/scout');

// Load before investigation
const prep = await memory.prepareInvestigationWithMemory({ tenantId, marketDefinition });

// Persist after investigation
await memory.persistInvestigationKnowledge(investigationResult, { tenantId, missionId });

// Direct store access
const store = memory.createMemoryIntelligenceStore();
await store.loadForMarket(tenantId, geography, segment);
```

## Acceptance Criteria

A second investigation into the same market requires dramatically less work than the first because Scout begins with validated acquisition intelligence rather than rediscovering known facts.

Verified by `test/scoutIntelligenceMemory.test.js`:
- Knowledge extraction from completed investigations
- Confidence decay over time
- Starting point classification (known/unknown/verify/discover)
- Contradiction detection and reconciliation
- Persist → reload → second investigation with prior knowledge
- Skipped investigation steps from prior runs

## Future Work

- Wire Postgres store as production default via `services/scoutAcquisitionIntelligence.js`
- Dual-write high-confidence claims to platform knowledge graph (`packages/knowledge`)
- Extend `ExistingIntelligence.loadRepository()` to merge claim memory with CRM records
- Persist operational Scout (`leadgen.js`) enrichment as company memory
- Max briefing section summarizing memory coverage per market
