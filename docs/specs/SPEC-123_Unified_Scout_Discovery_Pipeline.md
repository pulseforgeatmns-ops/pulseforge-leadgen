# SPEC-123 — Unified Scout Discovery Pipeline

## Purpose

Establish a single, canonical Discovery contract for Scout. Operators never need to understand whether Scout is performing retrieval, external discovery, enrichment, verification, or hybrid intelligence.

## Canonical Contract

```javascript
const { Scout } = require('@pulseforge/scout');
await Scout.discover({ mission, missionEngine, scoutPayload, opts });
```

Mission Engine calls `Scout.discover()` via `ScoutDiscoveryExecutor`. It never calls `runScoutAcquisitionIntelligence()` or `prospect_discovery()` directly.

## Internal Pipeline

```
Retrieve Existing Intelligence
  ↓
Gap Analysis
  ↓
External Discovery (demand-driven)
  ↓
Verification
  ↓
Enrichment
  ↓
Ranking
  ↓
Mission Update
```

## Strategy Selection (Internal)

Scout determines strategy automatically:

| Strategy | When |
|---|---|
| Retrieve Only | Existing intelligence sufficient; external skipped |
| Hybrid | Existing + gap discovery needed |
| External Heavy | No existing intelligence |
| Verification Only | Existing stale; re-verify before external |

Operators never select strategy.

## Discovery Outcomes

Explicit values only — never advisory prose:

- `DISCOVERY_COMPLETED`
- `DISCOVERY_PARTIAL`
- `DISCOVERY_BLOCKED`
- `DISCOVERY_FAILED`

## Observability Events

```
SCOUT_DISCOVERY_STARTED
  ↓
SCOUT_PHASE (per phase)
  ↓
SCOUT_GAP_ANALYSIS
  ↓
SCOUT_EXTERNAL_DISCOVERY
  ↓
SCOUT_VERIFICATION
  ↓
SCOUT_ENRICHMENT
  ↓
SCOUT_RANKING
  ↓
SCOUT_DISCOVERY_COMPLETED
```

## Implementation Map

| Layer | Module |
|---|---|
| Public contract | `packages/scout/index.js` → `Scout.discover()` |
| Pipeline orchestration | `packages/scout/Discovery.js` |
| Phase observability | `packages/scout/observability.js` |
| Mission integration | `packages/mission-engine/stageExecutors/ScoutDiscoveryExecutor.js` |
| External discovery (internal) | `packages/capabilities/discovery/ProspectDiscovery.js` |
| Hybrid intelligence (internal) | `packages/max/scoutAcquisition/ScoutAdapter.js` |
| Existing intelligence (internal) | `packages/max/scoutAcquisition/ExistingIntelligence.js` |
| Gap analysis (internal) | `packages/max/scoutAcquisition/CandidateUniverse.js` |
| AMO attach | `services/acquisitionMission.attachScoutDiscovery()` |

## Architectural Invariants

1. Scout owns Discovery.
2. Discovery strategy is an internal optimization.
3. Mission Engine requests outcomes, not implementations.
4. External discovery supplements existing intelligence; it does not replace it.
5. Discovery produces verified evidence before Mission updates.

## Acceptance Criteria

- [x] One public Scout Discovery contract exists (`Scout.discover()`)
- [x] Mission Engine never selects discovery strategy
- [x] Parallel discovery implementations unified behind single interface
- [x] Existing intelligence always consulted before external discovery
- [x] External discovery only fills verified gaps
- [x] Discovery outcomes are explicit
- [x] Mission state reflects complete discovery process
- [x] Operators interact only with "Scout Discovery," never implementation paths
