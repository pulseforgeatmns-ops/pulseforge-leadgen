# SPEC-141 — Scout Intelligence Pipeline

| Field | Value |
|---|---|
| **Status** | Implemented (architectural foundation v1) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Pulseforge |
| **Depends on** | [SPEC-100A](SPEC-100A_Scout_Acquisition_Discovery_Foundation.md), [SPEC-123](SPEC-123_Unified_Scout_Discovery_Pipeline.md), [SPEC-056](SPEC-056_Evidence_Driven_Capability_Planning.md) |

> **Numbering note:** Repository tests under `spec141.test.js` also cover the AMO Discovery Review Gate (prioritization approval before Understanding). This spec defines Scout's **intelligence operator** architecture — the 8-stage investigation pipeline that produces Mission Intelligence Reports.

## Objective

Transform Scout from a discovery worker into PulseForge's Acquisition Intelligence Engine.

Scout's responsibility is no longer "find companies." Scout develops a complete, evidence-backed understanding of an acquisition market.

## Philosophy

- Scout is not a search engine, Apollo wrapper, or Google Maps scraper.
- Scout is an intelligence operator; providers are instrumentation.
- Every conclusion has provenance; one provider never owns truth.
- Cost optimization is automatic: prefer free → cached → local → paid.

## Pipeline Stages

```
Mission
  ↓
Market Understanding
  ↓
Evidence Planning
  ↓
Provider Strategy
  ↓
Candidate Universe Discovery
  ↓
Evidence Collection
  ↓
Qualification
  ↓
Opportunity Ranking
  ↓
Market Coverage
  ↓
Mission Intelligence Report
```

| Stage | Purpose | Output |
|---|---|---|
| 1 — Market Understanding | What market am I investigating? | Market Definition |
| 2 — Evidence Planning | What evidence must exist before recommendations are trustworthy? | Evidence Plan |
| 3 — Provider Strategy | Where should evidence come from? | Provider assignments |
| 4 — Candidate Universe Discovery | Build candidate universe (not qualified prospects) | Universe + coverage % |
| 5 — Evidence Collection | Attach evidence to each candidate | Fused evidence + confidence |
| 6 — Qualification | Does this company meet ICP and buying criteria? | Qualified / Watch / Out |
| 7 — Opportunity Ranking | Priority among qualified candidates | Ranked opportunities |
| 8 — Market Coverage | Is the investigation finished? | Coverage metrics |

## Canonical Contract

```javascript
const { Scout } = require('@pulseforge/scout');

// Full 8-stage intelligence pipeline
const result = await Scout.investigate({ mission, scoutPayload, opts });

// Legacy unified discovery (SPEC-123) — still available
const discovery = await Scout.discover({ mission, missionEngine, scoutPayload, opts });
```

## Provider Capability Registry

Scout owns capabilities; providers advertise what they can supply.

| Provider | Capabilities | Cost Tier |
|---|---|---|
| PulseForge Repository | businesses, people, emails, contacts | cached |
| Google Maps | businesses, reviews, phone, hours, website | paid |
| LinkedIn | people, ownership, growth, hiring | paid |
| Hunter | emails, verification | paid |
| Prospeo | contacts, titles, enrichment, phone | paid |
| Business Websites | website, businesses, property_count | free |
| News | buying_signals, news | paid |

Selection is evidence-driven via `ProviderCapabilityRegistry.selectForCapabilities()`.

## Evidence Fusion

Multiple sources combine into a single confidence score with provenance:

```
Website + LinkedIn + Google + County Records → Confidence 0.94
```

Implemented in `packages/scout/intelligence/EvidenceFusion.js`.

## Deliverable — Mission Intelligence Report

Instead of "Found 14 companies," Scout returns:

```json
{
  "kind": "mission_intelligence_report",
  "market": "Manchester NH property management",
  "estimatedUniverse": 94,
  "coverage": 0.86,
  "qualified": 18,
  "strong": 7,
  "immediate": 2,
  "confidence": 0.92,
  "evidenceSources": ["google maps", "linkedin", "website"],
  "summary": "..."
}
```

## Implementation Map

| Module | Role |
|---|---|
| `packages/scout/Investigate.js` | `Scout.investigate()` public entry |
| `packages/scout/intelligence/Pipeline.js` | 8-stage orchestrator |
| `packages/scout/intelligence/MarketUnderstanding.js` | Stage 1 |
| `packages/scout/intelligence/EvidencePlanning.js` | Stage 2 |
| `packages/scout/intelligence/ProviderStrategy.js` | Stage 3 |
| `packages/scout/intelligence/ProviderCapabilityRegistry.js` | Provider registry + cost optimization |
| `packages/scout/intelligence/CandidateDiscovery.js` | Stage 4 (wraps SPEC-100A) |
| `packages/scout/intelligence/EvidenceCollection.js` | Stage 5 |
| `packages/scout/intelligence/EvidenceFusion.js` | Multi-source fusion |
| `packages/scout/intelligence/Qualification.js` | Stage 6 |
| `packages/scout/intelligence/OpportunityRanking.js` | Stage 7 |
| `packages/scout/intelligence/MarketCoverage.js` | Stage 8 |
| `packages/scout/intelligence/IntelligenceReport.js` | Report builder |

## Relationship to Existing Specs

- **SPEC-123** — `Scout.discover()` remains the mission-engine discovery contract.
- **SPEC-100A** — Candidate universe construction reused in Stage 4.
- **SPEC-141 Review Gate** — AMO prioritization approval (`spec141.test.js`) gates discover → understand; intelligence report feeds that gate.
- **Operational Scout (`leadgen.js`)** — Future work: emit normalized intelligence payloads through this pipeline.

## Acceptance Criteria

- [x] Eight distinct pipeline stages with separate outputs
- [x] Market definition derived before any search
- [x] Evidence plan created before provider selection
- [x] Provider Capability Registry with cost-tier optimization
- [x] Candidate universe distinct from qualified prospects
- [x] Evidence fusion with provenance and confidence
- [x] Qualification and ranking as separate stages
- [x] Market coverage metrics (estimated vs investigated vs qualified)
- [x] Mission Intelligence Report deliverable
- [x] `Scout.investigate()` canonical contract
- [x] Tests in `test/scoutIntelligencePipeline.test.js`

## Future Work

- Wire operational `leadgen.run()` cron output through intelligence pipeline
- Live Apollo adapter when API credentials available
- County records provider for property-count evidence
- Perplexity/news integration for buying signals
- AMO attach: map intelligence report → `normalizeScoutDiscoveryPayload()`
