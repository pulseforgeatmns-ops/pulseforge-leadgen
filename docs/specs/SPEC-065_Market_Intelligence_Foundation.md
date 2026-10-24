# SPEC-065 — Market Intelligence Foundation

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Max Platform |
| **Created** | 2026-08-03 |
| **Depends** | [SPEC-061](SPEC-061_Market_Intelligence_Ingestion.md), [ADR-045](../adr/ADR-045_Evidence_Before_Reasoning.md) |

## Objective

Establish Market Intelligence as a standalone observational layer on top of the SPEC-061 raw email archive. Every imported email becomes structured, evidence-linked observations; every company gets a chronological campaign timeline and a rebuilt profile; cross-market patterns are queryable via APIs — without scoring, recommendations, or Max prompt integration.

This layer answers: **"What is the market trying right now?"** It is descriptive, not prescriptive.

## Vision References

- [ADR-045](../adr/ADR-045_Evidence_Before_Reasoning.md) — Evidence Before Reasoning
- [SPEC-061](SPEC-061_Market_Intelligence_Ingestion.md) — Phase 1 ingestion (raw archive)
- [SPEC-066](SPEC-066_Max_Market_Intelligence_Integration.md) — future Max consumer (out of scope here)
- [SPEC-015](SPEC-015_Market_Intelligence_Domain.md) — **financial** MID; do not conflate

## Problem

SPEC-061 stores raw marketing emails and company chronology only. Without structured extraction, profiles, and query APIs, operators and later Max cannot inspect what competitors are saying, how messaging evolves, or what patterns exist across the corpus — except by reading individual bodies by hand.

## Scope

- Phase 2: Deterministic structured evidence extraction → `market_observations`
- Phase 3: Campaign timelines with observations + descriptive field diffs
- Phase 4: Company market profiles (`market_company_profiles`) rebuilt from evidence
- Phase 5: Cross-market analysis query service + GET-only HTTP APIs
- CLI backfill (`market:intel:extract`) and fail-soft post-import extraction hook
- Docs: this spec + ADR-045; draft SPEC-066 stub

## Out of Scope

- Scoring / ranking / declaring winners
- Recommendations or strategy generation
- Autonomous reasoning
- Max / ResponseComposer / `market_intelligence` domain prompt wiring (SPEC-066)
- LLM paraphrasing or synthetic summaries
- Knowledge graph dual-write of market observations
- Dashboards / UI
- Writes to CRM `companies` / `prospects`

## Dependencies

- Tables from `migrations/2026-08-01-market-intelligence-ingestion.sql`
- Migration `migrations/2026-08-03-market-intelligence-foundation.sql`
- Gmail-ingested rows in `market_emails` / `market_companies`

## Architecture

```
market_emails (raw)
        ↓
utils/marketEvidenceExtract.js     (deterministic, quote-backed)
        ↓
services/marketIntelligenceExtraction.js
        ↓
market_observations
        ↓
services/marketIntelligenceQuery.js
   ├─ campaign timelines + diffs
   ├─ company profiles (rebuild → market_company_profiles)
   └─ cross-market pattern / sequence stats
        ↓
routes/marketIntelligence.js       (GET-only)
```

## Data Model

### `market_observations`

One row per extracted field; FK to `market_emails` and `market_companies`. Unique `(email_id, category, field, value_text)` for idempotent re-runs. Categories: `identity`, `campaign`, `messaging`, `format`, `personalization`. Textual claims require `evidence_quote` + `evidence_path`.

### `market_company_profiles`

Materialized observational profile per company: counts, first/last seen, distinct offers, primary positioning, current CTA, latest direction, `profile_json` with `evidenceRefs`. No recommendation columns.

## Implementation Plan

1. ADR-045 + this spec + SPEC-066 draft
2. Additive migration + rollback
3. Extractors + extraction service + CLI + post-import hook
4. Query service (timeline, diffs, profiles, cross-market)
5. GET-only routes
6. Unit + route smoke tests

## Migration Strategy

- Forward: additive `CREATE TABLE IF NOT EXISTS`
- Rollback: drop `market_observations`, `market_company_profiles`
- Compatibility: no changes to CRM or Riley

## Testing

- Unit: extractors (quote presence, omit-when-absent, format/personalization)
- Unit: timeline diffs, profile rebuild, cross-market aggregations
- Route smoke: auth gates + response shapes (mocked pool)

## Acceptance Criteria

- [x] Every selected email can be converted into structured observations idempotently
- [x] Company campaign timeline includes observations and evidence quotes
- [x] Timeline field diffs are descriptive only (no improvement language)
- [x] Company profiles rebuild from observations with `evidenceRefs`
- [x] Cross-market pattern and sequence-stat APIs return counts + sample evidence refs
- [x] No scoring, recommendations, or Max wiring in this release

## Future Work

- SPEC-066 Max Market Intelligence Integration (consumer)
- Knowledge dual-write of campaign evidence
- Richer campaign segmentation beyond chronological company timeline
- Public-suffix-aware domain parsing (SPEC-061 carryover)
