# AUDIT-063 — Places API Cost Attribution

| Field | Value |
|---|---|
| **Status** | Remediated — traced client + `places_api_requests` ledger |
| **Date** | 2026-08-26 |
| **Report** | `node scripts/placesCostReport.js` |

## Executive summary

Google Places traffic was previously untraced. Multiple modules (`leadgen.js`, `PlacesProvider`, `scoutPublicSourcing.js`, `phoneEnrich.js`, diagnostics) called Text Search and Place Details directly with no shared attribution layer, making cost allocation by feature impossible.

**Remediation:** All production Places/Geocoding HTTP calls route through `utils/placesApi.js`, which records one row per request in `places_api_requests` via `utils/placesCostAttribution.js`.

## Recorded fields (per request)

| Field | Source |
|---|---|
| `caller` | Module name (e.g. `leadgen.js`, `PlacesProvider`) |
| `feature` | Business bucket: Leadgen, Discovery, Candidate Refresh, Warm Routing, Diagnostic, Geocode, Script |
| `mission_id`, `mission_stage` | Acquisition mission context |
| `tenant_id`, `execution_id`, `operator_id` | Tenant + execution + operator |
| `trigger_mode` | `scheduler`, `manual`, `operator`, `cron`, or `unknown` |
| `hypothesis_id`, `hypothesis_label` | Cognitive — why Scout called Google |
| `evidence_requirement`, `investigation_task`, `provider_id` | Evidence-native assignment |
| `cache_hit`, `cache_miss`, `cache_age`, `cache_key`, `cache_strategy` | Query cache observability |
| `original_query`, `normalized_query` | Duplicate-query analysis |
| `businesses_returned`, `businesses_accepted`, `candidates_created`, `qualified_candidates` | Yield funnel |
| `endpoint`, `cost_class`, `http_status`, `google_status`, `latency_ms`, `created_at` | API telemetry |

## Trace path

```text
Entry (leadgen / Discovery / setter enrich-phone / diagnostic)
  ↓  withPlacesContext({ mission, cognitive, tenant, operator, trigger })
utils/placesQueryCache.js (memory TTL — hit/miss observability)
utils/placesApi.js (legacyTextSearch, legacyPlaceDetails, …)
  ↓  recordPlacesRequest() with query canonicalization + yield
places_api_requests (PostgreSQL)
  ↓  scripts/placesCostReport.js --day YYYY-MM-DD
Spend / Cache / Cognitive / Efficiency tables
```

## Caller → feature mapping

| Report caller label | Feature key | Modules |
|---|---|---|
| leadgen.js | Leadgen | `leadgen.js` Scout cron/CLI |
| Scout Discovery | Discovery | `PlacesProvider`, `DiscoveryAdapters`, `scoutPublicSourcing.js` |
| Candidate Refresh | Candidate Refresh | `phoneEnrich.js`, setter `/enrich-phone` |
| Warm Routing | Warm Routing | Reserved — no Places usage today |
| Diagnostic | Diagnostic | `scoutPlacesDiagnostic.js` |
| Geocode | Geocode | `aoRoutePlanner.js` |

## Sample report

Run:

```bash
node scripts/placesCostReport.js
node scripts/placesCostReport.js --day 2026-08-25
node scripts/placesCostReport.js --since 2026-08-01 --tenant 1 --mission amo-123 --json
```

Produces markdown tables:

1. **Spend by caller** — Calls, % of total, estimated spend
2. **Spend by feature** — Calls and %
3. **Spend by endpoint** — Text Search, Place Details, …
4. **Cache** — Hit/miss rate, duplicate normalized queries
5. **Cognitive breakdown** — Hypothesis, evidence requirement, investigation task
6. **Spend by mission** — When mission_id is populated
7. **Efficiency funnel** — Calls → returned → accepted → candidates → qualified + cost per candidate
8. **Feature Details/Text ratio**

## Out of scope (this pass)

- One-off scripts (`scripts/sourceAcquisitionTargets.js`, `marketBakeoff.js`, `importAnchorCallList.js`) still call Places directly; migrate when those scripts run in production cadence.
- Warm Routing does not call Places today; the bucket exists for future routing geocoding.
