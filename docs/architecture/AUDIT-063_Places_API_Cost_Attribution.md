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
| `mission_id` | AsyncLocalStorage / explicit override when present |
| `tenant_id` | Client/tenant id from context |
| `execution_id` | Run or lead id when present |
| `trigger_mode` | `scheduler`, `manual`, `operator`, `cron`, or `unknown` |
| `endpoint` | `text_search`, `place_details`, `search_text`, `place_details_v1`, `nearby_search`, `autocomplete`, `find_place`, `geocode` |
| `is_autocomplete` / `is_nearby_search` / `is_find_place` | Endpoint flags |
| `cost_class` | Google SKU label (e.g. Place Search — Text Search) |
| `http_status`, `google_status`, `latency_ms`, `created_at` | Response telemetry |

## Trace path

```text
Entry (leadgen run / Discovery adapter / setter enrich-phone / diagnostic)
  ↓  withPlacesContext({ caller, feature, tenantId, missionId, executionId, triggerMode })
utils/placesApi.js (legacyTextSearch, legacyPlaceDetails, v1SearchText, v1PlaceDetails, geocodeAddress)
  ↓  recordPlacesRequest()
places_api_requests (PostgreSQL)
  ↓  scripts/placesCostReport.js
Caller / Endpoint / Details:Text ratio tables
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
node scripts/placesCostReport.js --since 2026-08-01 --tenant 1 --json
```

Produces markdown tables:

1. **By caller** — Calls and % of total (leadgen.js, Scout Discovery, Candidate Refresh, …)
2. **By endpoint** — Text Search, Place Details, Nearby Search, Autocomplete, …
3. **Feature Details/Text ratio** — e.g. Leadgen `842:421`, Discovery `12:432`

## Out of scope (this pass)

- One-off scripts (`scripts/sourceAcquisitionTargets.js`, `marketBakeoff.js`, `importAnchorCallList.js`) still call Places directly; migrate when those scripts run in production cadence.
- Warm Routing does not call Places today; the bucket exists for future routing geocoding.
