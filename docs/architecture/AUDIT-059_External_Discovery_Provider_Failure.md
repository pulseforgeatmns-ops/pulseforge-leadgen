# AUDIT-059 — External Discovery Provider Failure

| Field | Value |
|---|---|
| **Status** | Completed — first divergence identified and instrumented |
| **Date** | 2026-08-26 |
| **Related** | [SPEC-175](../specs/SPEC-175_External_Discovery_Capability.md), [SPEC-181](../specs/SPEC-181_Evidence_Native_Execution.md), [ADR-096](../adr/ADR-096_Evidence_Native_Execution.md), [AUDIT-006](AUDIT-006_Scout_Discovery_Execution_Audit.md) |
| **Scope** | Trace Mission → Investigation Plan → Provider Assignment → Provider Execution → 0 prospects. Stop at first divergence. |

## Executive summary

**First divergence is Provider Execution.** Scout reached Google Places. It did not abort at the capability gate. The operator-visible label `Property Manager` was never the request.

The actual Places Text Search queries for a property-management identity task in Manchester NH are:

```text
property management company Manchester NH
commercial property management Manchester NH
```

Those strings come from the market hypothesis registry (`property_manager` → `segmentKey: property_management`), not from Scout keyword search.

Before this audit, Places swallowed Google status (`REQUEST_DENIED`, `OVER_QUERY_LIMIT`, HTTP errors) and returned `[]` with no execution record. `rawResultCount` was the mapped candidate count. Failed HTTP looked identical to `ZERO_RESULTS` and to "never called Google." AMO then rendered **Discovery Blocked** from `qualifiedCount === 0` + `discoveryStatus === 'incomplete'`.

That is why "0 prospects" could not be diagnosed.

**Stop.** Identity, qualification, and evidence filters are not the first loss point when the provider response itself is unrecorded.

---

## Trace

```text
Mission
  ↓  valid market definition (segment + geography)
Investigation Plan          SPEC-180 — evidence tasks, not keywords
  ↓  identity task first
Provider Assignment         google_maps (Google Places) for identity
  ↓  EvidenceRequest { segment, evidenceType, geography }
Provider Execution          Places Text Search  ← FIRST DIVERGENCE
  ↓  previously: [] with no status / query / latency / quota
0 Prospects                 inferred, not observed
  ↓
Discovery Blocked           qualifiedCount=0 AND discoveryStatus=incomplete
```

---

## Answers

### 1. Did the provider execute, or did Scout abort before provider execution?

**On the mission path with an operational Places adapter: the provider executes.**

Abort-before-HTTP only happens in these recorded cases:

| Abort | When | `lastExecution` |
|---|---|---|
| Capability gate | No operational evidence-producing provider | Discovery never reaches Places (`capabilityBlocked`) |
| Adapter unavailable | `GOOGLE_PLACES_KEY` missing and no injected provider | `executeProviderAssignment` returns failed; HTTP not called |
| Invalid evidence request | Missing segment / evidenceType / cities | `abortReason: invalid_evidence_request` |
| Empty geography | `geography.cities = []` | `abortReason: empty_geography` |

A property-manager mission with Manchester NH and `GOOGLE_PLACES_KEY` present does **not** abort. Scout calls `adapter.discover({ evidenceRequest })` → `PlacesProvider.collectEvidence()`.

LinkedIn / Facebook are stubs. They are skipped (`assignment.status === 'unavailable'`), not executed.

### 2. Which provider?

**Google Places** (`providerId: google_maps`, adapter `public_business_places`).

| Provider | Role on this mission |
|---|---|
| Google Places / Google Maps | Identity (and later reviews/contact if identity is satisfied) |
| Registry / county records | Assigned for licensing; not the identity sensor |
| LinkedIn | Stub — not executed |
| Existing knowledge | Consulted first; does not replace Places for gap discovery |

### 3. What query actually ran?

**Not `Property Manager`.**

Scout never emits a search string on the mission path (ADR-096). The provider request is:

```json
{
  "segment": "property_management",
  "evidenceType": "identity",
  "geography": {
    "cities": ["Manchester"],
    "state": "NH"
  }
}
```

Places then expands `property_management` via `resolveMarketHypothesisBySegmentKey` → hypothesis `property_manager` → Google Places templates:

| Actual Text Search `query=` |
|---|
| `property management company Manchester NH` |
| `commercial property management Manchester NH` |

Endpoint: `https://maps.googleapis.com/maps/api/place/textsearch/json` (legacy Places, `?key=` auth). Not Places API (New).

### 4. Provider response

**Previously unrecorded — that is the first divergence.**

| Field | Before AUDIT-059 | After instrumentation |
|---|---|---|
| Results | Mapped candidates only (`rawResultCount = candidates.length`) | Google `results.length` on `execution.totals.results` |
| Status | Missing. Non-OK Google status returned `null` then `[]` | `googleStatus` + `httpStatus` per query |
| Latency | Missing | `latencyMs` per query and totals |
| Quota | Missing (`OVER_QUERY_LIMIT` swallowed) | `execution.quota` |
| Errors | Missing unless adapter `available() === false` | `execution.errors[]` with code `google_places_status_*` |
| Retries | Silent loop; failures indistinguishable from empty | `totals.retries`; retry only `OVER_QUERY_LIMIT` / `UNKNOWN_ERROR` |

`REQUEST_DENIED` is now `status: failed`, not `status: empty`.

### 5. Filtering — where did they disappear?

**Stop. They disappeared at Provider Execution.**

When Google returns `ZERO_RESULTS` or a failed status, counts are:

```text
Provider raw results     0
  ↓ Identity             0   (nothing to identify)
  ↓ Qualification        0
  ↓ Evidence             0
  ↓ Final candidates     0
```

Do not attribute loss to identity/qualification/evidence filters until `execution.totals.results > 0` and mapped count is lower.

### 6. Why did Discovery Blocked happen?

```text
blocked
  ↓
qualifiedCount === 0
  AND
discoveryStatus === 'incomplete'   (coverage.complete !== true)
```

`resolveBlocked()` in `ScoutDiscoveryArtifact.js`:

- Capability missing → blocked (`external_discovery_capability_unavailable`)
- Else: `qualifiedCount <= 0 && discoveryStatus === 'incomplete'` → **Discovery Blocked**
- Zero prospects with **complete** coverage is a legitimate empty market (SPEC-175 Scenario 4), not blocked

Hypothesis-driven coverage is `complete` only when `sufficientlyInvestigated`. A failed or empty identity pass leaves outstanding evidence, so coverage stays incomplete. AMO then shows **Discovery Blocked** even though Scout did run Places.

The `why` behind that incomplete zero is the provider execution record: `ZERO_RESULTS`, `REQUEST_DENIED`, quota, or abort reason — not "Property Manager returned nothing."

### 7. First divergence

**Provider Execution discarded the Google Text Search request/response.**

Stop there. Later pipeline stages cannot explain 0 prospects when the sensor result was never kept.

---

## Remediation in this change (stop at divergence)

| Change | Purpose |
|---|---|
| `PlacesProvider.lastExecution` | Durable per-call record: query, HTTP/Google status, latency, retries, quota, errors |
| Adapter + `ProviderEvidenceContract` | Surface `execution` on the provider report; `rawResultCount` is Google raw count |
| Failed Google status ≠ empty market | `REQUEST_DENIED` / quota / HTTP errors are `failed` |
| Failed providers do not satisfy evidence | Identity is not marked collected on a failed Places call |
| Scout payload `providerExecution` | Mission state can answer Q1–Q4 on the next run |

Filtering, city expansion, and cron (`leadgen.js`) are out of scope.

---

## Tests

`test/audit059ExternalDiscoveryProviderFailure.test.js`

---

## Architectural invariant

> A discovery provider that ran must leave an inspectable execution record. Empty candidates without that record are a sensor failure, not a market conclusion.
