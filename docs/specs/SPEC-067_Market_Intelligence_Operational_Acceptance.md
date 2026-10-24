# SPEC-067 — Market Intelligence Operational Acceptance

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Max Platform |
| **Created** | 2026-08-03 |
| **Depends** | [SPEC-061](SPEC-061_Market_Intelligence_Ingestion.md), [SPEC-065](SPEC-065_Market_Intelligence_Foundation.md), [ADR-045](../adr/ADR-045_Evidence_Before_Reasoning.md) |
| **Related** | [SPEC-066](SPEC-066_Max_Market_Intelligence_Integration.md) (consumer; not smartened here) |

## Objective

Make the Market Intelligence layer **operationally verifiable**: a single read-only readiness report that tells an operator whether Phase 1 (ingestion + foundation corpus) can be trusted yet — without scoring campaigns, generating recommendations, writing CRM rows, or creating Max side effects.

This spec makes MI inspectable, not smarter.

## Vision References

- [ADR-045](../adr/ADR-045_Evidence_Before_Reasoning.md) — Evidence Before Reasoning
- [SPEC-061](SPEC-061_Market_Intelligence_Ingestion.md) — raw email archive
- [SPEC-065](SPEC-065_Market_Intelligence_Foundation.md) — observations, profiles, query APIs
- [SPEC-066](SPEC-066_Max_Market_Intelligence_Integration.md) — Max consumer (must remain side-effect free)

## Problem

SPEC-061/065/066 code can be merged while the live corpus is empty, partially extracted, or schema-incomplete. Operators cannot tell whether “Market Intelligence is ready” from code presence alone. Empty success messages would falsely green-light Max / Phase 1 trust.

## Scope

- Read-only readiness service reporting:
  - migration/table readiness for `market_companies`, `market_emails`, `market_observations`, `market_company_profiles`, `market_intel_sync_state`
  - total imported emails
  - total observations
  - extraction coverage (email-level and company-level percentages)
  - companies observed
  - unknown-company signal (`unknownCompanyPresent`, `emailsAssignedToUnknown`)
  - last sync state
  - profile rebuild coverage
  - readiness status: `ready` | `partial` | `blocked`
  - blockers and next actions
- CLI: `npm run market:intel:readiness`
- GET-only admin route: `GET /api/v1/market-intel/readiness`
- Unit + route tests
- This spec document

## Out of Scope

- Recommendations, scoring, ranking, or strategy generation
- CRM writes (`companies` / `prospects`)
- Max prompt/Composer changes or any Max side effects
- New ingestion, extraction, or profile-rebuild algorithms
- Dashboards / UI beyond the JSON/CLI report
- Changing ADR-045 boundaries

## Dependencies

- Tables from `migrations/2026-08-01-market-intelligence-ingestion.sql`
- Tables from `migrations/2026-08-03-market-intelligence-foundation.sql`
- Existing query/extraction services (read-only consumption of row counts)

## Architecture

```
CLI / GET /api/v1/market-intel/readiness
        ↓
services/marketIntelligenceReadiness.js   (read-only)
        ↓
to_regclass + COUNT / sync_state SELECTs on market_* tables
        ↓
{ status, metrics, blockers, nextActions }
```

No writes. No Max runtime. No scoring fields.

## Readiness Rules

### Table readiness

Required tables (all must resolve via `to_regclass('public.<table>')`):

1. `market_companies`
2. `market_emails`
3. `market_observations`
4. `market_company_profiles`
5. `market_intel_sync_state`

### Metrics

| Metric | Definition |
|---|---|
| `totalEmails` | `COUNT(*)` from `market_emails` |
| `totalObservations` | `COUNT(*)` from `market_observations` |
| `companiesObserved` | Distinct `company_id` values on `market_emails` |
| `emailsWithObservations` | Distinct `email_id` on `market_observations` |
| `emailExtractionCoveragePct` | `emailsWithObservations / totalEmails * 100` (0 if no emails) |
| `companiesWithObservations` | Distinct `company_id` on `market_observations` |
| `companyExtractionCoveragePct` | `companiesWithObservations / companiesObserved * 100` (0 if none observed) |
| `companiesWithProfiles` | Observed companies that have a `market_company_profiles` row |
| `profileRebuildCoveragePct` | `companiesWithProfiles / companiesObserved * 100` (0 if none observed) |
| `unknownCompanyPresent` | Whether the `is_unknown = TRUE` singleton exists |
| `emailsAssignedToUnknown` | Emails whose `company_id` is the unknown singleton |
| `lastSyncState` | Row from `market_intel_sync_state` id=`default` (or null) |

Ready floors (sensible defaults; email-level coverage is the readiness gate):

- `EMAIL_EXTRACTION_READY_FLOOR = 50`
- `PROFILE_REBUILD_READY_FLOOR = 50`

### Status

| Status | When |
|---|---|
| `blocked` | Any required table missing **or** `totalEmails === 0` (`market_email_corpus_empty`) **or** corpus counts cannot be queried **or** optional Gmail probe reports hard ingestion-path failure |
| `partial` | All tables present, `totalEmails > 0`, but email extraction coverage **or** profile rebuild coverage is below floor, **or** `last_synced_at` is null |
| `ready` | All tables present, `totalEmails > 0`, email extraction coverage ≥ 50%, profile rebuild coverage ≥ 50%, and `last_synced_at` is present |

Company-level extraction coverage is reported for operator context but does not independently gate `ready`.

### Blockers & next actions

Derived deterministically from the metrics above (examples):

- Missing table → run the matching SPEC-061 / SPEC-065 migration
- Zero emails → `market_email_corpus_empty` → run `npm run market:intel:import`
- Low email extraction coverage → run `npm run market:intel:extract`
- Low profile coverage → run extract with profile rebuild (default)
- Null `last_synced_at` with emails present → confirm sync state write path / re-import
- Optional Gmail probe (`--probe-gmail`, SPEC-068) → auth/label hard failures demote readiness to `blocked`

## Implementation Plan

1. This spec + README index row
2. `services/marketIntelligenceReadiness.js`
3. `scripts/marketIntelReadiness.js` + `npm run market:intel:readiness`
4. GET route on existing market-intel router
5. Unit + route smoke tests

## Migration Strategy

None. Read-only acceptance over existing additive migrations.

## Testing

- Unit: status derivation for blocked (missing table / empty corpus), partial (low coverage), ready (meets floors)
- Route: GET readiness registered, auth-gated, POST/mutation routes still absent for this path family
- CLI: `--help` / JSON report shape via script exports

## Acceptance Criteria

- [x] Spec checked in as `docs/specs/SPEC-067_Market_Intelligence_Operational_Acceptance.md`
- [x] Readiness service reports all required fields
- [x] Empty corpus reports `blocked` (not success)
- [x] CLI `npm run market:intel:readiness` works locally
- [x] `GET /api/v1/market-intel/readiness` is admin/manager, GET-only, `Cache-Control: no-store`
- [x] Unit/route tests pass locally
- [x] No recommendations, scoring, CRM writes, or Max side effects

## Future Work

- Promote thresholds after a live corpus baseline exists
- Optional Postgres harness smoke against disposable DB
- Operator digest / Max briefing that *cites* readiness (still read-only; SPEC-066)
