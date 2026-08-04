# SPEC-068 — Email Ingestion Completion & Corpus Activation

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Max Platform |
| **Created** | 2026-08-03 |
| **Depends** | [SPEC-061](SPEC-061_Market_Intelligence_Ingestion.md), [SPEC-065](SPEC-065_Market_Intelligence_Foundation.md), [SPEC-067](SPEC-067_Market_Intelligence_Operational_Acceptance.md) |
| **Related** | [SPEC-066](SPEC-066_Max_Market_Intelligence_Integration.md) (consumer; not smartened here) |

## Objective

Complete the email ingestion path so Pulseforge can reliably import labeled marketing emails into `market_emails`, extract observations, rebuild profiles, and produce a non-empty Market Intelligence corpus Max can inspect.

Corpus rows carry an `import_intent` / `sourceIntent` (same field) so operators can tag acquisition context when ingesting mail (default `general_market_messaging` for Jake’s personal inbox). Intent is never a factual claim that the sender is a competitor.

This spec activates the corpus. It does **not** add reasoning.

## Vision References

- [ADR-045](../adr/ADR-045_Evidence_Before_Reasoning.md) — Evidence Before Reasoning
- [SPEC-061](SPEC-061_Market_Intelligence_Ingestion.md) — raw email archive
- [SPEC-065](SPEC-065_Market_Intelligence_Foundation.md) — observations / profiles
- [SPEC-067](SPEC-067_Market_Intelligence_Operational_Acceptance.md) — readiness gates

## Problem

Market Intelligence has storage, extraction, query, readiness, and Max read-only adapter layers, but the Gmail ingestion pipeline was not operationally proven. If `market_emails = 0`, Max has no market evidence to trust.

## Scope

1. Verify Gmail credentials/token path
2. Verify the configured Gmail label exists
3. Verify labeled emails are discoverable before import
4. Safe dry-run import
5. Real import after dry-run / preflight passes
6. Confirm `market_emails > 0`
7. Confirm post-import extraction creates `market_observations`
8. Confirm profiles rebuild into `market_company_profiles`
9. Confirm SPEC-067 readiness moves off `market_email_corpus_empty`
10. Document runbook and failure modes
11. Persist per-email `import_intent` (`sourceIntent` alias) with the SPEC-068 allowlist; default `general_market_messaging`; never treat intent as a competitor fact

## Out of Scope

- Recommendations, scoring, campaign generation, Composer strategy
- CRM writes (`companies` / `prospects`)
- Prospect/company writes outside `market_*`
- Max autonomous action

## Dependencies

- `DATABASE_URL`
- Gmail OAuth: `GMAIL_CREDENTIALS` (+ `GMAIL_TOKEN`) or `RILEY_*` tokens / `GOOGLE_CLIENT_ID`+`GOOGLE_CLIENT_SECRET`
- Gmail user label `MARKET_INTEL` (exact name) applied to intended marketing emails
- Migrations from SPEC-061 / SPEC-065

## Architecture

```
npm run market:intel:preflight
        ↓
services/marketIntelligencePreflight.js
        ↓
utils/gmailClient.js  (readonly auth + labels.list + messages.list)
        ↓
pass → market:intel:import --dry-run → real import → extract → readiness
```

Import remains write-scoped to `market_companies`, `market_emails`, `market_intel_sync_state` (plus fail-soft observation/profile writes from SPEC-065).

### Import / source intent

**Rule:** `import_intent` must never be used as a factual claim that the sender is a competitor. It is **acquisition context only** (why/how the batch entered the corpus).

#### Initial allowed intents (SPEC-068)

| Intent | Acquisition context (not a competitor fact) |
|---|---|
| `general_market_messaging` | Default. Personal / general marketing corpus (e.g. Jake inbox activation). |
| `competitive_watch` | Dedicated competitive-watch inbox / label source. |
| `vendor_newsletter` | Vendor newsletter / product update source. |
| `direct_competitor` | Operator tagged the *source channel* as direct-competitor watch — still not a factual claim about the sender. |
| `indirect_competitor` | Operator tagged the *source channel* as indirect-competitor watch — still not a factual claim about the sender. |
| `unknown` | Source intent not yet classified. |

CLI: `--intent=…`, `--import-intent=…`, or `--source-intent=…` (aliases; must agree if combined). Stored on `market_emails.import_intent` and last sync on `market_intel_sync_state.import_intent`. Values outside the allowlist are rejected. Expanding the allowlist is an explicit SPEC/code change.

## Readiness Meaning (SPEC-067 / SPEC-068)

| Status | When |
|---|---|
| `blocked` | Gmail auth/label unavailable (when probed), ingestion-path hard failure, missing tables, or `market_emails = 0` (`market_email_corpus_empty`) |
| `partial` | Emails imported but extraction/profile coverage below floor or sync stale |
| `ready` | Emails imported, extraction coverage healthy, profile coverage healthy, sync fresh |

## Implementation Plan

1. This spec + operator runbook
2. Gmail label/discovery helpers on `utils/gmailClient.js`
3. Preflight service + CLI (`market:intel:preflight`)
4. Import CLI auto-preflight + unknown-company rate reporting
5. Readiness empty-corpus blocker id + optional `--probe-gmail`
6. `import_intent` / `sourceIntent` column + CLI (default `general_market_messaging`)
7. Unit tests

## Migration Strategy

Additive only:

- `migrations/2026-08-04-market-intelligence-import-intent.sql` — `market_emails.import_intent` + `market_intel_sync_state.import_intent` (default `general_market_messaging`)
- Greenfield `2026-08-01` CREATE also includes the columns for new environments
- Rollback drops the columns/index only; CRM untouched

No reinterpretation of existing rows as competitors — backfilled/default intent is `general_market_messaging`.

## Testing

- Unit: preflight fail-closed for missing credentials/label/empty+require-messages; pass path; report formatting
- Import CLI: `--preflight` / `--skip-preflight` parsing; unknown-company rate in report
- Readiness: `market_email_corpus_empty`; Gmail probe merge demotes to blocked

## Acceptance Criteria

- [x] `npm run market:intel:import -- --dry-run` discovers labeled emails after preflight
- [x] Real import creates rows in `market_emails` (existing idempotent path)
- [x] Duplicate re-runs do not create duplicate emails
- [x] Extraction creates observations for imported emails (SPEC-065 path)
- [x] Company profiles rebuild (SPEC-065 default)
- [x] SPEC-067 readiness no longer reports `market_email_corpus_empty` once emails are imported
- [x] Unknown-company rate is reported after import
- [x] Runbook documents commands, env vars, label requirements, and rollback/no-op behavior
- [x] Imports default to `general_market_messaging`
- [x] Initial allowed intents are exactly: `general_market_messaging`, `competitive_watch`, `vendor_newsletter`, `direct_competitor`, `indirect_competitor`, `unknown`
- [x] Spec/code enforce: `import_intent` is acquisition context only and must never be used as a factual claim that the sender is a competitor

## Operator Runbook

See [`../MARKET_INTEL_INGESTION_RUNBOOK.md`](../MARKET_INTEL_INGESTION_RUNBOOK.md).

## Future Work

- Scheduled incremental sync cron (still observational only)
- Public-suffix-aware domain resolution to lower unknown-company rate
- Live production activation evidence attached to CHANGELOG when first non-empty corpus ships
