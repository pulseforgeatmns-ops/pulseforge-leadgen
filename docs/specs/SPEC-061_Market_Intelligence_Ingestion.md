# SPEC-061 — Market Intelligence Ingestion

| Field | Value |
|---|---|
| **Status** | In Progress |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Max Platform |
| **Created** | 2026-08-01 |
| **Note** | Draft was labeled SPEC-052 in planning; repo SPEC-052 is Typed Artifact Validation, so this landed as SPEC-061. |

## Objective

Enable Pulseforge to learn from real-world outbound campaigns by ingesting the last 12 months of marketing emails into a structured, queryable corpus that Max can reason over later.

This is an **ingestion feature only**.

## Vision References

- [SPEC-015](SPEC-015_Market_Intelligence_Domain.md) — financial MID (separate domain; do not conflate)
- [SPEC-003](SPEC-003_Temporal_Intelligence_Memory.md) — temporal chronology patterns
- Gmail auth patterns in `rileyAgent.js` / `utils/gmailClient.js`

## Problem

Marketing emails that demonstrate real outbound cadence, copy, and sequencing live only in Gmail. Without a durable import path, Max cannot later reason over competitor/vendor campaign chronologies.

## Scope

- Import the last 12 months of marketing emails
- Import only intentionally selected messages (Gmail label)
- Preserve raw evidence
- Build company timelines (chronology only)
- Make the corpus queryable
- Support incremental sync after the initial import

## Out of Scope

- AI summarization
- Campaign scoring
- Trend detection
- Copy recommendations
- Dashboard
- Automatic campaign generation
- New agents
- Recommendations / strategy generation

## Dependencies

- Gmail OAuth (`GMAIL_CREDENTIALS`, `GMAIL_TOKEN` or `RILEY_*` tokens)
- Postgres (`DATABASE_URL`)
- Migration `migrations/2026-08-01-market-intelligence-ingestion.sql`

## Architecture

```
Gmail label (MARKET_INTEL)
        ↓
utils/gmailClient.js          (read-only list + get)
        ↓
utils/marketEmailParse.js     (raw MarketEmail extraction)
        ↓
utils/marketCompanyResolve.js (apollo.io → Apollo; else Unknown Company)
        ↓
services/marketIntelligenceIngestion.js
        ↓
market_companies + market_emails (+ sync state)
```

CRM `companies` / `prospects` are **not** written. Marketing vendors stay in `market_companies`.

## Data Model

### `market_companies`

| Column | Notes |
|---|---|
| id | UUID PK |
| domain | Unique when present (`apollo.io`) |
| name | Display name (`Apollo`) |
| is_unknown | Singleton Unknown Company row |

### `market_emails`

| Column | Notes |
|---|---|
| id | UUID PK |
| company_id | FK → market_companies |
| gmail_id | Unique |
| message_id | Unique when present (Message-ID header) |
| thread_id, subject, body_text, body_html | Raw evidence |
| from_name, from_email | Sender |
| headers, links, attachments | JSONB |
| received_at, sent_at, imported_at | Timestamps |

### Deduplication

Duplicate if same Gmail ID **or** same Message-ID header. Never import duplicates.

### Timelines

`getCompanyTimeline(companyId)` returns ordered touches (Touch 1…N by `received_at`). No intelligence — chronology only.

## Implementation Plan

1. Migration for `market_companies`, `market_emails`, `market_intel_sync_state`
2. Parse + company resolution utilities
3. Read-only Gmail client (does not modify Riley)
4. Ingestion service + CLI
5. Unit + optional Postgres tests

## Migration Strategy

- Forward: additive `CREATE TABLE IF NOT EXISTS`
- Rollback: drop the three new tables
- Compatibility: no changes to existing CRM tables or agents

## Testing

- Unit: domain → company, parse, CLI flags, report formatting
- Postgres (opt-in `MARKET_INTEL_TEST_POSTGRES=true`): import, timeline, incremental dedupe, dry-run

## Acceptance Criteria

- [x] Imports labeled emails (`label:MARKET_INTEL`)
- [x] Ignores unlabeled emails (query is label-scoped)
- [x] Imports only last 12 months by default (`newer_than:365d`)
- [x] Stores complete raw evidence
- [x] Groups emails by company
- [x] Supports repeatable incremental imports
- [x] No duplicate records (gmail_id + message_id)
- [x] Existing Pulseforge infrastructure unchanged (Riley / CRM untouched)

## CLI

```bash
npm run market:intel:import -- --days=365 --label=MARKET_INTEL --limit=1000 --dry-run
pnpm market:intel:import -- --days=365 --label=MARKET_INTEL --limit=1000 --dry-run
```

Report:

```
Imported: 1,482
Skipped: 53
Duplicates: 12
Unknown Company: 97
Duration: 48s
```

## Future Work

- Recommendations / scoring / dashboards (later specs)
- Knowledge dual-write of campaign evidence
- Public-suffix-aware domain parsing
