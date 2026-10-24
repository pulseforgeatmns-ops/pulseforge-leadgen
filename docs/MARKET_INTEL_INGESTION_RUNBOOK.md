# Market Intelligence Ingestion Runbook (SPEC-068)

Activate a non-empty Phase 1 Market Intelligence corpus from labeled Gmail marketing emails. Observational only — no recommendations, scoring, campaign generation, CRM writes, or Max autonomous action.

## Prerequisites

| Requirement | Notes |
|---|---|
| Postgres | `DATABASE_URL` points at the target DB |
| Schema | SPEC-061 + SPEC-065 + SPEC-068 import_intent migrations (`market_companies`, `market_emails`, `market_observations`, `market_company_profiles`, `market_intel_sync_state`) |
| Gmail OAuth client | `GMAIL_CREDENTIALS` = full OAuth client JSON (`{"web":...}` or `{"installed":...}`), **or** `GOOGLE_CLIENT_ID` + `GOOGLE_CLIENT_SECRET` |
| Gmail tokens | `GMAIL_TOKEN` JSON, **or** `RILEY_ACCESS_TOKEN` + `RILEY_REFRESH_TOKEN` (+ optional expiry) |
| Gmail label | Exact user label name: `MARKET_INTEL` |
| Labeled mail | Apply `MARKET_INTEL` to marketing emails inside the lookback window (default 365d) |
| Import intent | Default `general_market_messaging`. Allowed: `general_market_messaging`, `competitive_watch`, `vendor_newsletter`, `direct_competitor`, `indirect_competitor`, `unknown`. |

Local file fallbacks (dev only): `./gmail_credentials.json`, `./gmail_token.json`. Prefer env vars in deployed environments. Scope used: `gmail.readonly`.

**Rule:** `import_intent` is acquisition context only. It must never be used as a factual claim that the sender is a competitor.

## Exact command sequence

```bash
# 1) Corpus readiness (DB-only). Expect blocked + market_email_corpus_empty before first import.
npm run market:intel:readiness

# 2) Gmail path check (auth + exact label + discovery). Prefer --require-messages before activating.
npm run market:intel:preflight -- --days=365 --label=MARKET_INTEL --limit=1000 --require-messages

# Optional combined probe:
npm run market:intel:readiness -- --probe-gmail --label=MARKET_INTEL --days=365

# 3) Safe dry-run (no market_* writes). Auto-runs preflight unless --skip-preflight.
#    Default intent = general_market_messaging (Jake personal inbox activation).
npm run market:intel:import -- --days=365 --label=MARKET_INTEL --limit=1000 --intent=general_market_messaging --dry-run

# 4) Real import (writes market_companies / market_emails / sync state; fail-soft extract+profile).
npm run market:intel:import -- --days=365 --label=MARKET_INTEL --limit=1000 --intent=general_market_messaging

# Later — dedicated competitive-watch inbox/label (same pipeline, different intent):
# npm run market:intel:import -- --days=365 --label=COMPETITIVE_WATCH --intent=competitive_watch --dry-run
# npm run market:intel:import -- --days=365 --label=COMPETITIVE_WATCH --intent=competitive_watch

# 5) Backfill / re-extract if needed (idempotent observations; profiles rebuild by default).
npm run market:intel:extract -- --limit=1000

# 6) Confirm readiness left market_email_corpus_empty and is partial or ready.
npm run market:intel:readiness
```

## Expected signals

### Preflight pass

- `credentials` / `auth` / `label` OK
- `discovery` reports `discoveredCount > 0` when `--require-messages` is set

### Dry-run report

```
Preflight OK — discovered N labeled messages
Mode: dry-run
Import intent: general_market_messaging
Imported: …
Skipped: …
Duplicates: …
Unknown Company: … (…%)
Duration: …s
```

Dry-run **does not** update `market_intel_sync_state` and **does not** insert emails.

### Real import

- `market_emails` count increases (or stays flat if all duplicates)
- Report includes **Unknown Company rate** (`unknown / imported`)
- Sync row `market_intel_sync_state` id=`default` gets `last_synced_at`
- Fail-soft post-import extraction may create observations/profiles immediately; re-run extract if coverage is low

### Readiness after import

| Before | After successful import |
|---|---|
| `blocked` + `market_email_corpus_empty` | `partial` or `ready` (no `market_email_corpus_empty`) |

`partial` is normal when extraction/profile coverage is below 50% or sync metadata is incomplete — run extract, then readiness again.

## Failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| `gmail_credentials_unavailable` | Missing/invalid `GMAIL_CREDENTIALS` | Set full OAuth client JSON |
| `gmail_auth_unavailable` | Missing/expired token | Set `GMAIL_TOKEN` or Riley token pair; refresh |
| `gmail_label_missing: MARKET_INTEL` | Label name mismatch | Create/rename label to exact `MARKET_INTEL` |
| `gmail_label_empty` | No labeled messages in window | Apply label, or widen `--days` |
| `gmail_discovery_failed` | Gmail API / permission error | Confirm readonly scope and mailbox |
| Dry-run `Imported: 0` after preflight | Parse skips / all already imported as duplicates on real re-run | Inspect `--json` preview; confirm labels |
| Readiness still `market_email_corpus_empty` | Real import never ran or wrong DB | Confirm `DATABASE_URL`; run import without `--dry-run` |
| Low extraction/profile coverage | Extract not run / all failures | `npm run market:intel:extract -- --limit=1000` |
| Rising Unknown Company % | Unresolvable From domains | Expected for thin senders; still imported under Unknown Company |

## Idempotency / rollback / no-op

- **Dedupe keys:** `market_emails.gmail_id` (unique) and lowercased `message_id` when present.
- **Re-run import:** duplicates counted, zero new rows — safe no-op for already-imported mail.
- **Dry-run:** always no-op for writes.
- **Preflight:** always read-only against Gmail; never writes DB.
- **Rollback of corpus rows:** not automatic. If an operator must unwind a mistaken import, delete only from `market_*` tables (observations/profiles cascade from emails when FKs require). Do **not** touch CRM `companies` / `prospects`. Prefer leaving history and labeling more carefully next sync.
- **CRM boundary:** ingestion never writes Pulseforge CRM tables.

## Operator checklist

- [ ] Migrations present
- [ ] Gmail env configured
- [ ] Label `MARKET_INTEL` exists and has mail
- [ ] Preflight passes with `--require-messages`
- [ ] Dry-run discovers messages
- [ ] Real import → `market_emails > 0`
- [ ] Extract/profiles as needed
- [ ] Readiness no longer lists `market_email_corpus_empty`
- [ ] Unknown-company rate recorded from import report
