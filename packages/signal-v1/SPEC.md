# SIGNAL-V1

Implementation of the attention-driven Solana market intelligence engine (paper-only).

See repository issue/spec for full doctrine. V1 explicitly excludes real-money execution paths.

## Ground truth (SIGNAL-V1-002)

- Historical market observations persist in `signal_market_observations` (Postgres in production).
- Default provider: **GeckoTerminal** public OHLCV (`aggregate=1` → 1-minute candles when trades exist; gaps are not interpolated).
- Operator commands: `npm run signal:ingest-history`, `npm run signal:replay`.
- PASS/FAIL/UNRESOLVED uses achievable entry (15s–5m delays) from observed prices only.

## Validation cohort 001 (SIGNAL-V1-003 Phase B)

- Cohort ID: `cohort-signal-v1-validation-001` (frozen, selection version `validation-001-selection-v1`).
- Acquisition pipeline: `packages/signal-v1/acquisition/` (providers, eligibility, deterministic selection, backfill, replay).
- Operator report: `signal-v1/VALIDATION_COHORT_001_REPORT.md`.
- Export: `npm run signal:validation-cohort-001` or `GET .../evaluation/export`.

## Prospective shadow mode (SIGNAL-V1-006)

- Cohort ID: `cohort-signal-v1-prospective-001` (EMPIRICAL, PROSPECTIVE, blinded until N=50).
- Historical validation remains separate: `cohort-signal-v1-validation-003`.
- Live collectors: `packages/signal-v1/collectors/` (`SIGNAL_CALLER_FEED_URL` JSON feed).
- Operator UI: `/signal-v1` Shadow Mode panel; API: `/api/v1/signal/shadow/*`.
- Cron tick: `GET/POST /cron/signal-shadow?secret={CRON_SECRET}` (set `SIGNAL_SHADOW_MODE=1` for in-process scheduler).
- Prospective-001 `startedAt` is set only after `SIGNAL_CALLER_FEED_URL` health reports `connected: true` (no synthetic backfill).

## Telegram empirical caller feed (SIGNAL-V1-007)

- Service: `services/telegramCallerFeed/` (MTProto read-only user client; deploy with `npm run signal:telegram-caller-feed`).
- Endpoints: `GET /feed` (JSON `calls[]` for `SIGNAL_CALLER_FEED_URL`), `GET /health` (latency + source availability; no secrets).
- Required secrets (Railway/env only): `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_SESSION_STRING`.
- Optional: `TELEGRAM_CALLER_SOURCES_JSON`, `TELEGRAM_CALLER_FEED_STATE_PATH`, `TELEGRAM_CALLER_FEED_POLL_MS`.
- PulseForge production: `SIGNAL_CALLER_FEED_URL=https://<feed-host>/feed`, `SIGNAL_SHADOW_MODE=1`, `SIGNAL_SHADOW_POLL_MS=60000`.

## Convergence integrity audit (SIGNAL-V1-004)

- Holdout cohort: `cohort-signal-v1-validation-002` (`validation-002-selection-v1`), built only from `procedural-holdout-catalog-v2` with evidence patterns decoupled from selection labels.
- Audit runner: `npm run signal:convergence-audit` → full audit table, provenance counts, Wilson intervals, negative controls, cross-cohort comparison.
- Regression tests: `packages/signal-v1/tests/convergenceIntegrityAudit.test.js`.
