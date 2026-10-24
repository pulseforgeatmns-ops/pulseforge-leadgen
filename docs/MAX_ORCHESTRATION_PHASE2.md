# Max Prospect Orchestration — Phase 2

Phase 2 connects the shadow engine to live signal paths while preserving the
existing operational behavior. New Max decisions and action recommendations
remain shadow-only. The only prospect mutation introduced here is an explicit,
authenticated manual update to the separate `lifecycle_state` field; legacy
`prospects.status` is never changed by orchestration code.

## Deployment order

1. Apply `migrations/2026-07-15-max-prospect-orchestration-v1.sql`.
2. Apply `migrations/2026-07-15-max-prospect-orchestration-phase2.sql`.
3. Run `npm run max:schema`. Do not enable ingestion until it reports valid.
4. Run the Phase 1 backfill in dry-run and bounded apply batches.
5. Preview explicit scoring with `npm run max:recalculate -- --client-id 1 --limit 25`.
6. Preview decay with `npm run max:decay`.
7. Enable shadow evaluation using the flags below.
8. Run a bounded write of shadow audits with
   `npm run max:recalculate -- --apply --client-id 1 --limit 25`.

Neither migration was applied automatically by this implementation.

## Required configuration

```text
MAX_ORCHESTRATION_ENABLED=true
MAX_SCORING_ENABLED=true
MAX_SHADOW_MODE=true
MAX_STATE_TRANSITIONS_ENABLED=false
MAX_ENRICHMENT_ACTIONS_ENABLED=false
MAX_WARM_SEQUENCE_ENABLED=false
MAX_CALL_TASKS_ENABLED=false
MAX_HOT_ESCALATIONS_ENABLED=false
MAX_RECYCLE_ACTIONS_ENABLED=false
MAX_SEQUENCE_ACTIONS_ENABLED=false
MAX_OPERATOR_TASKS_ENABLED=false
MAX_ENRICHMENT_RETRY_ENABLED=false
MAX_PROSPECT_ACTIONS_ENABLED=false
```

`DATABASE_URL` and `CRON_SECRET` remain required by the existing application.
No new provider credential is needed. Client overrides may be placed in
`clients.max_orchestration_config`; environment flags take precedence.

## Connected paths

- Brevo: delivery, human/proxy/unknown open, click, reply, unsubscribe, hard
  bounce, soft bounce, and invalid-address events.
- Riley: positive, meaningful, negative, unsubscribe, OOO, unknown reply, and
  tier-2 status-update classifications.
- ICP scoring: each persisted `icp_score_history` change.
- Enrichment: Hunter enrichment attempts and the active tiered-enrichment
  persistence path, including verified email and phone discoveries.
- Operators: `POST /api/prospects/:id/lifecycle-override`, restricted to admin
  and manager roles.

All integration hooks use a safe wrapper. A Max failure is logged to
`agent_log` and `max_orchestration_metrics` when available, but returns control
to the existing webhook or agent flow.

## Recalculation and decay

Explicit recalculation is dry-run by default and supports:

```text
--prospect-id <uuid>
--client-id <integer>
--changed-since <ISO timestamp>
--after-id <uuid>
--limit <1-2000>
--apply
```

The daily decay service selects prospects with signals near an expiration
window, stale warmth calculations, or an existing downgrade candidate. It can
run globally through the CLI or per client through `/cron/max_decay`. Apply mode
writes only scores, hysteresis metadata, decisions, unapplied transition
recommendations, skipped action recommendations, and observability records.

## Manual overrides

Example request:

```json
{
  "lifecycle_state": "warm",
  "reason": "Operator confirmed renewed buying interest",
  "source": "dashboard",
  "confirm_terminal_restore": false
}
```

Restoring `disqualified` or `null` to a non-terminal state requires
`confirm_terminal_restore: true`. Every override creates a normalized signal,
an immutable override record, an applied `operator_manual` lifecycle transition,
and updates only the canonical lifecycle columns.

## Canonical gaps

- Sequence enrollment has no single canonical table. `Verified → Sequenced` is
  unavailable until `sequence_started` signals are connected to an authoritative
  enrollment source.
- Discovery and qualification history is incomplete until Scout emits normalized
  events for all inserted prospects.
- Meeting booking has several operational representations. Phase 2 does not
  guess; `Positive reply → Meeting booked` remains unavailable until a canonical
  signal is selected.
- Historical normalized signals are not automatically reconstructed. Forward
  ingestion is canonical; Phase 1 backfill may read legacy source tables for its
  one-time score preview.
