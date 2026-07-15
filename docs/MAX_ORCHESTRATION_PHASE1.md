# Max Prospect Orchestration — Phase 1

Phase 1 adds a canonical lifecycle schema, normalized signal-event contract,
explainable warmth scoring, shadow state decisions, and immutable decision,
transition, and action-recommendation audits. It does not connect the new
orchestrator to Max's scheduled run or any webhook.

## Safety boundary

All flags default off except `max_shadow_mode`, which defaults on. The Phase 1
service rejects non-shadow operation. Recommended actions are stored in
`max_actions` with `action_status = 'skipped'` and `error_code = 'SHADOW_MODE'`.
It never sends email, changes enrollment, retries enrichment, or creates tasks.

## Flags

Set `MAX_ORCHESTRATION_ENABLED=true` and `MAX_SCORING_ENABLED=true` to permit
explicit callers to record shadow decisions. Leave `MAX_SHADOW_MODE=true`.
The remaining flags are present but default false:

- `MAX_STATE_TRANSITIONS_ENABLED`
- `MAX_ENRICHMENT_ACTIONS_ENABLED`
- `MAX_WARM_SEQUENCE_ENABLED`
- `MAX_CALL_TASKS_ENABLED`
- `MAX_HOT_ESCALATIONS_ENABLED`
- `MAX_RECYCLE_ACTIONS_ENABLED`
- `MAX_SEQUENCE_ACTIONS_ENABLED`
- `MAX_OPERATOR_TASKS_ENABLED`
- `MAX_ENRICHMENT_RETRY_ENABLED`
- `MAX_PROSPECT_ACTIONS_ENABLED`

Per-client overrides live in `clients.max_orchestration_config`. Environment
flags take precedence over client overrides. Invalid threshold ordering or a
request for non-shadow transitions fails during config load.

## Migration and backfill

1. Apply `migrations/2026-07-15-max-prospect-orchestration-v1.sql` with the same
   PostgreSQL migration process used for other explicit migrations.
2. Preview a resumable batch: `node scripts/backfillMaxOrchestration.js --limit 500`.
3. Review the JSON report, especially `by_state`, `errors`, and the four
   zero-valued `side_effects` counters.
4. Apply the batch: `node scripts/backfillMaxOrchestration.js --apply --limit 500`.
5. Continue with `--after-id <last_prospect_id>`. Optionally scope with
   `--client-id <id>`.

The backfill maps legacy status without overwriting it, recalculates warmth,
and creates idempotent `decision_source = 'migration'` audit rows. Reruns update
the score but do not duplicate decisions or transitions.

The backfill deliberately leaves `active_sequence_type` and
`active_sequence_id` unchanged because the current repository has no single
canonical enrollment table from which they can be reconstructed safely.

Legacy mapping: `cold → cold`, `contacted → heating`, `warm → warm`,
`hot → hot`, `closed → engaged`, `auto_responder → nurture`, `bounced → null`,
`dead → recycle`, and `do_not_email/disqualified → disqualified`. DNC records
map to `disqualified` unless already closed.

## Score strategy

Open-frequency scoring uses the highest matching tier, not cumulative points.
Only events explicitly classified as human opens count. Proxy and unknown opens
score zero. ICP ranges, ICP-delta ranges, and recency are also mutually exclusive
highest-tier components. The result is clamped to 0–100 and includes stable,
ordered explanation components.
