# Max Prospect Orchestration — Phase 2.5 production shadow runbook

Phase 2.5 validates and observes Max in production shadow mode. It does not
enable lifecycle transitions, sequence changes, sends, tasks, calls, or
enrichment retries. The readiness check reports whether Phase 3 prerequisites
are met but never changes configuration.

## Identifier contract

- `clients.id` / `--client-id`: positive integer, for example `10`.
- `prospects.id`, `companies.id`, `--prospect-id`, `--after-id`: UUID.
- Both `--client-id 10` and `--client-id=10` are supported.
- Malformed, fractional, negative, zero, or partially numeric identifiers fail
  closed. There is no default client for Phase 2.5 deployment commands.

## Connection and migration procedure

The application and every Max command use the shared `pg.Pool` in `db.js` with
`DATABASE_URL` and Railway-compatible TLS. Run commands inside the Railway
service environment when the external proxy is unreliable.

`DATABASE_SSL=false` is supported only for disposable/local PostgreSQL
validation. Leave it unset in Railway so production TLS remains enabled.

Before any DDL:

1. Confirm a recent Railway PostgreSQL backup exists and record its restore ID.
2. Record the currently deployed application revision and all Max environment
   variables.
3. Confirm an application rollback can be deployed without removing the new
   additive tables or columns.
4. Run the migrations in a disposable transaction/schema on the same PostgreSQL
   engine. This runs the ordered migration set twice and rolls everything back:

```bash
railway run npm run max:migrations:validate
```

The required migration order is:

1. `migrations/2026-07-15-max-prospect-orchestration-v1.sql`
2. `migrations/2026-07-15-max-prospect-orchestration-phase2.sql`
3. `migrations/2026-07-15-max-prospect-orchestration-phase2_5.sql`

Apply manually, stopping on the first error:

```bash
railway run psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f migrations/2026-07-15-max-prospect-orchestration-v1.sql
railway run psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f migrations/2026-07-15-max-prospect-orchestration-phase2.sql
railway run psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f migrations/2026-07-15-max-prospect-orchestration-phase2_5.sql
railway run npm run max:schema
```

The migrations are transactional and rerunnable. Reruns recreate named checks,
foreign keys, and append-only triggers inside a transaction. They can take
brief table locks; schedule the production application during a low-traffic
window. Phase 2.5 validates existing client foreign keys and fails without
committing if orphaned client IDs exist.

## Required shadow environment

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

The equivalent prospect-facing flag names above are explicit fail-closed gates.
Client JSON overrides cannot make an unsafe validation environment pass: the
readiness command resolves the final environment-plus-client configuration and
requires every action flag to be false.

## Production-safe smoke test

Choose a designated non-DNC test prospect. The smoke command locks that row,
inserts a synthetic positive-reply signal, persists its shadow decision and
skipped actions, verifies operational fields and task queues did not change,
then rolls the entire transaction back. It never contacts the prospect.

```bash
railway run npm run max:smoke -- \
  --client-id=10 \
  --prospect-id=550e8400-e29b-41d4-a716-446655440000
```

Pass requires `valid: true`, `actions_skipped_with_shadow_mode: true`,
`operational_state_unchanged: true`, and `rolled_back: true`.

## Historical signal backfill

The default window is the most recent 14 days, matching the longest temporary
warmth window. Human, proxy, and unknown opens remain separate. Durable reply,
unsubscribe, confirmed-invalid, and meeting records are labeled separately in
the report. The command reuses live stable signal identities, so overlapping
backfill and webhook events are duplicates rather than new evidence.

Preview:

```bash
railway run npm run max:signals:backfill -- \
  --client-id=10 --from=2026-07-01T00:00:00Z \
  --to=2026-07-15T00:00:00Z --limit=500
```

Write normalized signals and shadow decisions only:

```bash
railway run npm run max:signals:backfill -- \
  --apply --client-id=10 --from=2026-07-01T00:00:00Z \
  --to=2026-07-15T00:00:00Z --limit=500 \
  --cursor='2026-07-02T12:00:00.000Z|brevo|event-id|email_human_opened'
```

Continue with the emitted `next_cursor`. Each source query is bounded, the
combined page is bounded, and reports include counts by source/event type plus
unavailable source tables.

## Recalculation and daily decay

```bash
railway run npm run max:recalculate -- --client-id=10 --limit=100
railway run npm run max:recalculate -- --apply --client-id=10 --limit=100
railway run npm run max:recalculate -- \
  --client-id=10 --prospect-id=550e8400-e29b-41d4-a716-446655440000
railway run npm run max:decay -- --client-id=10 --limit=250
railway run npm run max:decay -- --apply --client-id=10 --limit=250
```

Schedule `GET /cron/max_decay?secret=...&client_id=10&dry_run=false&limit=250`
daily only after dry-run output is reviewed. This endpoint writes shadow audits
and score decay only.

## Scout and meeting contracts

Scout now emits `prospect_discovered` from successful inserts and
`prospect_qualified` only after the explicit setter-visibility qualification
gate. LinkedIn discovery has no reliable qualification score, so only discovery
is emitted. Prospect-addressable pre-outreach disqualification and a universal
enrichment-queued record do not exist and remain gaps.

Confirmed Cal bookings emit `meeting_booked` only after Google Calendar event
creation succeeds. Historical `prospects.booked_at` is also canonical for
backfill. `safeIngestMeetingSignal` is the isolated contract for future
`meeting_cancelled`, `meeting_showed`, and `meeting_no_showed` integrations.
Cancellation and no-show have no canonical source today. Existing
`closer_status='showed'` lacks a trustworthy event timestamp, so it is not
invented as historical event data.

## Read-only sequence state

```bash
railway run npm run max:sequence:resolve -- \
  --client-id=10 \
  --prospect-id=550e8400-e29b-41d4-a716-446655440000
```

The resolver reads Emmett pending logs, `next_touch_at`,
`email_sequence_completed_at`, latest send metadata, DNC, replies, and terminal
email events. Pending logs and completion are high-confidence; field-based
active state is medium-confidence. It performs no updates.

Current overlap prevention consists of a client advisory lock plus per-prospect
pending-log exclusion. The smallest Phase 3 schema change is an authoritative
`sequence_enrollments` table keyed by client/prospect with sequence ID, status,
current step, next send, version, exit reason, and an exclusion constraint or
partial unique index allowing one active enrollment per prospect.

## Review sampling

Generate a stratified, unreviewed sample:

```bash
railway run npm run max:review -- --client-id=10 --limit=25 --max-age-days=30
```

Record an immutable review:

```bash
railway run npm run max:review -- \
  --decision-id=max-decision-id \
  --reviewer=operator@example.com \
  --outcome=agree \
  --notes='Signals and transition match the prospect record'
```

Allowed outcomes are `agree`, `disagree`, `uncertain`, `bad_data`,
`wrong_signal_classification`, `wrong_score`, and `wrong_transition`. Reviews do
not tune weights or modify decisions.

## Readiness report and guard

```bash
railway run npm run max:readiness -- --client-id=10 --since-days=30
railway run npm run max:readiness:check -- --client-id=10 --since-days=30
```

The check exits nonzero and lists unmet criteria. It never enables flags. The
target client must have an explicit rollout row; the migration creates no
allowlisted rows. After review and rollback planning, an operator may configure
the prerequisite separately:

```sql
INSERT INTO max_rollout_readiness_config
  (client_id, phase3_allowlisted, minimum_reviewed_samples,
   rollback_documented, rollback_reference, intended_first_transition, updated_by)
VALUES
  (10, false, 100, true, 'incident-runbook/revision-or-ticket',
   'cold -> heating', 'operator@example.com')
ON CONFLICT (client_id) DO UPDATE SET
  minimum_reviewed_samples=EXCLUDED.minimum_reviewed_samples,
  rollback_documented=EXCLUDED.rollback_documented,
  rollback_reference=EXCLUDED.rollback_reference,
  intended_first_transition=EXCLUDED.intended_first_transition,
  updated_by=EXCLUDED.updated_by,
  updated_at=NOW();
```

Keep `phase3_allowlisted=false` throughout Phase 2.5. A future, separately
authorized Phase 3 change must set it explicitly.

## Incident response

If an unexpected action or operational mutation is detected:

1. Set `MAX_ORCHESTRATION_ENABLED=false` and keep every action flag false.
2. Stop the `max_decay` schedule and any backfill/recalculation process.
3. Do not delete append-only evidence. Record the decision, action, signal, and
   `agent_log` IDs involved.
4. Compare `prospects.status`, sequence fields, pending Emmett logs,
   `agent_actions`, and `cal_queue` against the pre-deployment backup/snapshot.
5. Roll back the application revision. The additive database schema can remain.
6. Restore data only from an approved backup or reviewed corrective migration.
7. Re-run schema validation and the transactional smoke test before resuming
   shadow ingestion.

## Recommended first limited-autonomy candidate

After a separate Phase 3 authorization and only if every readiness criterion
passes, client `10` (Anchor Cleaning) is the safest first candidate because its
configured agent roster is Scout-only and outbound email/social automation is
disabled. The first transition should be `cold -> heating` with no sequence,
task, call, enrichment, or messaging action attached.
