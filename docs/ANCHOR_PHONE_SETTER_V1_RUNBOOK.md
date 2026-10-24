# Anchor Phone Setter v1 runbook

Scope is strictly `client_id=10` (Anchor Cleaning). This slice records manual-phone work; it does not send email/SMS, place calls, create calendar events, activate agents, or create revenue jobs.

The immediate-cash category contract is, in priority order: `cleaning_company_overflow`, `str_manager`, `property_manager`, `realtor`, `restoration_remodeling_partner`, and `commercial_office`. Other office subsegments must not replace these keys without a separately approved revision.

## Before authorization

1. Freeze and record the commit being deployed.
2. Record SHA-256 values from the committed migration files (not a working copy).
3. Record the named operator, reviewer, rollback owner, UTC change window, and a fresh restore-tested production backup.
4. Run the read-only preflight and attach its JSON output to the authorization:

```sh
npm run anchor:phone-setter:preflight
```

The report must show: Anchor active, exactly `['scout']` enabled, autosend disabled, every revenue flag false, and the Anchor campaign paused with both `external_sends_enabled` and `revenue_writes_enabled` false. Any failed check is an abort.

## Rehearsal

Run the disposable PostgreSQL harness against the exact committed migration files:

```sh
npm run test:anchor-phone-setter:postgres
```

It proves forward migration, six Tier-A Scout targets, client-1 isolation, a transactionally blocked rollback once structured call history exists, clean rollback after removing only that test history, and deterministic reapply. CI runs the same command.

## Production boundary

Production execution requires separate signed approval. Apply only `2026-07-18-anchor-phone-setter-immediate-cash-v1.sql`; do not alter client capabilities, provider settings, agent configuration, or revenue flags.

If structured `call_dispositions.details` history exists, the normal rollback deliberately aborts. Preserve the history and use an approved archival rollback procedure; do not force or edit around this guard.
