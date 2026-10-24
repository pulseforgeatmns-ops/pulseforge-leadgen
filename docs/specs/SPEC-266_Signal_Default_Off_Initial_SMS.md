# SPEC-266 — Signal default-off initial SMS staging

Status: In progress, local reconciliation and disabled staging only, 2026-10-24.
This records the scope of PR 980; it does not retroactively approve activation,
change current priorities or authorize publication, deployment, SQL or sends.

## Objective
Reconcile the existing review-only initial-CA SMS lane and saved-evidence risk
card with newer operator guards, keeping the new lane off until separately approved.

## Vision References
- [Product Thesis](../vision/Product_Thesis.md): evidence and gated action.
- [ADR-002](../adr/ADR-002_Explainable_AI.md) and [ADR-003](../adr/ADR-003_Human_Approval.md).
- Proposed [ADR-109](../adr/ADR-109_Signal_Initial_SMS_Staging.md).

## Problem
Initial caller notification must not wait for email market enrichment. Missing
operational risk dimensions must remain UNKNOWN rather than imply safety or
validated research status. Earlier review code must not replace newer evidence,
retry-payload or transport-test approval guards.

## Scope
Separate durable SMS enqueue/dispatch, exact-string enablement plus consent,
fixed configured recipient, explicit tenant, bounded window and message cap;
read-only display of persisted operational evidence and unsupported dimensions.

## Out of Scope
No activation, real message, live database write, migration execution, provider
request, paid research, new collector, cohort/aggregate change, empirical-guard
relaxation, trading action or business-strategy change. Existing distribution
thresholds, cooldown, shared provider quota and frozen research remain unchanged.

## Dependencies
Existing authenticated, source-pinned operator feed and operation gate; current
operator store, email dispatcher and distribution hooks; existing Twilio sender
and configured owner destination. See the [local review/rollout proposal](../signal/INITIAL_SMS_LOCAL_REVIEW_2026-10-22.md).

## Architecture
Add an isolated initial-event hook after operational enqueue, independent of
email enrichment. Defer the SMS timer until operator construction succeeds and
fence it after stop. Both `SIGNAL_OPERATOR_SMS_ENABLED` and
`SIGNAL_OPERATOR_SMS_CONSENT` require exact string `1`; both remain off for
disabled staging. An authorized read-only production UI check at 13:34 UTC on
2026-10-24 found no variables matching the exact `SIGNAL_OPERATOR_SMS` filter,
verifying both opt-ins absent/off. No configuration values were changed.
Preserve newer email dispatch, frozen payload/latency and independent test approval.

## Data Model
Manual draft only: [two-table SQL](../sql-drafts/2026-10-22-signal-initial-sms.sql).
`signal_initial_sms_windows` holds immutable tenant/namespace windows of at most
24 hours and atomic reservations (0..20, default 5). `signal_initial_sms_deliveries`
holds channel/event uniqueness, lease fences and independent acceptance/UNKNOWN
states. Test namespace cannot send. Ambiguous or abandoned attempts are never
automatically resent; reservations are not refunded. Acceptance is not receipt.

## Implementation Plan
1. Preserve current main guards while adding only the reviewed hooks and card.
2. Verify default-off, manual schema, isolation and immutable delivery semantics
   with explicit fixtures and denied network; retain native PostgreSQL tests.
3. Complete independent review and final-head hosted CI before proposing release.
4. Obtain separate approvals for publication, deployment, SQL and any activation.

## Migration Strategy
The draft remains outside `migrations/` and is not loaded by startup. It has not
been applied. Missing schema blocks only the new SMS lane. Later approved rollout
requires schema review/backup, flags off during deployment, then verified tenant,
sender/destination, combined initial/distribution caps and bounded window before
separate activation approval. Rollback disables the lane with approval; retain
receipts/reservations and never reset UNKNOWN deliveries or delete audit tables.

## Testing
Network-denied local unit/DOM and existing-guard regressions passed. Native
PostgreSQL was not run in this reconciliation because of the established local
socket permission restriction; final-head hosted disposable-PostgreSQL checks
remain required. No production readiness or handset receipt is established.

## Acceptance Criteria
- [x] New lane is default off and preserves newer operator/distribution guards.
- [x] Manual SQL remains two additive tables, outside automatic loaders.
- [x] Unsupported structural/flow/FIRST_CALLER dimensions remain UNKNOWN; saved
  heuristic risk, stale evidence and provider acceptance never imply safety,
  validated trading status, current source freshness or handset delivery.
- [ ] Final-head hosted CI validates durable isolation, caps, leases and restart.
- [x] Authorized read-only production inspection verified both new opt-ins absent/off
  at 13:34 UTC on 2026-10-24; this is inspection evidence, not owner attestation or
  permission to activate. Reconfirm if configuration changes before release.

## Future Work
Live schema/deployment, activation, combined communications budget, sender
readiness and actual delivery verification are separate approvals, not tasks
authorized by this spec. New collectors, research work and trading stay excluded.
