# Initial CA SMS and explicit risk-card readiness: local review

The 2026-10-24 reconciliation is tracked by [SPEC-266](../specs/SPEC-266_Signal_Default_Off_Initial_SMS.md)
and proposed [ADR-109](../adr/ADR-109_Signal_Initial_SMS_Staging.md). These record
disabled staging only, not retrospective activation approval or changed priorities.

Status: LOCAL ONLY. This patch addresses verified differences between current code and the requested Oct.23 operating contract. It does not establish that an Oct.14 warning was available or that this change would have prevented a loss. No deployment, live schema change, external SMS/email, or credential change has occurred.

## Changes
- A separately durable initial-CA SMS lane enqueues validated operational caller events before market enrichment. It runs independently of the existing enriched Brevo email worker.
- Fixed configured recipient only; explicit enablement and consent; tenant/namespace isolation; bounded activation at most24h; durable per-channel dedupe and atomic message cap. Initial SMS says entry risk UNKNOWN and enrichment pending, not FIRST_CALLER or investment advice.
- Provider acceptance is recorded separately from unknown handset receipt. Ambiguous failure and abandoned sending claims become UNKNOWN and are never automatically resent. Crash-safe cap reservations are never refunded.
- Existing email retry semantics are not reused for SMS. Existing distribution thresholds, cooldown, polling/provider quota, prospective research and empirical guards are untouched.
- Risk card includes requested structural/flow/state dimensions. Only persisted operational observations are displayed; unsupported fields remain UNKNOWN. No invented historical or current structure, new collectors, or research joins.

## Draft schema for review
`docs/sql-drafts/2026-10-22-signal-initial-sms.sql` adds only `signal_initial_sms_windows` and `signal_initial_sms_deliveries`, with tenant+namespace scope, durable cap, event/channel uniqueness, lease and provider-acceptance timestamps. It is outside the auto-migration directory and is never auto-applied by startup. Tests apply it only in a fresh disposable PostgreSQL instance. Missing schema blocks the SMS lane while existing email continues and logs that SMS is not ready.

This reuses the existing operational durable-outbox pattern. It does not introduce another infrastructure service, provider, general-purpose scheduler, account grant, or persistent credential.

## Configuration requiring explicit deployment review
- SIGNAL_OPERATOR_SMS_ENABLED and SIGNAL_OPERATOR_SMS_CONSENT default off.
- SIGNAL_OPERATOR_TENANT_ID: verified intended tenant, not inferred from caller content.
- SIGNAL_OPERATOR_SMS_WINDOW_START/END: explicit timezone, approved bounded window<=24h.
- SIGNAL_OPERATOR_SMS_MAX_MESSAGES: approved budget cap0..20, default5; immutable for the same tenant/window. This is separate from the distribution SMS cap and must be included in the combined communications/cost review.
- Existing TWILIO sender and OPS_ALERT_PHONE only. Do not print secrets or create/change credentials. Verify campaign/sender authorization through approved account access before activation.

## Approval-gated rollout proposal, not executed
1. Review exact diff and additive schema. Verify tenant, sender/destination, message category, initial+distribution combined caps, dates and code release. Obtain explicit production and external test-message approval.
2. Back up relevant database using established procedure. Apply reviewed additive SQL with deployment flags off; verify table constraints/indexes. Deploy reviewed commit with SMS still off.
3. Verify caller ingestion, configured identities, current sender/campaign approval and provider availability. Confirm risk card shows UNKNOWN/stale explicitly. Do not infer production deployment from passing tests.
4. Enable only the approved bounded window and approved cap. Run an explicitly authorized operational test with synthetic fixture marked test-only in the test lane; test lane never sends. For handset validation, use a separately approved transport-test procedure and do not forge empirical CA evidence.
5. For the next genuine admitted CA, retain occurredAt/ingestedAt/knowledgeAt, SMS enqueued/claimed/authorized/accepted timestamps, provider SID/status, and actual handset receipt confirmation. Verify enrichment cannot block initial dispatch, email remains backup and gates close at expiry. Do not claim receipt from a SID alone.
6. Readiness remains FAIL until these operational proofs exist. HIGH-during-cooldown behavior and missing structural collectors remain separately disclosed gaps; neither is tuned from this case.

## Reversible rollback
Disable initial-SMS enablement/consent flags and stop this new lane using the established deployment process, with approval. Existing email/distribution remain unchanged. Keep the additive tables, cap reservations and receipts for audit; do not delete records or reset UNKNOWN deliveries. Revert code only after checking there are no sends in flight. No destructive SQL rollback is supplied.

## Known limits
- One-second worker tick is not a zero-latency guarantee; DB/provider/feed/network delay remains measurable.
- Initial event freshness admission is limited to five minutes and the approved activation start. Old CA backlog does not unexpectedly notify after enabling.
- No automatic retry for ambiguous SMS. Recovery requires receipt reconciliation and a reviewed operator decision, not replay.
- Each channel's status is independent. SMS accepted does not make email accepted, or vice versa.
- Sender campaign status, production activation, provider delivery and handset receipt remain UNKNOWN until separately verified.
