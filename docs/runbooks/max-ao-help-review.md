# One-event AO help review proof

This is a **manual, default-read-only** adapter for the existing Anchor/Evergreen
help request. It does not activate ingestion generally or automate follow-ups.
No production execution is implied by this runbook or by passing tests.

## Fixed scope and effects

- Client: `10`; AO: `24`.
- Prospect: `c36194b0-2203-4dc5-9f7f-40292b264ba1`.
- Help activity: `a299ce4e-4f43-4c62-b3d4-c962e890350c`.
- Companion CRM outcome evidence: `3cdf94e5-915a-4d75-8dee-dba2d5d3643a`.
- Stable key: `ao-help-review:v1:10:a299ce4e-4f43-4c62-b3d4-c962e890350c`.

A successful approved apply writes **exactly three rows** in one transaction:

1. A complete `max_operational_ingestions` receipt with source snapshot, version,
   coverage, uncertainty, telemetry and linked IDs.
2. An `ESCALATE_OPERATOR`, authority-C, `EXECUTION_BLOCKED` review decision.
3. An `ACTIVE`, `OPERATOR_VISIBLE`, `MANUAL` attention item with no due time.

No CRM fields, help status, ownership, source activities, claims, mutations,
expectations, intents, tasks, flags, providers, emails, calls, messages, schedules,
feature flags, or schema objects are written. No initialization/seed helpers run.
The CLI owns a separate pool and closes only that pool.

## Read-only preview and recovery

Use the existing authorized database connection environment securely. Never put
credentials in command arguments, reports, or screenshots. No `.env` is loaded
by this script and no app/shared-pool module is imported.

```sh
node scripts/reviewMaxAoHelpEvent.js \
  --client-id=10 \
  --prospect-id=c36194b0-2203-4dc5-9f7f-40292b264ba1 \
  --ao-id=24 \
  --activity-id=a299ce4e-4f43-4c62-b3d4-c962e890350c
```

The default preview is a bounded `REPEATABLE READ READ ONLY` transaction ending
in `ROLLBACK`. It returns source version, snapshot time, exact proposed IDs,
coverage, review reason and existing-chain state. Preview is not permission to
apply. A missing source/schema, inactive tenant/owner, reassignment, resolved
help, incomplete evidence, oversized history, or conflicting later evidence
fails closed. Old assignment-task completion is not proof of an email or of
completion of this help request.

The source version covers the complete bounded scoped AO activity/touchpoint
history, including equal-time/backdated records, current source/prospect/owner/
tenant versions, and all consulted contact/task/draft/flag/outbound evidence.
Eligibility is evaluated separately relative to the request. Unknown contact
timestamps require review. No unbounded or cross-tenant history is queried.

For recovery, add `--reconcile` to the same exact scope arguments. Reconcile is
read-only and reports the existing complete chain or `not_recorded`. It does not
repair, reopen, delete, rerun, or re-evaluate anything.

## Approval gates

Publication, deployment and production apply need separate explicit approval.
Suggested publication/deployment request after independent review:

> May I publish and deploy this reviewed one-event adapter, its review-only
> scheduler protections, and the no-initialization operator-queue read fix? This
> does not run the adapter, enable recurrence, send messages, or change CRM data.

Before any production apply, verify that **every active wake/scheduler runtime and
replica is running the reviewed guard revision**, and that old processes have
terminated. Do not apply during a rolling deployment or while any older worker
can wake/claim the row. Schema readiness and the local CLI files cannot establish
runtime readiness. Confirm the deployed commit, rollout completion and all
active execution surfaces separately; if this cannot be established, stop before
apply. Do not enable recurrence to perform the proof.

After that deployment verification and a fresh preview, request approval with the **actual** source
version and snapshot timestamp, not placeholders:

> May I run the one-shot Evergreen review proof for existing help event
> a299ce4e-4f43-4c62-b3d4-c962e890350c, using source version [actual hash] observed
> at [actual database snapshot time]? It will create only one durable ingestion
> receipt, one blocked operator-review decision, and one operator-visible
> attention item. It will not send anything, create a task or expectation,
> change prospect/help/ownership data, initialize schema, enable recurrence,
> access providers, or backfill other events. I will verify the saved chain and
> tenant-10 API visibility afterward.

A fresh approved apply uses the same exact scope plus these arguments:

```text
--apply
--expected-source-version=<approved preview source_version>
--preview-observed-at=<approved preview observed_at>
--confirm=operator-review-only:a299ce4e-4f43-4c62-b3d4-c962e890350c
```

Pass each as a shell-quoted `--name=value` argument. The preview must be no more
than 15 minutes old. A stale/changed source requires a new preview and approval,
not substitution of a new hash under an earlier approval.

## Freshness, concurrency and failure behavior

Apply uses a dedicated `SERIALIZABLE` transaction, a business-key advisory lock,
scoped source/prospect/owner/tenant row locks, readiness/unique-index checks and
an exact source-version comparison. It does not retry writes automatically.
Concurrent applies yield one durable chain; a loser may receive PostgreSQL
`40001`, `23505`, or a bounded lock timeout. Reconcile first, not blind reapply.
Identical successful sequential retries return the existing IDs without changing
rows. Closed attention is never reopened; an incomplete/changed chain is an
error rather than an invitation to repair it implicitly.

The receipt is explicitly **as of the database transaction snapshot**, not a
claim of commit-time or ongoing freshness. Contact records inserted after that
snapshot can serialize after the review transaction. Blocking all contact
writers would be disproportionate for a non-actioning review receipt. Outside-
system contact remains unknown. The review reason and returned result retain
the snapshot boundary; neither authorizes any follow-up.

Failures before commit roll back all three rows. If COMMIT acknowledgement is
lost, `AO_HELP_REVIEW_COMMIT_UNCERTAIN` requires read-only reconcile. If commit
succeeded but a fresh-lease readback failed, `AO_HELP_REVIEW_READBACK_REQUIRED`
also requires read-only reconcile. Never infer failure means nothing committed.
Do not delete a successful receipt to retry the proof.

## Visibility and scheduler protection

Existing read endpoint:
`GET /api/v1/max/attention/operator-queue?client_id=10`.

It revalidates active authenticated/effective admin/manager identity and tenant
binding. The operator must already be legitimately scoped to tenant 10; a query
parameter does not grant access. `listOperatorAttention` now performs SELECTs
without `init()`. Missing schema fails rather than migrating on a read.

**This proves API visibility only. No current dashboard/Command Deck UI consumes
this endpoint.** Verify the exact attention ID in the response, not just a count;
the queue is priority-limited (default 10). Do not describe this as a UI card or
as a completed user follow-up.

Reserved `ao-help-review:` fingerprints or the presence of reserved
`priority.manual_review_only` metadata exclude the item from both wake paths,
both claim stores, and the ownership-locked scheduler evaluator. Malformed flag
values fail closed. A later due time/trigger corruption must not turn it into an
automatic AO task. Ordinary pre-existing attention is unchanged.

## Local verification

Run against disposable local PostgreSQL only, never by substituting a production
connection into test fixtures:

```sh
npm run test:max
npm run test:max:attention:postgres
npm run test:max:expectations:postgres
```

The new guard suite is included in `test:max`; the durable adapter suite is
included in `test:max:attention:postgres`. They cover no-init reads, exact scope,
malformed guards, source freshness including equal/backdated history, concurrent
apply, assignment/completion uncertainty, exact three-row effects, durable final
receipts, closed/partial-chain replay, rollback and uncertain-commit recovery,
and authenticated API visibility with no task/provider execution.
