# Max canonical AO evidence context

## Scope

Local implementation and verification only. No publishing, merge, deployment, live endpoints, production writes, provider calls, message sends, credential inspection, migrations, or permission changes are performed by this patch.

Decisions refresh the canonical prospect and expectation by ID. PostgreSQL activity history comes from the actual `ao_prospect_activity` columns introduced in `2026-09-28-ao-crm-001.sql`. Caller-provided activity arrays and prose notes cannot establish completion.

- Only `spoke_decision_maker` on `call`/`outcome_logged`, or `booked_walkthrough` on `outcome_logged`/`walkthrough_booked`, produce supported resolving evidence for an `inbound_call` expectation. Contradictory `metadata.internal_outcome` is not promoted.
- Activity must fall between the canonical expectation window end and evaluation time. Other expectation types, stale/future timestamps, notes, negative calls, gatekeepers, interest, and requests for help cannot resolve this expectation.
- Activity receipts retain the original UUID, tenant, prospect, AO, activity type, outcome, metadata, and event timestamp.
- A current-owned prospect LEFT JOIN sentinel establishes explicitly empty history. A missing sentinel, changed ownership, unsupported reader, or DB error is unavailable evidence with a null count, not known zero.
- Expectation reads/updates used by resolution bind tenant, prospect, and current AO ownership. Supplied expectation status/window cannot bypass canonical refresh.
- Ingestion lists durable expectations for each target and preflights every canonical context before waking or writing. Each expectation is evaluated individually. Unidentified ingestion first checks durable resolution receipts matching its exact tenant and ingestion ID. An original single subject is reused for safe retry; multiple recorded subjects remain ambiguous. Without a receipt, fallback requires all open expectations to identify a single prospect.
- Execution/wake/persistence errors propagate instead of being reported as harmless read skips. Retry may repair supersession and attention from a verified resolution intent, but never invents a missing intent or repeats resolution effects. Retries include only resolved receipts tied to the same ingestion ID; newly open unrelated expectations cannot redirect an already established scope-less retry.
- The attention scheduler leaves unavailable context retryable. PostgreSQL attention execution remains under its existing one-connection transactional claim fence.

## Test gates

Use Node dependencies already present. `npm run test:max` includes the memory canonical evidence regression suite. `npm run test:max:expectations:postgres` includes both durable expectation verification and the new actual-schema AO evidence regression suite. The existing `npm run test:max:attention:postgres` remains a non-skipping lifecycle/fencing gate. Existing CI already invokes these commands; no workflow or migration changes are needed.

Local disposable PostgreSQL version: official Debian PostgreSQL 17.11 packages, extracted into a separate test-tools directory after explicit user approval. This is not production PostgreSQL 18.6 or prior CI PostgreSQL 16 parity. The execution sandbox rejects Unix-domain sockets; a clearly labeled local-only `pg_ctl` wrapper disables Unix sockets, while the repository harness exercises the real server over localhost TCP. SQL, restart, claim fencing, rollback, and cleanup tests are executed normally. Unix-socket transport itself is not covered in this environment.

All PG tests allocate their own disposable database, apply the actual AO and Max reliability migrations to minimal prerequisites, and clean up. They do not read `DATABASE_URL` or bootstrap the application.

Executed against the final local patch on 2026-10-14:

- `npm run test:max`: 1,422/1,422 passed.
- `npm run test:max:expectations:postgres`: 40/40 passed (24 actual-schema evidence tests, 16 durable expectation tests).
- `npm run test:max:attention:postgres`: 12/12 passed.
- CI plan, bounded log-summary, and socket-directory path unit checks: 16/16 passed.
- Every test gate had zero failures and zero skips; `git diff --check` passed. Disposable PostgreSQL processes were cleaned up.
- Local Node version: v24.19.0. Remote CI has not run because no patch was published.

## Existing lifecycle limit

Prior AO follow-up attention and resolution attention use different fingerprints. A prior follow-up attention row can remain ACTIVE after its decision/task is superseded, while the separate resolution attention row is RESOLVED. This patch verifies the expectation, stale AO task, and resolution receipt; it does not claim closure of every historical attention fingerprint. Broader attention cleanup remains a separately scoped change.

## Production verification plan, after separate approval

1. Review and publish the patch only after approval. Confirm the exact commit and required CI results, including the non-skipping PG gates. Run the same gates against the production-major PostgreSQL version in a disposable/staging environment before rollout.
2. In separately authorized read-only production inspection, verify the deployed commit, AO activity table columns/constraints, Max reliability tables, and tenant/prospect/AO relationships. No health/init routes or schema helpers should be invoked as inspection shortcuts.
3. Review a small explicitly scoped sample of canonical expectations and AO activity rows. Confirm event type/outcome/metadata/time are mapped exactly as the adapter expects; no missing history should be shown as known empty.
4. In an approved staging fixture, verify one documented conversation resolves only the matching overdue expectation and supersedes its stale AO task; another expectation with a later window remains waiting. Replaying must not create tasks/intents or repeat resolution writes.
5. Review production decision and scheduler receipts read-only after an independently approved deployment. Check unavailable-context abstentions, retry eligibility, verification failures, and owner/tenant scope. Keep external communication, cold outreach, authority escalation, and JEV shadow boundaries unchanged.
6. Any production data repair, replay execution, provider send, migration, merge, or deployment requires separate approval. A missing durable action intent is a failure to investigate, not permission to reconstruct or repeat its effect.
