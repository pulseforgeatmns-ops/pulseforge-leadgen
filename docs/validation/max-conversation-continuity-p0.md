# Max conversation continuity P0 validation

The P0 freeze remains in effect. This change adds validation only; no production
flags, routing behavior, migrations, outbound paths, tenants, or resources change.

## Automated execution

Use Node 22+, locked npm dependencies, and PostgreSQL `initdb` and `pg_ctl` on PATH
(or a working `pg_config --bindir`). Run as an ordinary user, not root:

```sh
npm ci --ignore-scripts
node --test test/maxConversationHttpPostgres.test.js packages/max/context/tests/authenticatedRecovery.test.js packages/max/context/tests/conversationContextPostgres.test.js
```

Missing PostgreSQL binaries fail the test; there are no integration skips.
CI runs the HTTP test in the Max orchestration PR lane and the dedicated Max
conversation context workflow. The dedicated workflow includes router, harness,
and validation-document changes in its path triggers.

The HTTP harness evaluates the complete current `routes/maxWorkspace.js` source
with an explicit dependency allowlist. It mounts real Express routes and real auth
middleware, uses the existing real-login-handler harness and signed session cookies,
and creates fresh WorkspaceEngine/ConversationContinuity/ConversationContextStore
instances over the same disposable PostgreSQL database. Authentication sessions stay
in a fixture MemoryStore to model a surviving browser identity. This simulates
replacement of Max's runtime; it does not claim a literal operating-system restart.

Continuity is written by a normal HTTP ask, drained, and checked directly in SQL.
Recovery happens only through the HTTP ask endpoint. Mission reference validation
uses a synthetic tenant-scoped SQL table, not the production mission schema. Tests
cover concurrent recovery, expired and corrupt rows, deleted mission references,
foreign signed identities, and a closed pool. A foreign identity before recovery
gets the generic 400 unavailable response without creating a session; after the
owner recovers, the in-memory ownership guard returns 403.

The test seeds sensitive transcript/customer/approval/execution-policy sentinels
and inspects recovery before the new question can append messages. Projection and
recovery must omit those values; `Approve it.` must produce no execution audit.
External TCP, TLS, fetch and paid provider construction fail the worker even if
application code catches the failure. Only the test's allocated loopback HTTP and
PostgreSQL ports are allowed. The route's process environment is empty. The worker
replaces DATABASE_URL with an unreachable local sentinel and removes the AI key;
no production secret is needed. It never imports server.js. Disposable processes,
pools and data directories are removed by test teardown and harness exit cleanup.

## Staging validation plan (not executed by this change)

1. Require passing focused CI and review of this validation diff before any deployment.
   Confirm the intended Railway project and `max-continuity-test` environment by ID.
   Confirm `max-continuity-api` is still empty; do not assume a deploy is authorized.
2. Prepare a minimal route-only staging entry point with the same dependency
   restrictions and outbound denial. Review it separately before deploying. Do not
   use server.js, copy production secrets, or change semantic-routing/outbound flags.
   Use newly generated staging-only cookie credentials and synthetic tenants.
3. Bind only the isolated staging PostgreSQL service. Review/apply the existing
   continuity migration only there. Enable continuity only in this isolated harness.
   No production service may receive MAX_CONVERSATION_CONTEXT=true.
4. Open/login/ask, verify the projected row, replace the runtime, and ask with the
   original authenticated cookie and conversation ID. Repeat the tenant, expiry,
   corruption, missing-reference, concurrent and database-outage checks above.
   Record commit SHA, statuses, row projection, no-authority assertions, and zero
   attempted external egress without recording cookies or credentials.
5. Roll back on any leak, unexpected provider attempt, authorization restoration,
   tenant crossover, or fail-open behavior: disable continuity on the staging
   harness and stop its deployment. Preserve sanitized failure evidence for review.
6. Cleanup: stop/remove only the newly deployed staging harness after checking
   service/environment IDs; delete only its synthetic rows and fixture objects.
   Remove staging-only credentials and any newly created ingress. Keep the existing
   isolated PostgreSQL service/environment unless separately authorized to remove
   them. No production cleanup or schema rollback is part of this procedure.

## Remaining production rollout blockers

Passing this harness is validation evidence, not production rollout approval.
The isolated Railway runtime replacement rehearsal above remains outstanding.
A separately reviewed minimal staging entry point, staging safety review, explicit
rollout approval, production migration readiness, flag ownership, monitoring and
rollback criteria are still required. Production continuity must stay disabled.
The controlled mission-reference loader and in-memory authentication store limit
this test's claim: production mission hydration and durable login-session restart
behavior require their own existing regression coverage and staging verification.
