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

## Issue #1108: one-shot staging runner (implementation only)

No deployment, merge, variable change or database preparation is authorized by this
PR. After separate review and approval, run exactly:

```sh
node scripts/maxContinuity/run.cjs
```

It exits 0 only after all checks and cleanup pass. One sanitized JSON line is the
only output. It has no public API; its test listener binds `127.0.0.1` on an ephemeral
port and is torn down. No `PORT`, ingress, TCP proxy, cron or healthcheck is needed.
A runtime replacement means fresh Max engine/store objects in the same child,
while the browser's signed fixture auth session survives. This does not validate
an OS restart or production auth persistence.

### Safety boundary and configuration for later approval

The exact project/environment/service IDs below are mandatory guards, not defaults:

| Variable | Required value |
| --- | --- |
| `RAILWAY_PROJECT_ID` | `5f6c50eb-6a61-4649-885e-dc3f3b80a2e5` |
| `RAILWAY_ENVIRONMENT_ID` | `bded6fd2-b370-45be-9dd0-6a8f3683608e` |
| `RAILWAY_SERVICE_ID` | `e88561a7-a46e-4f0f-b6aa-ffa4464cf4f9` |
| `MAX_CONTINUITY_DATABASE_SERVICE_ID` | `5acd3fa8-5e3e-4761-bd80-60cd8a59142b` |
| `MAX_CONTINUITY_REHEARSAL` | `staging-only` |
| `MAX_CONTINUITY_DATABASE_URL` | Reference to the dedicated rehearsal URL on the isolated Postgres service |

Railway supplies the first three variables; do not override them to bypass guards.
The production environment `7046b237-d3a6-4884-8a1e-bd75d809fa3b` is rejected.
Read-only inspection on 2026-11-10 confirmed the intended API service had no
variables, ingress or deployment, and the intended isolated Postgres service's
private endpoint was `postgres`. Reverify IDs and endpoint before future writes.

Preparation after approval, only inside `max-continuity-test`:

1. On the isolated Postgres service, an operator creates a **new dedicated database
   and login role**, both named `max_continuity_rehearsal`, with a new staging-only
   password. The role owns only this new database, has no role memberships, and has
   NOSUPERUSER/NOCREATEDB/NOCREATEROLE/NOREPLICATION/NOBYPASSRLS. Do not grant it
   access to application databases or tables. Revoke default PUBLIC connection
   permission on any other database where this rehearsal role could otherwise
   connect. This preparation is manual and outside the runner.
2. Store a new `MAX_CONTINUITY_REHEARSAL_URL` on that isolated Postgres service,
   using this role/database and the private host `postgres.railway.internal:5432`.
   Bind API `MAX_CONTINUITY_DATABASE_URL` to
   `${{Postgres.MAX_CONTINUITY_REHEARSAL_URL}}`. Do **not** bind the administrator
   `DATABASE_URL`, a public URL, a production reference, or copied secrets.
   The runner requires `postgresql:`, that exact host/port, the dedicated user and
   database, a password, and no query/fragment/SSL override parameters. It uses
   plain PostgreSQL only over Railway's private network; TLS/public access is denied.
3. Set only the two rehearsal guard variables above on the empty API service.
   Do not attach shared production variables or credentials. Do not set
   `MAX_CONVERSATION_CONTEXT` or any outbound/semantic-routing runtime flags.
   Continuity is enabled solely in the injected local ConversationContinuity object.
4. Pin the reviewed commit, disable automatic deploy triggers, and explicitly select
   `.railway/max-continuity-rehearsal.json` as this service's config file. It selects
   the separate `scripts/maxContinuity/Dockerfile`, the one-shot command, one replica,
   and restart policy NEVER. Verify there is no inherited healthcheck, predeploy
   migration command, cron, public domain, or TCP proxy. The default Dockerfile and
   root railway.json are unchanged. A successful process exits; no always-on API
   health status is expected. Deploy only after the separate approval.

The parent validates scope before opening any database connection. The child gets
an explicit environment allowlist and an empty temporary working directory, so
provider keys, PG defaults, NODE_OPTIONS, production feature flags and `.env`
files are unavailable. It installs egress denial before application module imports:
only the exact database endpoint and allocated loopback HTTP port can connect.
TLS, fetch, UDP, other DNS, subprocesses, and route-level provider construction are
denied. Unexpected attempts remain failures even when caught by application code;
a deliberate blocked probe checks the barrier without sending a packet.

The worker verifies the database's actual current user/database and role privileges
before DDL. It creates a random `max_continuity_<24 hex digits>` schema and only two
synthetic fixture tables. It never runs the production migration loader. Its schema
search path excludes public. The #1107 scenario is shared with the original
HTTP/PostgreSQL regression, including auth, no-authority, and outage assertions.
The login helper reads the existing login declaration source for its VM fixture;
it never imports or starts server.js. Mission references remain tenant-scoped
synthetic SQL, and the AMO runtime remains memory-only.

HTTP requests time out after 5 seconds; database connections after 3 seconds,
statements after 4 seconds, and client queries after 5 seconds. The parent kills
the child at 90 seconds. All ordinary paths close the listener and continuity
queues/pools and drop only this run's schema, using a separate cleanup connection
because the outage test closes the scenario's pool. Worker stdout/stderr are
suppressed; thrown errors are never printed. A timeout/kill cannot guarantee
schema cleanup and reports `cleanup: unknown`, which is a failure requiring the
operator cleanup procedure below.

### CI evidence

```sh
node --test test/maxContinuityStagingRunner.test.js test/maxContinuityStagingPostgres.test.js test/maxConversationHttpPostgres.test.js
```

The dedicated Max context CI runs the staging guard/egress tests, real disposable
PostgreSQL rehearsal, existing #1107 HTTP regression and continuity/store tests.
The staging fixture uses the same worker, dedicated database name and restricted
role, but only a disposable loopback server. The **staging CLI has no localhost
or CI bypass**. CI also verifies privileged-role rejection before DDL, sanitized
reports and that unrelated sentinel data survives cleanup. The uploaded artifact
`max-continuity-staging-runner-ci-evidence/ci-report.json` is synthetic CI evidence,
not proof of a Railway run. No CI secrets or Railway connection are used.

### Rollback, cleanup and evidence template

On failure, leave production untouched. Stop only this reviewed staging run;
disable its source/deploy trigger and remove the rehearsal variable bindings from
this exact API service. Keep the existing isolated Postgres service, environment,
and volumes. Revoke the new rehearsal login password after collecting sanitized
evidence. Do not restart the default application or change feature flags.

On an ordinary pass/fail the run schema is dropped automatically. If cleanup is
failed/unknown (including termination), an operator connects **only** to the new
`max_continuity_rehearsal` database, checks schema owners and names, and removes
only leftover `max_continuity_<24 hex digits>` schemas owned by that role. Inspect
before deletion; do not use an unbounded database/schema wildcard drop. Remove
the dedicated database/role only under separately approved cleanup, after stopping
all runs and verifying the exact isolated service/environment. Never delete the
existing Postgres service or its volume as rehearsal cleanup.

Record this reviewable evidence without connection strings, cookies or secrets:

```text
Evidence kind: CI disposable / approved Railway staging (choose one)
Reviewed commit SHA:
CI workflow/job URL and sanitized artifact:
Reviewer and separate staging approval reference:
Project/environment/API/Postgres IDs (match table above):
Railway deployment ID and timestamp (if actually run):
Sanitized runner JSON: {version, result, checks, cleanup, ...}
Expected checks: egress_denial, authenticated_http, durable_projection,
  concurrent_recovery_and_tenant_isolation, expired, corrupt, missing-mission,
  database_outage_and_no_authority
Exit code: 0 for pass; nonzero for failure
Cleanup: complete (required); failed/unknown requires operator follow-up
Unexpected outbound/provider attempts: none (required)
Production flags/resources changed: none (required)
Rollback/cleanup owner and follow-up (if any):
```

This implementation does not complete the Railway rehearsal. Production continuity
remains off until staging evidence, independent safety review, monitoring and
rollback ownership, migration readiness, and explicit rollout authorization exist.
