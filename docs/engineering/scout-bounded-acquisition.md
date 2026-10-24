# Bounded standalone Scout acquisition

This is preparation code, not authorization to run. It does not turn on the app,
change its research pause, install schema, or send/enrich any candidate. No live
provider execution is established by synthetic tests.

## Narrow path

The standalone command composes the real PlacesProvider, checkpoint session,
version 2 coverage grant/page store, candidate ticket store, entity resolution,
ownership checks and existing Scout queue-admission function. It deliberately
does not call defaultScoutRamp, whose recovery, enrichment and backlog passes are
outside this scope.

Only tenant 10 in confirmed legacy recovery mode is supported. An exact manifest
binds the current program and mission source (including timestamp revisions), the
existing mission fingerprint, one identity-evidence query, its original root and
its first refresh page, and a version 2 grant permitting one new page. A stale,
missing, revoked, expired or changed grant/source fails closed. Program authority
is the explicit projection `id, tenant_id, source_mission_id, policy, policy_hash,
scope_hash, authorized_by, authorized_at, mode`. Routine `last_tick_at` and
`last_error` telemetry do not authorize behavior and do not invalidate the hash.
The locked source/program authority is rechecked before every request. It must be a
handled legacy root; pre-existing refresh pages cannot be restarted or advanced.

Fetched results enter immutable page/checkpoint storage and the real ticket
transfer path. Admission claims only candidates whose memberships belong
exclusively to this new page, using SQL checks before claim/attempt increments.
The existing work-row lock is rechecked at a fresh snapshot, so concurrent
membership changes cannot authorize unrelated old work. Rediscovered businesses
may gain normal provenance memberships or changed-fact state under the existing
transfer rules, but this run never claims a candidate shared with an older query.
Old payloads and attempt counts are not reset. Canonical queue deduplication,
ownership and geography/segment checks remain unchanged.

The result is raw business acquisition and, when admission succeeds, queue entries
awaiting enrichment. It is **not verified or sendable contact inventory**.
`cleanVerifiedContacts` is always zero. Reports distinguish distinct page candidates,
shared-old memberships, new-page-only claims, unclaimed candidates, canonical
queue admissions, numeric admission/ownership rejection counters and conservative
reserved attempts/cost. A completed execution can have zero new queue admissions. Unclassifiable records retain normal
candidate responsibility without website investigation. No contact recovery,
Hunter, Prospeo, verification, Anthropic, enrichment, send, scheduler or backlog
drain is invoked.

## Actual transport and modeled cost

A durable run claim is unique by approval reference, exact manifest, grant and
first page. A restart, different process or modified run argument cannot renew
that claim. A separate attempt row is committed before shared policy reservation
and HTTP, retaining its debit even if later steps fail. All actual attempts,
including failed/retried searches, count cumulatively against:

- At most 3 legacy Text Search requests at 40000 microUSD each.
- At most 20 legacy Place Details requests at 25000 microUSD each.
- An explicitly approved modeled cap no greater than 620000 microUSD ($0.62).

These conservative public-rate estimates exclude tax and other account-specific
charges. They are not an invoice guarantee or permission to spend. The existing
shared API meter still reserves and enforces global daily/monthly request and USD
policy **per attempt**, not once for the envelope. Unconfirmed global settlement
stops further transport and keeps the local reservation. The default shared
runtime behavior is unchanged; this path opts into existing-schema-only checks.

The only enabled HTTP endpoint is the exact approved search or details for a
place ID returned by that search. No page token, different query, altered detail
field list, redirect, cache, inflight cache sharing, provider fallback or extra
endpoint is allowed. The standalone process blocks other global HTTP/fetch,
DNS resolution, arbitrary TCP/TLS, UDP, worker creation and subprocess transports;
only its exact existing DB endpoint and captive Google request path are allowed.
This confinement belongs in the standalone process, never the app process.

Before every attempt, after lock waits, the runner rechecks the existing research
pause, shared recovery advisory interlock, legacy state, exact active grant and
locked source rows. Maintenance can take its exclusive recovery lock between
requests. The shared lock is held during one request/body and durable evidence
write, not the whole run. HTTP/body timeout is 30 seconds; DB statements and
client queries are separately bounded to 30 seconds, lock waits to 2 seconds and
pool acquisition to 2 seconds. **30 seconds is not a total run/transaction bound.**

Immediately after a parsed body is received, allowlisted business response
fields are durably recorded and hashed before another call or transport commit.
Provider error prose, credentials and raw URLs with API keys are not saved in
this evidence. Uncertain evidence persistence, HTTP, settlement or commit stops
without a refund/refetch. A crash before body persistence can still lose response
data; its consumed allowance remains. Existing raw page failure behavior remains
conservative. The legacy attribution writer can defer search yield attribution
until details; failure then stops subsequent calls, not already completed calls.

For the bounded `existingSchemaOnly` path, the shared provider policy is read
under its existing advisory lock on every attempt. With no configured shared
request/USD limits, it skips historical totals that cannot affect permission;
it still enforces pause and records the attempt. Any configured limit, including
zero, requires the unchanged aggregate accounting and fails closed on timeout.
This does not install, remove or relax a shared spending policy. The standalone
lifetime limits apply independently even when the shared policy has no caps.

## Explicit prerequisites and invocation

A future separately approved setup must install the existing version 2 grant
migration and `2026-10-20-scout-bounded-acquisition.sql`: six new tables total,
plus the existing grant-format exclusion function/triggers. The latter migration
is intentionally not a blind retry script. Inspect/reconcile partial or uncertain
setup before doing anything else. Required existing API policy/usage/attribution,
page/ticket/ownership/queue schemas are probed without runtime DDL.

A separate approval must identify the exact manifest, spending cap and standalone
activation. The app pause remains unchanged. Only the isolated approved process
may receive `ANCHOR_RESEARCH_PROVIDERS_PAUSED=false` and a matching
`SCOUT_BOUNDED_APPROVAL_REF`. No production values or filled authorization are
provided here. A future operator command is:

```
node scripts/scout-bounded/run.cjs --execute-approved /path/to/approved-manifest.json
```

The process reuses the app's existing authorized DB connection policy and existing
credentials, without writing them or changing persistent configuration. It closes
only its new standalone pool, not the shared app pool. No source code is deployed
or automatically published by this script.

After any stop, inspect the durable run, attempt, shared usage, page and ticket
records before further action. Do not delete/reset claims or blindly retry.
Already-staged payloads remain available to the existing provider-zero recovery
path, but this standalone command intentionally does not automatically replay
admission after an uncertain run. Such reconciliation is separate from a paid
rerun and must not drain unrelated old work.

## Offline verification

`test/scoutBoundedAcquisitionPostgres.test.js` uses disposable PostgreSQL and the
actual provider/store/admission composition with synthetic HTTP. It covers new
page-only admission, old work preservation, retries and shared request policy,
lower spending caps, restart/concurrency exclusion, recovery/source/grant races,
missing schema, no runtime DDL, redirection refusal, and durable response evidence
after a lost transport COMMIT acknowledgement. Failure injections distinguish
attempts, completed evidence and uncertain outcomes.

`test/scoutBoundedTransport.test.js` validates exact manifest bounds and runs a
separate process to verify denial of unrelated transports and activation-less CLI
invocation. Tests do not verify current production grants, schema, prices,
credentials, contact yield, provider TLS or actual paid network execution.
