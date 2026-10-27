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

Only tenant 10 is supported. The original version 1 bounded manifest requires
confirmed legacy recovery mode; version 2 uses the distinct research authority
described below while the permanent recovery control stays armed. An exact manifest
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
pause, shared recovery advisory interlock, applicable legacy/research state, exact active grant and
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

## Original version 1 prerequisites and invocation

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

## Version 2: research under permanently armed recovery

This is a **separate research-only operator authority**, not a preparation/send
grant and not a global unpause. It requires all of the following throughout:

- Tenant 10, `ANCHOR_EMAIL_RECOVERY_MAINTENANCE=true`,
  `ANCHOR_RESEARCH_PROVIDERS_PAUSED=true`, and
  `ANCHOR_GOVERNED_OUTBOUND_ENABLED=false`.
- Permanent recovery singleton `maintenance_armed=true, manifest=NULL`, no
  recovery/scoped-resume activation ID, and no unrevoked scoped-resume grant.
- Exact deployed `RAILWAY_GIT_COMMIT_SHA`, program/source/fingerprint/query pins,
  verified quarantine decision, and the hash of the closed recovery projection.
- One handled historical root, no previously claimed refresh/run, unchanged
  coverage allocation/root bounds, and a fixed UTC window of at most 60 minutes.
- An explicitly installed immutable full research manifest, separately confirmed
  execution, and the unchanged one-page/3-search/20-details/modeled-cost limits.

The v2 document has every v1 field plus exactly `researchAuthority`:
`kind=bounded_places_research`, `provider=google_places`, `expectedRevision`,
`quarantineDecisionHash`, `recoveryControlHash`, `maxSearchAttempts=3`,
`maxDetailsAttempts=20`, and `enrichment`, `verification`, `preparation`, `email`
all false. Approval references are audit metadata, **not credentials**. This is
an operator command using existing authenticated infrastructure access, not a
new public HTTP endpoint, customer login permission, scheduler or cron route.

A private, nonserializable async scope is entered only after real database
verification and invokes only the native bounded runner. It permits Google
Places within that execution; ordinary callers and all other paid providers
remain paused. No caller-supplied `approved` callback, token, flag or JSON object
can create the scope. Scope expires and is invalidated in `finally`, including
escaped async descendants. Every plan/reservation/request and ticket claim or
admission rechecks durable authority. Coverage revocation also blocks admission.
The CLI installs captive transport before importing application modules.

### Schema and deployment are separate approvals

`migrations/2026-10-26-scout-bounded-research-authority.sql` is a **local proposal**,
not applied by this feature. It adds one table, an open-authority uniqueness index,
and immutable/revocation-only triggers. It does not change recovery, quarantine,
source, prospect review or historical usage. Existing coverage and bounded-ledger
migrations may also be needed; catalog preflight, not this document, determines
what production lacks. No runtime helper installs DDL.

The new migration uses a transaction, 5-second lock timeout and 30-second statement
timeout. It is deliberately create-once: an existing relation aborts rather than
silently accepting incompatible schema. After an uncertain result, inspect exact
catalog shape before any action; never blindly repeat it. A failed transaction
rolls back. Operational rollback retains tables and all audit rows, revokes the
research grant, and keeps app pauses/closed outbound. There is no destructive down
migration, budget reset, refund, un-revoke or history repair.

### Complete browser-Console workflow

The existing authenticated Railway service Console provides the shell at `/app`;
no desktop, new credential, session export, provider-key display or persistent
environment edit is required. The operator supplies the **same reviewed nonsecret
JSON document** on stdin (`-`) or by an approved local file. Each command is a
separate deliberate action, never a chained automatic grant-and-execute:

```sh
# Feed the exact reviewed JSON on stdin for each invocation; do not include keys.
node scripts/scout-bounded/run.cjs --preview -
node scripts/scout-bounded/run.cjs --install-approved - --confirm <manifest-sha256>
node scripts/scout-bounded/run.cjs --execute-approved - --confirm <manifest-sha256>
node scripts/scout-bounded/run.cjs --receipt -
node scripts/scout-bounded/run.cjs --revoke-approved - --confirm <manifest-sha256>
node scripts/scout-bounded/run.cjs --receipt -
```

Use a quoted heredoc or bounded stdin paste followed by EOF. Preview is READ ONLY
and returns exact source/control/coverage pins, scope, limits, window and manifest
hash; it does not install authority or create coverage pages. Missing schema or
stale prerequisites stop before approval. Installation and execution both compare
the explicit hash; execution never installs the research grant. Only execution
requires the existing Places credential; other lifecycle operations are DB-only.
An interrupted/incomplete stdin document does nothing. Incomplete output after a
mutation is **uncertain**: inspect receipt, do not re-execute. Repeated installation
only acknowledges the identical existing grant; it never resets consumption.

Receipt reads exact immutable grant/run/attempt counts and current page-work
state, never provider continuation tokens, execution tokens or credential values.
A consumed run without confirmed completion is reported as such. Page-work is a
current snapshot, not fabricated proof of historical queue yield. A successful
run can yield zero new candidates, and never proves verified contact inventory.
Revocation works despite expiry, source drift or lowered settings and retains all
consumption. A revoked tombstone can close an uncertain installation without
allowing a later install. In-flight finite requests may finish before revocation
acquires its lock; later attempts/admission cannot proceed. A lock-timeout or lost
revocation response is not a claim of closure: inspect receipt and stop dependent
work. Do not automatically retry, refund, renew, substitute or drain old work.

### Smallest future acceptance experiment (not authorized here)

After code release and separately approved schema setup, read-only preview must
identify one eligible historical root from the unchanged valid source, exact
revision/hashes, one city and one query. Obtain explicit approval for one fixed
window, one first-page refresh, raw-business queue admission only, provider attempt
and monetary ceilings, and cleanup by revocation. Verify current official Google
pricing/account policy before proposing spend; the modeled cap is not an invoice
guarantee. Do not fabricate a new source/fingerprint/window to evade consumption.

Pass evidence: durable one-page response, exact attempts/shared usage, new-only
membership/ownership/dedup outcomes, truthful raw queue yield, unchanged closed
preparation/send controls, and revoked research authority. Zero yield is possible.
Fresh enrichment, verified recipients, copy suitability, sustained replenishment
and sending remain separate unproven stages requiring their own bounded scope.

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


`test/scoutBoundedResearchAuthorityPostgres.test.js` exercises the installed v2
authority against native disposable PostgreSQL with actual recovery/quarantine
schemas and a 45-mission/298-contribution synthetic historical baseline. It
preserves invalid legacy history, two revoked preparation grants and consumed
usage while testing the real provider/page/ticket/admission path. A tripwire
asserts zero hydration/bootstrap invocation (existing transitive runtime module
imports remain), and a network guard asserts zero real provider attempts.

`test/scoutBoundedResearchCli.test.js` drives native child-process stdin/file
lifecycle commands, including concurrency, missing confirmation, interrupted
input, process death after reservation, lost result output, receipt/revoke after
revision drift, and credential/raw-error canaries. The existing captive transport
tests remain mandatory. These tests prove local operator usability, not a paid
production request, current vendor pricing or verified-contact yield.
