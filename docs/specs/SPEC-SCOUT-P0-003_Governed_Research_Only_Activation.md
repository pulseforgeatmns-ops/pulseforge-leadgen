# SPEC-SCOUT-P0-003: Governed Research-Only Activation (Tenant 10)

Status: implemented locally and tested on disposable PostgreSQL. Not deployed, not migrated, not activated.
Nothing in this change merges, deploys, changes production authorization or starts paid research.

## Problem

Anchor Cleaning's (Tenant 10) email recovery is in maintenance: one email
(`daily_ff07653a50f62342e4064ffc_0`) is permanently quarantined with an uncertain
delivery outcome, and the prepared envelope has not drained
(`prepared_envelope_not_drained:1`). The Max control loop halts the whole Tenant 10
cycle in that state, so Scout discovery halts with it, even though discovery never
touches email.

## What this adds

An explicit, durable **research-only grant** that lets the scheduler run Scout
discovery for Tenant 10 while recovery stays closed. Nothing else is opened.

| Bound by the grant | Value |
|---|---|
| Operator | Active, verified Tenant 10 admin. Install needs that admin's live session (the same identity check as CRM preparation approval). |
| Authorization evidence | `admin_session_approval` plus the operator's written statement. The approving session's hash is stored. |
| Tenant / program / mission | Tenant `10`, the current program id and its source mission. Pinned by hash, along with the Scout search fingerprint. |
| Recovery state | The closed recovery control row (hash), and the existing quarantine decision hash. Both must stay exactly as approved. |
| Deployment | `RAILWAY_GIT_COMMIT_SHA`: the grant only works on the reviewed revision. |
| Cycles | Exactly 8 (hard maximum). |
| Google spend | At most $5.00 per UTC day for Tenant 10 (`dailyGoogleUsdMicros` ≤ 5,000,000). |
| Window | `notBefore` to `expiresAt`, at most 7 days. Revocable at any time. |
| Permissions | `discovery: true`. `enrichment`, `verification`, `preparation` and `outbound` are all `false`. Providers: `google_places` only. |

Database CHECK constraints also enforce the permission set, provider list, cycle
maximum and $5 ceiling. Triggers make grants revoke-only, make cycles move
forward only, and make spend rows settle-only. Nothing can be deleted or refunded.

## How it works

1. **Scheduler branch** (`services/scoutResearchOnlyCycle.js`, called at the top of
   `runMaxOutboundControlLoop`). It applies only when all of these hold:
   `SCOUT_RESEARCH_ONLY_GRANTS_ENABLED=true`, the tenant is 10, recovery mode is
   `maintenance`, and an open grant exists. If there is no grant, the existing
   halt (`anchor_recovery_separate_preparation_approval_required`) runs as before.
   If the grant fails a check, the cycle halts with a durable
   `scout_replenishment_blocked` hold naming the reason.
2. **Cycle claim** (`claimResearchCycle`). This is one transaction. It takes the
   shared recovery lock and locks the grant row, then re-validates the
   environment, operator account, program and mission hashes, fingerprint,
   closed control, quarantine and window. It then inserts cycle N with a lease.
   - A live cycle blocks a second claim (unique partial index).
   - A crashed cycle is marked `abandoned` once its 30-minute lease expires. It
     stays consumed and is never refunded or replayed.
   - The unique `cycle_id` stops a durable-stage retry from running twice.
   - A claim is refused if today's ceiling cannot fund even one search, so no
     cycle is consumed for nothing.
3. **Discovery** runs inside the claimed scope, using the native
   `defaultScoutRamp` with `researchOnly: true`. The ramp:
   - skips existing-inventory recovery, every enrichment batch, verification retry
     and website business-evidence investigation;
   - requires durable tickets, pages and tenant discovery memory (one new page
     per cycle, and no query or place bought twice);
   - writes candidates to the raw `scout_unenriched` queue only.
   The branch then returns. The rest of the control loop never runs: no
   enrichment, AO refill, preparation refill or sending.
4. **Tenant 10 Google meter** (`googleMeter` at the `utils/placesApi.js`
   choke point). Inside a claimed cycle, every possible Places HTTP attempt first
   commits a durable debit in `scout_research_google_spend` at the priced upper
   bound. The debit is taken under an advisory lock, and the grant, cycle token,
   lease and environment are re-checked first.
   - The daily total counts this ledger plus any other metered Tenant 10 Google
     events today. Unknown cost fails closed.
   - The shared `reserveApiCall` / `api_spend_policies` meter still runs
     unchanged afterwards, and the debit is linked to its event.
   - If the debit cannot be written, the attempt is refused.
5. **Provider pause.** `ANCHOR_RESEARCH_PROVIDERS_PAUSED` stays `true`. Inside a
   claimed cycle the pause allows only two things: the generic `research` entry
   and `google_places` from inside a metered debit. Direct, unmetered Places
   transports (import, diagnostic, public sourcing, leadgen) and every other
   provider (Prospeo, Hunter, SerpAPI, Apollo, phone enrichment) stay paused.
   Outside a cycle, the pause behaves exactly as before.

`PlacesProvider`'s entry check now asserts the generic `research` authority. The
`google_places` check still happens on every HTTP attempt in `utils/placesApi.js`.
For every other tenant, and when the pause is unarmed, behavior is identical.
Under the armed pause, only the bounded-research and research-only scopes can
pass either check, and both accept `research`.

Two consequences of reusing the native Scout path:

- **Durable memory is tenant-wide.** A newly approved mission whose queries match
  ones Tenant 10 already bought never buys them again (tested).
- **Generation boundaries.** A cycle that finds its search generation fully
  handled spends $0 but still uses one of the 8 cycles; the next cycle advances
  the generation (existing control-loop semantics). A plan that is already
  exhausted (`approved_search_plan_exhausted`) is held *before* a claim, so it
  uses no cycle. For research-only rows, the discovery backoff counts raw queue
  admissions as yield; otherwise it would rotate away from unsearched queries
  after 3 cycles, because research-only cycles never produce clean inventory.
- A research-only cycle can run while the program is `paused` (not `revoked`).
  The grant pins the program row, including `mode`, by hash, and grants no
  outbound.

## What is unchanged (by test)

- Quarantine rows, recovery control, reservations, prepared envelopes and items,
  canonical executions, mission contributions, missions, programs and the shared
  `api_spend_policies` row are byte-identical after every scenario.
- `assertRecoveryAdmission` (preparation) and `assertRecoveryLeaf` (send) still
  reject with `anchor_recovery_maintenance`.
- Other tenants (including Babrun, tenant 13) keep the shared meter and are never
  debited as Tenant 10. A tenant 13 Places call inside a Tenant 10 cycle is
  refused before any HTTP.

## Mission activation (scope item D)

The immutable successor scripts (`activateAnchorInventoryCapacityMission.js`, and
`activateAnchorFacilityBuyerMission.js` on top of it) are unchanged. Both still
refuse with `prepared_envelope_not_drained:N` while the prepared item is pending.
**The successor mission cannot be activated safely now.** This change does not
bypass that guard.

The research-only grant therefore binds the program's **current** approved source
mission. Outbound scope is unchanged by construction: the grant never writes
`acquisition_outbound_programs` or `acquisition_missions`, and changing either one
invalidates the grant.

A research-only mission-scoping mechanism is possible without changing outbound
scope. It would mean inserting the facility-buyer successor as an immutable,
**non-activated** mission row and letting a grant pin it in place of the program's
source mission. This is **not implemented** for two reasons:

- it inserts a production mission row, which needs separate approval;
- candidates outside the current program scope would pile up in the raw queue
  until activation.

Recommendation: research under the current mission now, and activate the
facility-buyer successor through its existing guarded transaction once the
envelope legitimately drains.

## Migration and rollback

- Forward: `migrations/2026-10-28-scout-research-only-grants.sql`. It creates new
  tables only, is create-once and atomic, and has lock and statement timeouts. It
  reads and changes no existing table. Run it as a separately approved operator
  migration (never as startup DDL). It also requires
  `migrations/2026-10-27-scout-tenant-search-memory.sql` and the durable Scout
  tables, which are prerequisites of `SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED`.
- Normal rollback (preferred): revoke the grant, set
  `SCOUT_RESEARCH_ONLY_GRANTS_ENABLED=false`, and keep the tables. Revocation takes
  effect before the next cycle claim and the next Google debit. At most one
  in-flight HTTP attempt, already debited, finishes.
- Schema rollback: `migrations/2026-10-28-scout-research-only-grants.rollback.sql`
  drops the three tables only if they have **never been used**. Any grant, cycle
  or spend row aborts it, so audit evidence is never dropped.
- Code rollback: redeploy the previous revision. Once the revision changes, a
  pinned grant no longer validates (`research_only_revision_mismatch`).

## Production activation procedure (each step needs separate approval)

1. **Review and merge** this PR. Deploy the merged revision with
   `SCOUT_RESEARCH_ONLY_GRANTS_ENABLED` unset. Deploying alone changes nothing:
   the branch is default-off.
2. **Read-only preflight**. Confirm all of the following:
   - recovery control row: `maintenance_armed=true`, `manifest IS NULL`;
   - exactly one Tenant 10 quarantine row, with no open `anchor_scoped_resume_grants`
     or `scout_bounded_research_grants`;
   - the active Tenant 10 program and its source mission, which must have
     `payload.structuredMission.immutable=true`;
   - environment: `ANCHOR_EMAIL_RECOVERY_MAINTENANCE=true`,
     `ANCHOR_RESEARCH_PROVIDERS_PAUSED=true`, `ANCHOR_GOVERNED_OUTBOUND_ENABLED=false`,
     no recovery or scoped-resume manifest ids, and `RAILWAY_GIT_COMMIT_SHA`
     present;
   - the tenant discovery-memory tables exist.
3. **Apply the migration** (separate schema approval) and verify the six triggers
   and two unique indexes exist.
4. **Set** `SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED=true` (if not already set) and
   `SCOUT_RESEARCH_ONLY_GRANTS_ENABLED=true`. Railway redeploys, and
   `RAILWAY_GIT_COMMIT_SHA` stays the reviewed revision. With no grant installed,
   cycles still halt exactly as before.
5. **Preview, signed in as the approving Tenant 10 admin** on the app origin. The
   preview writes nothing; it returns the exact manifest, its hash and any
   blockers.
   ```js
   await (await fetch('/api/v1/tenant-outreach/anchor-program/research-only-grant/preview', { method: 'POST',
     headers: { 'Content-Type': 'application/json' }, credentials: 'same-origin',
     body: JSON.stringify({ approvalRef: 'anchor-research-only-2026-10-28', expiresAt: '2026-11-04T00:00:00.000Z',
       statement: '<operator authorization statement>' }) })).json();
   ```
   `blockers` must be `[]`. Review `manifest` (program, mission, fingerprint,
   quarantine hash, caps).
6. **Install** (the explicit authorization) from the same session, sending back
   the exact previewed object:
   `POST …/research-only-grant/install` with `{ manifest, manifestHash }`.
7. **Observe.** Each cycle writes one `scout_research_only_cycles` row, one debit
   per Places attempt, and a `max_outbound_control` event marked `researchOnly`.
   Read the receipt with:
   `node scripts/scout-research-only/run.cjs --inspect`
   or with `POST …/research-only-grant/inspect`.
8. **Revoke** at any time:
   `node scripts/scout-research-only/run.cjs --revoke <grantId> --approval-ref <ref> --confirm <grantId>`
   or with `POST …/research-only-grant/revoke`.
   The grant also stops on its own after 8 cycles, at expiry, on revision change,
   or on any pinned-state change.

## Remaining blockers and open questions

- **Production state is unverified from here.** That covers the program mode,
  whether the mission has `immutable: true`, that there is exactly one quarantine
  row, and that the control row's `manifest` is `NULL`. Preview reports each of
  these as a named blocker.
- **Scheduler enablement.** Cycles only run if the execution clock or durable
  stage scheduler dispatches Tenant 10 control
  (`ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED=true` plus clock/scheduler flags). If those
  are off in maintenance, they need their own approval.
- **The day is UTC**, matching the shared meter. It is not America/New_York.
- **Usage that never passes through the meter cannot be seen.** The ceiling counts
  every metered Tenant 10 Google attempt. A Google call made outside
  `utils/placesApi.js` while the pause is armed is refused for Tenant 10 and is
  not counted. Billing reconciliation against the Google console remains an
  operator check.
- **Pre-existing test failures.** Five unrelated test files
  (`aoCrmLinkRecovery`, `audit059ExternalDiscoveryProviderFailure`,
  `scoutAcquisitionDiscovery`, `scoutCandidateUniverseEstimate`,
  `scoutDiscoveryPipeline`) fail the same 13 tests on the base commit with and
  without this change.

## Files

- `migrations/2026-10-28-scout-research-only-grants.sql` (+ `.rollback.sql`)
- `services/scoutResearchOnlyGrant.js`: grant, cycle and meter
- `services/scoutResearchOnlyCycle.js`: scheduler branch
- `services/maxOutboundControlLoop.js`: branch hook and `researchOnly` ramp mode
- `utils/placesApi.js`: Tenant 10 debit at the Places choke point
- `utils/anchorResearchProviderPause.js`: research-only allowance and control codes
- `packages/capabilities/discovery/providers/PlacesProvider.js`: entry check uses `research`
- `routes/tenantOutreach.js`: admin preview, install, revoke and inspect routes
- `scripts/scout-research-only/run.cjs`: inspect and revoke CLI
- `test/scoutResearchOnlyGrantPostgres.test.js`, `test/scoutResearchOnlyGrant.test.js`, `test/helpers/scoutResearchOnlyFixture.js`
