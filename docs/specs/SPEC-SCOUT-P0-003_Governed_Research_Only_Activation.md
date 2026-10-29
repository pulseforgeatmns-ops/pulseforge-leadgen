# SPEC-SCOUT-P0-003: Governed Research-Only Activation (Tenant 10)

Status: implemented and tested on disposable PostgreSQL; CI green. Not deployed, not migrated, not activated.
Nothing in this change merges, deploys, changes production authorization or starts paid research.

## Problem

Anchor Cleaning's (Tenant 10) email recovery is in maintenance: one email
(`daily_ff07653a50f62342e4064ffc_0`) is permanently quarantined with an uncertain
delivery outcome, and the prepared envelope has not drained
(`prepared_envelope_not_drained:1`). The Max control loop halts the whole Tenant 10
cycle in that state, so Scout discovery halts with it, even though discovery never
touches email.

## What this adds

Two separately approved, durable records:

1. A **research mission**: an immutable, research-only copy of the already
   approved facility-buyer targeting. That targeting is managed facilities plus
   restaurant front-of-house across all eight Anchor service cities.
2. A **research-only grant** bound to that research mission's exact hash. It lets
   the scheduler run Scout discovery for Tenant 10 while recovery stays closed.

Nothing else is opened.

| Bound by the grant | Value |
|---|---|
| Operator | Active, verified Tenant 10 admin. Install needs that admin's live session (the same identity check as CRM preparation approval). |
| Authorization evidence | `admin_session_approval` plus the operator's written statement. The approving session's hash is stored. |
| Tenant / program / mission | Tenant `10`, the current program and its (unchanged, obsolete Oct 3) outbound source mission, both pinned by hash so any outbound-scope change invalidates the grant. |
| Discovery scope | The research mission id **and exact mission hash**, plus the research search fingerprint. Discovery never uses the outbound mission. |
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
   - searches and admits under the **research mission** only (facility-buyer
     segments, the eight cities, and the research fingerprint for its own
     tickets and pages);
   - retains admitted candidates in `scout_research_candidates`, **never** in
     `scout_unenriched`, `companies` or `prospects`.
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
  uses no cycle. For research-only rows, the discovery backoff counts newly
  retained research candidates as yield; otherwise it would rotate away from unsearched queries
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

## Research mission scoping

**Why a separate table.** The research mission lives only in
`scout_research_missions`, never in `acquisition_missions`. Mission
orchestration, hydration, Max workspaces, preparation and outbound all read
`acquisition_missions` and `acquisition_outbound_programs`, and none of them can
load a research mission. The outbound program keeps pointing at the obsolete
Oct 3 mission, and its prepared envelope is untouched.

**What it contains.** The document is built from the existing approved builder,
`facilityBuyerSuccessorMission` in `scripts/activateAnchorFacilityBuyerMission.js`.
It is not a second copy of the ICP. On top of that builder's output it adds:

- `researchOnly: true` and `outboundEligible: false`;
- `targetSuccessorMissionId`, the successor that would later be activated;
- a distinct id, `mission_anchor_facility_research_<20 hex>`.

Install checks the targeting equals exactly the approved subsegments
(`managed_facilities`, `restaurant_foh`) and the eight cities.

**How it is protected.**
- Database CHECKs require `researchOnly=true`, `outboundEligible=false` and
  `immutable=true`.
- Triggers forbid UPDATE and DELETE.
- Install needs a live Tenant 10 admin session and the exact previewed document.
  It is rebuilt from the live predecessor and compared by hash.

**What the grant checks.** The grant pins `researchMissionId` and
`researchMissionHash`. Every claim re-verifies:
- the stored mission hash;
- that the mission still belongs to this program;
- that its predecessor is still the program's current source mission;
- the research search fingerprint.

**Candidate ownership and retention.**
- Admission runs unchanged: geography, segment, suppression and ownership. A
  business Tenant 10 already owns is rejected (`owned_elsewhere`, tested).
- The single admission write point (`writeQueue` in `persistDiscoveredCompanies`)
  retains research-only candidates in `scout_research_candidates`, not in the
  enrichment queue.
- Rows are unique per Tenant 10 domain and per name. A business already in the
  enrichment queue is not copied.
- Rows keep their identity forever. Only the reconciliation fields can be set,
  and only once.

**Downstream non-admission.** Retained candidates are invisible to:
- the enrichment agent, which reads `scout_unenriched`;
- clean inventory, which reads `prospects` and `companies`;
- AO refill, preparation and sending.

The tests assert that `scout_unenriched`, `companies` and `prospects` are
unchanged after research cycles, that clean inventory contains none of the
retained domains, and that the recovery admission and send leaf still reject.

**Reconciliation.** This is explicit, one-time and never automatic:
`node scripts/scout-research-only/run.cjs --reconcile-plan <researchMissionId>`, then
`--reconcile-apply <id> --approval-ref <ref> --confirm <planHash>`.

It is refused unless all of the following hold:
- recovery is **not** in maintenance;
- no research grant is open;
- the program's active source mission has **identical targeting** to the
  research mission, i.e. the facility-buyer successor has been activated through
  its own guarded transaction.

For each retained candidate it:
1. re-checks ownership (owned candidates are held for later review, not queued);
2. inserts the candidate into `scout_unenriched` using the same deduplication
   predicate Scout uses;
3. records the reconciliation once.

It creates no prospect, company row, envelope or send. Ordinary enrichment,
verification and every outbound gate still apply afterwards. A second run
queues nothing.

This bridge is needed because tenant-wide search memory means the successor
mission will never re-buy queries the research mission already bought.

## Mission activation (scope item D)

The immutable successor scripts (`activateAnchorInventoryCapacityMission.js`, and
`activateAnchorFacilityBuyerMission.js` on top of it) are unchanged. Both still
refuse with `prepared_envelope_not_drained:N` while the prepared item is pending.
**The successor mission cannot be activated safely now.** This change does not
bypass that guard.

Research therefore runs under the separately approved research mission described
above, not the obsolete outbound mission. Outbound scope is unchanged by
construction: nothing here writes `acquisition_outbound_programs` or
`acquisition_missions`, and a test re-runs the successor activation `apply()` to
show it still refuses with `prepared_envelope_not_drained:1`.

Once the envelope legitimately drains and the successor is activated through its
existing transaction, retained research candidates can be reconciled into it.

## Migration and rollback

- Forward: `migrations/2026-10-28-scout-research-only-grants.sql`. It creates five
  new tables (`scout_research_missions`, `scout_research_only_grants`,
  `scout_research_only_cycles`, `scout_research_google_spend`,
  `scout_research_candidates`) and nothing else, is create-once and atomic, and has lock and statement timeouts. It
  reads and changes no existing table. Run it as a separately approved operator
  migration (never as startup DDL). It also requires
  `migrations/2026-10-27-scout-tenant-search-memory.sql` and the durable Scout
  tables, which are prerequisites of `SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED`.
- Normal rollback (preferred): revoke the grant, set
  `SCOUT_RESEARCH_ONLY_GRANTS_ENABLED=false`, and keep the tables. Revocation takes
  effect before the next cycle claim and the next Google debit. At most one
  in-flight HTTP attempt, already debited, finishes.
- Schema rollback: `migrations/2026-10-28-scout-research-only-grants.rollback.sql`
  drops the five tables only if they have **never been used**. Any research
  mission, grant, cycle, spend or candidate row aborts it, so audit evidence is
  never dropped.
- Retained candidates: rolling back never deletes them. If the facility-buyer
  successor is never activated, they simply stay unreconciled. They are
  invisible to enrichment and outbound, so leaving them costs nothing.
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
3. **Apply the migration** (separate schema approval). Verify the ten guard
   triggers (six grant/cycle/spend, four research-mission/candidate) and the two
   unique indexes exist.
4. **Set** `SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED=true` (if not already set) and
   `SCOUT_RESEARCH_ONLY_GRANTS_ENABLED=true`. Railway redeploys, and
   `RAILWAY_GIT_COMMIT_SHA` stays the reviewed revision. With no grant installed,
   cycles still halt exactly as before.
5. **Approve the research mission** (authorization 1), signed in as the
   approving Tenant 10 admin on the app origin. Preview writes nothing:
   ```js
   const post = (path, body) => fetch(`/api/v1/tenant-outreach/anchor-program/${path}`, { method: 'POST',
     headers: { 'Content-Type': 'application/json' }, credentials: 'same-origin', body: JSON.stringify(body) }).then(r => r.json());
   const mission = await post('research-mission/preview', { approvalRef: 'anchor-research-mission-2026-10-28' });
   ```
   Review the preview:
   - `eligibleSubsegments` must be `["managed_facilities","restaurant_foh"]`;
   - `cities` must be the eight service cities;
   - `outboundEligible` must be `false` and `blockers` must be `[]`.

   Then install it. Both install endpoints require a fresh, single-use password
   confirmation on the same session (valid 2 minutes), whether or not the login
   was remembered; send it immediately before each install:
   `await fetch('/api/v1/auth/fresh-confirmation', { method: 'POST', headers: { 'Content-Type': 'application/json' }, credentials: 'same-origin', body: JSON.stringify({ purpose: 'scout_research_approval', password: '<your password>' }) })`.
   The admin page `/admin/scout-research-approval` performs steps 5–8 with
   this confirmation built in and is the preferred surface.
   `await post('research-mission/install', { approvalRef: 'anchor-research-mission-2026-10-28', mission: mission.mission, missionHash: mission.missionHash })`.
6. **Approve the grant** (authorization 2) from the same session:
   ```js
   const grant = await post('research-only-grant/preview', { approvalRef: 'anchor-research-only-2026-10-28',
     expiresAt: '2026-11-04T00:00:00.000Z', statement: '<operator authorization statement>',
     researchMissionId: '<id from step 5>' });
   ```
   Review the preview:
   - `blockers` must be `[]`;
   - `manifest.researchMissionHash` must equal step 5's `missionHash`;
   - check the caps, the quarantine hash and the revision.

   Then confirm your password again (see step 5) and send back the exact object:
   `await post('research-only-grant/install', { manifest: grant.manifest, manifestHash: grant.manifestHash })`.
7. **Observe.** Each cycle writes one `scout_research_only_cycles` row, one debit
   per Places attempt, retained rows in `scout_research_candidates`, and a
   `max_outbound_control` event marked `researchOnly`. `scout_unenriched`
   should not grow from research.
   Read the receipt with:
   `node scripts/scout-research-only/run.cjs --inspect`
   or with `POST …/research-only-grant/inspect`.
8. **Revoke** at any time:
   `node scripts/scout-research-only/run.cjs --revoke <grantId> --approval-ref <ref> --confirm <grantId>`
   or with `POST …/research-only-grant/revoke`.
   The grant also stops on its own after 8 cycles, at expiry, on revision change,
   or on any pinned-state change.
9. **Later, separately: reconcile** (authorization 3). Only after recovery has
   ended and the facility-buyer successor has been activated through
   `activateAnchorFacilityBuyerMission.js --apply`. Run `--reconcile-plan`,
   review it, then run `--reconcile-apply … --confirm <planHash>`.

## Approval boundaries

These remain human decisions. None of them is taken by this PR.

| # | Boundary | Mechanism |
|---|---|---|
| 0 | Merge and deploy this revision | Review; deploy with `SCOUT_RESEARCH_ONLY_GRANTS_ENABLED` unset (a no-op) |
| 1 | Production schema | Separately approved operator migration |
| 2 | Enabling the branch | Set `SCOUT_RESEARCH_ONLY_GRANTS_ENABLED=true` (and discovery memory, if unset) |
| 3 | Research mission | Admin-session preview and install |
| 4 | Paid research (8 cycles, ≤ $5/day) | Admin-session grant preview and install bound to #3's hash |
| 5 | Scheduler dispatch | `ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED` and clock/scheduler flags, if not already on |
| 6 | Ending recovery maintenance and the quarantine decision | Unchanged recovery governance; out of scope |
| 7 | Activating the facility-buyer successor | Unchanged guarded transaction; still needs the envelope to drain |
| 8 | Reconciling retained candidates | Operator CLI with the exact plan hash, after #6 and #7 |

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
- **Classifier gap (observed in tests, not changed here).** The existing admission
  classifier labels generic "manufacturing facility" and "industrial facility"
  Places results as `unclassifiable_vertical` unless website evidence is
  investigated. Research-only cycles deliberately skip website investigation, so
  research will under-count manufacturers and industrial sites, which the
  facility-buyer ICP includes. Treat research yield for those segments as a
  lower bound. Improving the classifier is a separate change.
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
- `services/scoutResearchMission.js`: research mission, candidate retention, reconciliation
- `services/scoutResearchOnlyGrant.js`: grant, cycle and meter
- `services/scoutResearchOnlyCycle.js`: scheduler branch
- `services/maxOutboundControlLoop.js`: branch hook and `researchOnly` ramp mode
- `utils/placesApi.js`: Tenant 10 debit at the Places choke point
- `utils/anchorResearchProviderPause.js`: research-only allowance and control codes
- `packages/capabilities/discovery/providers/PlacesProvider.js`: entry check uses `research`
- `routes/tenantOutreach.js`: admin routes for research-mission preview and install, and grant preview, install, revoke and inspect
- `scripts/scout-research-only/run.cjs`: inspect, revoke and reconcile CLI
- `test/scoutResearchOnlyGrantPostgres.test.js`, `test/scoutResearchOnlyGrant.test.js`, `test/helpers/scoutResearchOnlyFixture.js`
