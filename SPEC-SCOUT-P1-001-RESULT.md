# SPEC-SCOUT-P1-001 Rev. 2 — implementation result (#1048)

Base: `54a1ad4`. Tenant 10 only. Nothing here merges, deploys, applies a migration,
creates a grant, runs paid research or authorizes outbound.

## What changed

- `config/anchorFacilityTiers.js`: immutable registry of allocation versions. `anchor-p1-001-v1`
  pins shares (50/35/15), window (10), mission scope (the eight approved service cities),
  concept additions (daycare, preschool, early learning center, medical office, dental office,
  multi-tenant office building), the category-to-tier map and `universeSha256`.
- `services/scoutFacilityTierAllocation.js`: pure allocator. Largest-remainder quotas,
  earliest-deadline anti-starvation, deficit allocation, activation windows, and a ledger
  derived from `scout_query_pages`.
- `services/scoutFacilityResearchCoverage.js`: tier ordering inside the existing planner.
  The planned set is still exactly the approved queries; only order changes. Inert unless the
  current version is activated, unsuperseded, in scope and its universe hash matches.
- `services/maxOutboundControlLoop.js`: passes the tier config to the planner.
- `migrations/2026-11-04-scout-tier-allocation.sql` (+ rollback): activation anchor and an
  unwritten commercial-qualification table. Both permanent once written.

## Decisions a reviewer should check

1. **No new execution or accounting system.** Allocation counters are read from the P0-005
   lifecycle (`scout_query_pages`, `scout_tenant_query_owners`). Crash safety is the existing
   lifecycle's: a reserved page is never refetched, a tenant query claim is permanent, a
   pre-network budget denial deletes only the reservation.
2. **Uncertain spend.** A `reserved` page (paid call possibly made, outcome unrecorded) counts
   toward allocation, conservatively. Receipts report it separately: `countsByStatus` per tier
   has `completed` and `uncertain`.
3. **Historical attribution.** A page's tier is computed with the version whose activation
   window contains it, using that version's pinned universe. Pages before the first activation
   are `pre_ledger` and get no tier. Pages under an unknown version are not reclassified.
   Drift in the classifier, concepts or query builder fails the universe-hash test and must
   ship as a new version.
4. **Eligibility.** A tier is eligible when this cycle's plan has an unclaimed first-page query
   in it. Ineligible tiers get an `eligibilityShortfalls` entry
   (`no_unclaimed_query_in_current_generation` or `tier_exhausted_in_version`). Claimed roots
   (continuation pages only) are ordered after every unclaimed query; nothing is repeated to
   fill a tier. The 10-selection guarantee covers first-page queries; continuation pages are
   counted when bought but are not part of eligibility.
   Eligibility history is not persisted. A tier returning after a generation gap is scheduled
   from its last selection, so it goes at once if overdue (`overdueOnReturn`). If two tiers are
   due at the same position, one window is missed; the receipt lists them in `windowAtRisk`
   rather than hiding it. Production should not reach that state, because the search generation
   only advances when no unclaimed query remains in the plan.
5. **Operational reporting is out of scope.** Candidate-outcome reporting by source tier
   (ownership, duplicate, geography and evidence holds; retained; spend in USD) is NOT in
   this PR. The planner receipt (persisted through the existing `scout_query_progression`
   event) carries allocation counts only. Follow-up: a read-only loader over
   `scout_candidate_memberships` / `scout_candidate_work` / API usage, with Postgres tests.
6. **Qualification.** Schema only. No code reads or writes it; absence means unknown.

## Activation (separate approval)

1. Apply `migrations/2026-11-04-scout-tier-allocation.sql`.
2. Insert the activation row:
   `INSERT INTO scout_query_allocation_activations(tenant_id,allocation_version,approval_ref)
   VALUES('10','anchor-p1-001-v1','<approval ref>');`
Until step 2 the planner records `status: not_activated` and uses the v1 order unchanged.
Rollback is code-only; the rollback SQL refuses to run once an activation exists.

## Tests

- `test/scoutFacilityTierAllocation.test.js`: rounding, convergence, eligibility churn.
- `test/scoutFacilityTierPlanner.test.js`: taxonomy, displacement, determinism, shortfalls,
  restaurant rotation, versioned attribution, universe pin, inert states, fixture.
- `test/scoutFacilityTierRecovery.test.js`: crash points through the real provider and
  checkpoint session over an in-memory table model.
- `test/scoutFacilityTierAllocationPostgres.test.js`: migration, constraints, rollback guard,
  and a crash after the paid call through the real tenant memory, page store and planner SQL.
- Fixture: `node scripts/scoutTierAllocationFixture.js --write` regenerates
  `test/fixtures/scoutTierAllocation.before-after.json`.
