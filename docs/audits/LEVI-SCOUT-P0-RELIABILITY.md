# Levi Scout P0 reliability patch (R1, R2) and audit reconciliation — 2026-11-24

Branch: `fix/levi-scout-p0-reliability`. It is based on `audit/levi-tenant21-post-1157`, which is based on `migration/render-current-main`.
Not merged, not deployed. The migration has not been applied. Nothing in production was touched, and Scout was not triggered.

## R1 — same-owner permits failed the claimed weekly cycle

**Cause.**
* Discovery does not dedupe by owner.
* The execution step flagged the second record as `duplicate`, but still passed it to persistence.
* The review-only import rejects any batch that repeats an import identity (`duplicate_contact`) and rolls back the whole import.
* Result: the diagnostic was recorded as `failed` with 0 persisted, the week's claim was already consumed, and the cycle could not be retried.

**Fix.**
* `services/reviewOnlyProspectImportService.js` now exports `importIdentityKey`. It is the exact key `previewReviewOnlyContacts` checks, so the merge key cannot drift from the rejection key.
* `services/leviScoutProductionDeps.js` adds `mergeSameImportIdentity`, which runs before `importReviewOnlyContacts`:
  * the first record is kept unchanged;
  * every other permit is retained under `acquisition_metadata.mergedPermits[]`, with its source URL, market, evidence date, city, provider and same-city flag, plus `mergedPermitCount`;
  * same-city permit URLs are also added to the record's `sourceUrls`, so they appear in the notes `Sources:` line;
  * a permit from another city stays only in `mergedPermits`, and the record gets `crossCityIdentityCollision: true`. The kept record's city provenance stays single-city, so no false city conflict is created.
* The diagnostic now reports `mergedSameIdentity` and `skippedExisting`.

**Not changed.**
* Cross-cycle dedup: the import still skips rows that already exist.
* Review-only flags: `setter_visible=false`, `ao_outreach_review_required=true`, `outreachAuthorized=false`.
* Qualification logic.

## R2 — weekly claim could double-claim

**Cause.** The claim was `INSERT … WHERE NOT EXISTS` on `agent_log` with no unique key. Under READ COMMITTED, two transactions can both see "not exists".

**Code fix (works without the migration).**
* `claimRun` runs in its own transaction.
* It takes `pg_advisory_xact_lock(0x4c455649 /*'LEVI'*/, 21)` before the insert, so the insert's snapshot sees any claim committed first.
* A unique violation (`23505`) is returned as `{ claimed:false, reason:'unique_claim_exists' }`. Any other error is still raised.
* A database handle without `connect()` (test doubles) uses the previous single-statement path.

**Database key (proposed, not applied).**
* `migrations/2026-11-24-levi-scout-cycle-claim-unique.sql`: a partial unique index `agent_log_scout_levi_cycle_claim_uniq ON agent_log (client_id, (payload->>'cycleId')) WHERE agent_name='scout_levi' AND action='research_cycle_claim'`, built `CONCURRENTLY`.
* Guards, in order:
  1. abort if duplicate claim groups exist (it never deletes);
  2. abort if an INVALID leftover index exists;
  3. after the build, verify the index is valid and unique.
* `lock_timeout 5s`. The script is idempotent.
* Read-only precheck: `….precheck.sql`. Rollback: `….rollback.sql` (`DROP INDEX CONCURRENTLY`; no rows touched).

## Evidence (all local, disposable PostgreSQL 16, no production)

`npm run test:scout:levi`, run as an unprivileged user so `initdb` works: **114 tests, 114 pass, 0 fail, 0 skipped, 0 todo** (after R8).
Shared setter, outreach and Anchor tests: 25 files run with the disposable-PG gates enabled (`MAX_SMOKE_DISPOSABLE_PG`, `WALKTHROUGH_NOTIFICATION_TEST_POSTGRES`, `ANCHOR_PHONE_SETTER_TEST_POSTGRES`): **163 tests, 161 pass, 2 fail, 0 skipped**. Both failures reproduce identically on unmodified `migration/render-current-main` (`2e5cc34`), so they predate this work:
* `walkthroughRoutes` "Anchor homepage ads contract → clickable phone/email": site copy;
* `setterPreMigrationCompat` "DNC checks fail closed on the pre-migration schema".

Other setter-visibility suites (`anchorAccountHoldAdmission`, `anchorOutreachReviewPostgres`, `anchorResearchProviderPauseOperators`): 32/32 pass. `scripts/testSetterVisibility.js` passes.

`test/leviScoutP0Reliability.test.js`:

* **R1 unit.** An unmerged same-owner batch throws `duplicate_contact`; this reproduces the defect. After the merge, `previewReviewOnlyContacts` accepts the batch. The import keys are unique. Each permit's location evidence is retained, and cross-city permits are kept without joining the record's sources.
* **R1 regression on disposable PostgreSQL.** The real path runs with mocked SODA: `discover` → enrich → `claimRun` → review-only import. The batch has 5 permits: one owner with 3 Chicago permits and 1 NYC permit, plus one solo owner. Results:
  * `status: completed`, `discovered 5`, `persisted 2`, `mergedSameIdentity 3`;
  * exactly 1 claim and 1 `completed` diagnostic;
  * all 3 Chicago permit URLs retained under `mergedPermits`, and the kept record's sources stay single-city;
  * no rows setter-visible;
  * a replay returns `replay_blocked` without calling discovery.
* **R2 baseline.** Two interleaved transactions running the legacy claim SQL both commit, giving **2 claims for one week**. This reproduces R2.
* **R2 fix.** 30 concurrent `claimRun` calls produce **exactly 1** claim. A different week is still claimable.
* **R2 lock wait.** A claimant blocks while another transaction holds the lock and an uncommitted claim. After that commit it returns `{claimed:false}`, and 1 row exists.
* **Migration.**
  * The precheck reports 1 duplicate group. The migration then aborts with `levi_claim_duplicates_present: 1`; the row count is unchanged and no index is created.
  * After the duplicate is resolved (in the disposable database only), the migration runs twice. The index is `indisvalid` and `indisunique`.
  * Other agents and actions can still insert.
  * With the index in place, even the legacy interleaving fails: the second transaction waits, then gets `23505`.
  * 20 concurrent `claimRun` calls produce exactly 1 claim. The rollback removes the index.

## Audit reconciliation (script + operator SQL)

Corrections made against the real schema and application logic:

| Item | Before | After |
|---|---|---|
| Email sends | `email_performance.sends` | `email_events` by `event_type`. `email_performance` is deprecated: `utils/emailPerformance.js` is a no-op and its counters are described as corrupted. |
| Outbound queues | not checked | `acquisition_outbound_items`, `tenant_outreach_scheduled_sends`, `emmett_send_plans` (tenant `'21'`). A missing table reports UNVERIFIED, never PASS. |
| `setter_status` | counted non-null, but the column defaults to `'new'` | counts `<> 'new'` |
| Qualification | `evaluateProspect` with no criteria | the same criteria source as the runner: `loadTenantUnderstanding` → `fromUnderstanding`, through `readOnlyQueryable`. Status is reported. A stored `qualified_prospect` must match recomputation. |
| Setter exposure | not checked | `gates.review_hold_never_visible`: FAIL if any held row is visible; also reports held rows the pre-fix gate would expose |
| Claim index | any UNIQUE index | the named index, plus a duplicate-group count |
| Parameter typing | `tenant_id = $1::text OR client_id = $1` failed (inconsistent types); caught as UNVERIFIED by the test | `$1::int::text` / `$1::int` |

PR #1158's `scripts/auditLeviTenant21ReadOnly.js` does not reflect Levi's actual storage:
* `scout_research_only_cycles` and `scout_research_candidates` are Anchor (tenant 10) tables with `CHECK (tenant_id = '10')`, so they always return 0 for tenant 21.
* Most of its grouping columns do not exist on `prospects`: `qualification_status`, `research_status`, `contact_status`, `outreach_status`, `outreach_authorized`, `campaign_enrollment_authorized`.

Its output is therefore safe but uninformative. `scripts/auditLeviTenant21PostPr1157.js` supersedes it; I did not change #1158's file.

## R8 — global review-hold setter invariant (fixed)

**Invariant.** A prospect with `ao_outreach_review_required = true` is never setter-visible. This holds for every tenant, score, review state and visibility reason, including `scout`, `handoff`, `recompute`, `manual`, `engagement` and `stage_change`.

**Change.** Made in the shared gate only, `utils/setterVisibility.js`; there is no tenant-specific code.
* `outreachReviewHeld(row)` = `row.ao_outreach_review_required === true` OR the existing tenant-10 rule. Tenant 10 still treats an unknown flag and a held same-company sibling as held.
* `isHardExcluded` uses `outreachReviewHeld`, so the per-row path (`setSetterVisibility` / `computeSetterVisible`) is covered.
* `recomputeSetterVisibility` uses its SQL twin, `outreachReviewHeldSql`, in both CASE branches, so bulk handoff, recompute and global (`clientId` null) runs are covered. A held row is hidden even when it carries a manual or engagement override, and its reason is stored as `hard_exclusion`.
* Rows without the hold behave exactly as before:
  * score ≥ 70 plus a service area is promoted;
  * manual and engagement overrides work;
  * outside tenant 10, a NULL flag is not a hold.

**Bypass review.**
* Every write that sets visibility goes through `setSetterVisibility` or `recomputeSetterVisibility`. Those writers are: `leadgen.js` (scout), `setterHandoffAgent.js` (handoff), `routes/setter.js` (stage change and booked), `routes/api.js` (assign setter, manual), `routes/webhooks.js` (reply, engagement), `lib/walkthroughCapture.js` (engagement), and `scripts/importAnchor*` (manual).
* The only direct writes are:
  * the synthetic setter-test insert in `routes/setter.js`, with a false flag and `do_not_contact = true`;
  * `studioSubstralScoutPersistence`, which only sets visibility to false.
* Read-side setter queries filter on `setter_visible`, so they inherit the gate.
* Outbound paths already applied the hold for every tenant before this change: `dbClient.checkDNC` and its variant, `samAgent`, `emmettAutosend`, `emmettOutboundSnapshot`, `governedOutboundStore`, `maxOutboundControlLoop`, `maxAgent`, `aoFieldService`, `aoBriefingService` and `sendingReadiness`. Setter visibility was the only tenant-10-only gate.

**Clearing the hold is explicit, never incidental.**
* No application code changes the flag from true to false.
* The only code that writes `false` is Anchor scoped resume (`services/anchorScopedResumeControl.js`). It is tenant 10 only and moves NULL to false, never true to false. It requires an approved manifest admission with an `approvalReference`, an identity fingerprint compare-and-set, the quarantine check and `do_not_contact = false`.
* The review-only "outreach lift" is a mission-level authorization amendment and does not touch the flag.
* The flag is set to true by review-only imports, by Max spreadsheet proposals when an email or phone changes or an account is created, and by the column default for new rows (false).
* The tests assert that no setter path changes the flag.

**Evidence: disposable PostgreSQL** (`test/setterVisibilityReviewHoldPostgres.test.js`, 5 tests).
* Pure gate: tenants 21, 10, 1, 5 and 2 × reasons scout, handoff, recompute, manual, engagement and stage_change all give hidden for a held row. Unheld behaviour is unchanged.
* Recompute and handoff for tenants 21, 10 and 1, plus a global recompute:
  * held rows, including previously visible manual and engagement rows, end up hidden with `hard_exclusion`, and the flag stays true;
  * an unheld qualified row stays visible; below threshold and blank area stay hidden;
  * an unheld manual override is preserved.
* Tenant 10 stricter rules: an unknown flag is held and a held sibling holds the company. A tenant-1 unknown flag is not a hold, which is unchanged.
* Per-row scout, handoff, manual, engagement and stage_change cannot expose a held row in tenants 21, 10 or 1. Unheld manual and scout promotion still work.
* Review-only `contact_now` imports (icp 80) for tenants 21 and 1 stay hidden after handoff.
* The former R8 todo in `test/leviScoutP0Reliability.test.js` now passes.

**Affected tenant workflows (compatibility).** Tenant 10 is unchanged: it already applied the hold. For every other tenant:
1. **Held rows already setter-visible** (legacy manual, engagement or score promotion) leave the setter queue at their next stage change, handoff, scout or recompute. They stay visible until one of those runs, because there is no backfill. Count them per tenant with `docs/audits/setter-review-hold-impact.sql` before deploying.
2. **Admin "assign to setter"** (`POST` in `routes/api.js`) on a held prospect still records the assignment and touchpoint, but returns `setter_visible: false`. Tenant 10 already worked this way.
3. **Reply or engagement promotion** (Brevo webhook) no longer exposes held prospects. Outbound to held prospects was already blocked, so this mainly affects inbound replies.
4. **Review-only `contact_now` imports** (LA Endeavors / Levi tenant 21 and any other tenant using the review-only import) can no longer reach the setter queue through handoff. An operator must clear the review hold first. There is currently no non-Anchor clearing path, and that is intended.
5. **Max spreadsheet proposals** set the hold when an email or phone is edited. In any non-Anchor tenant using them, those accounts now leave the setter queue until reviewed.

## Remaining risks

* The merge keeps the *first* record. With NYC interleaved first, a cross-city owner is stored as the NYC record, and its Chicago permits sit in `mergedPermits`. Chicago contact research for that entity is still attached only to the kept record's research.
* Same name with a different owner in another city is treated as one CRM identity. That is the import's existing name+company rule, unchanged; it is flagged with `crossCityIdentityCollision`.
* An advisory-lock collision across tenants is impossible: the second key is the tenant id, and only tenant 21 uses this path.
* R3–R7 from the earlier audit are unchanged.

## Migration plan (operator, after explicit approval)

1. Run the precheck, read-only, on `pulseforge_production`: `psql "$URL" -X -f migrations/2026-11-24-levi-scout-cycle-claim-unique.precheck.sql`.
   * Expect `duplicate_claim_groups = 0`, `invalid_index = 0`, and exactly one claim, for `levi-week-2026-11-23`.
2. If duplicates exist: stop. Review them with the operator. The migration never deletes, and there is no automatic resolution.
3. Apply outside a transaction: `psql "$URL" -X -v ON_ERROR_STOP=1 -f migrations/2026-11-24-levi-scout-cycle-claim-unique.sql`.
   * The build is CONCURRENTLY and does not block writers. The index is partial and covers about 1 row.
4. Verify: the audit's `cycle.claim_race_protection` is PASS.
5. Rollback if needed: `….rollback.sql`. The application stays correct without the index, because of the advisory lock.

## Deployment sequence (each step needs approval)

1. Review the branch. Open a PR into `migration/render-current-main`; Render deploys from that branch, with auto-deploy off.
2. CI: the `levi-scout` workflow runs `npm run test:scout:levi` on disposable PostgreSQL.
3. Merge, then trigger a manual Render deploy.
   * Startup catch-up for `levi-week-2026-11-23` returns `replay_blocked` (claim exists). No discovery runs.
   * One more `replay_blocked` briefing card is inserted (R4).
4. Run the read-only audit with `--expect-database=pulseforge_production` and confirm the checkpoint: 29/10, 0 qualified, 0 setter-visible, 0 outreach, 1 claim.
5. Run the migration precheck, then apply the migration (plan above), then re-run the audit.
6. Complete all of the above before **Mon 2026-11-30 00:15Z**, when `levi-week-2026-11-30` is claimed by the GitHub weekly job or the next startup.
7. Before deploying, run `docs/audits/setter-review-hold-impact.sql` (read only) and review per-tenant `held_visible_now` and `held_in_setter_pipeline`. Those rows leave setter queues at their next visibility evaluation.
8. Until the R8 gate is deployed, do not run `setter_handoff` for any tenant with `held_pre_fix_exposed > 0`.
