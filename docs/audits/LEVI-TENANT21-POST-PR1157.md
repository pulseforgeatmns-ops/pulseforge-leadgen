# Levi tenant 21 — post-PR #1157 audit (2026-11-24)

Scope: Scout research-only path for tenant 21 after PR #1157 (merged into
`migration/render-current-main`, deployed to Render `pulseforge-api-migration`
as `dep-dc2bfvs9v7es73bb1ij0`, commit `0728ba2`, live since 2026-11-23 22:03Z).
No production writes, replays, provider calls, scheduler or infrastructure changes.

**Production data was not read.** The Render Postgres instance has an empty IP
allowlist and this session has no database credential. Every production check
below is **UNVERIFIED** until an operator runs the audit.

## Verdict

| Area | Code + tests | Production data |
|---|---|---|
| City inference and conflict handling | PASS (one minor issue, R3) | UNVERIFIED |
| Chicago SODA budget and completeness | PASS | UNVERIFIED |
| Research-only classification | PASS | UNVERIFIED |
| Qualified-contact / setter gates | PASS | UNVERIFIED |
| Outreach prevention | PASS | UNVERIFIED |
| Deduplication | **FAIL**: latent defect R1 (from before #1157) | UNVERIFIED |
| Weekly cycle claim and bounded retry | PASS with risks R2, R4 | UNVERIFIED |
| Service uses `pulseforge_production` | Strongly indicated, not proven | UNVERIFIED |

## Database target

* Render service `pulseforge-api-migration` builds from branch `migration/render-current-main`; auto-deploy is off.
* There is one Render Postgres instance, `dpg-dbtgtloae00c73dd8040-a`. Its default database is `pulseforge_production_migration`.
* Render app logs record the cutover:
  * `FINAL_RESTORE_SWAP_COMMITTED {"primary":"pulseforge_production", "previousPrimaryKeptAs":"pulseforge_pre_cutover_20261122193557"}` at 2026-11-22T19:37:43Z;
  * later instance listings show `primary: pulseforge_production`.
* `scripts/migration/validateMigrationDatabaseUrl.js` sets `EXPECTED_PRODUCTION_DB = 'pulseforge_production'`.
* Not proven: which database the service's `DATABASE_URL` path names. Proving it would mean reading a secret. The audit asserts it at runtime with `--expect-database=pulseforge_production` and reports only `current_database()`.

## Automated tests (local, no production)

`npm run test:scout:levi`, run as an unprivileged user so the disposable-PostgreSQL tests execute instead of failing on `initdb`:
**97 tests: 96 pass, 0 fail, 0 skipped, 1 todo.** The todo is the documented defect R1.
`test/auditLeviTenant21ReadOnly.test.js` (#1158): 4/4 pass.

New test: `test/auditLeviTenant21PostPr1157.test.js`.

* It runs the **real** production research path against disposable PostgreSQL with mocked SODA:
  * `discover`, then enrich, then `claimRun`, then `importReviewOnlyContacts`;
  * the data matches the checkpoint shape: 19 review-only imports plus 10 `scout_research_only` rows.
* A second run of the same cycle returns `replay_blocked` and never calls `discover`.
* It then runs the audit and asserts:
  * zero write statements, and every row byte-identical before and after;
  * no PII or URLs in the output;
  * every gate check PASS;
  * one timed-out Chicago entity is `incomplete` and not counted as a zero-match;
  * the four clean empty reads are authoritative zero-matches.
* Each FAIL path is proven by fixture mutation.

Probe results (mocked fetch):

* Permanent SODA timeouts: exactly **4** HTTP calls, `completeness: incomplete`, `authoritativeZeroMatch: false`.
* Chicago and NYC hosts together: no city set; `cityInference.conflict: true`.
* Stored `New York` with a Chicago host: the existing city is kept and the conflict is recorded.

## Remaining risks / findings

* **R1 — FAIL (pre-existing, not introduced by #1157).** One owner with two permits in the same batch fails the whole weekly cycle *after* its claim.
  * Discovery never dedupes by owner.
  * The execution step marks the second row `duplicate`, but it is still passed to persist.
  * `previewReviewOnlyContacts` then throws `duplicate_contact` and rolls back the entire import.
  * The diagnostic is recorded as `failed` with 0 persisted, and the week cannot be retried. Reproduced; see the todo test.
  * Fix: drop duplicate-blocked records, or dedupe by import identity (name + company), before `persistReviewOnly`.
* **R2 — the claim is not race-safe.**
  * `claimRun` uses `INSERT … WHERE NOT EXISTS` on `agent_log` with no unique index.
  * A startup catch-up and a concurrent `/cron/scout_levi` call could both claim the same week. That is unlikely with one instance, but possible during deploy overlap.
  * The audit checks `pg_indexes`. The fix needs a separately approved operator migration: a partial unique index on `(client_id, (payload->>'cycleId'))` for `agent_name='scout_levi' AND action='research_cycle_claim'`.
* **R3 — minor.** A stored city that differs only in case (`chicago` vs a Chicago host) is flagged as a conflict and left as is. Safe, but noisy.
* **R4 — noise.** Every Render restart inside a week runs the catch-up, which is correctly `replay_blocked`. Each one still inserts a pending `levi_scout_research_briefing` card into `agent_actions`. At least one has been added since the 22:03Z deploy. The audit counts them.
* **R5.** The 4-request budget applies per entity. A 10-candidate cycle can make up to about 40 Chicago SODA calls (public, free; not paid). There is no cycle-wide cap.
* **R6.** Rows researched before #1157 have no `lookup.completeness`. The audit reports them as `pre_1157_not_recorded` and marks `soda.evidence_present` UNVERIFIED. It does not re-research them.
* **R7.** The GitHub weekly workflow has never reached Render: there are no `/cron/scout_levi` request logs. The week was claimed by the in-process startup catch-up. The next scheduled run is Mon 2026-11-30 00:15Z, for cycle `levi-week-2026-11-30`. That run is expected and is not a replay.

## Operator actions (to turn UNVERIFIED into PASS/FAIL)

1. Create a SELECT-only role on `pulseforge_production`. Do not reuse the app credential.
2. Run the audit from a host that already has database access (Render shell on the service, or an allowlisted machine). Opening the allowlist is an infrastructure change; it needs its own approval.

   ```
   AUDIT_DATABASE_URL=… node scripts/auditLeviTenant21PostPr1157.js \
     --tenant 21 --confirm-read-only --expect-database=pulseforge_production
   ```

   Exit codes:
   * 0 = all PASS;
   * 1 = a check failed;
   * 2 = blocked (no access);
   * 3 = some checks UNVERIFIED.

   Plain-psql equivalent: `docs/audits/levi-tenant21-post-pr1157.sql` (one READ ONLY transaction, rolled back).
3. Expected results at the checkpoint:
   * `inventory.checkpoint` 29 / 10;
   * `gates.*` 0;
   * `outreach.none` 0;
   * `cycle.claimed_once` 1;
   * `cycle.claim_race_protection` FAIL (R2) until the index exists.
4. Approve fixes for R1, and separately for R2, before 2026-11-30 00:15Z if the next cycle should not risk the R1 failure.
