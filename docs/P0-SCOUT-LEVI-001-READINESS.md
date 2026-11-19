# P0-SCOUT-LEVI-001 — Tenant 21 (LA Endeavors) research-only Scout: readiness record

Checkpoint date: 2026-11-19. Baseline: `main` @ `f065ce6` (PR #1143).

## Verified production status (from Railway metadata and logs, no database read)

| Item | Evidence | Status |
| --- | --- | --- |
| #1143 deployed | `pulseforge-leadgen` production deployment `bbace00b`, commit `f065ce6`, SUCCESS 2026-11-19 13:29 UTC | Deployed |
| Research-only execution enabled | `SCOUT_LEVI_RESEARCH_ONLY_ENABLED` and `SCOUT_LEVI_PAID_PROVIDERS_APPROVED` are **not set** on the production service | Disabled (fail-closed) |
| Scheduler | No Railway cron service, route, CLI or caller references `services/leviScoutResearchOnlyExecution.js`; production cron services are `max-decay-daily` and `max-context-cleanup-hourly` only | Not configured, and nothing exists to configure |
| Tenant 21 Scout runs | Deploy logs since 2026-11-05 show no Scout activity for client 21; `/cron/scout` received no HTTP requests since 2026-11-12. All Scout log lines are Anchor (tenant 10) `anchor_research_provider_paused` | No Scout output for tenant 21 |
| Tenant 21 inventory origin | Records come from `review_only_import` (operator import, `routes/reviewOnlyProspectImport.js`), not Scout | Imports only |

## Production audit

`railway run --service pulseforge-leadgen --environment production npm run scout:levi:production-audit`. The service and environment names are confirmed (project `charming-trust`). The npm script runs `node scripts/leviScoutProductionAudit.js`.

**Not executed from this session.** No Railway CLI and no database credential are available here. The Railway MCP integration cannot run SQL. A Railway environment fork was rejected: it would copy production variables, so the fork's services could reach real provider and sender credentials. Running the audit from a fork is therefore not a non-production validation. An operator must run it.

**Defect found in the merged audit (fixed on this branch).** The `workspace` query selected `blueprint_id` and `blueprint_version` from `cie_interview_sessions`, but no migration creates those columns. Run against the real schema, the section comes back as `{"unavailable":"42703"}`, and the Blueprint state is never reported. The fix:
- reads session status from existing columns;
- reads Blueprint status (id, version, status) from `cie_business_blueprints`;
- reduces the workspace correction to a boolean, so no correction content is emitted;
- reports `allTime` and `reviewOnlyContactNow` inventory as well as the 30-day weekly window. Records imported before the window were invisible before;
- breaks duplicate ties by id. Before, two copies of one identity created in the same transaction were both counted as duplicates.

`railway run` executes locally with production variables, so the operator can run the corrected audit from this branch without deploying it.

## Blueprint qualification criteria

- The approved canonical Blueprint is available. The operator-run read-only audit at `491f7b70` (docs/P0-MAX-INVESTIGATION-PRIORITY.md, "P0 acceptance verification") found **no recognised numeric financing range** in it.
- The $5,000–$20,000 range is therefore **not authoritative**. It appears only in operator prompts.
- `evaluateProspect` requires an approved Blueprint range. Without one, every record carries `financing_criteria_unverified` and **no record can qualify**. This is by design and is the primary blocker.

## Six historical review-only `contact_now` prospects

Recorded deal-claim coverage from the v5 audit: Zackary 3/5, Raul 2/5, Jon 2/5, Reginald 1/5, John 0/5, David 0/5. **None qualifies** under the contract, because no approved financing range exists. Two further notes:
- If $5k–$20k were later approved, Zackary's recorded `$35,000` request would fall outside it.
- Records with stage or financing absent fail `current_project_evidence_missing` or the financing check regardless of the range.

The corrected audit reports per-blocker counts for this cohort (`production.reviewOnlyContactNow`).

## Remaining blockers to a controlled research-only cycle

1. **Approved financing criteria (Levi).** The Blueprint needs one explicit KNOWN service fact of the form `Financing range: $X–$Y`, the only form `tenantCriteria` accepts. It is added through the approved Blueprint flow, never by hand.
2. **No borrower discovery source exists.**
   - `CLIENT_SCOUT_PLANS` has no tenant 21 entry, and Google Places and SerpAPI target businesses, not individual real-estate borrowers.
   - `runResearchOnlyCycle` needs five injected dependencies. None has a production implementation: `claimRun` (replay table), `discover`, `existingIdentityKeys`, `persistReviewOnly` and `recordDiagnostic`.
   - It has no entry point (route, CLI or cron) and no schema migration.
   - A source must be chosen and approved first. Candidates are public deal posts, county deed or recorder transfers to investor LLCs, and operator research. Each has its own cost, terms-of-service and provenance profile.
3. **Production audit not yet run** (see above).
4. **Weekly output metric.** The audit separates imports (`review_only_import`) from other sources, duplicates, suppressed records and contract-qualified records. `observedProductionQualifiedNew` stays `null` until a Scout source produces records with research provenance.

## Gates validated offline

- Qualification, duplicate, suppression and provenance blockers, and the authority and permission shape: `test/leviScoutProductionContract.test.js` and `test/leviScoutResearchOnlyExecution.test.js` (12/12).
- The audit end to end on disposable PostgreSQL with production column names: `test/leviScoutProductionAuditPostgres.test.js`. It checks:
  - every section is readable;
  - only tenant 21 is read;
  - every statement is a SELECT inside a READ ONLY transaction that ends in ROLLBACK;
  - all six tables are byte-identical before and after;
  - no names, emails or sources appear in the output;
  - duplicates and suppression are counted, and nothing is qualified without a range.

  This test fails against the merged audit (`42703`).
- The narrative extraction, review-only import and investigation suites pass (186 + 3), as does `test/maxInvestigationAcceptancePostgres.test.js` (3/3).

## Smallest safe activation plan (each step needs separate authorization)

1. **Operator (read-only).** From this branch, run the production audit and `node scripts/investigationNarrativeAudit.js` under `railway run`. Record the totals, duplicates, suppression, blocker counts and Blueprint status.
2. **Levi.** Approve the financing range, plus any other qualification criteria, through the Blueprint flow. Re-run step 1: `financing_criteria_unverified` must disappear.
3. **Interim producer, no code or spend.** Operator research continues through the existing review-only import. Each week's audit reports genuinely new contract-qualified records separately from imports and duplicates.
4. **Source approval.** Choose one borrower source and a budget ceiling. Then build its `discover` adapter and the four persistence dependencies behind `SCOUT_LEVI_RESEARCH_ONLY_ENABLED`, with an operator migration for the run-claim table, and test against disposable PostgreSQL.
5. **One cycle.** Install a single authority: `maxCandidates ≤ 20`, `maxProviderRequests` 0 (or ≤ 3, with `SCOUT_LEVI_PAID_PROVIDERS_APPROVED=true`), and permissions limited to discover and persist-review-only. Set the flag for that window only. All outreach, campaign and suppression permissions stay `false` (the validator rejects anything else). Records stay `setterVisible:false` and `aoOutreachReviewRequired:true`, and Levi keeps borrower contact.
6. **Rollback and monitoring.**
   - Rollback: unset `SCOUT_LEVI_RESEARCH_ONLY_ENABLED`. A replayed cycle id returns `replay_blocked`.
   - Monitoring: the weekly audit, plus the `recordDiagnostic` output per cycle (discovered, persisted, qualified, duplicate-blocked, `outreachAttempted:false`).
