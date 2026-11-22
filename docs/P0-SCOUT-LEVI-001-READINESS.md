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

## Production audit (platform-neutral)

The audit needs only `DATABASE_URL` (via `db.js`) and Node. It has no Railway dependency. Run it from the application image or a checkout with the production database URL in the environment:

```sh
npm run scout:levi:production-audit -- --expect-database=<database name>
```

- `--expect-database` makes the run fail closed (exit 2, no report) when the URL points at a different database. On Nov 17 the Render staging service was found pointing at the wrong database.
- Every report carries `target: { database, serverVersionNum, platform }`, where platform is `render`, `railway`, `local`, `other` or `unknown`. The URL, host and credentials are never printed.

Where to run it:
- **Railway (current live source, until cutover):** `railway run --service pulseforge-leadgen --environment production npm run scout:levi:production-audit`. This is read-only verification only.
- **Render (destination):** from a shell on a Render service built from a commit that contains this audit, connected over the internal URL: `npm run scout:levi:production-audit -- --expect-database=pulseforge_production_migration`. Until cutover, the Render database is the 2026-11-17 snapshot, so the report describes that snapshot and not live data.

**Not executed from this session.**
- No Railway CLI and no database credential are available here.
- The Railway MCP cannot run SQL.
- The Render database has an empty IP allowlist, so it refuses external connections, including the Render MCP's read-only query.
- A Railway environment fork was rejected: it copies production variables, so the fork's services could reach real provider and sender credentials.

**Defect found in the merged audit (fixed on this branch).** The `workspace` query selected `blueprint_id` and `blueprint_version` from `cie_interview_sessions`, but no migration creates those columns. Run against the real schema, the section comes back as `{"unavailable":"42703"}`, and the Blueprint state is never reported. The fix:
- reads session status from existing columns;
- reads Blueprint status (id, version, status) from `cie_business_blueprints`;
- reduces the workspace correction to a boolean, so no correction content is emitted;
- reports `allTime` and `reviewOnlyContactNow` inventory as well as the 30-day weekly window. Records imported before the window were invisible before;
- breaks duplicate ties by id. Before, two copies of one identity created in the same transaction were both counted as duplicates.


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

1. **Approved financing criteria (Levi).** Production reads the Blueprint through `tenantCriteria` and never substitutes the proposed $5k–$20k range. Until one KNOWN `Financing range: $X–$Y` fact is approved, qualification stays blocked and permit rows remain Qualify First.
2. **Research-only flag is still off.** `SCOUT_LEVI_RESEARCH_ONLY_ENABLED` is not set on the production service. The `scout_levi` clock, cron and smoke path fail closed. `SCOUT_LEVI_PAID_PROVIDERS_APPROVED` stays unset; `maxProviderRequests` is 0.
3. **Permit rows are signals, not qualified borrowers.** NYC and Chicago open-data permits persist as `property_record_signal` with `setter_visible=false`. They cannot become Contact Now without a contact path, borrower intent, and an approved range.
4. **Weekly output metric.** The audit now sees `scout_levi` claims in `agent_log`. `observedProductionQualifiedNew` stays `null` until a contract-qualified research record exists.

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

## Render migration compatibility (Render is the production destination)

Verified on 2026-11-19 through the Render API, read-only.

| Item | State |
| --- | --- |
| Render workspace | `Pulseforge` (team) |
| Web service | `pulseforge-api-migration` (Docker, manual deploy, branch `migration/render-isolated-staging`, Ohio). The live deploy is `1fe69d6` (2026-11-17). With `PF_MIGRATION_STAGING=1` it runs only `migrationStagingServer.js`, a read-only health probe. No application routes, agents, crons or outbound. |
| Database | `pulseforge-production-migration`: PostgreSQL 18, 10 GB, available, external access blocked. Holds the restored 2026-11-17 Pulseforge snapshot: 274 tables, 1,627,867 rows, row counts reconciled against the archive. Signal tables are present as empty schema. |
| Cron jobs | None |
| PR #1129 | Open draft, `mergeable_state: dirty` (conflicts with `main`), marked DO NOT MERGE during the P0 freeze. Its branch predates #1143, so the Render service image does not contain the Levi audit or contract. |

**What the Levi workflow needs on Render.** None of it is Railway-specific code.
1. A Render service image built from a commit that contains the Levi audit, contract and research-only modules. That means #1129 has to be rebased onto current `main`, or the staging Dockerfile gate has to be carried forward.
2. **TLS.** `db.js` keeps TLS on (`rejectUnauthorized:false`) unless `DATABASE_SSL=false`. The staging probe uses the same setting and confirmed the Render database identity over the internal URL on Nov 17, so no change is needed.
3. **Live data.** The Render database is a snapshot. Levi's live inventory stays on Railway until a separately authorized cutover with delta reconciliation.
4. **Research-only cycle on Render (future, unbuilt).** A Render Cron Job or one-off job that invokes a CLI wrapper of `runResearchOnlyCycle`. The flags would be Render environment variables (`SCOUT_LEVI_RESEARCH_ONLY_ENABLED`, `SCOUT_LEVI_PAID_PROVIDERS_APPROVED`), and the run-claim table would be added by an operator migration applied to the Render database. Nothing should be added to Railway.
5. The Railway references in the codebase (`RAILWAY_*` environment variables in Anchor and Scout services, `cieIntakePathTrace`, `maxDecayAgent`) are not on the Levi path. They belong to the general migration, not to this workstream.

## Borrower-discovery sources

Levi's approved segment (workspace correction): **active real-estate operators nationwide** who need short-term, asset-backed capital for fix-and-flip rehab costs or equity gaps. The first validation target is 10 decision-ready records with at least 6 accepted by Levi. No outreach.

The key distinction: **an active financing request** is a borrower stating a current need. **An ownership or investment signal** is a recent purchase, a flip, a permit or LLC ownership. A signal is inferred intent. It can support a record's "active project" evidence but can never make a record qualified on its own.

| Rank | Source | Intent | Access / terms | Cost | Freshness / coverage | Expected yield | Integration |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | **Operator-assisted research of public financing-request posts** (BiggerPockets Hard Money & Private Lending forum: "seeking $151K fix-and-flip financing", "private lender needed for gap loan"; similar public forums) | **Explicit, current request** with deal facts (amount, ARV, location) | A person reads public posts as an ordinary user. Automated collection is **not cleared**: I could not confirm the BiggerPockets terms clause on scraping, so any automation needs written permission. Many posts are lenders advertising, and contact is often platform messaging only. | $0 provider spend; operator time only | Daily, nationwide | Unmeasured. Hypothesis: a minority of posts are genuine borrowers, and fewer have a reachable contact path | **None.** The existing review-only import already takes `sourceUrl`/`biggerPocketsUrl` |
| 2 | **Levi's own inbound financing-request intake** (public form, same pattern as the existing Studio Substral / walkthrough intakes) | **Explicit request, with consent** | Fully owned | Low build cost. Traffic needs marketing, which is a separate outreach authorization | Continuous, nationwide | Depends on traffic | Small (existing public-intake pattern) |
| 3 | **Paid borrower-lead marketplaces**: BiggerPockets lender lead bundles (priced by state, quote-only, lenders vetted for an active license), Real Estate Bees pay-per-lead, Private Lender Link, LendingWise / Sherman Bridge marketplaces | Explicit request (consented) | Contractual. Levi must qualify as a featured or licensed lender | Quote-only; not verified | Continuous | **Mixed.** One BiggerPockets lender reported most leads were unqualified homebuyers seeking 100% financing | Moderate: an import adapter per vendor |
| 4 | **Property / deed / flip data**: PropStream "Flippers" and cash-buyer lists (about $99/mo, third-party figures), ATTOM (from $500/mo, 30-day trial), BatchData, county recorders (patchy; some bulk agreements, e.g. Pima County; NYC ACRIS open data) | **Signal only**: ownership or recent investor purchase | Licensed data | $99–$500+/mo | Weekly to monthly; nationwide through vendors | Cannot produce qualified records alone | Moderate: needs an enrichment or verification role |
| 5 | **Building-permit open data** (Socrata city portals) | Signal only: a project in progress | Open data, city by city; owner identity needs a parcel join | Free | Fresh, city-limited | Low for a nationwide lane | Moderate |
| Excluded | Google Places / SerpAPI (business listings, not borrowers); Facebook / LinkedIn groups (terms prohibit scraping); Reddit (commercial Data API use requires a Reddit contract) | — | — | — | — | — | — |

**Recommendation.**
- Start with **source 1, operator-assisted, through the existing review-only import**. It is the only option with explicit current intent, zero provider spend and zero new code.
- Run **source 2** alongside as the durable consent-based channel once Levi authorizes promotion.
- Use **source 4** later, only to verify the "active project / property acquired" claims on records that already show a request. Never use it as a qualifying source.
- Do not automate collection from any third-party site without written permission.

## Proposed first controlled validation experiment (needs owner approval)

- **Duration and caps:**
  - two weeks;
  - at most 10 operator hours per week;
  - at most 25 imported records per week;
  - $0 provider spend;
  - no outreach, no campaign enrollment, no suppression changes;
  - records stay `ao_outreach_review_required=true` and `setter_visible=false`.
- **Inclusion:** a public post from the last 14 days, by the borrower (not a lender or broker), stating a current fix-and-flip or equity-gap need for an identifiable deal. Record the source URL, post date, stated amount or range, stage, location, and any contact path.
- **Measurement:** the weekly audit's `allTime`, `last30Days` and `reviewOnlyContactNow` sections:
  - imported records;
  - duplicates and suppressed records;
  - per-blocker counts;
  - **explicit-request candidates**: stage plus amount recorded;
  - **Levi-accepted count**: Levi's review state.

  `contractQualified` stays 0 until Levi approves a Blueprint financing range. It is reported as 0, not estimated.
- **Success:** at least 6 of the first 10 accepted by Levi (the Blueprint validation target). Then 5–10 accepted per week, at a measured operator cost per accepted record.
- **Stop:** fewer than 3 accepted per week after two weeks, which means re-ranking the sources.
- **Known gap (small, separately authorized change):** the importer records `importedAt` as the evidence timestamp and has no field for the source's post date or source type. Freshness and provenance therefore depend on the operator writing them into the Fit evidence until the importer accepts `evidenceDate` and `sourceType`.

## Smallest safe activation plan (each step needs separate authorization)

1. **Operator (read-only).** Run the production audit with `--expect-database`, plus `node scripts/investigationNarrativeAudit.js`, against the live source. Until cutover that is Railway (read-only); after cutover it is Render. Record the totals, duplicates, suppression, blocker counts and Blueprint status.
2. **Levi.** Approve the financing range, plus any other qualification criteria, through the Blueprint flow. Re-run step 1: `financing_criteria_unverified` must disappear.
3. **Interim producer, no code or spend.** Operator research continues through the existing review-only import. Each week's audit reports genuinely new contract-qualified records separately from imports and duplicates.
4. **Source approval.** Choose one borrower source and a budget ceiling. Then build its `discover` adapter and the four persistence dependencies behind `SCOUT_LEVI_RESEARCH_ONLY_ENABLED`, with an operator migration for the run-claim table, and test against disposable PostgreSQL.
5. **One cycle.** Install a single authority: `maxCandidates ≤ 20`, `maxProviderRequests` 0 (or ≤ 3, with `SCOUT_LEVI_PAID_PROVIDERS_APPROVED=true`), and permissions limited to discover and persist-review-only. Set the flag, as a Render environment variable, for that window only. All outreach, campaign and suppression permissions stay `false` (the validator rejects anything else). Records stay `setterVisible:false` and `aoOutreachReviewRequired:true`, and Levi keeps borrower contact.
6. **Rollback and monitoring.**
   - Rollback: unset `SCOUT_LEVI_RESEARCH_ONLY_ENABLED`. A replayed cycle id returns `replay_blocked`.
   - Monitoring: the weekly audit, plus the `recordDiagnostic` output per cycle (discovered, persisted, qualified, duplicate-blocked, `outreachAttempted:false`).
