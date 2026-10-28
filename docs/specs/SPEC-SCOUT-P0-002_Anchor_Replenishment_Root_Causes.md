# SPEC-SCOUT-P0-002: Anchor Scout repeated discovery, ICP composition and replenishment truthfulness

Status: Code fixes and deterministic tests are complete locally. Production behavior is **not yet verified**. This session had no Railway, database or production-log access, so every production claim below is either quoted from the operator brief or labeled as an inference from source.

Tenant: Anchor Cleaning (`10`). Baseline: program `outbound_0e6e8d823ee3428405d90815`, mission `mission_anchor_weekend_20261003_approved_market`, fingerprint `22d50dc0…60b7`, last verified revision `039f206`. `main` is two commits ahead of that revision: #1008 (mission scope and facility categories) and #1009 (legacy buyer capability). Neither is confirmed deployed.

## Execution trace (scheduled path)

| Stage | Code | Trigger / gate | Durable state | Failure handling before this change |
|---|---|---|---|---|
| Scheduler | `governedOutboundExecutionClock` (60s timer; Max control every 15th tick) or `POST /cron/anchor-max-outbound-control`; durable stage scheduler when `GOVERNED_OUTBOUND_DURABLE_*` | `ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED`, advisory control lock | `governed_outbound_stage_runs` (durable mode only) | Legacy mode: dispatcher answers `started`, and a thrown cycle only logs to stdout. Durable mode: any well-formed cycle is `completed`, whatever the yield. |
| Authorization | `runMaxOutboundControlLoop` | Recovery state: `maintenance` halts the whole cycle. `scoped_resume` research is rejected by SPEC-262. Research pause blocks discovery. | `anchor_email_recovery_control`, `agent_actions` holds | Holds are durable since SPEC-SCOUT-P0-001. |
| Inventory assessment | `loadCleanInventory`, `buildControlPlan` | `plan.shouldReplenish` | none (computed) | — |
| Query selection | `scoutInput` → `BoundedContext` → `SearchDefinition` → `ConceptLibrary.expandConcepts` → `buildDiscoveryPlan` | Source mission `eligibleSubsegments` and `cities`, rotated by `discoveryGeneration` | none | Order follows mission segment order. |
| Discovery | `PlacesProvider.collectEvidence` + `scoutQueryCheckpoints` session | One new page per cycle **only on the durable path** | `scout_query_checkpoints`, `scout_query_pages`, `scout_tenant_query_owners`, `scout_tenant_place_observations` | Non-durable path: every unhandled query, full pagination, per-fingerprint memory only. |
| Candidate tickets | `scoutCandidateTicketDispatch` / `scoutCandidateTickets` | Durable path | `scout_candidate_*` | One attempt per cycle; deferrals durable. |
| Qualification / admission | `persistDiscoveredCompanies` → `evaluateReplenishmentAdmission` → `resolveReplenishmentVertical` | Mission segments and geography | `scout_unenriched` (raw queue) | — |
| Ownership | `store.candidateOwnership`, `classifyInventoryOwnership` | — | prospects / AO ownership | Unchanged. |
| Enrichment → clean inventory | `runEnrichmentBatches`, `loadCleanInventory` | Email verification, DNC, suppression | prospects | Fixed in P0-001. |
| Outbound readiness | `governedOutbound` refill / tick | `ANCHOR_GOVERNED_OUTBOUND_ENABLED` must equal `true` | `acquisition_outbound_items` | Unchanged; closed. |
| Zero-yield control | `loadScoutDiscoveryBackoff` | 3 zero-yield cycles → next generation; after generation 3, 60-minute cooldown then **reset to generation 0** | `acquisition_outbound_events` (`max_outbound_control`, legacy mode only) | Exhausted plans recycled forever with no operator signal. |

## Root causes

### Confirmed from source

| # | Problem | Cause | Code |
|---|---|---|---|
| RC1 | Composition | **Outdated approved targeting is still the source of truth.** The program's source mission approves `property_manager, str_manager, realtor, commercial_office` over six cities, without Nashua or Merrimack. Every query, admission and clean-inventory decision derives from it. No priority segment (schools/campuses, industrial, dealerships) is in scope. A successor that adds `managed_facilities` exists (`scripts/activateAnchorInventoryCapacityMission.js`), but the verified fingerprint is still the four-subsegment mission, so it was never applied. | `services/maxOutboundControlLoop.js` `sourceScope`/`scoutInput`; `test/scoutSourceCapability.test.js` fixture equals the production fingerprint |
| RC2 | Composition | **Brokerages leak in as property managers.** `resolveReplenishmentVertical` labelled any Google `real_estate_agency` place whose text contained the word "property" as `property_manager`. Almost every brokerage listing contains "property", so brokerages passed any mission that accepts property managers, including `managed_facilities`. | `utils/replenishmentVertical.js` |
| RC3 | Composition / spend | **Segment order decides spend, and realtor ranked ahead of commercial offices.** With one page per cycle, the first unsearched workload wins. Generation 0 also bought two synonyms ("Property Management" and "Property Manager"), which return largely the same Places businesses. That second purchase is paid rediscovery of known businesses. | `ConceptLibrary`, `scoutInput` |
| RC4 | Repeated discovery / spend | **Anchor could run the non-durable discovery path.** Unless `SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED` or `SCOUT_DURABLE_CANDIDATES_ENABLED` is `true`, the ramp creates no page store or tenant memory. Places then buys every unhandled query with up to 20 detail calls each in one cycle, and coverage is keyed by mission fingerprint, so a mission change re-buys everything. | `defaultScoutRampResearchScoped` (`durableEnabled`) |
| RC5 | Replenishment | **Exhausted plans recycle silently.** After generation 3 the backoff expires and generation 0 restarts. With durable memory every query is already handled, so cycles cost nothing but produce nothing indefinitely, and no operator item is raised. | `loadScoutDiscoveryBackoff` |
| RC6 | Replenishment truthfulness | **Success does not mean replenishment.** The durable scheduler classifies any well-formed control result as `completed`, and the legacy dispatcher drops cycle crashes after answering `started`. Zero new qualified prospects is indistinguishable from a successful refill at the scheduler level. | `governedOutboundStageScheduler.classify`, `governedOutboundControlDispatch.launchControlCycle` |

### Governance (intended, unchanged)

Maintenance halts the Max cycle. The research pause blocks discovery. Scoped resume rejects research (SPEC-262). Outbound is disabled. **In the current production state Scout cannot replenish by design.** No code change here alters that.

### Hypotheses (need read-only production evidence)

- H1: `SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED` is unset in production. If so, RC4 was the live discovery path whenever research was open. The scorecard `flags` section answers this.
- H2: the realtor-heavy backlog came from RC1 + RC2 together (realtor queries plus brokerage listings returned by property-management queries). The new scorecard `compositionByVertical` section measures the realtor share of the raw queue and prospects.
- H3: the October 28 recovery's "two existing queue entries + one ownership deferral" pattern is the expected outcome of RC3's synonym queries returning already-known businesses.

## Fixes

1. **Fail closed without durable memory (RC4).** Native Anchor discovery is blocked with `scout_discovery_memory_required` (event + `agent_actions` hold) unless durable memory is enabled. Existing tickets and enrichment still run. Other tenants are unchanged.
2. **Classifier (RC2).** The realty place type alone now needs management-specific evidence (management, leasing, multifamily, apartments, property services) and no brokerage evidence (realty, realtor, brokerage, listings, major residential franchises). Explicit property-management evidence still wins.
3. **Priority (RC3).**
   - `utils/anchorIcpPriority.js` orders Anchor's mission segments (and so query spend) and Anchor's clean inventory (and so first-touch selection) by ICP tier: institutions → commercial PM/offices → industrial → dealerships → residential PM → restaurants → STR → realtors. This deprioritizes without excluding: membership is unchanged.
   - The synonym "Property Manager" query is replaced by "Commercial Property Management", and generation 2 gains "Office Building Management".
   - Managed-facilities templates are reordered so schools/campuses and commercial PMs come first.
4. **Revised mission, proposed and not applied (RC1).** `scripts/activateAnchorFacilityBuyerMission.js` builds an immutable successor with `managed_facilities` + `restaurant_foh` and the eight-city geography, and retires `realtor`, `str_manager` and the standalone residential-leaning PM/office segments. It reuses the audited successor transaction (`--apply --confirm=anchor-facility-buyer-icp-2026-10-28`, prepared envelope must be drained). Medical/dental is **not** included: no Scout vertical, admission, enrichment or routing support exists for it yet.
5. **Exhaustion (RC5).** The ramp records `searchGenerationExhausted` when a durable cycle observes queries but nothing can be bought, replayed or deferred. The next cycle advances the generation at no provider cost. After the last generation, the result is `approved_search_plan_exhausted`: a durable `scout_approved_search_plan_exhausted` hold, a daily free re-observation, and no restart from generation 0. Anchor keeps a productive diversified generation instead of snapping back to an exhausted one.
6. **Truthful outcomes (RC6).**
   - Every control result and event carries `replenishment.status`: `not_required | not_executed | blocked | failed | replenished | zero_yield | search_plan_exhausted`, plus `newQualifiedInventory` and `newQueueAdmissions`.
   - The durable scheduler records it as the `completed` reason (`replenishment_zero_yield`, …) and as evidence.
   - Legacy-dispatch crashes write a `failed` `agent_log` row (`max_outbound_control`).
7. **Scorecard.** `scripts/anchorScoutScorecard.js` adds `replenishmentDaily`, `sourceMission` and `compositionByVertical`, and includes control-cycle failures. It remains read-only.

No flag, migration, recovery control, research grant, spend policy or send authority changes.

## Tests

- `test/scoutAnchorP0Replenishment.test.js` (no DB): classifier, commercial-PM eligibility, segment/query order, first-touch ordering, the revised mission, exhaustion advance/terminal/no-reset and sticky generation, zero-yield vs replenished outcomes, durable scheduler failure, the memory-required block, pause/kill-switch, and outbound disabled.
- `test/scoutAnchorP0ReplenishmentPostgres.test.js` (native disposable PostgreSQL, synthetic Places): ten real ramp cycles across simulated restarts. It checks:
  - one page per cycle in priority order;
  - no repeated query;
  - a known business reused without a detail call and not duplicated;
  - brokerages (including a "Property Group") not admitted while a commercial PM is;
  - the full plan covered exactly once and then explicit exhaustion with zero calls;
  - mission replacement buying nothing.
- Mutation check: restoring the old classifier fails three of these tests.
- Updated tests: `scoutSourceCapability` and `scoutRecoveryDiscovery` assert the same approved membership with Anchor's tier order.
- Pre-existing failures, unchanged and also red on the untouched base: `anchorMondayPreflight`, `anchorPreparationBinding` (5), `audit059ExternalDiscoveryProviderFailure`, `scoutCandidateUniverseEstimate`.
- Disposable PostgreSQL refuses to run as root, so the Postgres suites were run as an unprivileged user.

## Outstanding risks and dependencies

- Production state is unverified from this session (no Railway/DB access). The flags, deployed revision, installed schema and backlog composition all need the read-only scorecard.
- `scoped_resume` writes no `max_outbound_control` event, so the backoff, exhaustion and scorecard evidence are blind in that mode. Scoped research is currently rejected (SPEC-262), so the scheduled path is only reachable in legacy mode. The rationale for that guard is undocumented and it is left unchanged.
- Applying the revised mission moves existing realtor/STR prospects out of this program's clean inventory (kept, not deleted). That is a material scope change needing operator approval.
- Medical/dental/professional (priority 5) and "multi-location operator" as a segment need taxonomy, enrichment and AO-routing work.
- Place-type ambiguity remains for names with neither management nor brokerage evidence (now classified `realtor`), by design.

## Controlled production validation plan

Each step needs the approval named. Stop on any stop condition.

**V0. Read-only (already authorized).** Run `node scripts/anchorScoutScorecard.js --days 14` in the service console. Record:
- the revision;
- the flags;
- `sourceMission`;
- `compositionByVertical` (realtor share);
- `replenishmentDaily`;
- open holds;
- whether `2026-10-27-scout-tenant-search-memory.sql` is installed (`SELECT to_regclass('scout_tenant_query_owners')`).

**V1. Deploy this PR (approval A).** Change no flags. Expected while maintenance is armed: every cycle `halted: anchor_recovery_separate_preparation_approval_required`, zero provider calls, holds unchanged. Stop if any `api_usage_events` row for `google_places` appears for tenant 10.

**V2. Revise the mission (approval B).** Run `node scripts/activateAnchorFacilityBuyerMission.js` and review the plan output (pending must be 0). Then apply with the confirmation token. Acceptance: the program's `source_mission_id` changes, a `source_mission_succeeded` event exists, and `sourceMission` shows `managed_facilities, restaurant_foh` with eight cities.

**V3. Bounded scheduled-discovery window (approval C: lift maintenance to legacy mode and disarm the research pause for the window; outbound stays `false`).**

Preconditions:
- `SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED=true`;
- the memory schema is installed;
- the `google_places` spend policy is set to `daily_request_limit=170` and `daily_usd_limit=5`, which allows 8 cycles × (1 search + ≤20 details) and is ≤ $4.32 at the in-repo upper-bound prices.

Run 8 scheduled cycles (about 2 hours), then re-arm the pause and maintenance.

Stop immediately on any of:
- more than 1 Text Search in a cycle;
- any query purchased twice (`scout_tenant_query_owners` duplicates or repeated `search:` text);
- a `REQUEST_DENIED` or uncertain reservation;
- any admitted `realtor`/`str_manager` row;
- any `acquisition_outbound_items` or send;
- a `failed` control row.

Acceptance:
- every cycle has a `replenishment.status`;
- each cycle reserves ≤1 page;
- no repeated query;
- new raw-queue rows are ≥80% tiers 1–4;
- realtor share of new admissions is 0%;
- exhaustion or zero yield, if reached, appears as a hold rather than silence.

**V4. Sustained observation (approval D).** Run five business days in legacy mode with the same spend policy and outbound still disabled. Acceptance: daily scorecard, `replenished` cycles on days with deficit, measured clean-inventory growth, and no stop conditions. The five-first-touches/day objective needs separate outbound authorization and is not claimed here.

### V3 is structurally blocked by recovery governance (found Oct 28)

"Lift maintenance to legacy mode" is not a flag change once the recovery control table exists:

- `readState` (`services/anchorEmailRecovery.js`) returns `legacy` **only while `anchor_email_recovery_control` is absent**. With the table present, the singleton must stay `maintenance_armed`, and every state is `maintenance`, `active` (old canary) or `scoped_resume`.
- `runMaxOutboundControlLoop` halts in `maintenance` and `active`.
- `scoped_resume` manifests reject research outright (`resume_paid_research_unsupported`, `services/anchorScopedResume.js`).
- The Oct 28 backlog recovery ran in, and required, `maintenance / recovery_kill_switch`, so the table exists in production.

Scheduled Max Scout research therefore cannot run in production without removing a permanent recovery-control record or adding a reviewed research scope to scoped resume. Either is a recovery-governance change and needs its own spec and approval; neither is a validation step. Mission activation (V2) is independent of this and remains possible.

Before any step, run `node scripts/anchorScoutValidationPreflight.js --expect-revision <sha>` (read-only). It reports a pass or block for each gate (revision, successor scope, drained envelope, installed manifest, memory flag and schema, Places spend policy within 8 cycles × 21 calls and $5, outbound off, recovery mode, research pause, Max scheduling), plus `mayApplyMission` and `mayStartWindow`.

## Readiness verdict

**NOT YET VERIFIED.** The source defects are fixed and tested offline and on disposable PostgreSQL. Scheduled autonomous replenishment cannot run in the current governed state, and none of the production acceptance criteria has production evidence.
