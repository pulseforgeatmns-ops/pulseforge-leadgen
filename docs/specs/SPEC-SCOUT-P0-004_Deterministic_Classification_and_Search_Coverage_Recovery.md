# SPEC-SCOUT-P0-004: Deterministic Classification and Search Coverage Recovery

Status: Approved for investigation, specification and test implementation. Production activation requires separate approval.
Tenant: Anchor Cleaning (10). Priority: P0 reliability. Created: 2026-10-29.

## Objective

Implement B (deterministic classification) and E (category-diverse search coverage) on a dedicated branch. Recognize suitable facilities, reject demonstrably unsuitable ones, preserve ambiguous evidence and distribute bounded research across the existing approved markets. Open a reviewable PR; do not merge or deploy.

## Vision references

- PROJECT_CONTEXT.md: safety by default, explainability, client scoping.
- SPEC-SCOUT-P0-003: immutable research mission and bounded research-only grants.
- SPEC-SCOUT-P0-002: replenishment root causes and durable search memory.
- ADR-107: explicit tenant-scoped research-provider pause.
- ADR-SCOUT-P0-004: independent facility decisions and research coverage ordering.

## Problem and evidence

The October 29 research-only grant reportedly discovered 79 unique businesses, accepted 19 tickets and retained 18 candidates; 34 awaited website evidence. Supplied production screenshots show structured school types deferred, beauty/martial-arts schools retained, and Manchester Academy at a UK address retained. District/campus procurement overlap and school-heavy query coverage require review. Actual source records must be used where accessible; missing raw data must remain explicitly missing.

## Scope

Keep separate decisions for category recognition, facility suitability, geographic eligibility, physical location/procurement identity, and final research admission. A category never authorizes admission, independent purchasing authority, enrichment or outbound.

| Evidence | Recognized category | Exclusions / missing evidence |
| --- | --- | --- |
| school, primary_school, secondary_school | School facility | Driving, beauty, martial arts, music-only instruction and other instructional studios are not institutional schools |
| university, college, verified equivalent | Higher education | Administrative listing needs distinct service-premises evidence |
| car_dealer | Dealership | Automotive listing without physical facility is insufficient |
| Observed manufacturing operations | Manufacturer | Retailers/suppliers without manufacturing operations excluded |
| Observed warehouse/logistics operations | Warehouse/logistics | Self-storage/mailbox-only excluded |
| restaurant, cafe | Restaurant FOH | Food trucks/no suitable premises excluded |

Use observed provider categories/attributes, never discovery queries as business evidence. Record uncertainty and missing evidence; invent no size, ownership, cleaning need or decision-maker.

Validate address components and country/state before city membership. Manchester, England must fail New Hampshire eligibility. Ambiguous/conflicting geography remains deferred.

Keep physical locations distinct from possible parents/procurement authorities. Test Manchester schools versus the district, multiple UNH Manchester listings, and separate organizations at Manchester Community College's address. A shared domain or address is not sufficient to merge facilities or authorize a shared sales opportunity.

Rotate approved categories and towns, favor under-covered/unresolved work, and avoid repeatedly selecting saturated synonyms. Preserve durable query/page/place memory, grant bindings, provider pause, paid-call controls and one new page per cycle. Record selected/skipped queries, coverage and rationale. Test eight planned cycles for meaningful diversity subject to remaining approved work. Do not create new approved categories or towns.

## Architecture / data model

Default-off, Tenant-10 research-only rollout; no effect on normal replenishment, other tenants, bounded acquisition or backlog recovery. Pure classification and coverage planning support an offline evaluator. Preserve existing ledger rows, guard triggers, identity keys and grant permission semantics. No migration or automatic deferred-ticket backfill.

Retained candidates remain outside normal inventory. Existing unique research-domain/name constraints remain authoritative; immutable raw memberships preserve distinct physical listings even when retention deduplicates a domain. Procurement hints remain unverified until independent supporting evidence exists.

## Retrospective evaluation

Before any production behavior change, run read-only offline evaluation against the 79 records. Report previous/proposed classifications, newly recognized and still-deferred businesses, false-positive/false-negative evidence, geographic corrections, possible purchasing overlap and admission changes without applying them. Distinguish observed outcomes, recomputed outcomes and unavailable evidence. If the raw 79-row export is inaccessible, evaluate the accessible actual observations and mark the full-cohort gate incomplete; never fill absent records with synthetic businesses. Synthetic regression fixtures must be clearly separate.

## Safety / out of scope

No new Google searches, grants, paid enrichment, outbound preparation or sending. No automatic promotion of the 34 deferred records. No changes to recovery maintenance, authorization, suppression or other tenants. Website investigation is separate and unapproved because the fetcher lacks adequate SSRF/redirect protections. Do not invoke or broaden it.

## Implementation plan

1. Inspect classifier, scheduler, ledgers and tests; validate root causes against available production evidence.
2. Commit this specification before implementation; record it in CURRENT_STATE.
3. Implement independent facility decisions and evidence preservation with regression tests.
4. Implement bounded category/town coverage ordering with durable history and receipts.
5. Produce auditable offline retrospective and eight-query before/after coverage comparison.
6. Run relevant CI suites and open a PR with risks, missing evidence and rollback.

## Migration / rollout / rollback

No SQL migration, historical rewrite or production action in this PR. Leave the new flag off. Later activation requires explicit approval, a complete retrospective, exact reviewed revision, unchanged closed controls and valid native grant authorization. Roll back by disabling the isolated flag; preserve all evidence and use existing grant revocation if needed. Never reset paid history or refund consumed cycles.

## Acceptance criteria

- Positive and negative deterministic fixtures pass, including ambiguous/conflicting evidence.
- Manchester, England fails NH eligibility; ambiguous geography does not positively match.
- Public-school facilities are not represented as independent purchasing authorities without supporting evidence.
- Separate service locations and co-located organizations survive identity resolution.
- First eight eligible planned queries demonstrate broader category/town coverage; paid history and page limits hold across restarts.
- Existing records, authorization, suppression, recovery and non-Anchor behavior remain intact.
- Read-only before/after report states cohort coverage and all missing evidence honestly.
- No additional paid calls or downstream activity; relevant CI passes.

## Future work

Secure website investigation, verified procurement relationships, review/repair of historical admissions, and production rollout are separate approvals.
