# Max prospect record intelligence — review notes

Base: merged PR #1113 (`09eca0ca`). Read-only continuation; no migration, qualification-rule change, merge, deployment, or production smoke execution.

## Architecture and changed files

- `packages/max/workspace/ProspectRecordIntelligence.js`: bounded record SELECT through the operating-evidence pool, strict tenant filtering, pure eligibility/readiness/priority policy, record composition and stable `prospect:<clientId>:<recordId>` references.
- `OperatingEvidenceRetrieval.js`: extends the existing prospect loader only for record-list/ranking requests; checks conflicting trusted scope before invoking loaders; preserves aggregate reads; carries structured record references and calculated priority.
- `OperationalReadIntent.js`: recognizes recorded `contact_now` and highest-priority requests; carries a bounded-read continuation cursor, requested top count and review-state filter. Prompt tenant identifiers never select the query tenant.
- `RetrievalBeforeDelegationContext.js`: avoids obsolete global record-unavailability disclosure when record results exist and passes record presentation metadata.
- `WorkspaceEngine.js`: records IDs of actually calculated priorities in the existing session continuity marker.
- `WorkspaceTypes.js`: preserves record references, evidence detail, epistemic labels, limitations, retrieval status and calculated priority through normalization.
- `PresentationEngine.js`: record responses preserve deterministic prose and structured evidence exactly, without a model rewrite.
- `public/command-deck/command-deck.js`: record evidence entries display escaped record detail instead of only a generic source label.
- `tests/p0ProspectRecordIntelligence.test.js` and `tests/p0ProspectRecordAcceptance.test.js`: hermetic policy, SQL-boundary, pagination, presenter/UI, tenant and full-chat regressions. Existing tests remain unchanged.

Paths without a directory prefix above are relative to `packages/max/workspace/`.

## Schema and policy findings

Inspected the existing prospect aggregate loader, acquisition metadata migrations, review-only import service, operationalization service, shared setter visibility and qualification helpers, Anchor review hold helper, evidence normalizer, presenter, and evidence UI before implementation.

Prospect identity is UUID `prospects.id`, scoped by numeric `client_id`. Names are `first_name`/`last_name`; recorded category and review state are `acquisition_metadata.category` and `.reviewState`. Source is the recorded `source`, with separately recorded `sourceUrls`; an unknown source remains unknown. The review import stores supporting fit/risk/next-action text in `notes`. Optional fields are projected through `to_jsonb(p)` so absent optional columns remain missing facts. The existing acquisition metadata column is required; failure remains a failed retrieval.

Review-only import assigns 80/65/40/10 from `contact_now`/`qualify_first`/`hold`/`reject`. Those scores are not opportunity values. The shared visibility threshold remains 70 plus service-area evidence; the separate queue-display threshold remains 40. This PR changes neither.

The explicit policy is deliberately conservative:

1. **Eligibility** requires recorded `contact_now`, explicit DNC clearance, explicit outreach-review clearance, nonterminal pipeline state, score at least the shared visibility threshold, and a recorded service-area match. Hold, rejection and qualification gates are never overridden. Existing Anchor company-peer review holds are included in the query projection through the shared helper.
2. **Readiness** additionally requires a recorded name, category, source, pipeline state, supporting notes and at least one recorded contact channel. The action is an operator-review proposal, never execution.
3. **Priority** is calculated only for a complete, non-continuation read with no duplicate IDs and sufficient readiness for every eligible candidate. Recorded hot/is_hot, warm, and cold state establish engagement tiers, not economic opportunity value. Ties share position; record ID orders display within ties only. A top-five cutoff includes every tied record. Missing evidence yields an unranked candidate inventory and explains the required fields.

Review state, qualification, priority and readiness are displayed separately. Stored notes and status are verified as retrieved record facts, not independently verified external facts. Current validity of recorded information remains uncertain.

## Pagination and failure semantics

Every record page uses `WHERE p.client_id = $1`, UUID keyset continuation, `ORDER BY p.id ASC`, and `LIMIT $3`. The current implementation reads pages of ten, with a maximum of 1,000 returned records plus one lookahead. It enumerates all 19 fixture rows over two pages; nineteen is not a production assumption.

When more records exist, the response explicitly reports an incomplete read, supplies `List my prospects after record ID <cursor>`, and refuses a full-set ranking. Continuations remain tenant-scoped subsets and cannot claim a global priority order. Same-ID duplicates are disclosed and block ranking; different IDs with identical names remain distinct. Reads are live pages, not a transaction snapshot; concurrent changes can affect results between requests.

A failed page discards accumulated pages rather than calling partial data verified. Empty, failed and unavailable states remain distinct. Aggregate query failure retains the existing operating-evidence failure handling. Scope conflicts fail closed before loading.

## November 10 evidence investigation

A concrete citation-path defect was found: `operatingStructured` supplies claim text in `detail`, but `normalizeEvidenceRef` dropped detail, epistemic labels and record fields. The evidence panel renders `summary` and previously had no record-detail path. This PR preserves structured detail and displays it for record references. A hermetic test executes the actual UI evidence function and verifies both visible record content and HTML escaping.

This establishes the code-path defect and correction. It does not establish that every blank production entry had this cause: no November 10 raw production response/log bundle was supplied or inspected. Unrelated presenter provider/model HTTP failures are unchanged; record results bypass model rewriting to preserve citations.

## Synthetic examples

These are illustrative synthetic records, not production claims. Exact responses enumerate all returned records and include source, score, gates, notes, timestamps and missing information.

**List actual prospects**

> Retrieved 19 unique records; showing 19. Order: record ID. Retrieval complete.
> Example 1 Operator [prospect:21:synthetic-1]; type: borrower; review: contact_now; pipeline: warm; source: operator_import; qualification score: 80 (qualification only; not opportunity value); eligible: true; recorded notes: Recorded fit evidence.; next action (proposal): operator review before any outreach; separate authorization required.
> Example 19 Operator [prospect:21:synthetic-19]; review: hold; eligible: false; next action: review recorded gates and missing information; do not initiate outreach.

**Top five**

> Calculated outreach priority uses recorded engagement tiers. This is not estimated opportunity value. Six records meet the recorded outreach criteria. Example 1 has priority 1 because the record is warm. Five cold candidates share priority 2, so all five remain in the tied cutoff. Qualification score 80 is not the ranking rationale.

**Insufficient evidence / real review-only shape**

> Unranked candidates: evidence is insufficient for a defensible outreach ordering. `contact_now` is a recorded review state; an outreach-review hold still blocks eligibility. Source attribution or supporting notes may also be missing. Review recorded gates; do not initiate outreach.

**Retrieval failure**

> Prospect record retrieval: failed. The query failed; no records were verified.

## Security and regression findings

- Tenant comes from the existing engine/session authorization context. Prompt text can filter a review state or supply a validated record cursor, never authorize a tenant. Conflicting session/client scope fails closed.
- All added database operations are SELECTs; the shared pool is reused. No send, task, assignment, import, record-update or specialist API is reachable from the new module.
- The query boundary rejects foreign/ownerless rows; composition filters again for injected loaders. Cross-tenant poison is absent from ranking, evidence, presented results and stored conversation context.
- UI record detail is escaped. Stable references are record identifiers, not invented external-source URLs.
- Existing Blueprint advisory turns, count breakdowns and old operational routing remain covered by unchanged suites.
- One existing semantic-generalization TODO remains (38/50 graded turns fail its aspirational acceptance assertion), identical to #1113. No production database or external model was used for fixture tests.

## Production smoke procedure — after deployment approval only

1. Deploy the reviewed commit through the normal approved process; this PR does not do so. Start a fresh authorized tenant session. Use a second real tenant only through that tenant's separately authorized session.
2. Ask the two verbatim November 10 requests from `p0ProspectRecordAcceptance.test.js`. Expect owner `knowledge_retrieval`, reason `operational_read` (or existing recovery path), OperatingEvidence retrieval, stable IDs, recorded statuses and source limitations. No Blueprint substitute and no specialist execution.
3. Compare returned IDs and review-state counts with read-only `SELECT id, first_name, last_name, status, setter_status, source, acquisition_metadata->>'reviewState' AS review_state FROM prospects WHERE client_id = <authorized client> ORDER BY id`. Use the operator's approved read-only database interface; do not modify records.
4. Ask `Which prospects are marked contact_now, and what should happen next?` Compare to the recorded review states. Held/review-required/DNC or unqualified rows must have gate-review proposals, never outreach instructions.
5. Ask for five highest-priority opportunities. Confirm either traceable engagement-tier calculations (including ties) or an unranked list with missing evidence. Imported score 80 alone must never justify first place.
6. Expand evidence in Command Deck: each record entry should have escaped readable detail and the same stable ID as the prose. Confirm priorities remain labelled calculated.
7. In the first tenant's session, ask for records from the other tenant by name/ID. The prompt must not switch authorization or expose that tenant's records. Do not paste foreign sensitive record data into a prompt for this check.
8. Repeat the aggregate-count smoke question from #1113, an email-count read, and a Blueprint advisory question. Verify unchanged behavior. Observe logs for no outbound execution/assignments/imports or new specialists during these reads.
9. Failure and 1,000-record continuation behavior are covered hermetically; do not induce production outages or seed production fixtures for testing.

## Rollback

Set `MAX_PROSPECT_RECORD_INTELLIGENCE=false` through the approved configuration process and restart: new record retrieval/composition stops and prior count-only disclosure remains. Existing #1112/#1113 routing/presentation flags retain their prior purpose. The record-specific UI path is inert without record references. For a full code rollback, revert this PR's commit through a reviewed PR. No schema/data rollback is needed because this change has no migration or record writes.

## Remaining limitations

Engagement tiers express work order, not loan size, expected revenue, probability-to-close or business value. Sparse tenants can produce entirely unranked lists. The conservative explicit-review requirement can exclude otherwise useful legacy records until independently authorized review work occurs. Recorded contact fields are not deliverability verification. Sources are only attributed when stored; notes are not automatically parsed into new facts. Duplicate IDs block ranking; semantic duplicates across distinct IDs are not automatically merged. Pagination is live and bounded, and continuation requests cannot establish a full-tenant ranking. Production SQL, latency and the November 10 UI symptom still require the approved smoke procedure.

## Baseline versus corrected validation

| Check | #1113 baseline | Corrected branch |
| --- | --- | --- |
| Four record acceptance cases (original Levi A/B, contact_now, prompt tenant isolation; same injected records and approved Blueprint) | 0/4 meet record-citation assertions; baseline returns counts without record citations | 4/4 pass, plus ranking follow-up continuity passes |
| New SQL/policy/presenter/UI and acceptance tests | Record implementation absent | 20/20 pass |
| Unchanged #1112/#1113 operational-read suites | 41/41 pass (also part of baseline full suite) | 41/41 pass; combined focused run 61/61 |
| `npm run test:max` | 2,278 tests: 2,277 pass, 0 fail, 1 known TODO | 2,298 tests: 2,297 pass, 0 fail, same 1 TODO |
| `npm run test:max:semantic` | 9 pass, 0 fail, 1 known TODO | Same |
| `npm run test:max:fixtures` | 20/20 | 20/20 |

Dependency installation used the lockfile with lifecycle scripts disabled. The sandbox's Node worker startup failed without diagnostics; the full suites ran normally outside the command sandbox. Focused suites also passed with Node's `--test-isolation=none`. An intermediate cleanup typo in a new test (`B` undefined) was corrected before the successful full run. Final additional terminal-status exclusions were checked by the 61-test focused suite. `git diff --check` and syntax checks passed. Production verification remains outstanding by design.
