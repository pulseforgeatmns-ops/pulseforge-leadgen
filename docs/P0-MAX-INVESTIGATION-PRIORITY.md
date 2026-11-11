# Max investigation priority correction

## Scope and baseline

Fresh branch from `main` at `650e8f07` (merged #1114). The prior clean branch and #1114 history are preserved. The reported Claude implementation was absent after checking workspace, fetched branches, history, worktrees and unreachable commits; this is the explicitly authorized reconstruction.

Two regressions reproduced independently on that baseline:

- Six records generated through the real `normalizeContact` / `noteFor` import normalizer have `reviewState=contact_now`, source `review_only_import`, review clearance required, `outreachAuthorized=false`, and no contact channels. All are correctly outreach-ineligible. #1114 nevertheless uses outreach eligibility as the candidate pool for analytical priority, so it produces no ranking.
- `Which of these contact_now prospects should I prioritize for investigation?` is classified as advisory despite referring to recorded prospect state. A regression test demonstrates the conflict before the bounded classifier fix.

No production database, outbound service, specialist, migration or deployment was used.

## Architecture and changed files

| File (under `packages/max/workspace/` unless stated) | Change |
| --- | --- |
| `ProspectInvestigationPriority.js` | Pure, deterministic evidence-coverage policy, approved tenant criteria, concise record composition and stale-cohort checks. No database, model or execution dependency. |
| `ProspectRecordIntelligence.js` | Calls the analytical policy after the existing tenant filter; legacy outreach evaluation and retrieval are unchanged. |
| `OperatingEvidenceRetrieval.js` | Passes the existing business-understanding result to the composer and preserves analytical evidence through the existing structured response. |
| `RetrievalBeforeDelegationContext.js` | Carries investigation metadata and review-state filter through the response. |
| `WorkspaceTypes.js` | Preserves record fingerprints, recorded claims and investigation metadata through normalization. |
| `OperationalReadIntent.js` | Narrow recorded-state `should I` fix, explicit investigation/outreach mode, explicit top-count handling and selected-cohort continuity. |
| `tests/p0InvestigationPriority.test.js` | Production request, policy, evidence, tenant, rollback, pagination, presenter and continuity regressions. |
| `tests/fixtures/investigationProspects.js` | Six synthetic records built using the real review-only import normalizer; no production identities. |
| `docs/examples/max-investigation-six-record-response.txt` | Actual six-record engine/presenter output from the synthetic fixture and an approved synthetic Blueprint. |
| `docs/P0-MAX-INVESTIGATION-PRIORITY.md` | This delivery, validation and acceptance record. |

No existing tests or assertions were weakened or changed. All 61 earlier focused regression tests remain unchanged.

The new path runs for record-ranking requests explicitly about investigation/analysis/financing, or ranking requests over retrieved review-only records. Explicit outreach-priority requests retain #1114's outreach policy. Other legacy non-investigation ranking stays on its existing path. There is no competing intent classifier or alternate retrieval system.

## Investigation policy

`recorded-deal-coverage-v1` compares the coverage of five explicitly labelled recorded claims: financing request, deal stage, lien/security position, exit plan and closing date. Labels come from stored notes, including the import's `Fit evidence` section; multiline content is retained. This is a provisional order for checking concrete cases, **not** a financing recommendation, economic-value score, underwriting assessment or predicted likelihood of funding.

- Greater coverage makes a case easier to investigate first. Review state, qualification score, names and contact-channel availability do not manufacture priority.
- A comparison requires traceable name/source/notes, at least two candidates with meaningfully different coverage, no detected contradictory labelled claims, and complete, non-continuation retrieval without duplicate IDs. Otherwise candidates remain unranked and limitations are stated.
- Unknown or unverified labelled values do not count as available deal details. Unsupported amount formats and conflicting labelled amounts/security claims are flagged rather than guessed. Blank labels cannot consume the next line or double-count another field. Free-form prose and profile descriptions remain recorded evidence, but are not parsed into invented financing facts.
- `contact_now` and `qualify_first` can be considered analytically. Hold, reject, DNC, terminal and setter-held records remain visible when requested but receive no priority. Review-only import clearance can remain blocked while analytical priority is calculated. Contact channels are not required for read-only review.
- Equal coverage shares a position. ID determines display order within a tie, never business superiority. No implicit five-item cutoff is used on this path. An explicit top-five request returns five only when the cutoff is distinguishable; all boundary ties are included and explained. Unranked lists are not shortened to imply five defensible winners.
- Proposed next steps inspect existing evidence and reconcile missing claims/title/lien/exit documents. They do not initiate research specialists, communications, assignments or state changes.

Financing concerns are a separate output. A case can be easy to investigate while its recorded financing request is outside the recorded business range. Numeric range overlap alone never establishes financing suitability or funding probability.

## Tenant criteria and evidence integrity

Criteria come through the existing authorized business-understanding retrieval. Tenant and client IDs must match. The policy accepts an approved canonical snapshot or a newly retrieved approved Blueprint record. Original `KNOWN` service/constraint facts must have IDs and provenance; canonical contract fields are a fallback when a canonical snapshot exists. Neither the natural-language question nor arbitrary prompt-supplied business criteria supplies a range.

During output inspection, the existing business-summary projection was found to split comma-formatted money into list fragments (`$9,000–$15,000` becomes malformed prose). This PR leaves that broader projection unchanged and uses original approved provenance-backed facts before that lossy projection.

Only one explicit labelled financing/loan/lending range in service facts can be compared. Missing, conflicting, ambiguous, legacy-only, unapproved, failed or foreign context does not supply a range. Unsupported currency/format/terms still require verification. The implementation contains no tenant ID, prospect name or hardcoded financing range; fixture amounts are synthetic and intentionally differ from the production range described in the handoff.

Record identity, stored source and state are verified as retrieved facts. Deal descriptions and risks are labelled **recorded claims, unverified**. Priority is separately labelled **calculated**. Imported raw notes, recorded URLs and parsed claims remain in structured evidence; concise prose displays the decision-relevant claims and risks. Source URLs are recorded attribution, not independently checked evidence. Stable `prospect:<client>:<id>` and Blueprint references trace the output to retrieved state.

The existing SQL tenant boundary, bounded keyset pages, record-status handling and deterministic presenter remain in use. Foreign and ownerless rows cannot enter the analytical result, citations or session. No added query, migration, sender, assignment or execution path exists. Explicit `outreachAuthorized=false` remains blocked in the analytical answer even if a clearance field is inconsistent.

## Follow-up continuity

The existing operational-read continuity marker now retains the review filter, selected IDs, record fingerprints, policy and criteria fingerprint. It stores references and calculated context, not a new prospect state.

A follow-up such as `Why is the first prospect ranked highest?` rereads the authorized records and explains the actual selected cohort. It does not add unrelated records or expand a prior top-five selection to six. Unchanged selected records and criteria reproduce the prior order; the answer explicitly says it is explaining that earlier cohort, not establishing a new workspace-wide ranking.

Changed data, changed review state, missing selected records, different policy or changed criteria invalidate the positions. The reply shows only current retrieved facts unranked and clears the prior ranking marker. Missing IDs are disclosed without treating missing records as verified. A follow-up after an unranked result explicitly says that no first-ranked record exists. A foreign-tenant continuity marker cannot supply IDs or criteria.

Fingerprints detect changes visible to the fresh read, not changes after it. Live pagination is not an atomic snapshot. A ranking is a provisional read-time calculation, not a durable current-fact claim.

## Output inspection

Full generated response: [six-record example](examples/max-investigation-six-record-response.txt). It was produced through the actual `WorkspaceEngine.ask` and deterministic presenter using normalized synthetic imports and an approved synthetic Blueprint, then inspected in full. All six lack contact channels and remain outreach-blocked. Trailing display whitespace is removed in the saved text.

Representative faithful excerpt:

> Retrieved 6 unique authorized records; selected cohort 6; showing 6. Retrieval complete.
>
> Alex Example [prospect:21:00000000-0000-0000-0000-000000000001] ... Investigation position 1: 5/5 labelled deal claims available to check.
> Recorded claims (unverified): amount $12,000; purchase contract recorded; first lien claimed; exit sale; closing 2026-12-01.
> The request is within the synthetic approved range, which alone does not establish suitability or probability of funding. Title and borrower claims remain unverified. Outreach eligibility: blocked.
>
> Blair Example ... position 2: 4/5 claims. Its recorded $30,000 request is outside the synthetic $9,000–$15,000 range. Existing senior debt needs verification. This remains a financing concern, separate from the investigation ordering.
>
> Casey, Devon, Emery and Frankie are also represented with their IDs, recorded risks, missing verification and read-only next steps. Profile-only Frankie has no explicit deal details; a fixture containing six such profile-only records remains wholly unranked.

Output improvements made during inspection include readable field names, retained multiline fit/risk text, explicit source attribution, separation of financing concerns, and shared rather than repeated explanations of unverified claims. The response does not dump entire raw records; full imported notes remain in structured evidence.

## Rollback

Set `MAX_PROSPECT_INVESTIGATION_PRIORITY=false` (also accepts `0`, `off`, `no`, `disabled`) through the approved configuration process and restart. This disables the new analytical path and restores #1114's outreach-only composition, including its refusal to rank the blocked fixture. It does not relax any execution gate. The minor explicit-count/filter and continuity fields can remain inert safely. Existing `MAX_PROSPECT_RECORD_INTELLIGENCE` and operational-read flags retain their prior purpose.

For a full code rollback, revert this PR's commit through a reviewed PR. No schema or data rollback is needed. This task did not change production configuration.

## Production acceptance procedure — only after explicit deployment approval

1. In an authorized LA Endeavors session, request the six recorded `contact_now` prospects and compare IDs against a read-only tenant-scoped database read. Do not import or change any records for this check.
2. Ask the original failure prompt verbatim:

   `Of these six contact_now prospects, which should Levi prioritize first, considering his working financing range, deal readiness, security position, available evidence, and likelihood of progressing to a funded transaction? Rank only where evidence supports it. Explain blockers and do not initiate outreach.`

   Expect all six represented, IDs and recorded status/source visible, provisional coverage-based investigation priority only where supported, or an honestly unranked result when the records lack explicit comparative evidence. Outreach must remain blocked. No invented financing range, lien certainty, borrower intent or funding probability.
3. Ask `Which of these contact_now prospects should I prioritize for investigation?` Expect the operating-record read path rather than generic Blueprint advice. Generic `What campaign should I run?` remains advisory.
4. Ask `Why is the first prospect ranked highest?` Expect the actual previous cohort and references, or an explicit no-prior-ranking message. In a controlled nonproduction fixture, change a selected record or criterion and repeat: expect invalidated positions, not stale certainty. Do not mutate production to test invalidation.
5. Compare `Rank all six contact_now prospects for investigation` with `Rank my top five contact_now prospects for investigation`. All six must remain in the first response; an explicit top-five cutoff is applied only when defensible, with boundary ties disclosed.
6. In another separately authorized tenant, repeat a record-ranking request. Its own criteria or an explicit absence must be used. Naming another tenant in a prompt must not switch authorization or expose records.
7. Expand record evidence and inspect the structured response: stable references, recorded claims/risks/source URLs, calculated priority and criteria provenance must survive presentation. Check no specialist, sender, assignment, review-state change or other execution was invoked.
8. Recheck #1112/#1113 aggregate-count and Blueprint-advisory prompts, plus #1114 record listing. They must retain their behavior.
9. Test failed, incomplete, duplicate, missing-source and rollback cases in the hermetic fixtures rather than inducing production failures.

## Known limitations

This bounded policy compares explicit deal-evidence coverage, not the semantic quality or truth of arbitrary free prose. It can leave real-world records unranked even when a human could form a hypothesis from narrative context. It is not a universal industry ranking model. Recorded-dollar range comparison supports explicit integer-dollar labels, not shorthand, multi-currency underwriting, capacity, deal terms or guaranteed economic fit. Raw source material is not fetched. Semantic duplicates across different record IDs are not merged. Reconciliation and additional specialist research remain proposals requiring their own authorization. No production smoke test or deployment was performed.

## Validation evidence

| Check | Baseline `650e8f07` | Corrected |
| --- | --- | --- |
| Two original regression assertions: blocked six-record analytical priority + `should I` record routing | 0 pass, 2 fail (independently rerun in detached worktree) | Both pass |
| New investigation suite | Implementation absent | 29 pass, 0 fail |
| Combined focused investigation + unchanged operational/record regressions | Existing 61 pass | 90 pass, 0 fail |
| Full `test:max` | 2,298 tests: 2,297 pass, 0 fail, 1 TODO | 2,327 tests: 2,326 pass, 0 fail, same 1 TODO |
| Semantic ratchet | Same known TODO confirmed in baseline full suite | 9 pass, 0 fail, 1 TODO |
| Max fixtures | Prior baseline 20/20 | 20 pass, 0 fail |
| Related import, tenant and operational authorization regressions | No new failing baseline to investigate | 85 pass, 0 fail |
| Knowledge suites | No new failing baseline to investigate | 48 pass, 0 fail |

Exact current commands:

```sh
node --test --test-isolation=none packages/max/workspace/tests/p0InvestigationPriority.test.js packages/max/workspace/tests/p0ProspectRecord*.test.js packages/max/workspace/tests/p0OperationalRead*.test.js
npm run test:max
npm run test:max:semantic
npm run test:max:fixtures
node --test test/reviewOnlyProspectImportService.test.js test/tenantWorkspace.test.js test/registrationWorkspace.test.js test/maxOperationalAuthorization.test.js packages/max/workspace/tests/tenantContext.test.js
npm run test:knowledge
node --check packages/max/workspace/ProspectInvestigationPriority.js
git diff --check
```

Full Max was also run in a detached worktree at `650e8f07` with the same installed lockfile dependencies. The two initial reproduction tests and their normalizer fixture were copied there without changing baseline production code. The known TODO is the existing semantic-generalization acceptance target (38/50 graded turns fail its aspirational assertion), identical on baseline and corrected runs; it is not counted as a passing acceptance test. No new failure or environment-dependent regression remained. Node worker suites ran outside the command sandbox because the sandbox's worker startup was previously nondiagnostic; the focused suite additionally passed with isolation disabled. Tests deny unexpected network access and never call the real import execution function.

Final diff review found no existing-test changes, dependency changes, schema migrations, production identities, hardcoded tenant criteria, alternate database path, or execution-gate relaxation. New evidence is routed through the existing deterministic record presenter. The broader CIE monetary-summary projection defect remains outside scope; the bounded criteria adapter reads original approved fact evidence instead.

## Post-deploy regression: record-level precedence (Nov 11 2026)

After deployment, the LA Endeavors production prompt ("Rank the six prospects currently marked contact_now by investigation priority, using their actual **recorded** evidence. …") was answered with generic Business Intelligence plus Campaign 001 / AO / Yelp / walkthrough absence statements. Production log: `WORKSPACE_OWNER_SELECTED reason:"knowledge_retrieval"`, no `operationalRead`.

Root cause: `WorkspaceOwnershipResolver.claimsOperationalRead` deferred to `claimsKnowledgeRetrieval` *before* classifying the read. The legacy `shouldRetrieveOperatingEvidence` detector matches any operating object ("prospects") plus an existing-state word ("recorded", "existing", "already", "inventory"), so it claimed the turn, and the engine sent it to the legacy composer without `operationalRead`. Neither #1113's focused presentation nor the #1115 investigation composer ran. The #1115 fixture question contained no legacy trigger word, so it passed. No exception, presenter failure or flag value was involved.

Correction (routing only): operator updates, claim challenges and claim corrections keep priority; the operational-read classifier then runs, and the legacy detector keeps only **non-record-level** reads. Record-level means `wantsRanking`, `wantsRecordList`, a `reviewState` filter or a pagination `cursor`. The operational-read execution guard now also recognizes "change/update/set/mark the review state", so a review-state modification request is never answered with a record listing. Ranking policy, tenant criteria, outreach gates and presenters are unchanged.

Rollback: `MAX_OPERATIONAL_READ_PRECEDENCE=false` restores pre-#1110 routing (the legacy route claims these turns again), or revert the commit.

Tests: `packages/max/workspace/tests/p0RecordReadPrecedence.test.js`: the exact production prompt through the full engine, a trigger-word routing matrix, legacy non-record controls, operator update / challenge / correction guards, record-modification controls and plain listings.

## Narrative deal-evidence correction (Nov 11 2026)

After #1117 routed the LA Endeavors prompt correctly, all six real `contact_now` records were still **unranked**, and Max listed financing request, deal stage, security position, exit plan and closing date as missing even though the recorded notes stated them (for example a `$35k` request, a proposed first lien and a DSCR-refinance exit).

### Root cause

- The review-only import (`services/reviewOnlyProspectImportService.js`) accepts free-text `fit`/`risks` and `noteFor()` stores them as prose in `prospects.notes` (`Fit evidence: …`, `Risks: …`). There are **no structured deal columns**; `acquisition_metadata` holds only import mode, outreach flag, review state, category and source URLs.
- `recorded-deal-coverage-v1` counted a claim only when written as an exact `Label: value` line. The real import never produces that format, so every record scored 0/5, no two records differed and the cohort was unranked. The #1115 fixture had been authored in the labelled format, so the mismatch was not caught.
- Three parser defects compounded it: `$35k` was "unparsed", and an unparsed amount was pushed into `conflicts`, so one shorthand amount blocked the **whole** cohort; a value was discarded if it contained `no`/`unverified` anywhere; and the response said "Missing verification: <field>" for fields that were recorded.
- The `$5k–$20k` range appears in prospect risk prose and in the operator's question. Neither is an authorized criterion by design; only approved tenant Blueprint facts are. This correction keeps that rule and does not change the Blueprint criteria parser.

### Correction

`packages/max/workspace/ProspectDealClaims.js` (pure, deterministic) replaces the label-only parser behind the unchanged `recordedDetails()` export:

| State | Meaning | Effect |
| --- | --- | --- |
| `claim` | Labelled value, or a narrowly defined explicit statement in **Fit evidence** only. Keeps the exact recorded sentence (`snippet`) and matched text; `epistemic: recorded_unverified`. | Counts toward coverage. |
| `absent` | Not recorded, or recorded only as unknown/TBD/not verified. | Listed under "Not recorded". |
| `unparsed` | Recorded but not comparable: amount range, approximate amount, unsupported format (`$12.5k`, `$1.2M`), competing values (`$12k … and $18k`, first and second lien in one record), exit alternatives (`sale or refinance`). | **Record-local**: the field does not count for that record; the cohort can still rank. |
| `conflict` | Contradictory **labelled** claims (Fit vs Risks), unchanged rule. | Withholds the cohort ranking, as before. |

Narrative patterns are deliberately narrow: request verb + single amount (`requesting $35k`, `needs $15,000`), `$35k bridge loan request`, `funding request of $20k`; `first/second(-position) lien|mortgage|position` (ignoring existing/senior/underlying debt); `exit via/plan …`, `plans to refinance/sell`; `under contract`, `a purchased/acquired <property>`, `owned free and clear`, `property identified`, `rehab underway`; `closing scheduled/set for <date>`, `targeting close in <N weeks>`. A clause with a negation (`not`, `no`, `unknown`, …) or a hedge (`may`, `might`, `if`, …) is not a claim; "not yet verified" is a verification status, not a negation. Rehab budgets, purchase prices, ARVs, property values, deal sizes and down payments are never a financing request; projected/estimated/expected amounts and fund raises are not explicit requests; a lien the borrower already owes, holds, carries or assumes is not the offered security. A timeline after an exit/refinance in the same sentence is never a closing date. `Risks`, `Next action` and other sections are never mined. Dollar ranges found in notes are disclosed as "not authorized business criteria; not used for financing fit". This is not an underwriting engine.

Presentation: `Recorded claims (unverified)` lists each claim with field names, `Claim source text (exact, as recorded)` quotes the source sentences, then `Not recorded`, `Recorded but not comparable for this record`, `Contradictory labelled claims`, financing fit (contradictory vs non-comparable amount vs criteria comparison) and verification status. Structured evidence carries `recordedClaims.claims` (value, origin, snippet, match), `notRecorded`, `unparsed`, `contradictory` and `extractor`.

Unchanged: the coverage policy and ranking thresholds (≥2 comparable candidates, differentiated coverage, ties share position, no implicit cutoff), contradiction abstention, tenant isolation, the Blueprint criteria parser, outreach gates, and the no-write/no-send/no-assignment behaviour. `investigationPriority.extractor` is recorded in the continuity marker; a ranking produced by a different extractor is treated as stale on follow-up.

### Approved assertion change

`recordedDetails('Financing request: $12k').amount` now equals `12000` (was `null`). Real imports write amounts as integer-k shorthand; rejecting it left the request "unrecorded" and, because unparsed amounts were then treated as conflicts, blocked the whole cohort. The same test now also asserts that a genuinely unsupported format (`$12.5k`) stays out of the amount and is record-local `unparsed`, not a conflict. The second approved change (unparsed vs conflicting amounts) needed no assertion edit: the existing test's `$12,000` vs `$18,000` case is a genuine labelled contradiction and still blocks; the existing `/Conflicting or unparsed/` assertions still hold through a cohort summary line that now reports the two categories separately.

### Evidence

Same six synthetic narrative records (built through `normalizeContact`/`noteFor`), same approved synthetic Blueprint (`Financing range: $5,000–$20,000`), same production prompt, through `WorkspaceEngine.ask`:

- Before: [`max-investigation-narrative-six-record-response.before.txt`](examples/max-investigation-narrative-six-record-response.before.txt) — "Unranked investigation candidates"; Zane: "No explicit financing request recorded … Missing verification: financing request, deal stage, lien/security position, exit plan, closing date."
- After: [`max-investigation-narrative-six-record-response.txt`](examples/max-investigation-narrative-six-record-response.txt) — positions 1, 2, 3, 4, 5, 5 at coverage 5, 4, 2, 1, 0, 0; Zane's `$35k (= $35,000)` request is flagged outside `$5,000–$20,000` while remaining investigation position 1; Cam's `$8k–$12k` range and Eli's competing amounts/liens are record-local; outreach blocked for all six.
- The labelled #1115 example keeps identical positions and counts; only wording changed.

**Pre-merge blocker: real-data compatibility is unverified.** The six production records' notes and the approved LA Endeavors Blueprint facts were not read: database credentials are not available to this session (and must not be), the Railway integration cannot execute SQL, and the application's record endpoints require an authenticated operator session. The synthetic fixtures prove the extractor's behaviour, not compatibility with the real wording. Before merge, an operator should run a read-only, tenant-scoped read of the six `contact_now` records' `notes` and of the approved Blueprint service facts, and compare them against these patterns (per record: which of the five categories are detected, false positives, missed claims; and whether a `Financing range: $X–$Y` fact is approved).

### Rollback

- Narrative extraction only: `MAX_PROSPECT_NARRATIVE_CLAIMS=false` (also `0`, `off`, `no`, `disabled`) and restart. Labelled claims, `$Nk` amounts and record-local unparsed handling remain; narrative cohorts return to unranked. Extractor becomes `labelled-claims-v1`, so earlier rankings are stale.
- Whole analytical path: `MAX_PROSPECT_INVESTIGATION_PRIORITY=false` (unchanged).
- Code: revert this PR. No schema, data or configuration change was made.

Tests: `packages/max/workspace/tests/p0NarrativeDealEvidence.test.js` (fixture `tests/fixtures/narrativeInvestigationProspects.js`). 14 of its 15 original tests fail against the pre-correction composer; the remaining one is the budget/ARV guard, which the old parser also satisfied.

### Real-data audit (operator-run, read-only)

`scripts/investigationNarrativeAudit.js` is a standalone diagnostic for the pre-merge blocker above. It is not wired into any request path, cron or startup hook. It reuses `DATABASE_URL` (`db.js`), Max's prospect loader (`ProspectRecordIntelligence.loadRecords`) and Blueprint loader (`ClientIntelligenceContext.loadApprovedClientIntelligence`, with only `getApprovedClientBlueprint` exposed). It adds no credential, permission or endpoint.

- **Scope:** hard-coded to tenant 21. It audits only `review_only_import` records whose review state is `contact_now`, and expects exactly six. A row from another tenant returned by the database makes the run fail closed, with no records reported.
- **Read-only:** one connection, `BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY` (checked with `SHOW transaction_read_only`), every statement through `OperationalInspection.readOnlyQueryable` (a single SELECT/WITH statement; multi-statement text is rejected), and always `ROLLBACK`. No writes, imports, assignments, outreach or specialist actions.
- **Nested transactions (fix after the first production run):** the canonical Blueprint projection takes a connection via `connect()` and runs its own `BEGIN READ ONLY … COMMIT/ROLLBACK`. The first run's guard rejected that `BEGIN` (and the cleanup `ROLLBACK`), so the projection reported itself unavailable and the loader raised `CANONICAL_PROJECTION_FAILURE` before any canonical data was read. On a `connect()`-ed session the audit now maps `BEGIN` to `SAVEPOINT audit_nested_N`, `COMMIT`/`END` to `RELEASE SAVEPOINT`, and `ROLLBACK`/`ABORT` to `ROLLBACK TO SAVEPOINT` + `RELEASE`. It rejects `READ WRITE`, a second nested `BEGIN`, `COMMIT` with no nested transaction, explicit `SAVEPOINT`/`RELEASE`/`ROLLBACK TO`, `AND CHAIN`, `PREPARE TRANSACTION` and `SET TRANSACTION`. A `ROLLBACK` with no nested transaction is never sent. A session released with an open savepoint is rolled back to it. At the end the outer transaction must still report `transaction_read_only = on` (or be aborted, which cannot write); otherwise the report is withheld (exit 2). Max's own Blueprint loader is unchanged.
- **Explainable warnings:** each `possible_missed_claim` (up to three cue occurrences per category) carries a context window of at most six words either side of the cue, plus:
  - **confirmed** facts from running the real extractor: `no_pattern_matches_sentence` (the sentence alone yields nothing for the category), `sentence_matches_in_isolation` (it does, so other record content explains the abstention), and `labelled_field_governs`;
  - **hypotheses (unconfirmed)**, the script's own heuristics, e.g. no request wording before an amount, budget/ARV/debt context, approximate or range amounts, track-record wording, a generic strategy term without exit wording, no date after a closing word, past deals, exit-timeline context, or a negation/hedge in the clause.

  Hypotheses are not extractor behaviour and do not change it.
- **Fail-closed redaction of context:** the record's name tokens, emails, URLs, phone numbers, street addresses and digit runs of five or more are masked. Any capitalized word that is not a known deal acronym, month/weekday or common deal word is masked as `[word]`. The result is re-checked: residual `@`/URL/phone markers or name tokens withhold the context (`redaction_unverified`), and so do windows that are more than half placeholders (`mostly_redacted`). Lowercase personal names other than the record's own cannot be recognized; the capitalization rule and the final check are the safeguards.
- **Output:** 8-character record-ID prefixes; a coverage matrix showing `R` recorded-unverified, `-` absent, `N` non-comparable and `C` contradictory; the minimal matched phrase per category, with names, emails, phones and URLs redacted and phrases capped at 80 characters; and review findings (a possible missed claim is a cue word with no detected claim; a possible false positive is a guard word inside a claim's sentence). For the Blueprint it reports whether it is approved and trusted, the range the current parser recognizes, and only the facts that mention financing, loans or dollar amounts. Never names, contact details or full notes. `--json` gives the same redacted report.
- **Result:** `PASS` (exit 0) requires the six-record cohort, no review findings, no contradictions and a ranked cohort. `FAIL` (exit 1) lists the reasons. `ACCESS UNAVAILABLE` / `AUDIT FAILED` (exit 2) means nothing was read or the read could not complete.

Run from a checkout of the PR branch, with the Railway CLI logged in as an authorized operator and linked to project `charming-trust`:

```sh
railway run --service pulseforge-leadgen --environment production \
  node scripts/investigationNarrativeAudit.js
```

Tests: `p0NarrativeAuditScript.test.js` (tenant scoping, foreign-row fail-closed, Blueprint binding, read-only transaction and write rejection, denied access without secrets, redaction, matrix/PASS/FAIL, real Blueprint normalization path), `p0NarrativeAuditTransactions.test.js` (savepoint success, nested rollback, error recovery, write/escape prevention, the real canonical projection inside a savepoint, end-state verification), `p0NarrativeAuditExplanations.test.js` (context windows, confirmed vs hypothesis separation, fail-closed redaction), `p0NarrativeAuditDiagnostics.test.js` (the "in rehab funds" false positive found while probing; fixed in v2).

### Extractor correction v2 (`fit-narrative-claims-v2`, after the production audit at `c1e93601`)

The read-only production audit (tenant 21, six `contact_now` records) read the canonical Blueprint and confirmed real extraction gaps. An operator approved a narrowly scoped correction. Only these changes were made, in `ProspectDealClaims.js`:

| # | Change | Guard |
| --- | --- | --- |
| 1 | Financing request: `$35,000 remaining rehab request` (amount followed directly by an optional qualifier — remaining/outstanding/additional/current, rehab/gap/bridge/…, loan/financing/funding/capital — and the word `request`), and `current public request for $35,000–$45,000 …` (`current/public/active/open/pending/stated/explicit/outstanding … request of/for/is <amount>`). | A range stays a range: it is record-local `unparsed` (`reason: range`, value kept verbatim) and is never turned into one amount. `budget request`, `request for $250k properties/deals`, hedged and negated forms abstain. |
| 2 | Deal stage: `executed purchase contract` was already recognised; a negation **after** a stage phrase no longer cancels it (`Executed purchase contract with no financing contingency`), while a negation before or inside it still does (`No executed purchase contract`, `Not yet under contract`). `already purchased/acquired/bought/closed on` (optionally `property/SFR/duplex … was/is already purchased`) is `property acquired`. | Track record abstains: a count or plural property noun after the phrase (`already purchased 14 homes`), `and sold/flipped`, or clause words such as `portfolio`, `track record`, `since 2019`, `previously`. Two different current-property stages are record-local `multiple_values`. |
| 3 | Exit: `sale exit`, `retail sale exit`, `refinance exit`, `DSCR refinance exit`. | Singular only (`sale exits` is history); `projected/possible/potential/prior/past/completed … exit` abstains; `BRRRR exit` and `hold exit` are not accepted in this form. |
| 4 | False positive: `$35K in rehab funds needed` no longer yields stage `in rehab`. `in rehab` is now a stage only as a property state (`is/are/currently/now/already/still in rehab`, not followed by funds/financing/budget/costs/draws/…); `mid-rehab` and `rehab underway` are unchanged. | The phrase is **not** added as a financing-request form; it stays absent. |
| 6 | Refinance-target wording is evaluated separately: `sub-90-day refinance target`, `refinance target under 90 days`, `targeting a refinance in under 90 days` are **not** exits (they state a timeline target, not the intended exit) and never closing dates. Explicit forms (`exit via refinance`, `plans to refinance`, `refinance exit`) remain exits. A `;`-joined clause now carries the previous clause's exit context, so `Exit plan is a refinance; target close in 90 days` is not a closing date. | — |

Unchanged (5, 7): generic BRRRR descriptions, rehab budgets, ARVs, projected resale values, past transactions and undated future closings still abstain. Financing criteria still come only from the approved canonical Blueprint; with no recognised range, financing fit is reported as undetermined. Ranking thresholds, tenant isolation, outreach authorization, Blueprint parsing and rollback behaviour are unchanged. The extractor version is now `fit-narrative-claims-v2`, so a v1 ranking is stale on follow-up.

Tests: `packages/max/workspace/tests/p0NarrativeClaimsV2.test.js` — a six-record production-shaped cohort built through `normalizeContact`/`noteFor` from the redacted audit fragments (sentences around the fragments and all names are synthetic), checked field-by-field at record level, plus adversarial near-matches, the no-range financing-fit path and the unchanged synthetic cohort positions.

### Range policy refinement v3 (`fit-narrative-claims-v3`)

Approved policy: an explicit, current, deal-specific financing range is **recorded-unverified financing-request evidence** and counts toward investigation coverage.

- **Kept as a range.** The claim keeps the verbatim value and source sentence plus `form: 'range'` and `range: { min, max }`; `amount` stays `null`. It is never turned into one amount. Bounds must be readable unambiguously (`$35–45k` stays `unparsed: unsupported_format`), and an approximate range (`about $20k–$30k`) stays `unparsed: approximate`. A labelled `Financing request: $10,000–$15,000` is handled the same way.
- **Supersession is narrow.** A single exact request supersedes a range only when (a) it is in the same statement (same sentence or `;`-joined clause), (b) the statement carries an explicit update cue (`narrowed to`, `revised to`, `now`, `specifically`, `settled on`, …), and (c) it names no different financing need (gap vs rehab, etc.). The claim then records `supersedes: { value, range, snippet }`, so the range stays visible. An update alone (`Revised to $40k.`) is never a request, and updates of other things (`purchase price reduced to $150k`, `rehab budget lowered to $30k`) are ignored.
- **Ambiguity retains evidence.** An update in a different sentence, a different financing need, a second request with no update cue, or more than one update: the amount becomes record-local `unparsed: multiple_values` with every value listed. Contradictory **labelled** claims still block the cohort, as before.
- **Not current requests:** general preferences (`typically/usually/generally`, `prefers`, `per deal`, `on average`), history (`past`, `previously`, `last year`, `in 2024`), other or future deals (`next deal`, `future projects`, `for deals …`), and, as before, budgets, purchase prices, ARVs, property values and projected amounts. These apply to exact amounts and ranges alike.
- **Separate from criteria.** A prospect-stated range is never compared with or used as business criteria. Financing fit for a range record reads "a range, not a single amount … financing fit cannot be assessed for this record. The prospect-stated range is not compared with or used as business criteria." Exact requests are still compared against an approved Blueprint range. `tenantCriteria` and outreach gates are unchanged.

Effect on the synthetic narrative cohort: Cam's `$8k–$12k` now counts (coverage 2 → 3); positions are unchanged (1, 2, 3, 4, 5, 5). The example response was regenerated for Cam's block and the cohort summary line.

Tests: `packages/max/workspace/tests/p0NarrativeRangePolicy.test.js` (exact requests, explicit ranges, superseded ranges, ambiguous precedence, conflicting requests, unrelated/future/past deals, general preferences, separation from Blueprint criteria and outreach).

### Range policy correctness fixes v4 (`fit-narrative-claims-v4`)

The policy is unchanged and confirmed by the operator. Probing v3 against each policy rule found four places where it did not hold, now fixed:

- **History cue in an earlier clause.** In "On a previous deal, requested $50k–$60k" the history cue sits in a clause before the amount, and v3 checked only the amount's own clause. Now cues anywhere earlier in the same sentence apply, unless the amount's own clause is explicitly current ("…, now requesting $40k"). "previous" was added to the history cues.
- **Other deal or property.** "Requested $45k on a different property" counted as this deal's request. A different/another/other/separate/unrelated property, deal, project, flip, house, home or purchase in the amount's clause now excludes it. ("Another loan" is not treated as another deal.)
- **Different financing need superseding a range.** The need was read from the next four tokens after the amount, but a leading empty token cut "for the down payment" to "for the down", and a missing need counted as compatible. The need window now takes the next six words, so "…for rehab; narrowed to $10k for the down payment" is record-local `multiple_values`.
- **"Current request is for $X to $Y"** was not recognized; the adjective + `request` form now accepts `request is (for)`.

A superseding claim's `snippets` now include its own sentence as well as the superseded range's, so the answer shows both source texts.

Tests: `packages/max/workspace/tests/p0NarrativeRangePolicyRegressions.test.js` covers exact requests, explicit ranges, superseded ranges, ambiguous precedence, conflicting requests, unrelated deals, general preferences, and an end-to-end check that a prospect range adds coverage, is never compared with Blueprint criteria, and leaves outreach blocked. Four of its eight tests fail against the v3 extractor. The synthetic example response is unchanged.

### Executed-contract deal stage v5 (`fit-narrative-claims-v5`, after the v4 production audit)

**Root cause.** The v4 audit found that Jon's record recognised `sale exit` but not `Current waterfront flip has an executed $260,000 purchase contract`. The deal-stage pattern required `signed|executed` to be directly followed by `purchase contract|agreement`. A contract price between the two words broke the match. No other pattern applied: `under contract` is absent, and the amount patterns correctly ignore a price that is not followed by request wording.

**Change (stage only).**
- The executed/signed form now accepts one optional price between the status word and the noun (`executed $260,000 purchase contract`, `signed $1.2M purchase agreement`, `executed $260k purchase contract`). The price is never a financing request.
- Abstention rules now apply to this pattern family, both with and without a price. They reuse the amount extractor's current/history scoping: an explicitly current clause (`Current waterfront flip has …`) is read on its own; otherwise, earlier clauses in the sentence count too.
  - **Historical:** `previously`, `prior`, `last year`, `in 2023`, `had`, plural `contracts`, track-record clauses, or a contract that `fell through`, `terminated`, `cancelled`, `expired` or `lapsed`.
  - **Not executed:** `unsigned`, `proposed`, `draft`, `awaiting`, `expects`, `needs`, `will`, `to be signed`, or hedged (`may`).
  - **Negated:** `no executed …`, `does not have an executed …`. Negation before or inside the phrase cancels it; negation after it does not (unchanged from v2).
  - **Another deal:** `on a different property`, `on another flip`, `on the next deal`.
- `under contract` and the other stage patterns are unchanged.

**Audit warning attribution.** A financing-request claim was flagged `possible_false_positive` whenever its sentence contained a value cue (ARV, budget, price, value, projected, …). That flagged Zackary's correctly extracted `$35,000` request because an ARV for a different amount was in the same sentence. The warning is now downgraded to an informational `NOTE … value cue describes a different amount` only when **every** cue occurrence meets both conditions:
- it is within two words of a **different** dollar amount, with no clause break or other amount in between (`$250k ARV`, `ARV of $250k`);
- it is **not** within three words of the claimed amount.

The warning stays if any cue touches the claimed amount (`requesting $35,000 for the budget`), has no amount of its own (`requesting $35k, projected`), is too far from the other amount to attribute, or if the claimed amount cannot be located in its sentence. Notes do not count toward FAIL. Missed-claim detection is unchanged.

Tests: `packages/max/workspace/tests/p0NarrativeExecutedContract.test.js`. It covers the verbatim audit phrase and adversarial near-matches (historical, unsigned/proposed, negated, unrelated). It also runs full-record extraction through the real `importReviewOnlyContacts` INSERT and `ProspectRecordIntelligence.loadRecords` read, using an in-memory pool. Jon's record then has stage and exit recorded, no financing request, and no stage review finding; each near-match record keeps stage absent and still raises the missed-claim review. The attribution cases cover both directions.

Unchanged: financing range policy, ranking thresholds, Blueprint criteria, tenant isolation, outreach authorization and rollback flags.

### P0 acceptance verification at `491f7b70` (v5)

**Production audit (operator-run, read-only, tenant 21), as reported by the operator:**
- Six of six review-only `contact_now` records were retrieved.
- Coverage: Zackary 3/5, Raul 2/5, Jon 2/5 (including the executed $260,000 purchase contract), Reginald 1/5, John 0/5, David 0/5.
- No purchase price, rehab budget or ARV was promoted to a financing request.
- The approved canonical Blueprint is available, but it has no recognized numeric financing range.
- The read-only transaction completed with no writes or outreach.
- The v4 false-positive warning on Zackary's `$35,000` request no longer appears.
- Four records are reported with `possible_missed_claim`. By the script's definition the audit is therefore `FAIL`. The PASS rule is not changed, and no warning is suppressed. Each finding needs an individual disposition, recorded below once the redacted REVIEW lines are reviewed.

**Disposition rules for missed-claim findings.**

An *acceptable abstention* is a cue that the extractor deliberately does not promote:
- amounts that are budgets, purchase prices, contract prices, ARVs or values;
- generic strategy terms with no exit wording (`BRRRR investor`);
- closing words that refer to past deals (`closed on 12 deals`);
- exit or refinance timelines;
- negated, hedged or historical statements.

A *defect* is an explicit, current, deal-specific statement in Fit evidence, stated in a form the policy says should count, that the extractor misses (as Jon's contract was in v4).

**Isolated end-to-end Max verification:** `test/maxInvestigationAcceptancePostgres.test.js`. The test:
- Starts a disposable PostgreSQL 16 database. No production access is used.
- Imports six tenant-21 records through the real `importReviewOnlyContacts`, plus one foreign tenant-22 record.
- Records reproduce the audited coverage pattern 3/2/2/1/0/0 with synthetic text. Jon's sentence is the audit phrase verbatim.
- Uses an approved Blueprint with no numeric range.
- Runs `WorkspaceEngine.ask` with the original production prompt, the short investigation prompt and the "why first" follow-up.
- Uses the real default prospect loader, both the aggregate SQL and the `loadRecords` keyset SQL.
- Allows network access only to the disposable database. HTTP(S), fetch, TLS and DNS are denied.

It asserts:
- the exact six IDs, with the foreign record absent;
- order by coverage, positions 1, 2, 2, 4, 5, 5;
- every claim is `recorded_unverified`, and exact source sentences appear in the prose;
- no within/outside-range conclusion: five records say "comparison unavailable", and the range record says the fit "cannot be assessed";
- six "Outreach eligibility: blocked" lines;
- `executed`, `specialistDelegated` and `delegated` are false;
- every SQL statement Max sent was a SELECT;
- the full `prospects` and `companies` tables are byte-identical before and after.

The same synthetic cohort, run through the audit classifier, produces four records with `possible_missed_claim`, all acceptable abstentions: Raul's rehab budget, Jon's contract price, and generic BRRRR wording for Reginald and John. This shows the pattern but does not stand in for reviewing the real findings.

**Limitation.** Production runs `main`, which does not contain this PR, so the production Max response path cannot exercise v5 until deployment is approved. The remaining acceptance test is steps 1–8 of the "Production acceptance procedure" above, run in an authorized tenant-21 session after deployment.

**Follow-ups (separately scoped, not in this PR):**
1. Zero-evidence ranking presentation: records with 0/5 claims receive a shared last position (`Investigation position 5`), which implies an ordering with no evidence behind it. They should be listed as "not ranked: no recorded deal claims".
2. Exit-plan display casing: the raw value is shown (`exit plan: Sale`) rather than the normalized value (`sale`).
3. The audit hypothesis list does not name contract prices (`$260,000 purchase contract`) as a non-request amount context. The confirmed fact (`no_pattern_matches_sentence`) is still reported.
