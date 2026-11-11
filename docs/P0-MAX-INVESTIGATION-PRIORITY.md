# Max: investigation priority separated from outreach eligibility (P0)

Follow-up to PR 2 (#1114, `ProspectRecordIntelligence`). Read-only. No migration, no record writes, no change to any outreach gate.

## Production failure

In LA Endeavors (tenant 21), Max retrieved six named `contact_now` prospects with record-level evidence, then refused to rank them: "Unranked candidates … 0 records meet the conservative recorded outreach criteria."

## Root cause

`composeRecords` computed a ranking only when `eligible.length > 0 && eligible.every(c => c.ready)`, where *eligible* is **outreach** eligibility (`evaluate()`: `contact_now`, DNC cleared, outreach-review hold cleared, score ≥ 70 and service-area match, non-terminal state). Analytical prioritization was coupled to outreach authorization.

The review-only import (`services/reviewOnlyProspectImportService.js`) writes every record with `ao_outreach_review_required = true`, `status = 'cold'`, `icp_score = 80`, and `service_area_match` = market or NULL. So every LA Endeavors record is outreach-ineligible by design, and the ranker could never produce anything for that tenant. Even when records were eligible, the only ranking signal was engagement tier (hot/warm/cold). All imported records are cold, so they would have tied anyway.

## Data inspection (requirement 10)

The structured columns do **not** carry deal specificity, financing amount, security position, exit, track record or timing. That evidence exists only in `prospects.notes`, which the import writes as labelled lines (`Fit evidence:`, `Risks:`, `Market:`, `Next action:`, `Sources:`). This change therefore uses **bounded, pattern-based extraction from recorded notes**, with no model call:

- ≤ 4,000 characters and ≤ 80 sentences per record. Workflow boilerplate, `Review state:`, `Next action:` (a plan, not evidence), `Sources:` and `Public profile:` lines are ignored.
- Each dimension is ordinal: `+1` supported, `0` not recorded / conflicting / not assessable, `−1` contrary. Each value cites the recorded sentence it came from.
- A negated sentence ("No exit plan", "Track record unverified", "Collateral ownership unclear") counts only as contrary evidence. A positive pattern that appears only in a `Risks:` line is reported as "flagged as a risk", not as support.
- Dollar amounts count as a capital need only in a financing context (loan, capital, funding, need, seeking, gap, bridge, …). A purchase price on its own is ignored.
- Verification gaps can only subtract. If no gap is flagged, that does not mean the record is verified.

## Policy: `evidence-dominance-v1`

Dimensions: deal specificity, financing amount vs tenant criteria, security position / collateral clarity, exit clarity, operator track record, verification gaps, timing.

- **Ordering by dominance only.** A record is placed above another only when it is at least as strong on every dimension and stronger on at least one. Positions are successive non-dominated layers. No weights are used, so no weighting is invented.
- **No forced order.** If everything lands in one layer, no order is given. The response says whether the candidates are indistinguishable or stronger on different dimensions.
- **Tenant criteria are read, not hardcoded.** A financing-amount range is compared only when the tenant's approved business understanding contract (`services`, `targetCustomers`, `constraints`, `valueProposition`, `businessGoals`, `currentPriorities`) explicitly records one ("$100k to $1.5M", "up to $750k", "minimum $50k"). Without a recorded range, amounts are shown as "not assessable". The module contains no tenant names, IDs or preferences, and a test enforces this.
- **Security position is judged on clarity, not seniority.** A first or second position is reported as written. The policy does not assume a tenant prefers senior debt.
- **Candidates.** Rejected, held and terminal-state records are excluded and the count is disclosed. Outreach gates (DNC, outreach-review hold, score) do **not** exclude a record from read-only analysis. `qualify_first` records are investigation candidates.
- **Full-set reads only.** If the read is partial, a continuation, or contains conflicting duplicate IDs, the investigation order is withheld. This keeps the PR 2 behaviour.
- **Output size.** "Rank them" over an explicitly filtered set (for example `contact_now`) shows the whole set. An unfiltered request with no stated count keeps the default top five. Ties at the cutoff are included.

## Response contract (requirement 6/7)

The response separates three things.

**Response level:**
- Investigation priority: provisional, calculated, read-only.
- Outreach authorization: N of M eligible. "Outreach remains blocked for every candidate" when N = 0.
- Method.
- Tenant criteria, or a statement that none are recorded.
- Exclusions.

**Per candidate:**
- Investigation position.
- Relative position: above or below named records, with the dimensions that differ. Shared positions are labelled "indistinguishable" or "stronger on different dimensions".
- Financing suitability: summarised from amount vs criteria, security and exit.
- Outreach authorization: `blocked — <gates>`, or "requires separate operator authorization".
- Cited evidence for each dimension.
- Uncertainties.
- One next **investigative** step. Contrary evidence is addressed before missing evidence. The step always says it is investigation only and that contacting the prospect needs separate authorization.

Structured evidence carries `investigationPriority {position, epistemic:'calculated', policy, provisional:true, dimensions}`, `financingSuitability` and `outreachAuthorization`. The existing outreach `priority` (engagement tier) is still produced only for eligible, ready records on a full-set read, now tagged `scope:'outreach_order_eligible_only'`. Outreach eligibility never raises investigation priority.

## Safety

- Pure function over the rows PR 2 already loads: the same tenant-scoped SELECT and the same second tenant filter. No new queries, writes, models, assignments, campaigns or specialist calls.
- `evaluate()` eligibility logic is unchanged. It now also returns `blockers` for display.
- Session continuity stores `reviewState` and `investigationRankedRecordIds`. A "why is the top one ranked first?" follow-up re-reads the data with the same filter.

## Changed files

- `packages/max/workspace/ProspectInvestigationPriority.js`: new pure module.
- `ProspectRecordIntelligence.js`: `blockers`; ranking requests go through `composeInvestigation`; flag `MAX_PROSPECT_INVESTIGATION_PRIORITY`.
- `OperatingEvidenceRetrieval.js`: passes the business understanding to record composition; passes the new evidence fields through.
- `WorkspaceTypes.js`: preserves the new evidence fields.
- `OperationalReadIntent.js`: `topCountExplicit`; follow-ups keep `reviewState`; continuity records investigation IDs.
- Tests: new `tests/p0ProspectInvestigationPriority.test.js` (13). PR 2 tests updated for the intended change: investigation candidates now include `qualify_first`, and the outreach `priority` stays on the six eligible records only.

## Known limitations

- Pattern extraction is lexical. Unusual phrasing is reported as "not recorded" and never guessed. One recorded phrase can affect two dimensions (for example "collateral ownership unclear" is both a security concern and a verification gap). This is disclosed in the cited evidence.
- Dominance is deliberately conservative. With sparse notes, most records share a position. Finer ordering would need tenant-recorded weights, which do not exist yet.
- Recorded notes are not independently verified facts, and the response says so.

## Rollback

Set `MAX_PROSPECT_INVESTIGATION_PRIORITY=false` through the approved configuration process and restart. Ranking requests then return exactly the PR 2 outreach-coupled output (covered by a test). `MAX_PROSPECT_RECORD_INTELLIGENCE=false` still disables record retrieval entirely. For a full rollback, revert the commit through a reviewed PR. No schema or data rollback is needed.

## Production acceptance test (after approved deploy only)

1. In a fresh, authorized LA Endeavors session, repeat the November ranking request (for example *"Which of my contact_now prospects are the highest priority? Rank them using actual workspace data."*).
2. Expect:
   - `Provisional investigation priority`
   - `Outreach authorization: 0 of 6 …`
   - `Outreach remains blocked for every candidate`
   - No `Unranked candidates: evidence is insufficient for a defensible outreach ordering`
3. Expect all six named records with stable `prospect:21:<uuid>` references. Each must show investigation priority, financing suitability, `outreach authorization: blocked — …`, cited note text, uncertainties and an investigative next step.
4. Check every cited quote against `SELECT id, notes FROM prospects WHERE client_id = 21 AND acquisition_metadata->>'reviewState' = 'contact_now' ORDER BY id` (approved read-only interface). No dimension may be "supported" without a matching recorded sentence.
5. If positions are given, check that each "above" claim names dimensions where the higher record really is stronger and none where it is weaker. If the evidence does not differentiate, the response must say so and list the records unordered.
6. Ask *"Why is the top one ranked first?"*. Expect a fresh read scoped to `contact_now`.
7. Confirm in logs and the DB that nothing changed: no `touchpoints`, `agent_actions`, assignments, campaign or `prospects` row changes, and no specialist dispatch.
8. Repeat one PR 2 list request and one aggregate-count request. Both must behave as before.
