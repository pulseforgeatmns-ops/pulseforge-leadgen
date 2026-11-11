'use strict';
// fit-narrative-claims-v2: narrowly scoped extractor correction approved after
// the read-only production audit of PR #1120 (tenant 21, six contact_now
// records). The phrases below are the redacted fragments reported by that
// audit; names, sentences around the fragments and record IDs are synthetic.
// Records are built through the real normalizeContact/noteFor import path and
// checked at record level (all five fields), not as isolated regex matches.
require('./workspaceTestHarness');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { composeRecords } = require('../ProspectRecordIntelligence');
const { extractDealClaims, EXTRACTOR } = require('../ProspectDealClaims');
const { recordedDetails, tenantCriteria } = require('../ProspectInvestigationPriority');
const { narrativeFixture } = require('./fixtures/narrativeInvestigationProspects');
const { bundle } = require('./fixtures/investigationProspects');

const fit = s => extractDealClaims(`Fit evidence: ${s}`);
// Full record-level shape: every field is claim, unparsed(reason) or absent.
function shape(d) {
  const out = {};
  for (const f of ['amount', 'stage', 'security', 'exit', 'closing']) {
    const u = d.unparsed.find(x => x.field === f);
    out[f] = d.claims[f] ? (d.claims[f].normalized || d.claims[f].value) : u ? `unparsed:${u.reason}` : d.conflicts.includes(f) ? 'conflict' : '-';
  }
  return out;
}
const rankRead = { wantsRanking: true, reviewState: 'contact_now', priorityMode: 'investigation' };

const PRODUCTION_SHAPED = [
  { name: 'Ada Example', fit: 'Executed purchase contract on the subject SFR. $35,000 remaining rehab request. Sale exit after rehab.',
    risks: 'Contract, title and rehab scope unverified.' },
  { name: 'Ben Example', fit: 'Current public request for $35,000–$45,000 gap positions. Property already purchased.',
    risks: 'Request range exceeds the working range.' },
  { name: 'Zackary Example', fit: 'BRRRR investor with a sub-90-day refinance target on the current property.',
    risks: 'No financing request recorded.' },
  { name: 'Dee Example', fit: '$35K in rehab funds needed. Rehab budget of $60k; ARV $260k; projected resale value $310k.',
    risks: 'Request wording informal.' },
  { name: 'Eve Example', fit: 'Long-term BRRRR investor; has already purchased 14 homes and completed 9 sale exits since 2019. Closing on the next deal soon.',
    risks: 'No active deal recorded.' },
  { name: 'Finn Example', fit: 'Already purchased the subject duplex; exit plan is a DSCR refinance within 90 days. Proposed first lien.',
    risks: 'Lien position unverified.' },
];

test('extractor version is bumped so earlier rankings are treated as stale', () => {
  assert.equal(EXTRACTOR, 'fit-narrative-claims-v4'); // v4: range-policy correctness fixes (history scope, other deals, need window, "request is for")
});

test('record-level extraction over the production-shaped cohort', () => {
  const rows = narrativeFixture(21, PRODUCTION_SHAPED);
  const shapes = rows.map(r => shape(recordedDetails(r.notes)));
  assert.deepEqual(shapes, [
    { amount: '35000', stage: 'under contract', security: '-', exit: 'sale', closing: '-' },
    // v3 policy: the explicit current range is recorded-unverified evidence.
    { amount: 'range:35000-45000', stage: 'property acquired', security: '-', exit: '-', closing: '-' },
    { amount: '-', stage: '-', security: '-', exit: '-', closing: '-' },
    { amount: '-', stage: '-', security: '-', exit: '-', closing: '-' },
    { amount: '-', stage: '-', security: '-', exit: '-', closing: '-' },
    { amount: '-', stage: 'property acquired', security: 'first lien', exit: 'DSCR refinance', closing: '-' },
  ]);
  // Exact source text is kept for every claim.
  const ada = recordedDetails(rows[0].notes);
  assert.equal(ada.claims.amount.snippet, '$35,000 remaining rehab request.');
  assert.equal(ada.amount, 35000);
  assert.equal(ada.claims.stage.snippet, 'Executed purchase contract on the subject SFR.');
  assert.equal(ada.claims.exit.snippet, 'Sale exit after rehab.');
});

// 1. Financing requests: single amount vs range ------------------------------
test('"remaining rehab request" is a single-amount request; a public range request stays a range', () => {
  const single = fit('$35,000 remaining rehab request.');
  assert.equal(single.amount, 35000); assert.deepEqual(single.unparsed, []);
  const range = fit('Current public request for $35,000–$45,000 gap positions.');
  assert.equal(range.amount, null); assert.deepEqual(range.unparsed, []);
  assert.equal(range.claims.amount.value, '$35,000–$45,000'); assert.equal(range.claims.amount.form, 'range');
  assert.deepEqual(range.claims.amount.range, { min: 35000, max: 45000 });
  assert.equal(range.claims.amount.snippet, 'Current public request for $35,000–$45,000 gap positions.');
  for (const [text, value] of [['Current public request for $35,000 to $45,000.', '$35,000 to $45,000'],
    ['Open request for $35k-$45k gap funding.', '$35k-$45k'], ['$35,000–$45,000 gap request.', '$35,000–$45,000']]) {
    const d = fit(text);
    assert.equal(d.amount, null, text);
    assert.equal(d.claims.amount?.form, 'range', text); assert.equal(d.claims.amount.value, value, text);
  }
  assert.equal(fit('Current request for $35k.').amount, 35000);
  assert.equal(fit('$20k gap request.').amount, 20000);
});

test('adversarial near-matches are not financing requests', () => {
  for (const text of ['$60k rehab budget request.', '$35,000 remaining rehab budget.', '$35,000 remaining on the rehab.',
    'Public request for $250k properties.', 'Current request for $200k deals.', 'Remaining rehab of $35,000.',
    '$310k projected resale value.', 'Request for ARV comps on a $260k house.', 'Has $35,000 remaining in reserves.',
    '$35K in rehab funds needed.', 'No current request for $35k.', 'May request $35,000 remaining rehab request later.']) {
    const d = fit(text);
    assert.equal(d.values.amount, undefined, text);
    assert.equal(d.unparsed.some(u => u.field === 'amount'), false, text);
  }
});

// 2. Deal stage --------------------------------------------------------------
test('"executed purchase contract" and "already purchased" are current-property deal stages', () => {
  for (const text of ['Executed purchase contract on the subject SFR.', 'Has an executed purchase contract.',
    'Executed purchase contract with no financing contingency.', 'Executed purchase contract; closing date not yet set.']) {
    assert.equal(fit(text).claims.stage?.normalized, 'under contract', text);
  }
  for (const text of ['Property already purchased.', 'The duplex was already purchased with cash.',
    'Already purchased the subject property in March.', 'Already purchased; needs rehab capital.']) {
    assert.equal(fit(text).claims.stage?.normalized, 'property acquired', text);
  }
});

test('track record, negated and hedged stage near-matches abstain', () => {
  for (const text of ['Has already purchased 14 homes in the county.', 'Has already purchased and sold several flips.',
    'Already purchased over 30 properties since 2015.', 'Already purchased several rentals.', 'Already purchased two duplexes this year.',
    'Track record: already purchased and renovated homes.', 'No executed purchase contract yet.', 'Not yet under contract.',
    'May have an executed purchase contract.', 'Has not already purchased the property.', 'Purchase contract not executed.']) {
    assert.equal(fit(text).values.stage, undefined, text);
    assert.equal(fit(text).unparsed.some(u => u.field === 'stage'), false, text);
  }
  // Two different current-property stages stay record-local, never a conflict.
  const both = fit('Executed purchase contract on a duplex. Property already purchased.');
  assert.deepEqual(both.unparsed.map(u => [u.field, u.reason]), [['stage', 'multiple_values']]);
  assert.deepEqual(both.conflicts, []);
});

// 3. Sale exit ---------------------------------------------------------------
test('an explicit "sale exit" is an exit plan; plural, projected, past and negated forms are not', () => {
  for (const [text, exit] of [['Sale exit after rehab.', 'sale'], ['Planned sale exit.', 'sale'],
    ['Retail sale exit on completion.', 'sale'], ['DSCR refinance exit after rehab.', 'DSCR refinance']]) {
    assert.equal(fit(text).claims.exit?.normalized, exit, text);
  }
  for (const text of ['Completed 9 sale exits since 2019.', 'Projected resale exit at $320k.', 'Prior sale exit on another flip.',
    'No sale exit identified.', 'Possible sale exit.', 'BRRRR exit.', 'Long-term BRRRR investor.', 'Hold exit.']) {
    assert.equal(fit(text).values.exit, undefined, text);
    assert.equal(fit(text).unparsed.some(u => u.field === 'exit'), false, text);
  }
  assert.deepEqual(fit('Sale exit or refinance.').unparsed.map(u => [u.field, u.reason]), [['exit', 'alternatives']]);
});

// 4. Known false positive ----------------------------------------------------
test('"$35K in rehab funds needed" no longer produces deal stage "in rehab"', () => {
  const d = fit('$35K in rehab funds needed.');
  assert.deepEqual(shape(d), { amount: '-', stage: '-', security: '-', exit: '-', closing: '-' });
  for (const text of ['Needs $20k in rehab financing.', 'Short on in-rehab draws.', 'Asked about in rehab costs.']) {
    assert.equal(fit(text).values.stage, undefined, text);
  }
  for (const text of ['The house is currently in rehab.', 'Property is in rehab.', 'Duplex is mid-rehab.', 'Rehab underway.']) {
    assert.equal(fit(text).claims.stage?.normalized, 'rehab underway', text);
  }
});

// 5/6. Conservative abstention and the refinance-target wording --------------
test('generic BRRRR descriptions, budgets, ARVs, projected resale values, past deals and undated closings abstain', () => {
  for (const text of ['Long-term BRRRR investor.', 'BRRRR investor focused on rentals.', 'Rehab budget of $60k.', 'ARV $260k.',
    'Projected resale value $310k.', 'Sold 3 flips last year.', 'Closed on 3 flips in 2025.', 'Closing on the next deal soon.',
    'Closing expected after appraisal.', 'Will close once title clears.']) {
    assert.deepEqual(shape(fit(text)), { amount: '-', stage: '-', security: '-', exit: '-', closing: '-' }, text);
  }
});

test('refinance-target wording: an exit only when refinance is explicitly the intended exit; the target is never a closing date', () => {
  for (const text of ['Sub-90-day refinance target.', 'Refinance target under 90 days.', 'BRRRR investor with a sub-90-day refinance target.',
    'Targeting a refinance in under 90 days.', 'Targeting refinance close in under 90 days.', 'Refinance target of 60 days after rehab.']) {
    const d = fit(text);
    assert.equal(d.values.exit, undefined, text); assert.equal(d.values.closing, undefined, text);
    assert.deepEqual(d.unparsed, [], text);
  }
  for (const text of ['Exit via refinance, with a sub-90-day refinance target.', 'Plans to refinance within 90 days.',
    'Exit plan is a refinance; target close in 90 days.', 'Refinance exit targeted within 90 days.']) {
    const d = fit(text);
    assert.equal(d.claims.exit?.normalized, 'refinance', text);
    assert.equal(d.values.closing, undefined, text);
  }
});

// 7. Financing criteria come only from the approved Blueprint ---------------
test('without an approved financing range, financing fit stays undetermined; note ranges are disclosed, never used', () => {
  const rows = narrativeFixture(21, PRODUCTION_SHAPED);
  const understanding = { _bundle: { tenantId: '21', knowledgeState: 'available',
    summary: { clientId: 21, approved: true, semanticAuthority: 'CANONICAL', canonicalSnapshotId: 'synthetic-approved-snapshot' },
    contract: { services: 'Private lending for residential rehab projects.', constraints: 'Verify title and exit evidence.' } } };
  assert.equal(tenantCriteria(understanding, bundle(rows)).range, null);
  for (const u of [undefined, understanding]) {
    const r = composeRecords(bundle(rows), rankRead, { businessUnderstanding: u });
    assert.doesNotMatch(r.prose, /(?:within|outside) the recorded business range/);
    const ada = r.items.find(i => i.recordId === rows[0].id).claim;
    assert.match(ada, /Financing fit: Financing-fit comparison unavailable: no unambiguous authorized financing range/);
    const ben = r.items.find(i => i.recordId === rows[1].id).claim;
    assert.match(ben, /A financing request is recorded but is a range, not a single amount \("\$35,000–\$45,000"\); financing fit cannot be assessed/);
    assert.match(r.prose, /Dollar ranges appear in recorded prospect notes \("\$35,000–\$45,000"\)/);
    assert.match(r.prose, /Outreach eligibility: blocked/);
  }
});

test('ranking over the production-shaped cohort uses the unchanged coverage policy', () => {
  const rows = narrativeFixture(21, PRODUCTION_SHAPED);
  const r = composeRecords(bundle(rows), rankRead, {});
  assert.equal(r.investigationPriority.ranked, true);
  assert.equal(r.investigationPriority.extractor, EXTRACTOR);
  const byId = Object.fromEntries(r.items.map(i => [i.recordId, i.priority]));
  assert.deepEqual(rows.map(row => byId[row.id]?.evidenceCoverage), [3, 2, 0, 0, 0, 3]);
  assert.deepEqual(rows.map(row => byId[row.id]?.position), [1, 3, 4, 4, 4, 1]);
});

test('the original synthetic cohort keeps its positions (Cam\'s range now counts: coverage 2 -> 3)', () => {
  const rows = narrativeFixture();
  const r = composeRecords(bundle(rows), rankRead, {});
  assert.deepEqual(r.items.map(i => i.priority?.position), [1, 2, 3, 4, 5, 5]);
  assert.deepEqual(r.items.map(i => i.priority?.evidenceCoverage), [5, 4, 3, 1, 0, 0]);
});
