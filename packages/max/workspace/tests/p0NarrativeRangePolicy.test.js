'use strict';
// fit-narrative-claims-v3: approved policy refinement for PR #1120.
// An explicit, current, deal-specific financing range is recorded-unverified
// financing-request evidence and counts toward investigation coverage. It is
// kept verbatim with its bounds and never turned into one amount. A more
// specific current request supersedes it only when clearly the same statement
// and financing need; otherwise both are retained as non-comparable. General
// preferences, history, budgets, prices, ARVs and projections never count.
// Prospect-stated ranges stay separate from approved Blueprint criteria.
require('./workspaceTestHarness');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { composeRecords, evaluate } = require('../ProspectRecordIntelligence');
const { extractDealClaims, parseRange } = require('../ProspectDealClaims');
const { recordedDetails, tenantCriteria } = require('../ProspectInvestigationPriority');
const { narrativeFixture } = require('./fixtures/narrativeInvestigationProspects');
const { bundle } = require('./fixtures/investigationProspects');

const fit = s => extractDealClaims(`Fit evidence: ${s}`);
const rankRead = { wantsRanking: true, reviewState: 'contact_now', priorityMode: 'investigation' };
const approved = (services = 'Bridge loans. Financing range: $5,000–$20,000.') => ({ _bundle: { tenantId: '21', knowledgeState: 'available',
  summary: { clientId: 21, approved: true, semanticAuthority: 'CANONICAL', canonicalSnapshotId: 'synthetic-approved-snapshot' },
  contract: { services, constraints: 'Verify title and exit evidence.' } } });
const amountState = d => d.claims.amount
  ? (d.claims.amount.form === 'range' ? `range ${d.claims.amount.range.min}-${d.claims.amount.range.max}` : `exact ${d.claims.amount.amount}`)
  : (d.unparsed.find(u => u.field === 'amount') ? `unparsed:${d.unparsed.find(u => u.field === 'amount').reason}` : 'absent');

test('exact requests stay exact single amounts', () => {
  for (const [text, n] of [['Requesting $35k for rehab.', 35000], ['$35,000 remaining rehab request.', 35000],
    ['Funding request of $20k.', 20000], ['Current request for $18,500.', 18500]]) {
    const d = fit(text);
    assert.equal(amountState(d), `exact ${n}`, text); assert.equal(d.amount, n, text);
    assert.equal(d.claims.amount.form, undefined, text);
  }
});

test('explicit current ranges are recorded-unverified claims that keep the full range and source wording', () => {
  for (const [text, value, min, max] of [
    ['Current public request for $35,000–$45,000 gap positions.', '$35,000–$45,000', 35000, 45000],
    ['Seeking $8k–$12k for cosmetic rehab on a property owned free and clear.', '$8k–$12k', 8000, 12000],
    ['Requesting $20,000 to $30,000 for the rehab.', '$20,000 to $30,000', 20000, 30000],
    ['$15k-$25k bridge loan request.', '$15k-$25k', 15000, 25000]]) {
    const d = fit(text);
    assert.equal(amountState(d), `range ${min}-${max}`, text);
    assert.equal(d.amount, null, text); // never a single requested amount
    assert.equal(d.claims.amount.value, value, text);
    assert.equal(d.claims.amount.snippet, text, text);
    assert.equal(d.claims.amount.epistemic, 'recorded_unverified', text);
    assert.equal(d.values.amount, value, text); // counts toward coverage
  }
  // Ranges whose bounds cannot be read unambiguously, or that are approximate, are not claims.
  assert.equal(amountState(fit('Requesting $35–45k.')), 'unparsed:unsupported_format');
  assert.equal(amountState(fit('Requesting about $20k–$30k.')), 'unparsed:approximate');
  assert.equal(parseRange('$45k–$35k'), null);
  // A labelled range is treated the same way.
  const labelled = recordedDetails('Fit evidence: Financing request: $10,000–$15,000');
  assert.equal(amountState(labelled), 'range 10000-15000'); assert.equal(labelled.claims.amount.origin, 'label');
});

test('a specific current request supersedes a range only in the same statement, with an explicit update and the same need', () => {
  for (const [text, n] of [['Requesting $35k–$45k for the rehab; now narrowed to $40k.', 40000],
    ['Requesting $35k–$45k for the rehab; specifically requesting $40k.', 40000],
    ['Requesting $35k–$45k for the rehab; revised to $38,000.', 38000]]) {
    const d = fit(text);
    assert.equal(amountState(d), `exact ${n}`, text);
    assert.ok(d.claims.amount.supersedes, text);
    assert.equal(d.claims.amount.supersedes.value, '$35k–$45k', text); // the superseded range is retained
  }
  const shown = composeRecords(bundle(narrativeFixture(21, [{ name: 'Sam Example', fit: 'Requesting $35k–$45k for the rehab; now narrowed to $40k.', risks: 'Unverified.' },
    { name: 'Tia Example', fit: 'Public profile only.', risks: 'Unverified.' }])), rankRead, {}).items[0].claim;
  assert.match(shown, /financing request: \$40k \(= \$40,000\) \(supersedes the recorded range "\$35k–\$45k" in the same statement\)/);
});

test('ambiguous association or precedence keeps the evidence and marks the amount non-comparable', () => {
  for (const [text, values] of [
    // different sentences: association unclear
    ['Requesting $35k–$45k for the rehab. Revised to $40k.', ['$35k–$45k', '$40k']],
    // different financing needs
    ['Requesting $35k–$45k gap funding; now $40k for rehab.', ['$35k–$45k', '$40k']],
    ['Current public request for $35,000–$45,000 gap positions. $35,000 remaining rehab request.', ['$35,000–$45,000', '$35,000']],
    // no explicit update wording
    ['Requesting $35k–$45k for the rehab; requesting $40k.', ['$35k–$45k', '$40k']],
    // more than one update
    ['Requesting $35k–$45k; narrowed to $40k, then revised to $42k.', ['$35k–$45k', '$40k', '$42k']]]) {
    const d = fit(text);
    assert.equal(amountState(d), 'unparsed:multiple_values', text);
    assert.deepEqual(d.unparsed.find(u => u.field === 'amount').values, values, text);
    assert.equal(d.amount, null, text); assert.deepEqual(d.conflicts, [], text);
  }
  // Updates of other things never touch the request.
  for (const text of ['Requesting $35k–$45k; purchase price reduced to $150k.', 'Requesting $35k–$45k, rehab budget lowered to $30k.',
    'Requesting $35k–$45k. Purchase price reduced to $150k.']) {
    assert.equal(amountState(fit(text)), 'range 35000-45000', text);
  }
  assert.equal(amountState(fit('Purchase price reduced to $150k.')), 'absent');
  assert.equal(amountState(fit('Revised to $40k.')), 'absent'); // an update alone is not a request
});

test('conflicting requests stay record-local; contradictory labels still block the cohort', () => {
  for (const text of ['Requesting $12k or $15k.', 'Requesting $20k–$30k and $40k–$50k.', 'Requesting $20k–$30k. Requesting $50k.']) {
    const d = fit(text);
    assert.equal(amountState(d), 'unparsed:multiple_values', text); assert.deepEqual(d.conflicts, [], text);
  }
  assert.deepEqual(fit('Requesting $20k–$30k and $40k–$50k.').unparsed[0].values, ['$20k–$30k', '$40k–$50k']);
  const labelled = recordedDetails('Fit evidence: Financing request: $10,000–$15,000\nRisks: Financing request: $25,000');
  assert.deepEqual(labelled.conflicts, ['amount']);
});

test('unrelated, future and past deals are not current financing requests', () => {
  for (const text of ['Requesting $25k for the next deal.', 'Wants $20k–$30k for future projects.', 'Requested $50k–$80k on past flips.',
    'Borrowed $40k–$60k last year.', 'Requested $30k–$40k in 2024.', 'Seeking $10k–$15k for deals in the county.']) {
    assert.equal(amountState(fit(text)), 'absent', text);
  }
  // Two current properties with different requests: retained, not comparable.
  assert.equal(amountState(fit('Two properties: requesting $12k on one and $18k–$20k on the other.')), 'unparsed:multiple_values');
});

test('general borrowing preferences, budgets, prices, ARVs and projected values never count', () => {
  for (const text of ['Typically borrows $20k–$50k per deal.', 'Prefers loans of $10k–$30k.', 'Usually seeks $25k–$75k.',
    'Generally requests $20k–$40k.', 'Averages $30k–$50k per project.', 'Rehab budget of $35k–$45k.', 'Purchase price $150k–$170k.',
    'ARV $250k–$270k.', 'Projected resale value $300k–$320k.', 'Needs a $40k–$60k rehab budget.', 'Seeking $200k–$300k properties.',
    'Expects to need $30k–$40k.']) {
    const d = fit(text);
    assert.equal(amountState(d), 'absent', text); assert.equal(d.count, 0, text);
  }
});

test('prospect-stated ranges stay separate from approved Blueprint criteria; outreach is unchanged', () => {
  const rows = narrativeFixture(21, [
    { name: 'Ann Example', fit: 'Current public request for $35,000–$45,000 gap positions. Proposed first lien.', risks: 'Unverified.' },
    { name: 'Bo Example', fit: 'Requesting $15k for rehab. Under contract on a duplex.', risks: 'Unverified.' },
    { name: 'Cy Example', fit: 'Public profile only.', risks: 'Unverified.' }]);
  // Prospect ranges never become criteria.
  assert.equal(tenantCriteria(undefined, bundle(rows)).range, null);
  assert.deepEqual(tenantCriteria(approved(), bundle(rows)).range, { min: 5000, max: 20000 });
  for (const u of [undefined, approved()]) {
    const r = composeRecords(bundle(rows), rankRead, { businessUnderstanding: u });
    const ann = r.items.find(i => i.recordId === rows[0].id);
    assert.equal(ann.priority.evidenceCoverage, 2);
    assert.match(ann.claim, /financing request: \$35,000–\$45,000 \(stated range \$35,000–\$45,000; not a single amount\)/);
    assert.match(ann.claim, /Financing fit: A financing request is recorded but is a range, not a single amount \("\$35,000–\$45,000"\); financing fit cannot be assessed for this record\. The prospect-stated range is not compared with or used as business criteria\./);
    assert.doesNotMatch(ann.claim, /(?:within|outside) the recorded business range/);
    assert.match(r.prose, /Outreach eligibility: blocked/);
    assert.ok(r.items.every(i => /Outreach eligibility: blocked/.test(i.claim)));
  }
  // The exact request is still compared against the approved range.
  const bo = composeRecords(bundle(rows), rankRead, { businessUnderstanding: approved() }).items.find(i => i.recordId === rows[1].id);
  assert.match(bo.claim, /Recorded request is within the recorded business range/);
  assert.ok(rows.every(row => !evaluate(row).eligible));
});
