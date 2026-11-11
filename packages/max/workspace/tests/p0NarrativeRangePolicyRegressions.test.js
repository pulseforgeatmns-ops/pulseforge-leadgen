'use strict';
// Approved range policy for PR #1120: regressions for exact requests, explicit
// ranges, superseded ranges, conflicting requests, unrelated deals and general
// preferences, including the defects found when probing v3:
//   - a history cue in an earlier clause ("On a previous deal, requested …")
//   - an amount for a different property counted as this deal's request
//   - a different financing need superseding the range (the need window
//     dropped its last word, so "for the down payment" read as no need)
//   - "current request is for $X to $Y" not recognized
//   - a superseding claim not showing its own source sentence
require('./workspaceTestHarness');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { extractDealClaims } = require('../ProspectDealClaims');
const { composeRecords, evaluate } = require('../ProspectRecordIntelligence');
const { normalizeContact, noteFor } = require('../../../../services/reviewOnlyProspectImportService');

const fit = s => extractDealClaims(`Fit evidence: ${s}`);
const amount = s => {
  const d = fit(s);
  const u = d.unparsed.find(x => x.field === 'amount');
  return { claim: d.claims.amount || null, unparsed: u || null, conflicts: d.conflicts };
};

test('exact requests stay exact single amounts with their source wording', () => {
  for (const [text, n] of [['Requesting $35k for rehab.', 35000], ['Needs $35,000 to finish rehab.', 35000],
    ['$35,000 remaining rehab request.', 35000], ['Previous lender approved $40k; now requesting $35k.', 35000]]) {
    const { claim } = amount(text);
    assert.equal(claim.amount, n, text); assert.equal(claim.form, undefined, text);
    assert.equal(claim.epistemic, 'recorded_unverified'); assert.ok(text.includes(claim.snippet.replace(/^Fit evidence: /, '')) || `Fit evidence: ${text}`.includes(claim.snippet));
  }
});

test('explicit current ranges are recorded-unverified claims kept as full ranges, never one amount', () => {
  for (const [text, value, min, max] of [
    ['Requesting $35k–$45k for rehab.', '$35k–$45k', 35000, 45000],
    ['Current request is for $35,000 to $45,000 in gap funding.', '$35,000 to $45,000', 35000, 45000],
    ['Seeking $8k-$12k for cosmetic rehab.', '$8k-$12k', 8000, 12000],
  ]) {
    const { claim, unparsed } = amount(text);
    assert.equal(unparsed, null, text);
    assert.equal(claim.form, 'range', text); assert.equal(claim.value, value, text);
    assert.deepEqual(claim.range, { min, max }, text); assert.equal(claim.amount, undefined, text);
    assert.ok(`Fit evidence: ${text}`.includes(claim.snippet), text);
  }
  for (const text of ['Requesting $35–45k.', 'Requesting $45k–$35k.']) {
    assert.equal(amount(text).unparsed.reason, 'unsupported_format', text); // bounds not read unambiguously
  }
});

test('a specific request supersedes a range only in the same statement, with an update cue and the same need', () => {
  for (const text of ['Requesting $35k–$45k for rehab; narrowed to $40k.', 'Requesting $35k–$45k for rehab, now requesting $40k.',
    'Requesting $35k–$45k for rehab; revised to $40k for rehab.']) {
    const { claim } = amount(text);
    assert.equal(claim.amount, 40000, text);
    assert.equal(claim.supersedes.value, '$35k–$45k', text); assert.deepEqual(claim.supersedes.range, { min: 35000, max: 45000 });
    // Both the superseding sentence and the superseded range are shown as source text.
    for (const s of claim.snippets) assert.ok(`Fit evidence: ${text}`.includes(s), s);
    assert.ok(claim.snippets.includes(claim.snippet));
  }
});

test('ambiguous association or precedence retains every value as record-local non-comparable', () => {
  for (const [text, values] of [
    ['Requesting $35k–$45k for rehab. Later narrowed to $40k.', ['$35k–$45k', '$40k']],               // different statement
    ['Requesting $35k–$45k for rehab; narrowed to $10k for the down payment.', ['$35k–$45k', '$10k']], // different need
    ['Requesting $35k–$45k for rehab and $40k for purchase.', ['$35k–$45k', '$40k']],                   // two needs, no update cue
    ['Requesting $35k–$45k for rehab; narrowed to $38k, then revised to $42k.', ['$35k–$45k', '$38k', '$42k']], // competing updates
  ]) {
    const { claim, unparsed, conflicts } = amount(text);
    assert.equal(claim, null, text);
    assert.equal(unparsed.reason, 'multiple_values', text); assert.deepEqual(unparsed.values, values, text);
    assert.deepEqual(conflicts, [], text); // never a cohort-level contradiction
  }
});

test('conflicting requests stay record-local; contradictory labelled requests still block the cohort', () => {
  for (const text of ['Requesting $20k. Requesting $30k.', 'Needs $20k or $30k.', 'Requesting $12k on one and $18k on the other.']) {
    const { claim, unparsed, conflicts } = amount(text);
    assert.equal(claim, null, text); assert.equal(unparsed.reason, 'multiple_values', text); assert.deepEqual(conflicts, [], text);
  }
  const labelled = extractDealClaims('Fit evidence: Financing request: $20,000\nRisks: Financing request: $30,000');
  assert.deepEqual(labelled.conflicts, ['amount']);
});

test('unrelated, past and future deals are not current financing requests', () => {
  for (const text of ['On a previous deal, requested $50k–$60k.', 'Last year he requested $40k.', 'For his next project he will need $80k.',
    'His prior flip needed $25k-$30k.', 'Requested $45k on a different property.', 'Needs $30k for another deal.']) {
    const { claim, unparsed } = amount(text);
    assert.equal(claim, null, text); assert.equal(unparsed, null, text);
  }
  // A current request in the same record is still recognized next to an unrelated one.
  assert.equal(amount('Requesting $35k for rehab on this property; previously requested $50k on another deal.').claim.amount, 35000);
});

test('general preferences, budgets, prices, ARVs and projected values never count as current requests', () => {
  for (const text of ['Typically borrows $20k–$50k.', 'Usually seeking $30k–$60k per deal.', 'Prefers to borrow $25k-$50k.',
    'Generally looking for $50k loans.', 'Rehab budget $40k–$60k.', 'ARV $250k–$300k.', 'Purchase price $150k-$170k.',
    'Projected resale $300k-$320k.', 'Seeking $250k-$300k properties.', 'Needs $8k-$12k in equity.', 'Purchase price reduced to $140k.']) {
    const { claim, unparsed } = amount(text);
    assert.equal(claim, null, text); assert.equal(unparsed, null, text);
  }
});

test('in the composed answer a prospect range is coverage evidence only: never compared with Blueprint criteria; outreach stays blocked', () => {
  const rows = [
    ['Range Example', 'Requesting $35k–$45k for rehab on a purchased SFR. Proposed first lien. Exit via sale.'],
    ['Exact Example', 'Requesting $12k for rehab. Exit via refinance.'],
  ].map(([name, f], i) => {
    const c = normalizeContact({ name, fit: f, risks: 'Unverified.', reviewState: 'contact_now', category: 'borrower' }, i);
    return { id: `00000000-0000-0000-0000-20000000000${i + 1}`, client_id: 21, first_name: c.firstName, last_name: c.lastName,
      status: c.status, source: 'review_only_import', notes: noteFor(c), do_not_contact: false, ao_outreach_review_required: true,
      acquisition_metadata: { importMode: 'review_only', outreachAuthorized: false, reviewState: 'contact_now', category: 'borrower' } };
  });
  const understanding = { _bundle: { tenantId: '21', knowledgeState: 'available', summary: { clientId: 21, approved: true,
    semanticAuthority: 'CANONICAL', canonicalSnapshotId: 'snap' }, contract: { services: 'Financing range: $5,000–$20,000.' } } };
  const r = composeRecords({ tenantId: '21', clientId: 21, prospects: { records: { status: 'found', rows, complete: true } } },
    { wantsRanking: true, reviewState: 'contact_now', priorityMode: 'investigation' }, { businessUnderstanding: understanding });
  const range = r.items.find(i => i.recordId === rows[0].id);
  assert.equal(range.priority.evidenceCoverage, 4); // amount (range) + stage + security + exit
  assert.match(range.claim, /financing request: \$35k–\$45k \(stated range \$35,000–\$45,000; not a single amount\)/);
  assert.match(range.claim, /Financing fit: A financing request is recorded but is a range, not a single amount/);
  assert.doesNotMatch(range.claim, /(?:within|outside) the recorded business range/);
  assert.match(r.items.find(i => i.recordId === rows[1].id).claim, /within the recorded business range/);
  assert.ok(rows.every(row => !evaluate(row).eligible));
  assert.ok(r.items.every(i => /Outreach eligibility: blocked/.test(i.claim)));
});
