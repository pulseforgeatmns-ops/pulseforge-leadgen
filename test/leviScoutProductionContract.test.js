'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { evaluateProspect, weeklyProduction } = require('../services/leviScoutProductionContract');
const criteria = { approved: true, source: 'approved_blueprint', minAmount: 5000, maxAmount: 20000 };

function complete(overrides = {}) {
  return {
    created_at: '2026-11-18T12:00:00Z', source: 'scout', job_title: 'Real-estate operator',
    email: 'borrower@example.test', service_area_match: 'Akron, OH', do_not_contact: false,
    notes: 'Review state: contact now.\nFit evidence: The operator is under contract and needs $12,000 rehab funding. Exit plan: sale. Security position: first lien.\nSources: https://example.test/project',
    acquisition_metadata: { reviewState: 'contact_now', category: 'borrower', sourceUrls: ['https://example.test/project'], evidenceTimestamp: '2026-11-18T11:00:00Z' },
    ...overrides,
  };
}

test('complete evidence is Contact Now and counts toward the contract', () => {
  const result = evaluateProspect(complete(), { criteria });
  assert.equal(result.qualified, true);
  assert.equal(result.classification, 'Contact Now');
  assert.deepEqual(result.blockers, []);
});

test('incomplete evidence is held for qualification and never counted', () => {
  const result = evaluateProspect(complete({ email: null, notes: 'Review state: contact now.\nFit evidence: An operator may have a project.',
    acquisition_metadata: { reviewState: 'contact_now', category: 'borrower' } }), { criteria });
  assert.equal(result.qualified, false);
  assert.equal(result.classification, 'Qualify First');
  assert.ok(result.blockers.includes('contact_path_missing'));
  assert.ok(result.blockers.includes('financing_amount_missing'));
  assert.ok(result.blockers.includes('source_reference_missing'));
});

test('out-of-range, duplicate and suppressed records fail closed', () => {
  const result = evaluateProspect(complete({
    do_not_contact: true, duplicate_of: 'existing',
    notes: 'Review state: contact now.\nFit evidence: The operator is under contract and needs $35,000 rehab funding.\nSources: https://example.test/project',
  }), { criteria });
  assert.equal(result.qualified, false);
  assert.ok(result.blockers.includes('financing_amount_outside_blueprint'));
  assert.ok(result.blockers.includes('duplicate'));
  assert.ok(result.blockers.includes('suppressed'));
});

test('weekly production distinguishes discovered rows from qualified new output', () => {
  const rows = [complete(), complete({ email: null }), complete({ created_at: '2026-11-10T12:00:00Z' })];
  const weeks = weeklyProduction(rows, new Date('2026-11-19T00:00:00Z'), { criteria });
  assert.deepEqual(weeks.map(row => [row.weekOf, row.discovered, row.contractQualifiedCandidates]), [
    ['2026-11-16', 2, 1], ['2026-11-09', 1, 1],
  ]);
});

test('foreign review states cannot become Contact Now', () => {
  const result = evaluateProspect(complete({ acquisition_metadata: { ...complete().acquisition_metadata, reviewState: 'hold' } }), { criteria });
  assert.equal(result.qualified, false);
  assert.equal(result.classification, 'Hold');
});

test('an unverified financing range can never produce a qualified candidate', () => {
  const result = evaluateProspect(complete(), { criteria: { minAmount: 5000, maxAmount: 20000 } });
  assert.equal(result.qualified, false);
  assert.ok(result.blockers.includes('financing_criteria_unverified'));
});
