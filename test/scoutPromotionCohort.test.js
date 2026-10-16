'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { retainPromotionIds, promotionCohort } = require('../services/scoutPromotionCohort');
const { controlCycleDiagnostic } = require('../services/outboundControlDiagnostics');

test('promotion cohort uses exact retained IDs, not whole inventory or count inference', () => {
  const first = retainPromotionIds({ promoted: 2, recovered: 1 }, ['101', '102', '103']);
  const second = retainPromotionIds({ promoted: 1, recovered: 0 }, ['104']);
  const cohort = promotionCohort([first, second], {
    clean: [{ prospectId: '101' }, { prospectId: '999' }],
    excluded: [{ prospectId: '102', reason: 'contact_classification_not_authorized' },
      { prospectId: '103', reason: 'business_fit' }, { prospectId: '998', reason: 'email_not_verified' }],
  });
  assert.equal(cohort.outcomes, 4);
  assert.equal(cohort.identifiedProspects, 4);
  assert.equal(cohort.clean, 1);
  assert.equal(cohort.unmatched, 1);
  assert.equal(cohort.unidentifiedOutcomes, 0);
  assert.deepEqual({ ...cohort.exclusionCounts }, { contact_classification_not_authorized: 1, business_fit: 1 });
  const logged = JSON.stringify(controlCycleDiagnostic('13', { scoutPromotionCleanCohort: cohort }));
  for (const id of ['101', '102', '103', '104', '998', '999']) assert.ok(!logged.includes(id));
  assert.equal(JSON.stringify(first), '{"promoted":2,"recovered":1}');
  assert.equal(Object.keys(first).length, 2);
});

test('missing IDs, old summaries and duplicates are explicit unknowns, never invented exclusions', () => {
  const cohort = promotionCohort([
    retainPromotionIds({ promoted: 2, recovered: 0 }, ['101', '101']),
    retainPromotionIds({ promoted: 2, recovered: 0 }, [null, 'private@example.test']),
    { promoted: 1, recovered: 0 },
  ], { clean: [], excluded: [] });
  assert.equal(cohort.outcomes, 5);
  assert.equal(cohort.identifiedProspects, 1);
  assert.equal(cohort.duplicateOutcomeReferences, 1);
  assert.equal(cohort.unidentifiedOutcomes, 3);
  assert.equal(cohort.unmatched, 1);
  assert.deepEqual({ ...cohort.exclusionCounts }, {});
  assert.equal(promotionCohort(undefined, { clean: [], excluded: [] }), null);
  assert.equal(promotionCohort([{ promoted: '2', recovered: 0 }], { clean: [], excluded: [] }), null);
  assert.equal(promotionCohort([], {}), null);
  assert.equal(promotionCohort([], { clean: [], excluded: [] }).outcomes, 0);
});

test('unknown exclusion text is sanitized even for exact cohort matches', () => {
  const cohort = promotionCohort([retainPromotionIds({ promoted: 1, recovered: 0 }, ['1'])], {
    clean: [], excluded: [{ prospectId: '1', reason: 'private@example.test' }],
  });
  const diagnostic = controlCycleDiagnostic('13', { scoutPromotionCleanCohort: cohort }).scoutPromotionCleanCohort;
  assert.equal(diagnostic.exclusions.unrecognizedReasonCount, 1);
  assert.equal(diagnostic.exclusions.recordedExcludedTotal, 1);
  assert.ok(!JSON.stringify(diagnostic).includes('private'));
});
