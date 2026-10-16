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

test('PostgreSQL UUID identities correlate case-insensitively with clean and excluded rows', () => {
  const cleanId = 'a1b2c3d4-1111-4222-8333-123456789abc';
  const excludedId = 'b2c3d4e5-2222-4333-8444-23456789abcd';
  const cohort = promotionCohort([retainPromotionIds({ promoted: 2, recovered: 1 },
    [cleanId.toUpperCase(), excludedId, excludedId.toUpperCase()])], {
    clean: [{ prospectId: cleanId }],
    excluded: [{ prospectId: excludedId.toUpperCase(), reason: 'contact_classification_not_authorized' }],
  });
  assert.equal(cohort.identifiedProspects, 2);
  assert.equal(cohort.unidentifiedOutcomes, 0);
  assert.equal(cohort.duplicateOutcomeReferences, 1);
  assert.equal(cohort.clean, 1);
  assert.equal(cohort.unmatched, 0);
  assert.equal(cohort.exclusionCounts.contact_classification_not_authorized, 1);
  assert.ok(!JSON.stringify(controlCycleDiagnostic('13', { scoutPromotionCleanCohort: cohort })).includes(cleanId));
});

test('malformed UUIDs and unsafe integer IDs remain unidentified even if inventory echoes them', () => {
  const invalid = [' a1b2c3d4-1111-4222-8333-123456789abc', 'a1b2c3d4-1111-4222-8333-123456789abz',
    'a1b2c3d4111142228333123456789abc', 'a1b2c3d4-1111-4222-8333-123456789abc@example.test',
    '', '0', '01', -1, 1.5, Number.MAX_SAFE_INTEGER + 1, {}, null, undefined];
  const cohort = promotionCohort([retainPromotionIds({ promoted: invalid.length, recovered: 0 }, invalid)], {
    clean: invalid.map(prospectId => ({ prospectId })), excluded: [],
  });
  assert.equal(cohort.identifiedProspects, 0);
  assert.equal(cohort.unidentifiedOutcomes, invalid.length);
  assert.equal(cohort.clean, 0);
  const legacy = promotionCohort([retainPromotionIds({ promoted: 2, recovered: 0 }, [42, '43'])], {
    clean: [{ prospectId: '42' }, { prospectId: 43 }], excluded: [],
  });
  assert.equal(legacy.clean, 2);
  assert.equal(legacy.unidentifiedOutcomes, 0);
});
