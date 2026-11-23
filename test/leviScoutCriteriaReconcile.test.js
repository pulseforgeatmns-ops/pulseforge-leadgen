'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { reconcileResearchProfile, RESEARCH_PROFILE } = require('../services/leviScoutCriteriaReconcile');

const trusted = (facts = {}, extras = {}) => ({
  _bundle: {
    tenantId: '21',
    knowledgeState: 'available',
    blueprintSource: 'blueprint',
    summary: {
      clientId: 21,
      approved: true,
      semanticAuthority: 'CANONICAL',
      canonicalSnapshotId: 'synthetic-approved-snapshot',
      businessFacts: facts,
      ...extras.summary,
    },
    contract: extras.contract || { services: extras.services || 'Lending to investors.' },
  },
});

const known = (id, value) => ({
  id, provenance: `operator-answer-${id}`, epistemic_state: 'KNOWN', value,
});

test('a trusted Blueprint with empty slots does not promote the $5k–$20k research target', () => {
  const report = reconcileResearchProfile(trusted());
  assert.equal(report.invented, false);
  assert.equal(report.authorizedChanges, false);
  assert.equal(report.proposedRangeAuthorized, false);
  assert.equal(report.criteriaRange, null);
  assert.equal(report.fields.find(field => field.id === 'financingAmountRange').status, 'research_only_not_approved');
  assert.equal(report.fields.find(field => field.id === 'financingAmountRange').promoted, false);
  assert.equal(RESEARCH_PROFILE.financingAmountRange.min, 5000);
  assert.equal(RESEARCH_PROFILE.financingAmountRange.max, 20000);
});

test('an approved financing range is reported as approved evidence rather than silently copied from research', () => {
  const report = reconcileResearchProfile(trusted({
    services: [known('svc-1', 'Bridge loans. Financing range: $5,000–$20,000.')],
  }, { services: 'Bridge loans. Financing range: $5,000–$20,000.' }));
  const range = report.fields.find(field => field.id === 'financingAmountRange');
  assert.equal(range.approvedPresent, true);
  assert.equal(range.matchesResearchAssumption, true);
  assert.equal(range.status, 'approved_and_matches_research');
  assert.equal(range.promoted, false);
  assert.deepEqual(report.criteriaRange, { min: 5000, max: 20000 });
});

test('workspace copy still cannot become approved nationwide rehab criteria', () => {
  const report = reconcileResearchProfile({
    _bundle: {
      tenantId: '21',
      knowledgeState: 'never_learned',
      blueprintSource: 'none',
      summary: { clientId: 21, approved: false },
      contract: { services: 'Nationwide rehabilitation and equity-gap financing. Financing range: $5,000–$20,000.' },
    },
  });
  assert.equal(report.trusted, false);
  assert.ok(report.fields.every(field => field.status === 'research_only_not_approved'));
  assert.ok(report.fields.every(field => field.promoted === false));
});
