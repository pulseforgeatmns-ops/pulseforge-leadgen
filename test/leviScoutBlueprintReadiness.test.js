'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { inspectApprovedBlueprint, QUESTIONS, SLOT_IDS, loadTenantUnderstanding } = require('../services/leviScoutBlueprintReadiness');

const approved = (facts = {}, extras = {}) => ({
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

const known = (id, value, slot = 'services') => ({
  id, provenance: `operator-answer-${id}`, epistemic_state: 'KNOWN', value, slot,
});

test('an untrusted Blueprint asks Levi for every lending criterion and invents nothing', () => {
  const report = inspectApprovedBlueprint(undefined);
  assert.equal(report.trusted, false);
  assert.equal(report.invented, false);
  assert.equal(report.authorizedChanges, false);
  assert.equal(report.criteriaRange, null);
  assert.deepEqual(report.missing.map(item => item.id), SLOT_IDS);
  assert.deepEqual(report.questionsForLevi, SLOT_IDS.map(id => QUESTIONS[id]));
  assert.equal(report.slots.financingAmountRange.evidence.length, 0);
});

test('workspace-style copy is ignored unless it is a trusted KNOWN Blueprint fact', () => {
  const report = inspectApprovedBlueprint({
    _bundle: {
      tenantId: '21',
      knowledgeState: 'never_learned',
      blueprintSource: 'none',
      summary: { clientId: 21, approved: false },
      contract: { services: 'Prospect active real-estate operators nationwide seeking rehab capital.' },
    },
  });
  assert.equal(report.trusted, false);
  assert.equal(report.slots.geographicCoverage.present, false);
  assert.equal(report.slots.eligiblePropertyTypes.present, false);
});

test('a single trusted financing range fills only that slot; other questions stay open', () => {
  const report = inspectApprovedBlueprint(approved({
    services: [known('svc-1', 'Bridge loans. Financing range: $5,000–$20,000.')],
  }, { services: 'Bridge loans. Financing range: $5,000–$20,000.' }));
  assert.equal(report.trusted, true);
  assert.deepEqual(report.criteriaRange, { min: 5000, max: 20000 });
  assert.equal(report.slots.financingAmountRange.present, true);
  assert.equal(report.slots.borrowerEligibility.present, false);
  assert.ok(report.questionsForLevi.includes(QUESTIONS.borrowerEligibility));
  assert.ok(report.questionsForLevi.includes(QUESTIONS.eligiblePropertyTypes));
  assert.ok(report.questionsForLevi.includes(QUESTIONS.geographicCoverage));
  assert.ok(!report.questionsForLevi.includes(QUESTIONS.financingAmountRange));
});

test('KNOWN geography, borrower, product and exclusion facts are reported without rewriting them', () => {
  const report = inspectApprovedBlueprint(approved({
    services: [known('svc-1', 'Bridge and rehab loans. Financing range: $50,000–$250,000.')],
    ideal_customers: [known('ic-1', 'Entity-owned rehab operators and sponsors', 'ideal_customers')],
    geography: [known('geo-1', 'New York City and Chicago permit markets', 'geography')],
    disqualified_customers: [known('dq-1', 'Owner-occupant and consumer loans are excluded', 'disqualified_customers')],
  }));
  assert.equal(report.slots.eligiblePropertyTypes.present, true);
  assert.equal(report.slots.geographicCoverage.present, true);
  assert.equal(report.slots.borrowerEligibility.present, true);
  assert.equal(report.slots.financingProductsAndExclusions.present, true);
  assert.equal(report.questionsForLevi.length, 0);
  assert.equal(report.slots.geographicCoverage.evidence[0].value, 'New York City and Chicago permit markets');
});

test('loadTenantUnderstanding without a database does not invent an approved Blueprint', async () => {
  const loaded = await loadTenantUnderstanding({});
  assert.equal(loaded._bundle.knowledgeState, 'unavailable');
  assert.equal(loaded.loadError, 'blueprint_db_unavailable');
  assert.equal(inspectApprovedBlueprint(loaded).trusted, false);
});
