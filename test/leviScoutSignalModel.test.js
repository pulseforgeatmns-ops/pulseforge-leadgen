'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  LAYERS,
  PERMIT_INTENT_LIMITATION,
  fromPermit,
  intentFromPublicRequest,
  qualificationSignal,
  summarizeLayers,
} = require('../services/leviScoutSignalModel');
const { evaluateProspect } = require('../services/leviScoutProductionContract');

const candidate = {
  name: 'Ada Holdings LLC',
  company: 'Ada Holdings LLC',
  service_area_match: '10 WALL STREET, Manhattan, New York, NY',
  acquisition_metadata: {
    city: 'New York',
    sourceType: 'property_record_signal',
    sourceUrls: ['https://data.cityofnewyork.us/resource/w9ak-ipjd.json?job_filing_number=B00-TEST-1'],
    evidenceDate: '2026-11-18',
  },
};

test('a permit is property activity and never verified borrower intent', () => {
  const result = fromPermit({
    row: {
      job_type: 'Alteration',
      job_description: 'Interior renovation of dwelling units',
      initial_cost: '30000',
    },
    candidate,
    permalink: candidate.acquisition_metadata.sourceUrls[0],
    observedAt: '2026-11-18',
    city: 'New York',
  });
  assert.equal(result.verifiedBorrowerIntent, false);
  assert.equal(result.sourceType, 'property_record_signal');
  const layers = result.signals.map(item => item.layer);
  assert.ok(layers.includes(LAYERS.PROPERTY_ACTIVITY));
  assert.ok(layers.includes(LAYERS.POTENTIAL_FINANCING_NEED));
  assert.ok(!layers.includes(LAYERS.VERIFIED_BORROWER_INTENT));
  assert.match(result.signals[0].limitations, /does not establish verified borrower intent/i);
  assert.equal(result.signals[0].source, candidate.acquisition_metadata.sourceUrls[0]);
  assert.equal(result.signals[0].observedAt, '2026-11-18');
  const need = result.signals.find(item => item.layer === LAYERS.POTENTIAL_FINANCING_NEED);
  assert.match(need.evidence, /\$30,000/);
  assert.match(need.limitations, /not a financing request/i);
});

test('Chicago reported_cost is potential financing need with confidence limits', () => {
  const result = fromPermit({
    row: {
      permit_type: 'PERMIT - RENOVATION/ALTERATION',
      work_description: 'Interior renovation of a two-flat dwelling',
      reported_cost: '185000',
    },
    candidate: { ...candidate, service_area_match: '100 N MICHIGAN AVE, Chicago, IL' },
    permalink: 'https://data.cityofchicago.org/resource/ydr8-5enu.json?permit_=100519123',
    observedAt: '2026-11-18',
    city: 'Chicago',
  });
  const need = result.signals.find(item => item.layer === LAYERS.POTENTIAL_FINANCING_NEED);
  assert.match(need.evidence, /\$185,000/);
  assert.equal(need.confidence, 'low');
  assert.equal(result.verifiedBorrowerIntent, false);
});

test('verified intent requires a public financing request, not a permit', () => {
  const empty = intentFromPublicRequest({});
  assert.equal(empty.verifiedBorrowerIntent, false);
  const verified = intentFromPublicRequest({
    source: 'https://example.test/public-rfp',
    evidence: 'Operator publicly requested $120,000 rehab financing.',
    observedAt: '2026-11-18',
  });
  assert.equal(verified.verifiedBorrowerIntent, true);
  assert.equal(verified.signals[0].layer, LAYERS.VERIFIED_BORROWER_INTENT);
});

test('qualification stays a separate layer and a permit still fails evaluateProspect', () => {
  const mapped = {
    ...candidate,
    job_title: 'Real-estate operator',
    email: null,
    notes: 'Review state: qualify first.\nFit evidence: The operator owns the property at 10 WALL STREET. Renovation is underway\nSources: https://example.test/p',
  };
  const evaluation = evaluateProspect(mapped, {
    criteria: { approved: true, source: 'approved_blueprint', minAmount: 5000, maxAmount: 20000 },
  });
  const qualification = qualificationSignal({
    evaluation,
    source: mapped.acquisition_metadata.sourceUrls[0],
    observedAt: '2026-11-18',
  });
  assert.equal(evaluation.qualified, false);
  assert.ok(evaluation.blockers.includes('borrower_intent_unverified'));
  assert.equal(qualification.layer, LAYERS.QUALIFICATION);
  assert.match(qualification.evidence, /Not qualified/);
  assert.equal(summarizeLayers([qualification])[LAYERS.VERIFIED_BORROWER_INTENT], false);
  assert.match(PERMIT_INTENT_LIMITATION, /does not establish/i);
});
