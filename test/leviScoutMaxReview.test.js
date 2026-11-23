'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  composeRecordReview,
  composeReviewBundle,
  formatReviewAnswer,
  RESEARCH_CANDIDATE,
  QUALIFIED_PROSPECT,
} = require('../services/leviScoutMaxReview');
const { maybeHandleScoutAcquisitionTurn } = require('../packages/max/workspace/ScoutAcquisitionContext');
const { evaluateProspect } = require('../services/leviScoutProductionContract');
const { QUESTIONS } = require('../services/leviScoutBlueprintReadiness');

const permitCandidate = {
  name: 'Ben Operator LLC',
  company: 'Ben Operator LLC',
  job_title: 'Real-estate operator',
  email: null,
  phone: null,
  service_area_match: '100 N MICHIGAN AVE, Chicago, IL',
  notes: 'Review state: qualify first.\nFit evidence: The operator owns the property at 100 N MICHIGAN AVE, Chicago, IL. Renovation is underway\nSources: https://data.cityofchicago.org/resource/ydr8-5enu.json?permit_=1',
  acquisition_metadata: {
    reviewState: 'qualify_first',
    category: 'borrower',
    sourceType: 'property_record_signal',
    sourceUrls: ['https://data.cityofchicago.org/resource/ydr8-5enu.json?permit_=1'],
    evidenceDate: '2026-11-18',
    city: 'Chicago',
    permitDoesNotEstablishIntent: true,
    signals: [{
      layer: 'property_activity',
      source: 'https://data.cityofchicago.org/resource/ydr8-5enu.json?permit_=1',
      evidence: 'Renovation permit at 100 N MICHIGAN AVE, Chicago, IL',
      observedAt: '2026-11-18',
      confidence: 'high',
      limitations: 'A permit does not establish verified borrower intent.',
    }],
    contactResearch: { verifiable: false, fabricated: false },
  },
};

test('Max review separates research candidates from qualified prospects', () => {
  const evaluation = evaluateProspect(permitCandidate, {
    criteria: { approved: true, source: 'approved_blueprint', minAmount: 5000, maxAmount: 20000 },
  });
  const review = composeRecordReview({ candidate: permitCandidate, evaluation });
  assert.equal(review.bucket, RESEARCH_CANDIDATE);
  assert.equal(review.qualificationStatus, 'not_qualified');
  assert.equal(review.verifiedBorrowerIntent, false);
  assert.equal(review.permitDoesNotEstablishIntent, true);
  assert.ok(review.missingInformation.some(line => /intent/i.test(line)));
  assert.match(review.nextResearchAction, /explicit financing request/i);
  assert.match(review.evidenceSummary, /property activity/i);
});

test('review bundle lists Levi questions and never invents qualification', () => {
  const bundle = composeReviewBundle({
    records: [permitCandidate],
    understanding: {
      _bundle: {
        tenantId: '21',
        knowledgeState: 'available',
        blueprintSource: 'blueprint',
        summary: {
          clientId: 21, approved: true, semanticAuthority: 'CANONICAL',
          canonicalSnapshotId: 'snap',
          businessFacts: { services: [{ id: 's1', provenance: 'p', epistemic_state: 'KNOWN', value: 'Lending to investors.' }] },
        },
        contract: { services: 'Lending to investors.' },
      },
    },
  });
  assert.equal(bundle.counts.researchCandidates, 1);
  assert.equal(bundle.counts.qualifiedProspects, 0);
  assert.equal(bundle.outreachAttempted, false);
  assert.ok(bundle.blueprint.questionsForLevi.includes(QUESTIONS.financingAmountRange));
  const answer = formatReviewAnswer(bundle);
  assert.match(answer, /Research candidates: 1/);
  assert.match(answer, /Qualified prospects: 0/);
  assert.match(answer, /Missing Blueprint decisions/);
  assert.doesNotMatch(answer, /needs \$/);
});

test('tenant 21 Max find/discover answers from persisted Scout records and does not enter the general loop', async () => {
  let loopCalled = 0;
  const turn = await maybeHandleScoutAcquisitionTurn({
    question: 'Find borrowers in New York this week',
    context: { enabledAgents: ['max'], tenantId: '21' },
    runLoop: async () => { loopCalled += 1; return { kind: 'observe', prose: 'should not run' }; },
    reviewRecords: [permitCandidate],
    understanding: { _bundle: { tenantId: '21', knowledgeState: 'never_learned', summary: null, contract: null } },
  });
  assert.equal(loopCalled, 0);
  assert.equal(turn.reason, 'levi_scout_research_review');
  assert.match(turn.prose, /Research candidates: 1/);
  assert.match(turn.prose, /permit never establishes borrower intent/i);
  assert.equal(turn.structured.metadata.operationalRead.kind, 'levi_scout_research_review');
  assert.equal(turn.structured.metadata.operationalRead.qualifiedProspects, 0);
  assert.equal(turn.structured.metadata.operationalRead.outreachAttempted, false);
  assert.equal(turn.structured.metadata.operationalRead.paidProvidersUsed, false);
  assert.ok(turn.structured.nextInvestigations.length >= 1);
});

test('a complete verified record can be a qualified prospect without mixing buckets', () => {
  const qualified = {
    created_at: '2026-11-18T12:00:00Z',
    source: 'scout_research_only',
    job_title: 'Real-estate operator',
    email: 'borrower@example.test',
    service_area_match: 'Akron, OH',
    notes: 'Review state: contact now.\nFit evidence: The operator is under contract and needs $12,000 rehab funding. Exit plan: sale. Security position: first lien.\nSources: https://example.test/project',
    acquisition_metadata: {
      reviewState: 'contact_now',
      category: 'borrower',
      sourceUrls: ['https://example.test/project'],
      evidenceTimestamp: '2026-11-18T11:00:00Z',
      sourceType: 'public_financing_request',
      contactResearch: { verifiable: true },
    },
  };
  const review = composeRecordReview({
    candidate: qualified,
    evaluation: evaluateProspect(qualified, {
      criteria: { approved: true, source: 'approved_blueprint', minAmount: 5000, maxAmount: 20000 },
    }),
  });
  assert.equal(review.bucket, QUALIFIED_PROSPECT);
  assert.equal(review.qualificationStatus, 'qualified');
});
