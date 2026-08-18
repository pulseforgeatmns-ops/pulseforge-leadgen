'use strict';

/**
 * SPEC-110 — business intelligence synthesis at the shared classification layer.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  classifyCognitiveMode,
  COGNITIVE_MODES,
} = require('../packages/max/specialistDelegation/CognitiveMode');
const { shouldInvokeSpecialist } = require('../packages/max/specialistDelegation/RetrievalGate');
const { shouldClaimClientIntelligenceTurn } = require('../packages/max/workspace/ClientIntelligenceContext');
const {
  CONTRACT_IDS,
  selectResponseContract,
} = require('../packages/max/workspace/ResponseContract');
const {
  CATEGORIES,
  isChannelEffectivenessQuestion,
  isFocusQuestion,
  synthesizeBusinessIntelligence,
} = require('../packages/max/workspace/BusinessIntelligence');
const { shouldRetrieveOperatingEvidence } = require('../packages/max/workspace/OperatingEvidenceRetrieval');
const training = require('../packages/max/training');

describe('SPEC-110 shared classification', () => {
  it('binds acceptance prompts to contracts that can carry business intelligence', () => {
    const cases = [
      ['What outreach has already been sent?', CONTRACT_IDS.RETRIEVAL, COGNITIVE_MODES.RETRIEVAL],
      ['How is Anchor Cleaning doing?', CONTRACT_IDS.SUMMARY, COGNITIVE_MODES.RETRIEVAL],
      ['What should we do next?', CONTRACT_IDS.RECOMMENDATION, COGNITIVE_MODES.RECOMMENDATION],
      ['Are Yelp Ads working?', CONTRACT_IDS.RETRIEVAL, COGNITIVE_MODES.RETRIEVAL],
      ['Where should we focus next?', CONTRACT_IDS.RECOMMENDATION, COGNITIVE_MODES.RECOMMENDATION],
    ];
    for (const [question, contractId, kind] of cases) {
      const mode = classifyCognitiveMode(question);
      const contract = selectResponseContract(question, mode);
      assert.equal(mode.kind, kind, question);
      assert.equal(contract.id, contractId, question);
      if (contractId !== CONTRACT_IDS.CHALLENGE && contractId !== CONTRACT_IDS.INVESTIGATION) {
        assert.ok(contract.required.includes('business_intelligence'), question);
      }
    }
    assert.equal(isChannelEffectivenessQuestion('Are Yelp Ads working?'), true);
    assert.equal(isFocusQuestion('Where should we focus next?'), true);
    assert.equal(shouldRetrieveOperatingEvidence('Are Yelp Ads working?'), true);
  });

  it('does not send effectiveness or summary questions to CIE or specialists', () => {
    for (const question of ['Are Yelp Ads working?', 'How is Anchor Cleaning doing?']) {
      assert.equal(shouldInvokeSpecialist(question), false, question);
      assert.equal(
        shouldClaimClientIntelligenceTurn(question, null, { approvedBlueprint: true }),
        false,
        question
      );
    }
  });
});

describe('SPEC-110 shared synthesis constraints', () => {
  it('returns unknown rather than speculation when a channel has no evidence', () => {
    const synthesis = synthesizeBusinessIntelligence({
      question: 'Are Yelp Ads working?',
      bundle: {
        items: [
          {
            epistemic: 'not_recorded',
            claim: 'No durable Yelp activity is recorded for this tenant.',
            provenance: 'activity log',
            sourceKind: 'yelp',
          },
        ],
        prospects: { counts: { total: 72, qualified: 54 } },
        scout: { intelligence: { counts: { matched: 69 } } },
        outcomes: { jobs: 0, payments: 0 },
      },
    });
    assert.ok(synthesis.objects.length >= 1);
    assert.ok(synthesis.objects.every((obj) => obj.category === CATEGORIES.UNKNOWN));
    assert.match(synthesis.prose, /Insufficient evidence to determine Yelp Ads effectiveness/i);
    assert.doesNotMatch(synthesis.prose, /are working|likely converting|should be performing/i);
    assert.ok(synthesis.objects.every((obj) => obj.supporting_claims.length >= 1));
  });
});

describe('SPEC-110 competency registry', () => {
  it('registers business_intelligence_synthesis as a graduated competency', () => {
    const competency = training.getCompetency('business_intelligence_synthesis');
    assert.ok(competency);
    assert.equal(competency.stage, training.STAGES.GRADUATED);
    assert.ok(competency.specRefs.includes('SPEC-110'));
    assert.match(competency.exercises[0].generalLesson, /intelligence/i);
  });
});
