'use strict';

/**
 * SPEC-111 — operator intent taxonomy at the shared classification layer.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  classifyCognitiveMode,
  COGNITIVE_MODES,
} = require('../packages/max/specialistDelegation/CognitiveMode');
const { shouldInvokeSpecialist } = require('../packages/max/specialistDelegation/RetrievalGate');
const { shouldClaimClientIntelligenceTurn } = require('../packages/max/workspace/ClientIntelligenceContext');
const { shouldHandleScoutAcquisition } = require('../packages/max/workspace/ScoutAcquisitionContext');
const {
  CONTRACT_IDS,
  selectResponseContract,
  listResponseContracts,
} = require('../packages/max/workspace/ResponseContract');
const {
  OPERATOR_INTENTS,
  looksLikeDiagnosis,
  looksLikeUnknownAnalysis,
} = require('../packages/max/workspace/OperatorIntentRegistry');
const { shouldRetrieveOperatingEvidence } = require('../packages/max/workspace/OperatingEvidenceRetrieval');
const training = require('../packages/max/training');

describe('SPEC-111 shared classification', () => {
  it('binds acceptance prompts to distinct analysis modes', () => {
    const cases = [
      ["What's preventing us from growing faster?", CONTRACT_IDS.DIAGNOSIS, COGNITIVE_MODES.DIAGNOSIS],
      ["What don't we know yet that matters?", CONTRACT_IDS.UNKNOWN_ANALYSIS, COGNITIVE_MODES.UNKNOWN_ANALYSIS],
      ["What's our biggest operational risk?", CONTRACT_IDS.RISK, COGNITIVE_MODES.RISK],
      ['How much progress have we made?', CONTRACT_IDS.PROGRESS, COGNITIVE_MODES.PROGRESS],
      ['What should we do next?', CONTRACT_IDS.RECOMMENDATION, COGNITIVE_MODES.RECOMMENDATION],
      ['What outreach has already been sent?', CONTRACT_IDS.RETRIEVAL, COGNITIVE_MODES.RETRIEVAL],
      ['How is Anchor Cleaning doing?', CONTRACT_IDS.SUMMARY, COGNITIVE_MODES.RETRIEVAL],
    ];
    for (const [question, contractId, kind] of cases) {
      const mode = classifyCognitiveMode(question);
      const contract = selectResponseContract(question, mode);
      assert.equal(mode.kind, kind, question);
      assert.equal(contract.id, contractId, question);
    }
    assert.equal(listResponseContracts().length, 9);
    assert.equal(looksLikeDiagnosis("What's the bottleneck?"), true);
    assert.equal(looksLikeUnknownAnalysis("What's missing?"), true);
  });

  it('does not send diagnosis or unknown analysis to CIE or Scout', () => {
    for (const question of [
      "What's preventing us from growing faster?",
      "What don't we know yet that matters?",
    ]) {
      assert.equal(shouldInvokeSpecialist(question), false, question);
      assert.equal(shouldRetrieveOperatingEvidence(question), true, question);
      assert.equal(
        shouldClaimClientIntelligenceTurn(question, null, { approvedBlueprint: true }),
        false,
        question
      );
      assert.equal(
        shouldHandleScoutAcquisition({ question, context: { tenantId: '10' } }),
        false,
        question
      );
    }
  });
});

describe('SPEC-111 competency registry', () => {
  it('registers operator_intent_taxonomy as a graduated competency', () => {
    const competency = training.getCompetency('operator_intent_taxonomy');
    assert.ok(competency);
    assert.equal(competency.stage, training.STAGES.GRADUATED);
    assert.ok(competency.specRefs.includes('SPEC-111'));
    assert.match(competency.exercises[0].generalLesson, /Intent determines analysis/i);
  });
});

describe('SPEC-111 intent constants', () => {
  it('keeps the explicit nine-intent registry', () => {
    assert.deepEqual(Object.values(OPERATOR_INTENTS), [
      'retrieval',
      'summary',
      'recommendation',
      'diagnosis',
      'unknown_analysis',
      'risk',
      'progress',
      'challenge',
      'investigation',
    ]);
  });
});
