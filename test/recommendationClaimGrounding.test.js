'use strict';

/**
 * SPEC-107A — claim-challenge classification at the shared layer.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  classifyCognitiveMode,
  COGNITIVE_MODES,
} = require('../packages/max/specialistDelegation/CognitiveMode');
const { shouldInvokeSpecialist } = require('../packages/max/specialistDelegation/RetrievalGate');
const {
  isOperatingEvidenceQuestion,
  shouldRetrieveOperatingEvidence,
} = require('../packages/max/workspace/OperatingEvidenceRetrieval');
const { shouldClaimClientIntelligenceTurn } = require('../packages/max/workspace/ClientIntelligenceContext');
const { isOperatorOperatingUpdate } = require('../packages/max/workspace/OperatorOperatingUpdate');
const {
  isClaimChallenge,
  isOperatorClaimCorrection,
} = require('../packages/max/workspace/RecommendationClaimChallenge');

const CHALLENGES = [
  'What evidence supports that?',
  'What evidence in PulseForge tells you that?',
  'Where did you get that?',
  'How do you know that?',
  'Can you verify that?',
  'Why do you believe that?',
  "That's not right.",
];

describe('SPEC-107A shared classification', () => {
  it('routes claim challenges away from inventory, CIE, and specialists', () => {
    for (const question of CHALLENGES) {
      const mode = classifyCognitiveMode(question);
      assert.equal(isClaimChallenge(question), true, question);
      assert.equal(isOperatingEvidenceQuestion(question), false, question);
      assert.equal(mode.kind, COGNITIVE_MODES.EXPLANATION, question);
      assert.equal(mode.via, 'claim_challenge', question);
      assert.equal(shouldInvokeSpecialist(question), false, question);
      assert.equal(
        shouldClaimClientIntelligenceTurn(question, null, { approvedBlueprint: true }),
        false,
        question
      );
    }
  });

  it('keeps inventory evidence questions on the SPEC-105 path', () => {
    const question = 'What evidence do we already have?';
    assert.equal(isClaimChallenge(question), false);
    assert.equal(isOperatingEvidenceQuestion(question), true);
    assert.equal(shouldRetrieveOperatingEvidence(question), true);
    assert.equal(classifyCognitiveMode(question).via, 'operating_evidence');
  });

  it('does not treat an email-outbound correction as a SPEC-106 write', () => {
    const question = "No, email outbound isn't running right now.";
    assert.equal(isOperatorClaimCorrection(question), true);
    assert.equal(isOperatorOperatingUpdate(question), false);
  });
});
