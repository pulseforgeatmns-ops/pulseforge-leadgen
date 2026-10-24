'use strict';

/**
 * SPEC-107 — compound recommendation classification at the shared layer.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { classifyCognitiveMode, COGNITIVE_MODES } = require('../packages/max/specialistDelegation/CognitiveMode');
const { shouldInvokeSpecialist } = require('../packages/max/specialistDelegation/RetrievalGate');
const {
  isOperatingEvidenceQuestion,
  isOperatingGroundedRecommendation,
  shouldRetrieveOperatingEvidence,
} = require('../packages/max/workspace/OperatingEvidenceRetrieval');
const { shouldHandleScoutAcquisition } = require('../packages/max/workspace/ScoutAcquisitionContext');
const { shouldClaimClientIntelligenceTurn } = require('../packages/max/workspace/ClientIntelligenceContext');

const COMPOUND = [
  'Given that update and what PulseForge already knows about Campaign 001, what should I focus on next to build the repeatable commercial pipeline?',
  "Given what's already in motion for Anchor, where is the highest-leverage constraint or opportunity I should focus on next to establish a repeatable commercial pipeline?",
  'What should I focus on next given what we\'ve already tried?',
  'Based on what PulseForge knows about Campaign 001, what should we do next?',
  'What should I prioritize based on our current acquisition activity?',
  'Given the campaign evidence, what is our next move?',
];

describe('SPEC-107 shared classification', () => {
  it('keeps inventory questions as retrieval', () => {
    const question = "What's the current state of Campaign 001?";
    assert.equal(isOperatingEvidenceQuestion(question), true);
    assert.equal(isOperatingGroundedRecommendation(question), false);
    assert.equal(classifyCognitiveMode(question).kind, COGNITIVE_MODES.RETRIEVAL);
    assert.equal(shouldInvokeSpecialist(question), false);
  });

  it('classifies compound recommendation prompts as recommendation + retrieval', () => {
    for (const question of COMPOUND) {
      const mode = classifyCognitiveMode(question);
      assert.equal(shouldRetrieveOperatingEvidence(question), true, question);
      assert.equal(isOperatingGroundedRecommendation(question), true, question);
      assert.equal(mode.kind, COGNITIVE_MODES.RECOMMENDATION, question);
      assert.equal(shouldInvokeSpecialist(question), false, question);
      assert.equal(shouldHandleScoutAcquisition({ question, context: { tenantId: '10' } }), false, question);
      assert.equal(
        shouldClaimClientIntelligenceTurn(question, null, { approvedBlueprint: true }),
        false,
        question
      );
    }
  });

  it('still allows CIE to claim business-understanding focus questions', () => {
    const question = 'Based on what you know about my business, what should we focus on first?';
    assert.equal(isOperatingGroundedRecommendation(question), false);
    assert.equal(shouldRetrieveOperatingEvidence(question), false);
    assert.equal(
      shouldClaimClientIntelligenceTurn(question, null, { approvedBlueprint: true }),
      true
    );
  });
});
