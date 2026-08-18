'use strict';

/**
 * SPEC-105 — operating-evidence classification at the shared cognitive-mode layer.
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

const INVENTORY = [
  'What campaigns have we run?',
  'What evidence do we already have?',
  'What acquisition activity is already recorded?',
  'What happened with Campaign 001?',
  'Before recommending anything, tell me what we\'ve already tried.',
];

describe('SPEC-105 operating evidence classification', () => {
  it('classifies inventory prompts as retrieval and not CIE/Scout', () => {
    for (const question of INVENTORY) {
      assert.equal(isOperatingEvidenceQuestion(question), true, question);
      assert.equal(classifyCognitiveMode(question).kind, COGNITIVE_MODES.RETRIEVAL, question);
      assert.equal(shouldInvokeSpecialist(question), false, question);
      assert.equal(shouldHandleScoutAcquisition({ question, context: { tenantId: '10' } }), false, question);
      assert.equal(
        shouldClaimClientIntelligenceTurn(question, null, { approvedBlueprint: true }),
        false,
        question
      );
    }
  });

  it('does not treat business-understanding questions as operating evidence or CIE advisory', () => {
    assert.equal(
      isOperatingEvidenceQuestion('What do you currently understand about Anchor Cleaning?'),
      false
    );
    assert.equal(isOperatingEvidenceQuestion('Who are our ideal customers?'), false);
    assert.equal(
      shouldClaimClientIntelligenceTurn('Who are our ideal customers?', null, {
        approvedBlueprint: true,
      }),
      false
    );
  });

  it('retrieves before recommending on grounded next-step questions', () => {
    const question =
      "Given what we've already tried and what PulseForge can actually verify, what should I focus on next to build the repeatable commercial pipeline?";
    const mode = classifyCognitiveMode(question);
    assert.equal(shouldRetrieveOperatingEvidence(question), true);
    assert.equal(isOperatingGroundedRecommendation(question), true);
    assert.equal(mode.kind, COGNITIVE_MODES.RECOMMENDATION);
    assert.equal(mode.requiresOperatingRetrieval, true);
    assert.equal(shouldHandleScoutAcquisition({ question, context: { tenantId: '10' } }), false);
  });

  it('still treats new-market investigation as Scout work', () => {
    const question = 'Find 20 additional property managers matching what we learned from Campaign 001.';
    assert.equal(isOperatingEvidenceQuestion(question), false);
    assert.equal(classifyCognitiveMode(question).kind, COGNITIVE_MODES.INVESTIGATION);
    assert.equal(shouldHandleScoutAcquisition({ question, context: { tenantId: '10' } }), true);
  });
});
