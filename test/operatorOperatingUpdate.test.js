'use strict';

/**
 * SPEC-106 — operating-update classification at the shared routing layer.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { classifyCognitiveMode, COGNITIVE_MODES } = require('../packages/max/specialistDelegation/CognitiveMode');
const { shouldRetrieveOperatingEvidence } = require('../packages/max/workspace/OperatingEvidenceRetrieval');
const { isOperatorOperatingUpdate } = require('../packages/max/workspace/OperatorOperatingUpdate');
const { shouldHandleScoutAcquisition } = require('../packages/max/workspace/ScoutAcquisitionContext');
const { shouldClaimClientIntelligenceTurn } = require('../packages/max/workspace/ClientIntelligenceContext');

describe('SPEC-106 shared routing classification', () => {
  it('sends mailed assertions to SPEC-106, not CIE', () => {
    const question = 'Campaign 001 was mailed August 6.';
    assert.equal(isOperatorOperatingUpdate(question), true);
    assert.equal(shouldRetrieveOperatingEvidence(question), false);
    assert.equal(
      shouldClaimClientIntelligenceTurn(question, null, { approvedBlueprint: true }),
      false
    );
  });

  it('keeps mailed questions on SPEC-105 retrieval', () => {
    const question = 'Was Campaign 001 mailed?';
    assert.equal(isOperatorOperatingUpdate(question), false);
    assert.equal(shouldRetrieveOperatingEvidence(question), true);
    assert.equal(classifyCognitiveMode(question).kind, COGNITIVE_MODES.RETRIEVAL);
  });

  it('keeps next-step questions off the write path', () => {
    const question = 'What should we do next with Campaign 001?';
    assert.equal(isOperatorOperatingUpdate(question), false);
    assert.equal(classifyCognitiveMode(question).kind, COGNITIVE_MODES.RECOMMENDATION);
  });

  it('keeps new investigation on Scout', () => {
    const question = 'Find additional property managers for Campaign 001.';
    assert.equal(isOperatorOperatingUpdate(question), false);
    assert.equal(shouldHandleScoutAcquisition({ question, context: { tenantId: '10' } }), true);
  });
});
