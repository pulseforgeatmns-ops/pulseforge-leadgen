'use strict';

/**
 * SPEC-109 — intent-bound response contracts at the shared classification layer.
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

describe('SPEC-109 shared classification', () => {
  it('binds each acceptance prompt to a distinct response contract', () => {
    const cases = [
      ['What outreach has already been sent?', CONTRACT_IDS.RETRIEVAL, COGNITIVE_MODES.RETRIEVAL],
      ['What have we completed recently?', CONTRACT_IDS.RETRIEVAL, COGNITIVE_MODES.RETRIEVAL],
      ['How is Anchor Cleaning doing?', CONTRACT_IDS.SUMMARY, COGNITIVE_MODES.RETRIEVAL],
      ['What should we do next?', CONTRACT_IDS.RECOMMENDATION, COGNITIVE_MODES.RECOMMENDATION],
      ["That's incorrect.", CONTRACT_IDS.CHALLENGE, COGNITIVE_MODES.EXPLANATION],
      ['Investigate commercial prospects.', CONTRACT_IDS.INVESTIGATION, COGNITIVE_MODES.INVESTIGATION],
    ];
    for (const [question, contractId, kind] of cases) {
      const mode = classifyCognitiveMode(question);
      const contract = selectResponseContract(question, mode);
      assert.equal(mode.kind, kind, question);
      assert.equal(contract.id, contractId, question);
    }
    assert.equal(listResponseContracts().length, 9);
  });

  it('does not send retrieval or summary to CIE or specialists', () => {
    for (const question of [
      'What outreach has already been sent?',
      'What have we completed recently?',
      'How is Anchor Cleaning doing?',
    ]) {
      assert.equal(shouldInvokeSpecialist(question), false, question);
      assert.equal(
        shouldClaimClientIntelligenceTurn(question, null, { approvedBlueprint: true }),
        false,
        question
      );
    }
  });

  it('keeps investigation off the CIE advisory path', () => {
    const question = 'Investigate commercial prospects.';
    const contract = selectResponseContract(question);
    assert.equal(contract.id, CONTRACT_IDS.INVESTIGATION);
    assert.equal(shouldHandleScoutAcquisition({ question, context: { tenantId: '10' } }), true);
    assert.equal(
      shouldClaimClientIntelligenceTurn(question, { context: { responseContract: contract } }, {
        approvedBlueprint: true,
      }),
      false
    );
  });
});
