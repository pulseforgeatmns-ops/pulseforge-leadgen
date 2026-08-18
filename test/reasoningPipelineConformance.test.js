'use strict';

/**
 * PILOT-0 AUDIT-001 — shared classification layer.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  bindGovernedReasoning,
  COMPOSER_ID,
} = require('../packages/max/workspace/ReasoningPipeline');
const { CONTRACT_IDS } = require('../packages/max/workspace/ResponseContract');
const { OPERATOR_INTENTS } = require('../packages/max/workspace/OperatorIntentRegistry');
const { shouldClaimClientIntelligenceTurn } = require('../packages/max/workspace/ClientIntelligenceContext');
const training = require('../packages/max/training');

describe('AUDIT-001 shared classification', () => {
  it('binds the audit prompt table to analysis modes', () => {
    const cases = [
      ['How is Anchor doing?', CONTRACT_IDS.SUMMARY, OPERATOR_INTENTS.SUMMARY],
      ['What should I do next?', CONTRACT_IDS.RECOMMENDATION, OPERATOR_INTENTS.RECOMMENDATION],
      ["What's preventing growth?", CONTRACT_IDS.DIAGNOSIS, OPERATOR_INTENTS.DIAGNOSIS],
      ["What don't we know?", CONTRACT_IDS.UNKNOWN_ANALYSIS, OPERATOR_INTENTS.UNKNOWN_ANALYSIS],
      ["What's risky?", CONTRACT_IDS.RISK, OPERATOR_INTENTS.RISK],
      ['What outreach has been sent?', CONTRACT_IDS.RETRIEVAL, OPERATOR_INTENTS.RETRIEVAL],
      ['Should Scout investigate?', CONTRACT_IDS.INVESTIGATION, OPERATOR_INTENTS.INVESTIGATION],
    ];
    for (const [prompt, contractId, intent] of cases) {
      const bound = bindGovernedReasoning(prompt);
      assert.equal(bound.contract.id, contractId, prompt);
      assert.equal(bound.analysis.intent, intent, prompt);
      assert.equal(bound.composer, COMPOSER_ID, prompt);
      assert.equal(
        shouldClaimClientIntelligenceTurn(prompt, null, { approvedBlueprint: true }),
        false,
        prompt
      );
    }
  });
});

describe('AUDIT-001 competency registry', () => {
  it('registers reasoning_pipeline_conformance as graduated', () => {
    const competency = training.getCompetency('reasoning_pipeline_conformance');
    assert.ok(competency);
    assert.equal(competency.stage, training.STAGES.GRADUATED);
    assert.ok(competency.specRefs.includes('SPEC-112'));
  });
});
