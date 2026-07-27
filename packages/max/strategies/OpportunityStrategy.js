'use strict';

const { STRATEGY_IDS } = require('../reasoning/ReasoningTypes');
const { strategyResult, confidenceFromEvidence } = require('./StrategyInterface');
const { matchSignals } = require('../context/ReasoningContextBuilder');

const POSITIVE = [
  'hiring',
  'expansion',
  'expanding',
  'new service',
  'new services',
  'growth',
  'opening',
  'opened',
  'new location',
  'scaling',
  'raising',
  'funded',
];
const NEGATIVE = [
  'downsizing',
  'layoffs',
  'closing',
  'closed location',
  'bankruptcy',
  'declining revenue',
];

/**
 * Opportunity Strategy — growth, hiring, expansion, new services.
 * Observations only; never invents facts.
 */
const OpportunityStrategy = Object.freeze({
  id: STRATEGY_IDS.OPPORTUNITY,
  name: 'Opportunity Strategy',
  /**
   * @param {import('../reasoning/ReasoningTypes').ReasoningContext} context
   */
  evaluate(context) {
    const pos = matchSignals(context, POSITIVE, 'supporting');
    const neg = matchSignals(context, NEGATIVE, 'contradicting');
    const scoreDelta = Math.min(100, pos.refs.length * 22) - Math.min(60, neg.refs.length * 25);
    const allForConfidence = [...pos.refs, ...neg.refs];
    return strategyResult({
      strategy: STRATEGY_IDS.OPPORTUNITY,
      scoreDelta,
      confidence: confidenceFromEvidence(allForConfidence, {
        base: allForConfidence.length === 0 ? 20 : 15,
      }),
      supportingEvidence: pos.refs,
      contradictingEvidence: neg.refs,
      claims: [...pos.claimIds, ...neg.claimIds],
      summary: `opportunity:support=${pos.refs.length};oppose=${neg.refs.length}`,
    });
  },
});

module.exports = { OpportunityStrategy };
