'use strict';

const { STRATEGY_IDS } = require('../reasoning/ReasoningTypes');
const { strategyResult, confidenceFromEvidence } = require('./StrategyInterface');
const { matchSignals } = require('../context/ReasoningContextBuilder');

const POSITIVE = [
  'hiring velocity',
  'hiring rapidly',
  'vendor request',
  'looking for vendor',
  'rfp',
  'service demand',
  'overflow',
  'understaffed',
  'capacity issue',
  'operational strain',
  'too busy',
  'backlog',
  "can't keep up",
  'cannot keep up',
];
const NEGATIVE = [
  'fully staffed',
  'no need',
  'slow season',
  'low demand',
  'overcapacity',
];

/**
 * Overflow Strategy — hiring velocity, vendor requests, service demand, operational strain.
 */
const OverflowStrategy = Object.freeze({
  id: STRATEGY_IDS.OVERFLOW,
  name: 'Overflow Strategy',
  /**
   * @param {import('../reasoning/ReasoningTypes').ReasoningContext} context
   */
  evaluate(context) {
    const pos = matchSignals(context, POSITIVE, 'supporting');
    const neg = matchSignals(context, NEGATIVE, 'contradicting');
    const scoreDelta =
      Math.min(100, pos.refs.length * 28) - Math.min(50, neg.refs.length * 20);
    const all = [...pos.refs, ...neg.refs];
    return strategyResult({
      strategy: STRATEGY_IDS.OVERFLOW,
      scoreDelta,
      confidence: confidenceFromEvidence(all, {
        base: all.length === 0 ? 18 : 15,
        perItem: 16,
      }),
      supportingEvidence: pos.refs,
      contradictingEvidence: neg.refs,
      claims: [...pos.claimIds, ...neg.claimIds],
      summary: `overflow:support=${pos.refs.length};oppose=${neg.refs.length}`,
    });
  },
});

module.exports = { OverflowStrategy };
