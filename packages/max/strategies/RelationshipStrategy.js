'use strict';

const { STRATEGY_IDS, evidenceRef } = require('../reasoning/ReasoningTypes');
const { strategyResult, confidenceFromEvidence } = require('./StrategyInterface');
const { matchSignals } = require('../context/ReasoningContextBuilder');
const { EDGE_TYPES } = require('../../knowledge');

const POSITIVE = [
  'referral',
  'referred',
  'mutual',
  'previous work',
  'prior client',
  'existing relationship',
  'warm intro',
  'introduction',
];
const NEGATIVE = ['cold', 'no relationship', 'unknown contact', 'unrelated'];

/**
 * Relationship Strategy — existing relationships, referrals, mutual contacts, previous work.
 */
const RelationshipStrategy = Object.freeze({
  id: STRATEGY_IDS.RELATIONSHIP,
  name: 'Relationship Strategy',
  /**
   * @param {import('../reasoning/ReasoningTypes').ReasoningContext} context
   */
  evaluate(context) {
    const pos = matchSignals(context, POSITIVE, 'supporting');
    const neg = matchSignals(context, NEGATIVE, 'contradicting');
    const supporting = [...pos.refs];
    const contradicting = [...neg.refs];

    const knowsEdges = (context._signalIndex.edgeTypes || []).filter(
      (e) => e.type === EDGE_TYPES.KNOWS
    );
    for (const e of knowsEdges) {
      supporting.push(
        evidenceRef({
          id: e.id,
          kind: 'edge',
          summary: `KNOWS edge ${e.fromId}->${e.toId}`,
        })
      );
    }

    for (const related of context.relatedCompanies || []) {
      supporting.push(
        evidenceRef({
          id: related.id,
          kind: 'company',
          summary: `Related company in neighborhood: ${related.name || related.id}`,
        })
      );
    }

    if (supporting.length === 0 && contradicting.length === 0) {
      contradicting.push(
        evidenceRef({
          id: `metric:no_relationship_signal:${context.company.id}`,
          kind: 'metric',
          summary: 'no_relationship_signals_in_graph',
        })
      );
    }

    const scoreDelta =
      Math.min(90, supporting.length * 20) - Math.min(50, contradicting.length * 15);
    const all = [...supporting, ...contradicting];
    return strategyResult({
      strategy: STRATEGY_IDS.RELATIONSHIP,
      scoreDelta,
      confidence: confidenceFromEvidence(all, { base: 18, perItem: 14 }),
      supportingEvidence: supporting,
      contradictingEvidence: contradicting,
      claims: [...pos.claimIds, ...neg.claimIds],
      summary: `relationship:support=${supporting.length};oppose=${contradicting.length};knows=${knowsEdges.length}`,
    });
  },
});

module.exports = { RelationshipStrategy };
