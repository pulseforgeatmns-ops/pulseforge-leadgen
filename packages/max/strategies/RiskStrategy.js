'use strict';

const { STRATEGY_IDS, evidenceRef } = require('../reasoning/ReasoningTypes');
const { strategyResult, confidenceFromEvidence } = require('./StrategyInterface');
const { matchSignals, daysSince } = require('../context/ReasoningContextBuilder');

const RISK_KEYWORDS = [
  'declined',
  'decline',
  'existing vendor',
  'vendor contract',
  'under contract',
  'do not contact',
  'dnc',
  'unsubscribe',
  'hostile',
  'angry',
  'lawsuit',
  'complaint',
  'low confidence',
  'not interested',
  'stop contacting',
];
const MITIGATING = ['open to change', 'contract ending', 'renewal window', 're-engage'];

/**
 * Risk Strategy — negative responses, existing contracts, inactivity, low confidence.
 * scoreDelta is negative when risks are present (opposes pursuit).
 */
const RiskStrategy = Object.freeze({
  id: STRATEGY_IDS.RISK,
  name: 'Risk Strategy',
  /**
   * @param {import('../reasoning/ReasoningTypes').ReasoningContext} context
   */
  evaluate(context) {
    const risks = matchSignals(context, RISK_KEYWORDS, 'contradicting');
    const mitigating = matchSignals(context, MITIGATING, 'supporting');
    const supporting = [...mitigating.refs]; // mitigating risk = supporting pursuit
    const contradicting = [...risks.refs];

    // Low-confidence claims about subject
    for (const c of context.claims || []) {
      const conf = c.confidence == null ? null : Number(c.confidence);
      if (conf != null && conf < 0.4 && (c.status || 'active') === 'active') {
        contradicting.push(
          evidenceRef({
            id: c.id,
            kind: 'claim',
            summary: `Low-confidence claim confidence=${conf}`,
            confidence: conf,
          })
        );
      }
    }

    // Inactivity from interactions
    const interactions = context._signalIndex.interactionTexts || [];
    let lastDays = null;
    for (const i of interactions) {
      const d = daysSince(i.occurredAt, context.builtAt);
      if (d != null && (lastDays == null || d < lastDays)) lastDays = d;
    }
    if (lastDays != null && lastDays >= 180) {
      contradicting.push(
        evidenceRef({
          id: `metric:inactivity:${context.company.id}`,
          kind: 'metric',
          summary: `inactivity_days=${Math.round(lastDays)}`,
        })
      );
    }

    // Negative interaction actions
    for (const i of interactions) {
      if (['bounce', 'unsubscribe', 'spam', 'declined', 'negative'].includes(i.actionType)) {
        contradicting.push(
          evidenceRef({
            id: i.id,
            kind: 'interaction',
            summary: `Risk interaction action=${i.actionType}`,
          })
        );
      }
    }

    // Higher risk → more negative scoreDelta
    const riskPenalty = Math.min(100, contradicting.length * 22);
    const mitigation = Math.min(40, supporting.length * 15);
    const scoreDelta = -riskPenalty + mitigation;

    const all = [...supporting, ...contradicting];
    return strategyResult({
      strategy: STRATEGY_IDS.RISK,
      scoreDelta,
      confidence: confidenceFromEvidence(all, {
        base: all.length === 0 ? 30 : 20,
        perItem: 12,
      }),
      supportingEvidence: supporting,
      contradictingEvidence: contradicting,
      claims: [...risks.claimIds, ...mitigating.claimIds],
      summary: `risk:risks=${contradicting.length};mitigations=${supporting.length}`,
    });
  },
});

module.exports = { RiskStrategy };
