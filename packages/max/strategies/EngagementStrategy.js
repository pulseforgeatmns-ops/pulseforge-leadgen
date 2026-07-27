'use strict';

const { STRATEGY_IDS, evidenceRef } = require('../reasoning/ReasoningTypes');
const { strategyResult, confidenceFromEvidence } = require('./StrategyInterface');
const { daysSince } = require('../context/ReasoningContextBuilder');

const POSITIVE_ACTIONS = new Set([
  'reply',
  'replied',
  'open',
  'opened',
  'click',
  'clicked',
  'interested',
  'meeting',
  'booked',
]);
const NEGATIVE_ACTIONS = new Set([
  'bounce',
  'bounced',
  'unsubscribe',
  'unsubscribed',
  'spam',
  'negative',
  'declined',
  'decline',
  'not_now',
]);

/**
 * Engagement Strategy — outreach history, replies, opens, time since contact.
 */
const EngagementStrategy = Object.freeze({
  id: STRATEGY_IDS.ENGAGEMENT,
  name: 'Engagement Strategy',
  /**
   * @param {import('../reasoning/ReasoningTypes').ReasoningContext} context
   */
  evaluate(context) {
    const index = context._signalIndex;
    const supporting = [];
    const contradicting = [];
    let lastContactDays = null;

    for (const i of index.interactionTexts) {
      const days = daysSince(i.occurredAt, context.builtAt);
      if (days != null && (lastContactDays == null || days < lastContactDays)) {
        lastContactDays = days;
      }
      if (POSITIVE_ACTIONS.has(i.actionType) || POSITIVE_ACTIONS.has(i.text.split(/\s+/)[0])) {
        supporting.push(
          evidenceRef({
            id: i.id,
            kind: 'interaction',
            summary: `Positive engagement action=${i.actionType}`,
          })
        );
      }
      if (NEGATIVE_ACTIONS.has(i.actionType)) {
        contradicting.push(
          evidenceRef({
            id: i.id,
            kind: 'interaction',
            summary: `Negative engagement action=${i.actionType}`,
          })
        );
      }
    }

    if (lastContactDays != null) {
      const ref = evidenceRef({
        id: `metric:days_since_contact:${context.company.id}`,
        kind: 'metric',
        summary: `days_since_contact=${Math.round(lastContactDays)}`,
      });
      if (lastContactDays <= 14) supporting.push(ref);
      else if (lastContactDays >= 90) contradicting.push(ref);
    }

    if (index.interactionTexts.length === 0) {
      contradicting.push(
        evidenceRef({
          id: `metric:no_interactions:${context.company.id}`,
          kind: 'metric',
          summary: 'no_interaction_history',
        })
      );
    }

    let scoreDelta =
      Math.min(80, supporting.length * 18) - Math.min(70, contradicting.length * 20);
    if (lastContactDays != null && lastContactDays <= 7) scoreDelta += 10;
    if (lastContactDays != null && lastContactDays >= 120) scoreDelta -= 15;

    const all = [...supporting, ...contradicting];
    return strategyResult({
      strategy: STRATEGY_IDS.ENGAGEMENT,
      scoreDelta,
      confidence: confidenceFromEvidence(all, {
        base: index.interactionTexts.length === 0 ? 35 : 20,
        perItem: 10,
      }),
      supportingEvidence: supporting,
      contradictingEvidence: contradicting,
      claims: [],
      summary: `engagement:support=${supporting.length};oppose=${contradicting.length};days_since=${
        lastContactDays == null ? 'null' : Math.round(lastContactDays)
      }`,
    });
  },
});

module.exports = { EngagementStrategy };
