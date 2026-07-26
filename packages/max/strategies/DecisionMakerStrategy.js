'use strict';

const { STRATEGY_IDS, evidenceRef } = require('../reasoning/ReasoningTypes');
const { strategyResult, confidenceFromEvidence } = require('./StrategyInterface');

const DECISION_TITLES = [
  'owner',
  'founder',
  'co-founder',
  'ceo',
  'president',
  'principal',
  'partner',
  'managing director',
  'general manager',
  'decision maker',
  'decision-maker',
];
const WEAK_TITLES = ['intern', 'assistant', 'receptionist', 'unknown', 'staff'];

/**
 * Decision Maker Strategy — contact quality, role certainty, DM identification.
 */
const DecisionMakerStrategy = Object.freeze({
  id: STRATEGY_IDS.DECISION_MAKER,
  name: 'Decision Maker Strategy',
  /**
   * @param {import('../reasoning/ReasoningTypes').ReasoningContext} context
   */
  evaluate(context) {
    const people = context._signalIndex.people || [];
    const supporting = [];
    const contradicting = [];

    for (const p of people) {
      const title = p.titleLower || '';
      const hasEmail = Boolean(p.email);
      const isDm = DECISION_TITLES.some((t) => title.includes(t));
      const isWeak = WEAK_TITLES.some((t) => title.includes(t));

      if (isDm) {
        supporting.push(
          evidenceRef({
            id: p.id,
            kind: 'person',
            summary: `Decision-maker title="${p.title}" person=${p.id}`,
            confidence:
              p.metadata && typeof p.metadata.confidence === 'number'
                ? p.metadata.confidence
                : null,
          })
        );
      }
      if (hasEmail && isDm) {
        supporting.push(
          evidenceRef({
            id: `${p.id}:email`,
            kind: 'person',
            summary: `Reachable decision-maker email present person=${p.id}`,
          })
        );
      }
      if (isWeak) {
        contradicting.push(
          evidenceRef({
            id: p.id,
            kind: 'person',
            summary: `Weak contact title="${p.title}" person=${p.id}`,
          })
        );
      }
      if (!p.title) {
        contradicting.push(
          evidenceRef({
            id: `${p.id}:no_title`,
            kind: 'person',
            summary: `Missing title person=${p.id}`,
          })
        );
      }
    }

    if (people.length === 0) {
      contradicting.push(
        evidenceRef({
          id: `metric:no_people:${context.company.id}`,
          kind: 'metric',
          summary: 'no_people_linked_to_company',
        })
      );
    }

    const dmCount = supporting.filter((r) => r.summary.startsWith('Decision-maker')).length;
    const scoreDelta =
      Math.min(100, dmCount * 35 + (supporting.length - dmCount) * 10) -
      Math.min(60, contradicting.length * 12);

    const all = [...supporting, ...contradicting];
    return strategyResult({
      strategy: STRATEGY_IDS.DECISION_MAKER,
      scoreDelta,
      confidence: confidenceFromEvidence(all, {
        base: people.length === 0 ? 40 : 25,
        perItem: 15,
      }),
      supportingEvidence: supporting,
      contradictingEvidence: contradicting,
      claims: [],
      summary: `decision_maker:people=${people.length};dm_signals=${dmCount};oppose=${contradicting.length}`,
    });
  },
});

module.exports = { DecisionMakerStrategy };
