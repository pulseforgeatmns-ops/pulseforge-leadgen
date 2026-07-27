'use strict';

const { STRATEGY_IDS, evidenceRef, asLower } = require('../reasoning/ReasoningTypes');
const { strategyResult, confidenceFromEvidence } = require('./StrategyInterface');
const { matchSignals } = require('../context/ReasoningContextBuilder');

const POSITIVE = [
  'automation',
  'crm',
  'software',
  'platform',
  'tech stack',
  'saas',
  'digital',
  'online booking',
  'guesty',
  'jobber',
  'housecall',
  'service titan',
];
const NEGATIVE = [
  'no website',
  'outdated',
  'paper-based',
  'no software',
  'technophobic',
  'manual only',
];

/**
 * Technology Strategy — software detected, platform maturity, automation fit.
 */
const TechnologyStrategy = Object.freeze({
  id: STRATEGY_IDS.TECHNOLOGY,
  name: 'Technology Strategy',
  /**
   * @param {import('../reasoning/ReasoningTypes').ReasoningContext} context
   */
  evaluate(context) {
    const pos = matchSignals(context, POSITIVE, 'supporting');
    const neg = matchSignals(context, NEGATIVE, 'contradicting');
    const supporting = [...pos.refs];
    const contradicting = [...neg.refs];

    const meta = context._signalIndex.companyMeta || {};
    const techFields = [
      meta.technology,
      meta.tech_stack,
      meta.techStack,
      context.company && context.company.technology,
    ];
    for (let i = 0; i < techFields.length; i += 1) {
      const value = techFields[i];
      if (value == null || value === '') continue;
      const text = Array.isArray(value) ? value.join(',') : String(value);
      if (!text.trim()) continue;
      supporting.push(
        evidenceRef({
          id: `meta:technology:${context.company.id}:${i}`,
          kind: 'company',
          summary: `Detected technology field=${text}`,
          confidence: typeof meta.confidence === 'number' ? meta.confidence : null,
        })
      );
    }

    const website = meta.website;
    if (website && asLower(website) !== 'pending_build') {
      supporting.push(
        evidenceRef({
          id: `meta:website:${context.company.id}`,
          kind: 'company',
          summary: `Website present=${website}`,
        })
      );
    } else if (website && asLower(website) === 'pending_build') {
      contradicting.push(
        evidenceRef({
          id: `meta:website_pending:${context.company.id}`,
          kind: 'company',
          summary: 'Website pending_build',
        })
      );
    }

    const scoreDelta =
      Math.min(90, supporting.length * 18) - Math.min(50, contradicting.length * 18);
    const all = [...supporting, ...contradicting];
    return strategyResult({
      strategy: STRATEGY_IDS.TECHNOLOGY,
      scoreDelta,
      confidence: confidenceFromEvidence(all, {
        base: all.length === 0 ? 22 : 15,
        perItem: 14,
      }),
      supportingEvidence: supporting,
      contradictingEvidence: contradicting,
      claims: [...pos.claimIds, ...neg.claimIds],
      summary: `technology:support=${supporting.length};oppose=${contradicting.length}`,
    });
  },
});

module.exports = { TechnologyStrategy };
