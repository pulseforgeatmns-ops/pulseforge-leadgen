'use strict';

/**
 * COG-103 — Assumption Extraction
 * Max surfaces implicit assumptions before proceeding.
 */

/** @type {import('../types').CognitiveDomain} */
const COG_103 = Object.freeze({
  id: 'COG-103',
  shortName: 'Assumption Extraction',
  objective: 'Verify Max identifies and surfaces implicit assumptions rather than treating them as established facts.',
  conversation: {
    id: 'cog-103-assumptions-v1',
    title: 'Surfacing hidden assumptions',
    description: 'Operator asks Max to reason under incomplete information.',
    context: { tenantId: '10', page: 'command-deck' },
    turns: [
      { role: 'operator', content: 'Should we prioritize law firms or accounting practices for Anchor Cleaning this quarter?' },
      { role: 'operator', content: 'What assumptions are you making to answer that?' },
      { role: 'operator', content: 'Which of those assumptions are verified vs unverified?' },
    ],
  },
  expectedBehaviors: [
    {
      id: '103-qualified-answer',
      description: 'Initial answer acknowledges uncertainty or conditional reasoning.',
      turnIndex: 0,
      checkType: 'pattern',
      pattern: [/assum|if |depends|without|uncertain|don't know|unknown|need.*(data|evidence)/i],
      failureCode: 'R-003',
      requiresHumanReview: true,
    },
    {
      id: '103-explicit-assumptions',
      description: 'Max lists explicit assumptions when asked.',
      turnIndex: 1,
      checkType: 'pattern',
      pattern: [/assum/i],
      failureCode: 'R-003',
    },
    {
      id: '103-verify-separation',
      description: 'Max separates verified from unverified assumptions.',
      turnIndex: 2,
      checkType: 'pattern',
      pattern: [/verif|unverified|unknown|confirmed|not confirmed|evidence/i],
      failureCode: 'R-003',
      requiresHumanReview: true,
    },
    {
      id: '103-no-fabrication',
      description: 'Max does not invent conversion rates or market data as fact.',
      turnIndex: 0,
      checkType: 'absence',
      absencePattern: [/definitely|guaranteed|\d+% conversion/i],
      failureCode: 'R-003',
    },
  ],
  evaluationCriteria: [
    'Surfaces assumptions before committing to recommendations',
    'Lists assumptions explicitly when prompted',
    'Distinguishes verified from unverified assumptions',
    'Does not treat assumptions as operating facts',
  ],
  rubric: {
    domainId: 'COG-103',
    levels: [
      { score: 0, label: 'Blind', criteria: 'Proceeds without surfacing assumptions' },
      { score: 5, label: 'Partial', criteria: 'Some assumptions named but not separated from facts' },
      { score: 10, label: 'Explicit', criteria: 'Clear assumption inventory with verification status' },
    ],
  },
});

module.exports = { COG_103 };
