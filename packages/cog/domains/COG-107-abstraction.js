'use strict';

/**
 * COG-107 — Abstraction
 * Max moves between concrete facts and abstract conclusions.
 */

/** @type {import('../types').CognitiveDomain} */
const COG_107 = Object.freeze({
  id: 'COG-107',
  shortName: 'Abstraction',
  objective: 'Verify Max synthesizes concrete facts into abstract conclusions and can drill back down to specifics.',
  conversation: {
    id: 'cog-107-abstraction-v1',
    title: 'Concrete-to-abstract reasoning',
    description: 'Operator asks for synthesis then requests supporting detail.',
    context: { tenantId: '10', page: 'command-deck' },
    turns: [
      { role: 'operator', content: 'How is Anchor Cleaning doing overall?' },
      { role: 'operator', content: 'Summarize that in one sentence — what is the headline?' },
      { role: 'operator', content: 'Now give me the three specific facts that support that headline.' },
    ],
  },
  expectedBehaviors: [
    {
      id: '107-synthesis',
      description: 'Max provides intelligence synthesis, not raw inventory dump.',
      turnIndex: 0,
      checkType: 'pattern',
      pattern: [/bottleneck|progress|risk|readiness|unknown|focus|stage/i],
      failureCode: 'R-001',
      requiresHumanReview: true,
    },
    {
      id: '107-headline',
      description: 'Max produces a concise abstract headline.',
      turnIndex: 1,
      checkType: 'abstraction',
      pattern: /^.{10,200}$/,
      failureCode: 'R-001',
      requiresHumanReview: true,
    },
    {
      id: '107-grounding',
      description: 'Max grounds the headline in specific supporting facts.',
      turnIndex: 2,
      checkType: 'pattern',
      pattern: [/\d+|prospect|scout|phone|email|blueprint|aim/i],
      failureCode: 'R-001',
      requiresHumanReview: true,
    },
    {
      id: '107-no-inventory-dump',
      description: 'Initial answer is not a raw count list without conclusion.',
      turnIndex: 0,
      checkType: 'absence',
      absencePattern: [/^\s*\d+\s+prospects?\s*,\s*\d+/i],
      failureCode: 'R-001',
      requiresHumanReview: true,
    },
  ],
  evaluationCriteria: [
    'Synthesizes concrete data into intelligence',
    'Produces concise abstract headlines on request',
    'Grounds abstractions in specific supporting facts',
    'Avoids inventory-as-answer without conclusions',
  ],
  rubric: {
    domainId: 'COG-107',
    levels: [
      { score: 0, label: 'Concrete only', criteria: 'Raw counts without synthesis' },
      { score: 5, label: 'Partial', criteria: 'Some synthesis but weak drill-down' },
      { score: 10, label: 'Fluid', criteria: 'Clean abstraction with grounded specifics' },
    ],
  },
});

module.exports = { COG_107 };
