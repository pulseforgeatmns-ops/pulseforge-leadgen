'use strict';

/**
 * COG-104 — Counterfactual Reasoning
 * Max maintains and reasons about alternative scenarios.
 */

/** @type {import('../types').CognitiveDomain} */
const COG_104 = Object.freeze({
  id: 'COG-104',
  shortName: 'Counterfactual Reasoning',
  objective: 'Verify Max can explore what-if scenarios without collapsing to a single deterministic path.',
  conversation: {
    id: 'cog-104-counterfactual-v1',
    title: 'Alternative scenario exploration',
    description: 'Operator asks Max to compare paths and explore counterfactuals.',
    context: { tenantId: '10', page: 'command-deck' },
    turns: [
      { role: 'operator', content: 'We are considering direct mail instead of cold email for law firms. What would change?' },
      { role: 'operator', content: 'What if reply rates on email were half what we expect — would your recommendation change?' },
      { role: 'operator', content: 'Compare the two paths side by side: email-first vs direct-mail-first.' },
    ],
  },
  expectedBehaviors: [
    {
      id: '104-scenario-delta',
      description: 'Max identifies what would change under direct mail vs email.',
      turnIndex: 0,
      checkType: 'pattern',
      pattern: [/direct mail|mail|email|cost|timeline|response|channel/i],
      failureCode: 'R-004',
      requiresHumanReview: true,
    },
    {
      id: '104-conditional-revision',
      description: 'Max adjusts recommendation under the counterfactual condition.',
      turnIndex: 1,
      checkType: 'pattern',
      pattern: [/if|would|change|recommend|shift|instead|lower|half/i],
      failureCode: 'R-004',
      requiresHumanReview: true,
    },
    {
      id: '104-side-by-side',
      description: 'Max compares both paths rather than picking one silently.',
      turnIndex: 2,
      checkType: 'pattern',
      pattern: [/email|direct mail|vs|versus|compare|trade/i],
      failureCode: 'R-004',
    },
    {
      id: '104-no-collapse',
      description: 'Max does not refuse to engage with the counterfactual.',
      turnIndex: 1,
      checkType: 'absence',
      absencePattern: [/cannot answer|impossible to say|no way to know without trying/i],
      failureCode: 'R-004',
      requiresHumanReview: true,
    },
  ],
  evaluationCriteria: [
    'Identifies deltas between alternative approaches',
    'Revises recommendations under stated counterfactuals',
    'Compares paths side by side when asked',
    'Does not collapse or refuse reasonable what-if exploration',
  ],
  rubric: {
    domainId: 'COG-104',
    levels: [
      { score: 0, label: 'Collapsed', criteria: 'Single path only; counterfactuals ignored' },
      { score: 5, label: 'Partial', criteria: 'Acknowledges alternatives but weak comparison' },
      { score: 10, label: 'Exploratory', criteria: 'Clear counterfactual reasoning with tradeoffs' },
    ],
  },
});

module.exports = { COG_104 };
