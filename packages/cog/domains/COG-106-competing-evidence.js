'use strict';

/**
 * COG-106 — Competing Evidence
 * Max weighs conflicting evidence rather than cherry-picking.
 */

/** @type {import('../types').CognitiveDomain} */
const COG_106 = Object.freeze({
  id: 'COG-106',
  shortName: 'Competing Evidence',
  objective: 'Verify Max weighs conflicting evidence and presents tension rather than cherry-picking a single signal.',
  conversation: {
    id: 'cog-106-evidence-v1',
    title: 'Conflicting signal synthesis',
    description: 'Operator presents competing evidence and asks for synthesis.',
    context: { tenantId: '10', page: 'command-deck' },
    turns: [
      { role: 'operator', content: 'Scout found 72 ICP-qualified prospects but only 3 have phone numbers. How should I read that?' },
      { role: 'operator', content: 'One signal says the list is strong. Another says outreach readiness is weak. What do you conclude?' },
      { role: 'operator', content: 'Which evidence is more decisive for the next action?' },
    ],
  },
  expectedBehaviors: [
    {
      id: '106-dual-signal',
      description: 'Max acknowledges both strong ICP count and weak contact coverage.',
      turnIndex: 0,
      checkType: 'pattern',
      pattern: [/72|qualif|phone|contact|enrich|coverage/i],
      failureCode: 'R-003',
      requiresHumanReview: true,
    },
    {
      id: '106-tension',
      description: 'Max names the tension between competing signals.',
      turnIndex: 1,
      checkType: 'pattern',
      pattern: [/however|but|tension|conflict|on one hand|while|weak|strong/i],
      failureCode: 'R-007',
      requiresHumanReview: true,
    },
    {
      id: '106-decisive-evidence',
      description: 'Max identifies which evidence is more decisive and why.',
      turnIndex: 2,
      checkType: 'pattern',
      pattern: [/decisive|priority|first|because|readiness|phone|contact/i],
      failureCode: 'R-007',
      requiresHumanReview: true,
    },
    {
      id: '106-no-cherry-pick',
      description: 'Max does not celebrate the 72 count while ignoring contact gaps.',
      turnIndex: 0,
      checkType: 'absence',
      absencePattern: [/great list|ready to outreach|start calling/i],
      failureCode: 'R-003',
      requiresHumanReview: true,
    },
  ],
  evaluationCriteria: [
    'Acknowledges all relevant signals',
    'Names tension between conflicting evidence',
    'Identifies decisive evidence for next action',
    'Does not cherry-pick the favorable signal',
  ],
  rubric: {
    domainId: 'COG-106',
    levels: [
      { score: 0, label: 'Cherry-picked', criteria: 'Single signal dominates; tension ignored' },
      { score: 5, label: 'Acknowledged', criteria: 'Both signals mentioned but weak synthesis' },
      { score: 10, label: 'Balanced', criteria: 'Clear tension analysis with decisive read' },
    ],
  },
});

module.exports = { COG_106 };
