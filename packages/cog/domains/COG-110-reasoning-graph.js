'use strict';

/**
 * COG-110 — Reasoning Graph Evolution
 * Max builds and evolves a reasoning graph across turns.
 */

/** @type {import('../types').CognitiveDomain} */
const COG_110 = Object.freeze({
  id: 'COG-110',
  shortName: 'Reasoning Graph Evolution',
  objective: 'Verify Max builds connected reasoning (claims → evidence → conclusions) that evolves when new information arrives.',
  conversation: {
    id: 'cog-110-graph-v1',
    title: 'Reasoning graph evolution',
    description: 'Operator builds a reasoning chain, adds evidence, and asks Max to update the graph.',
    context: { tenantId: '10', page: 'command-deck' },
    turns: [
      { role: 'operator', content: 'Hypothesis: law firms are the best beachhead because they have stable office leases. Evaluate that claim.' },
      { role: 'operator', content: 'New evidence: our top 5 scored prospects are all accounting practices, not law firms. Update your reasoning.' },
      { role: 'operator', content: 'Show the reasoning chain: claim → evidence → conclusion.' },
      { role: 'operator', content: 'What node in that chain is weakest?' },
    ],
  },
  expectedBehaviors: [
    {
      id: '110-evaluate-claim',
      description: 'Max evaluates the hypothesis with evidence requirements.',
      turnIndex: 0,
      checkType: 'graph',
      pattern: [/claim|hypothesis|evidence|lease|verify|need|assum/i],
      failureCode: 'R-002',
      requiresHumanReview: true,
    },
    {
      id: '110-graph-update',
      description: 'Max updates reasoning when contradictory evidence arrives.',
      turnIndex: 1,
      checkType: 'revision',
      pattern: [/update|revise|accounting|contradict|weaker|re-evaluat|shift/i],
      failureCode: 'R-002',
      requiresHumanReview: true,
    },
    {
      id: '110-explicit-chain',
      description: 'Max presents an explicit claim → evidence → conclusion chain.',
      turnIndex: 2,
      checkType: 'graph',
      pattern: [/claim|evidence|conclusion|because|therefore|→|->/i],
      failureCode: 'R-002',
      requiresHumanReview: true,
    },
    {
      id: '110-weakest-node',
      description: 'Max identifies the weakest link in the reasoning chain.',
      turnIndex: 3,
      checkType: 'graph',
      pattern: [/weak|uncertain|assum|unverified|gap|weakest|least/i],
      failureCode: 'R-003',
      requiresHumanReview: true,
    },
  ],
  evaluationCriteria: [
    'Evaluates hypotheses with evidence requirements',
    'Updates reasoning graph when new evidence arrives',
    'Presents explicit reasoning chains',
    'Identifies weakest nodes in the graph',
  ],
  rubric: {
    domainId: 'COG-110',
    levels: [
      { score: 0, label: 'Flat', criteria: 'No connected reasoning; isolated answers' },
      { score: 5, label: 'Partial graph', criteria: 'Some links but no update on new evidence' },
      { score: 10, label: 'Evolving', criteria: 'Explicit graph that updates with evidence' },
    ],
  },
});

module.exports = { COG_110 };
