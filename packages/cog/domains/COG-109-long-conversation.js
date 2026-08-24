'use strict';

/**
 * COG-109 — Long Conversation
 * Max maintains coherence over extended multi-turn dialogue.
 */

/** @type {import('../types').CognitiveDomain} */
const COG_109 = Object.freeze({
  id: 'COG-109',
  shortName: 'Long Conversation',
  objective: 'Verify Max maintains reasoning coherence and proposition tracking over an extended conversation (8+ turns).',
  conversation: {
    id: 'cog-109-long-v1',
    title: 'Extended conversation coherence',
    description: 'Multi-phase dialogue establishing context, planning, challenge, and recall.',
    context: { tenantId: '10', page: 'command-deck' },
    turns: [
      { role: 'operator', content: 'Our Q3 goal is 20 commercial clients for Anchor Cleaning. Acknowledge.' },
      { role: 'operator', content: 'Current focus vertical is law firms in Greater Manchester. Acknowledge.' },
      { role: 'operator', content: 'Constraint: no outbound email until operator approves the daily send plan. Acknowledge.' },
      { role: 'operator', content: 'Given those three facts, what is the first investigation step?' },
      { role: 'operator', content: 'What is our Q3 goal again?' },
      { role: 'operator', content: 'What vertical are we focused on?' },
      { role: 'operator', content: 'Can we send cold email today?' },
      { role: 'operator', content: 'Summarize everything we established in this conversation.' },
    ],
  },
  expectedBehaviors: [
    {
      id: '109-ack-q3',
      description: 'Max acknowledges Q3 goal of 20 commercial clients.',
      turnIndex: 0,
      checkType: 'pattern',
      pattern: [/20|commercial|Q3|acknowledge/i],
      failureCode: 'R-005',
    },
    {
      id: '109-ack-vertical',
      description: 'Max acknowledges law firms in Greater Manchester.',
      turnIndex: 1,
      checkType: 'pattern',
      pattern: [/law firm|Manchester|acknowledge/i],
      failureCode: 'R-005',
    },
    {
      id: '109-ack-constraint',
      description: 'Max acknowledges email approval constraint.',
      turnIndex: 2,
      checkType: 'pattern',
      pattern: [/approv|email|constraint|acknowledge/i],
      failureCode: 'R-005',
    },
    {
      id: '109-recall-q3',
      description: 'Max recalls Q3 goal in turn 5.',
      turnIndex: 4,
      checkType: 'continuity',
      propositionKey: 'goal:20_commercial',
      pattern: [/20|commercial/i],
      failureCode: 'R-005',
    },
    {
      id: '109-recall-vertical',
      description: 'Max recalls vertical focus in turn 6.',
      turnIndex: 5,
      checkType: 'continuity',
      propositionKey: 'vertical:law_firms',
      pattern: [/law firm/i],
      failureCode: 'R-005',
    },
    {
      id: '109-recall-constraint',
      description: 'Max recalls email constraint in turn 7.',
      turnIndex: 6,
      checkType: 'continuity',
      propositionKey: 'constraint:email_approval',
      pattern: [/approv|not.*(yet|today)|cannot send/i],
      failureCode: 'R-005',
    },
    {
      id: '109-final-summary',
      description: 'Final summary includes all three established facts.',
      turnIndex: 7,
      checkType: 'pattern',
      pattern: [/20|law firm|approv/i],
      failureCode: 'R-005',
      requiresHumanReview: true,
    },
  ],
  evaluationCriteria: [
    'Acknowledges each established fact explicitly',
    'Recalls facts correctly mid-conversation',
    'Maintains constraints through later turns',
    'Produces accurate end-of-conversation summary',
  ],
  rubric: {
    domainId: 'COG-109',
    levels: [
      { score: 0, label: 'Incoherent', criteria: 'Facts lost by mid-conversation' },
      { score: 5, label: 'Degrading', criteria: 'Early recall ok; summary incomplete' },
      { score: 10, label: 'Coherent', criteria: 'All facts stable through 8 turns' },
    ],
  },
});

module.exports = { COG_109 };
