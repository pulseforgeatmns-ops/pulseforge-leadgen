'use strict';

/**
 * COG-102 — Conversation Continuity
 * Max maintains proposition continuity across multi-turn dialogue.
 */

/** @type {import('../types').CognitiveDomain} */
const COG_102 = Object.freeze({
  id: 'COG-102',
  shortName: 'Conversation Continuity',
  objective: 'Verify Max tracks established propositions across turns without silent drift or conversation reset.',
  conversation: {
    id: 'cog-102-continuity-v1',
    title: 'Proposition continuity across turns',
    description: 'Operator establishes facts, then probes continuity.',
    context: { tenantId: '10', page: 'command-deck' },
    turns: [
      { role: 'operator', content: 'Assume our primary bottleneck is prospect quality, not outreach volume. Acknowledge that.' },
      { role: 'operator', content: 'Given that bottleneck, what should we investigate first?' },
      { role: 'operator', content: 'You mentioned a bottleneck earlier — what was it?' },
    ],
  },
  expectedBehaviors: [
    {
      id: '102-acknowledge-bottleneck',
      description: 'Max acknowledges prospect quality as the stated bottleneck.',
      turnIndex: 0,
      checkType: 'pattern',
      pattern: [/prospect quality|quality.*bottleneck|acknowledge/i],
      failureCode: 'R-005',
    },
    {
      id: '102-reason-from-bottleneck',
      description: 'Max reasons from quality bottleneck, not volume, in turn 2.',
      turnIndex: 1,
      checkType: 'pattern',
      pattern: [/quality|scoring|ICP|qualification|fit/i],
      failureCode: 'R-002',
      requiresHumanReview: true,
    },
    {
      id: '102-recall-bottleneck',
      description: 'Max correctly recalls the bottleneck stated in turn 1.',
      turnIndex: 2,
      checkType: 'continuity',
      propositionKey: 'bottleneck:prospect_quality',
      pattern: [/prospect quality|quality.*not.*volume/i],
      failureCode: 'R-005',
    },
    {
      id: '102-no-volume-pivot',
      description: 'Max does not silently pivot to volume as the bottleneck.',
      turnIndex: 1,
      checkType: 'absence',
      absencePattern: [/send more|increase volume|more emails as the (main|primary) fix/i],
      failureCode: 'R-002',
    },
  ],
  evaluationCriteria: [
    'Acknowledges operator-stated propositions explicitly',
    'Reasons from established context in subsequent turns',
    'Recalls prior propositions when asked',
    'Does not silently shift the framing without revision',
  ],
  rubric: {
    domainId: 'COG-102',
    levels: [
      { score: 0, label: 'Reset', criteria: 'Each turn answered in isolation' },
      { score: 5, label: 'Partial recall', criteria: 'Some continuity but drift on key proposition' },
      { score: 10, label: 'Continuous', criteria: 'Stable propositions across all turns' },
    ],
  },
});

module.exports = { COG_102 };
