'use strict';

/**
 * SPEC-125 — Cal call coaching specialist ownership stub.
 * Routes owned coaching requests directly; Cal agent integration is advisory-only.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleCalCoachingTurn(input = {}) {
  const question = String(input.question || '').trim();
  if (!question) return null;

  const prose =
    'Cal owns call coaching for this request. I can help you prepare a discovery call script, ' +
    'objection handling, and a concise opener — tell me the prospect context and call goal.';

  const structured = buildStructuredResponse({
    answer: prose,
    reasoning: ['SPEC-125 — Cal specialist owns call coaching requests.'],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: 0.85,
    nextInvestigations: [],
    recommendedActions: [{ id: 'continue', type: 'review', label: 'Continue' }],
    confidenceContributors: ['spec_125', 'cal_coaching'],
    timelineReferences: [],
    relatedEntities: [],
    metadata: {
      sourcesUsed: { briefing: false, reasoning: true, memory: false, policy: false, knowledge: false },
      evidenceCount: 0,
      asOf: new Date().toISOString(),
      unavailable: [],
      workspaceOwner: 'specialist_cal',
      specialist: 'cal',
    },
  });

  return {
    reason: 'cal_call_coaching',
    prose,
    structured,
  };
}

module.exports = {
  maybeHandleCalCoachingTurn,
};
