'use strict';

/**
 * Shared fixtures for policy tests.
 */

function recommendation(overrides = {}) {
  return {
    id: overrides.id || 'rec:10:co-1',
    subject: {
      id: overrides.companyId || 'co-1',
      name: overrides.name || 'Acme',
      type: 'company',
    },
    type: overrides.type || 'follow_up',
    priority: overrides.priority || 'high',
    score: overrides.score == null ? 72 : overrides.score,
    confidence: overrides.confidence == null ? 80 : overrides.confidence,
    recommendedAction: overrides.recommendedAction || 'follow_up_outreach',
    supportingSignals: overrides.supportingSignals || [
      {
        kind: 'evidence',
        id: 'ev-1',
        summary: 'Decision-maker: Owner',
        occurredAt: overrides.evidenceAt || '2026-07-20T12:00:00.000Z',
      },
    ],
    opposingSignals: overrides.opposingSignals || [],
    claims: overrides.claims || ['claim-1'],
    evidence: overrides.evidence || ['ev-1'],
    reasoningSummary: {
      whyThis: ['signal'],
      whyNow: ['now'],
      whyNot: [],
      confidenceBasis: [],
    },
    channel: overrides.channel,
    risk: overrides.risk,
  };
}

const AS_OF = '2026-07-26T12:00:00.000Z'; // Sunday
const AS_OF_MONDAY = '2026-07-20T12:00:00.000Z'; // Monday

module.exports = {
  recommendation,
  AS_OF,
  AS_OF_MONDAY,
};
