'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  snapshotMetrics,
  extractCoverageSummary,
  extractEvidenceGaps,
} = require('../scripts/continueAnchorStrScoutInvestigation');
const { EXECUTION_INTENTS, OPERATOR_DECISION_KINDS } = require('../packages/acquisition-mission');

describe('continueAnchorStrScoutInvestigation helpers', () => {
  it('extractCoverageSummary reads city coverage fields', () => {
    const summary = extractCoverageSummary({
      coverage: {
        complete: false,
        cities: { searched: 2, planned: 6, names: ['Manchester', 'Bedford'] },
        warnings: ['Only 2 / 6 cities searched.'],
      },
    });
    assert.equal(summary.searched, 2);
    assert.equal(summary.planned, 6);
    assert.deepEqual(summary.names, ['Manchester', 'Bedford']);
    assert.equal(summary.complete, false);
  });

  it('snapshotMetrics flags pending investigation and candidate count', () => {
    const metrics = snapshotMetrics({
      mission: {
        id: 'mission_82e8102f-249c-4f44-b88e-2de76b13898e',
        stage: 'discover',
        pendingOperatorDecision: {
          kind: OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION,
          prompt: 'Continue investigation?',
        },
      },
      contributions: [
        {
          specialist: 'scout',
          kind: 'discovery',
          payload: {
            qualifiedCount: 15,
            discoveryStatus: 'incomplete',
            coverage: { complete: false, cities: { searched: 3, planned: 6, names: ['Manchester'] } },
          },
        },
      ],
    });
    assert.equal(metrics.candidateCount, 15);
    assert.equal(metrics.pendingIntent, EXECUTION_INTENTS.CONTINUE_INVESTIGATION);
    assert.equal(metrics.investigationContinuationPending, true);
    assert.equal(metrics.prioritizationApprovalPending, false);
    assert.equal(metrics.discoveryStatus, 'incomplete');
  });

  it('extractEvidenceGaps surfaces prioritization blockers', () => {
    const gaps = extractEvidenceGaps({
      discoveryStatus: 'incomplete',
      qualifiedCount: 15,
      rankedProspects: [{ name: 'Example STR', readinessState: 'unknown' }],
      evidence: [{ label: 'Google Places', source: 'google_places' }],
      coverage: { complete: false, cities: { searched: 3, planned: 6 } },
    });
    assert.equal(gaps.sufficientForPrioritization, false);
    assert.ok(gaps.gaps.some((row) => row.code === 'coverage_incomplete'));
  });
});
