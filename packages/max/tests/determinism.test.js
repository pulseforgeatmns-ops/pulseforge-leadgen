'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { seedReasoningFixture, AS_OF } = require('./fixtures');
const { createMaxReasoningRuntime } = require('..');

function stripVolatile(report) {
  return {
    recommendation: {
      id: report.recommendation.id,
      subject: report.recommendation.subject,
      type: report.recommendation.type,
      priority: report.recommendation.priority,
      score: report.recommendation.score,
      confidence: report.recommendation.confidence,
      recommendedAction: report.recommendation.recommendedAction,
      supportingSignals: report.recommendation.supportingSignals,
      opposingSignals: report.recommendation.opposingSignals,
      claims: report.recommendation.claims,
      evidence: report.recommendation.evidence,
      reasoningSummary: report.recommendation.reasoningSummary,
    },
    strategyResults: report.strategyResults.map((r) => ({
      strategy: r.strategy,
      scoreDelta: r.scoreDelta,
      confidence: r.confidence,
      summary: r.summary,
      supportingEvidence: r.supportingEvidence,
      contradictingEvidence: r.contradictingEvidence,
      claims: r.claims,
    })),
    normalizedScores: report.normalizedScores,
  };
}

describe('Determinism tests', () => {
  it('same graph → same recommendation every time', async () => {
    const fixture = await seedReasoningFixture();
    const max = createMaxReasoningRuntime({ knowledge: fixture.knowledge });

    const a = await max.evaluate({
      tenantId: fixture.tenantId,
      companyId: fixture.company.id,
      asOf: AS_OF,
    });
    const b = await max.evaluate({
      tenantId: fixture.tenantId,
      companyId: fixture.company.id,
      asOf: AS_OF,
    });

    assert.deepEqual(stripVolatile(a.report), stripVolatile(b.report));
    assert.equal(a.recommendation.score, b.recommendation.score);
    assert.equal(a.recommendation.confidence, b.recommendation.confidence);
    assert.equal(a.recommendation.recommendedAction, b.recommendation.recommendedAction);
  });
});
