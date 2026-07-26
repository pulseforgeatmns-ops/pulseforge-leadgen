'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { seedReasoningFixture, seedSparseFixture, AS_OF } = require('./fixtures');
const { createMaxReasoningRuntime, RECOMMENDATION_TYPES } = require('..');

describe('Recommendation snapshot tests', () => {
  it('rich fixture produces stable structured recommendation', async () => {
    const fixture = await seedReasoningFixture();
    const max = createMaxReasoningRuntime({ knowledge: fixture.knowledge });
    const { recommendation } = await max.evaluate({
      tenantId: fixture.tenantId,
      companyId: fixture.company.id,
      asOf: AS_OF,
    });

    assert.equal(recommendation.id, `rec:${fixture.tenantId}:${fixture.company.id}`);
    assert.equal(recommendation.subject.id, fixture.company.id);
    assert.equal(recommendation.subject.name, 'Lodgism');
    assert.ok(Object.values(RECOMMENDATION_TYPES).includes(recommendation.type));
    assert.ok(recommendation.score >= 0 && recommendation.score <= 100);
    assert.ok(recommendation.confidence >= 0 && recommendation.confidence <= 100);
    assert.equal(typeof recommendation.recommendedAction, 'string');
    assert.ok(recommendation.reasoningSummary.whyThis);
    assert.ok(recommendation.reasoningSummary.whyNot);
    assert.ok(recommendation.reasoningSummary.whyNow);
    assert.ok(recommendation.reasoningSummary.confidenceBasis);

    // Snapshot-ish: score/confidence finite and action non-empty
    const snapshot = {
      type: recommendation.type,
      priority: recommendation.priority,
      recommendedAction: recommendation.recommendedAction,
      score: recommendation.score,
      confidence: recommendation.confidence,
      supportingCount: recommendation.supportingSignals.length,
      opposingCount: recommendation.opposingSignals.length,
    };
    assert.ok(snapshot.supportingCount >= 1);
    assert.ok(snapshot.opposingCount >= 1);
    assert.ok(snapshot.score > 40); // rich positive signals dominate
  });

  it('sparse fixture can score low with higher relative confidence from evidence', async () => {
    const fixture = await seedSparseFixture();
    const max = createMaxReasoningRuntime({ knowledge: fixture.knowledge });
    const { recommendation } = await max.evaluate({
      tenantId: fixture.tenantId,
      companyId: fixture.company.id,
      asOf: AS_OF,
    });
    assert.ok(recommendation.score <= 60);
    assert.equal(typeof recommendation.recommendedAction, 'string');
  });
});
