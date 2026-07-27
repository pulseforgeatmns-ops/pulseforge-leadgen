'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { seedReasoningFixture, AS_OF } = require('./fixtures');
const { createMaxReasoningRuntime } = require('..');

describe('Explanation tests', () => {
  it('evidence chain is complete', async () => {
    const fixture = await seedReasoningFixture();
    const max = createMaxReasoningRuntime({ knowledge: fixture.knowledge });
    const { recommendation, explanation, report } = await max.evaluate({
      tenantId: fixture.tenantId,
      companyId: fixture.company.id,
      asOf: AS_OF,
    });

    assert.equal(explanation.recommendationId, recommendation.id);
    assert.equal(explanation.subjectId, recommendation.subject.id);
    assert.equal(explanation.confidence, recommendation.confidence);
    assert.ok(explanation.chain);
    assert.equal(explanation.chain.recommendation, recommendation.id);
    assert.ok(Array.isArray(explanation.chain.supportingClaims));
    assert.ok(Array.isArray(explanation.chain.evidence));
    assert.ok(Array.isArray(explanation.chain.originalSources));
    assert.ok(Array.isArray(explanation.chain.contradictions));
    assert.equal(typeof explanation.chain.confidence, 'number');

    // At least one original source from website/crm evidence
    assert.ok(explanation.originalSources.length >= 1);
    assert.ok(explanation.originalSources.every((s) => s.sourceType || s.sourceId));

    assert.ok(report.explanation);
    assert.equal(report.recommendation.id, recommendation.id);
  });
});
