'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createMaxReasoningRuntime,
  createCRMStrategyRegistry,
  createDefaultStrategyRegistry,
} = require('..');
const { seedReasoningFixture, AS_OF } = require('./fixtures');

describe('SPEC-015A CRM parity via ReasoningRuntime', () => {
  it('createCRMStrategyRegistry matches default registry ids', () => {
    assert.deepEqual(
      createCRMStrategyRegistry().ids().sort(),
      createDefaultStrategyRegistry().ids().sort()
    );
  });

  it('evaluate via subjectId alias matches companyId', async () => {
    const fixture = await seedReasoningFixture();
    const max = createMaxReasoningRuntime({ knowledge: fixture.knowledge });
    const byCompany = await max.evaluate({
      tenantId: fixture.tenantId,
      companyId: fixture.company.id,
      asOf: AS_OF,
    });
    const bySubject = await max.evaluate({
      tenantId: fixture.tenantId,
      subjectId: fixture.company.id,
      asOf: AS_OF,
    });
    assert.equal(byCompany.recommendation.id, bySubject.recommendation.id);
    assert.equal(byCompany.recommendation.score, bySubject.recommendation.score);
    assert.equal(
      byCompany.recommendation.confidence,
      bySubject.recommendation.confidence
    );
    assert.equal(
      byCompany.recommendation.recommendedAction,
      bySubject.recommendation.recommendedAction
    );
  });

  it('engine exposes runtime with CRM pack', async () => {
    const fixture = await seedReasoningFixture();
    const max = createMaxReasoningRuntime({ knowledge: fixture.knowledge });
    assert.equal(max.engine.runtime.strategyPack.domain, 'crm');
    assert.equal(max.engine.runtime.strategyPack.id, 'crm');
    const out = await max.evaluate({
      tenantId: fixture.tenantId,
      companyId: fixture.company.id,
      asOf: AS_OF,
    });
    assert.ok(out.explanation.historicalAnalogs);
    assert.ok(out.explanation.reasoningTrace);
    assert.ok(out.explanation.confidenceChanges);
    assert.equal(out.report.strategyResults.length, 7);
  });
});
