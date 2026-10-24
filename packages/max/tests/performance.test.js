'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { seedReasoningFixture, AS_OF } = require('./fixtures');
const { createMaxReasoningRuntime, PERFORMANCE_TARGET_MS } = require('..');

describe('Performance tests', () => {
  it('reasoning completes under target latency', async () => {
    const fixture = await seedReasoningFixture();
    const max = createMaxReasoningRuntime({ knowledge: fixture.knowledge });

    const { report, meta } = await max.evaluate({
      tenantId: fixture.tenantId,
      companyId: fixture.company.id,
      asOf: AS_OF,
    });

    assert.ok(report.performance.executionTimeMs >= 0);
    assert.ok(report.performance.graphQueries >= 1);
    assert.ok(typeof report.performance.nodesTraversed === 'number');
    assert.ok(report.performance.repositoryType);
    assert.ok(Object.keys(report.strategyTimings).length === 7);
    assert.equal(meta.performanceTargetMs, PERFORMANCE_TARGET_MS);
    assert.ok(
      meta.executionTimeMs < PERFORMANCE_TARGET_MS,
      `expected < ${PERFORMANCE_TARGET_MS}ms, got ${meta.executionTimeMs}ms`
    );
  });
});
