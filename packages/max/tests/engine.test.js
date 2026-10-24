'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createMaxReasoningRuntime,
  createDefaultStrategyRegistry,
  StrategyRegistry,
  ReasoningContextBuilder,
  STRATEGY_IDS,
} = require('..');
const { seedReasoningFixture, AS_OF } = require('./fixtures');

describe('Registry + engine wiring', () => {
  it('default registry has seven strategies', () => {
    const registry = createDefaultStrategyRegistry();
    assert.deepEqual(registry.ids().sort(), Object.values(STRATEGY_IDS).sort());
  });

  it('new strategies require zero engine modifications', () => {
    const registry = new StrategyRegistry();
    registry.register({
      id: 'custom',
      name: 'Custom',
      evaluate() {
        return {
          strategy: 'custom',
          scoreDelta: 0,
          confidence: 10,
          supportingEvidence: [],
          contradictingEvidence: [],
          claims: [],
          summary: 'custom:ok',
        };
      },
    });
    assert.equal(registry.ids().length, 1);
  });

  it('ReasoningContextBuilder uses query surface only', async () => {
    const fixture = await seedReasoningFixture();
    const builder = new ReasoningContextBuilder({ knowledge: fixture.knowledge });
    const context = await builder.build({
      tenantId: fixture.tenantId,
      companyId: fixture.company.id,
      asOf: AS_OF,
    });
    assert.equal(context.company.name, 'Lodgism');
    assert.ok(context.people.length >= 1);
    assert.ok(context.interactions.length >= 1);
    assert.ok(context.claims.length >= 1);
    assert.ok(context.evidence.length >= 1);
    assert.ok(context.metrics.graphQueries >= 1);
  });

  it('end-to-end evaluate returns report', async () => {
    const fixture = await seedReasoningFixture();
    const max = createMaxReasoningRuntime({ knowledge: fixture.knowledge });
    const out = await max.evaluate({
      tenantId: fixture.tenantId,
      companyId: fixture.company.id,
      asOf: AS_OF,
    });
    assert.ok(out.recommendation);
    assert.ok(out.explanation);
    assert.ok(out.report);
    assert.equal(out.report.strategyResults.length, 7);
  });
});
