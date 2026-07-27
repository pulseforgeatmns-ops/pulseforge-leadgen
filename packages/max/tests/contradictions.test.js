'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { seedReasoningFixture, AS_OF } = require('./fixtures');
const { createMaxReasoningRuntime } = require('..');

describe('Contradiction tests', () => {
  let evaluate;
  let companyId;
  let tenantId;

  beforeEach(async () => {
    const fixture = await seedReasoningFixture();
    const max = createMaxReasoningRuntime({ knowledge: fixture.knowledge });
    evaluate = (input) => max.evaluate(input);
    companyId = fixture.company.id;
    tenantId = fixture.tenantId;
  });

  it('report includes both supporting and contradicting evidence', async () => {
    const { recommendation, report } = await evaluate({
      tenantId,
      companyId,
      asOf: AS_OF,
    });

    assert.ok(recommendation.supportingSignals.length >= 1);
    assert.ok(recommendation.opposingSignals.length >= 1);
    assert.ok(report.contradictions.length >= 1);

    const risk = report.strategyResults.find((r) => r.strategy === 'risk');
    assert.ok(risk);
    assert.ok(risk.contradictingEvidence.length >= 1);

    // Positive opportunity alongside negative risk
    const opportunity = report.strategyResults.find((r) => r.strategy === 'opportunity');
    assert.ok(opportunity.supportingEvidence.length >= 1);
  });

  it('contradictions are first-class on explanation chain', async () => {
    const { explanation } = await evaluate({
      tenantId,
      companyId,
      asOf: AS_OF,
    });
    assert.ok(Array.isArray(explanation.contradictions));
    assert.ok(explanation.contradictions.length >= 1);
    assert.ok(Array.isArray(explanation.chain.contradictions));
  });
});
