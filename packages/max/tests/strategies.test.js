'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { seedReasoningFixture, AS_OF } = require('./fixtures');
const { ReasoningContextBuilder } = require('../context/ReasoningContextBuilder');
const {
  OpportunityStrategy,
  EngagementStrategy,
  RelationshipStrategy,
  DecisionMakerStrategy,
  OverflowStrategy,
  TechnologyStrategy,
  RiskStrategy,
  STRATEGY_IDS,
} = require('..');

describe('Strategy tests (independent)', () => {
  /** @type {import('../reasoning/ReasoningTypes').ReasoningContext} */
  let context;

  beforeEach(async () => {
    const fixture = await seedReasoningFixture();
    const builder = new ReasoningContextBuilder({ knowledge: fixture.knowledge });
    context = await builder.build({
      tenantId: fixture.tenantId,
      companyId: fixture.company.id,
      asOf: AS_OF,
    });
  });

  it('OpportunityStrategy finds growth signals and opposing risks', () => {
    const result = OpportunityStrategy.evaluate(context);
    assert.equal(result.strategy, STRATEGY_IDS.OPPORTUNITY);
    assert.ok(result.supportingEvidence.length >= 1);
    assert.ok(result.scoreDelta > 0);
    assert.ok(result.confidence >= 0 && result.confidence <= 100);
    assert.ok(!Object.prototype.hasOwnProperty.call(result, 'recommendedAction'));
  });

  it('EngagementStrategy scores opens/replies and recency', () => {
    const result = EngagementStrategy.evaluate(context);
    assert.equal(result.strategy, STRATEGY_IDS.ENGAGEMENT);
    assert.ok(result.supportingEvidence.length >= 1);
    assert.match(result.summary, /days_since=/);
  });

  it('RelationshipStrategy detects KNOWS / referral / related companies', () => {
    const result = RelationshipStrategy.evaluate(context);
    assert.equal(result.strategy, STRATEGY_IDS.RELATIONSHIP);
    assert.ok(result.supportingEvidence.length >= 1);
    assert.ok(result.scoreDelta > 0);
  });

  it('DecisionMakerStrategy identifies Owner title', () => {
    const result = DecisionMakerStrategy.evaluate(context);
    assert.equal(result.strategy, STRATEGY_IDS.DECISION_MAKER);
    assert.ok(
      result.supportingEvidence.some((e) => e.summary.includes('Decision-maker'))
    );
    assert.ok(result.scoreDelta > 0);
  });

  it('OverflowStrategy finds vendor/demand signals', () => {
    const result = OverflowStrategy.evaluate(context);
    assert.equal(result.strategy, STRATEGY_IDS.OVERFLOW);
    assert.ok(result.supportingEvidence.length >= 1);
  });

  it('TechnologyStrategy reads Guesty metadata', () => {
    const result = TechnologyStrategy.evaluate(context);
    assert.equal(result.strategy, STRATEGY_IDS.TECHNOLOGY);
    assert.ok(
      result.supportingEvidence.some((e) => /guesty|technology/i.test(e.summary))
    );
  });

  it('RiskStrategy reports contradicting evidence as first-class', () => {
    const result = RiskStrategy.evaluate(context);
    assert.equal(result.strategy, STRATEGY_IDS.RISK);
    assert.ok(result.contradictingEvidence.length >= 1);
    assert.ok(result.scoreDelta <= 0);
  });

  it('context is frozen (strategies cannot mutate)', () => {
    assert.ok(Object.isFrozen(context));
    assert.throws(() => {
      // @ts-ignore
      context.people = [];
    });
  });
});
