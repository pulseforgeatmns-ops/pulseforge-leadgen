'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { resetOntologyRegistry, getOntologyRegistry, CORE_EDGE_TYPES } = require('@pulseforge/knowledge/ontology');
const { createKnowledgeRuntime, NODE_TYPES } = require('@pulseforge/knowledge');
const {
  createMarketOntology,
  registerMarketOntology,
  MARKET_CLAIM_VOCABULARY,
  MARKET_OBSERVATION_TYPES,
  MARKET_ENTITY_TYPES,
  assetId,
  priceTickId,
} = require('..');

describe('SPEC-017 Market Ontology', () => {
  beforeEach(() => {
    resetOntologyRegistry();
    registerMarketOntology();
  });

  it('registers market vocabulary without modifying core engines', () => {
    const registry = getOntologyRegistry();
    const ontology = registry.getDomain('market');
    assert.equal(ontology.label, 'Market');
    assert.ok(registry.isNodeType(MARKET_ENTITY_TYPES.ASSET));
    assert.ok(registry.isObservationType(MARKET_OBSERVATION_TYPES.PRICE_TICK));
    assert.ok(registry.isObservationType(MARKET_OBSERVATION_TYPES.CHART_SNAPSHOT));
    assert.ok(registry.isEdgeType('TRADES_ON'));
    assert.ok(registry.isSubjectType(MARKET_ENTITY_TYPES.ASSET));
    assert.ok(registry.isSubjectType(MARKET_ENTITY_TYPES.CONTRACT));
    assert.equal(registry.getClaimTerm('momentum_continuation').label, 'Momentum Continuation');
    assert.equal(registry.getOutcomeTerm('breakout_confirmed').label, 'Breakout Confirmed');
  });

  it('aligns claim vocabulary with SPEC-016 market strategy pack', () => {
    const claimIds = MARKET_CLAIM_VOCABULARY.map((t) => t.id).sort();
    assert.deepEqual(claimIds, [
      'elevated_volatility',
      'liquidity_contraction',
      'mean_reversion',
      'momentum_continuation',
      'momentum_exhaustion',
      'news_driven_expansion',
      'regime_transition',
    ]);
  });

  it('produces deterministic market identities', () => {
    const btc = assetId('BTC');
    assert.equal(btc, assetId('btc'));
    const tick = priceTickId({
      asset: 'BTC',
      observationType: MARKET_OBSERVATION_TYPES.PRICE_TICK,
      observedAt: '2026-07-26T18:05:00Z',
      venue: 'Coinbase',
    });
    assert.equal(tick.length, 32);
    assert.equal(
      tick,
      priceTickId({
        asset: 'btc',
        observationType: MARKET_OBSERVATION_TYPES.PRICE_TICK,
        observedAt: '2026-07-26t18:05:00z',
        venue: 'coinbase',
      })
    );
  });

  it('allows graph writes for market entity types after registration', async () => {
    const { knowledge } = createKnowledgeRuntime();
    const asset = await knowledge.createNode({
      tenantId: '10',
      type: MARKET_ENTITY_TYPES.ASSET,
      id: assetId('BTC'),
      name: 'Bitcoin',
      naturalKey: 'BTC',
    });
    const exchange = await knowledge.createNode({
      tenantId: '10',
      type: MARKET_ENTITY_TYPES.EXCHANGE,
      name: 'Coinbase',
      naturalKey: 'coinbase',
    });
    const edge = await knowledge.createEdge({
      tenantId: '10',
      type: 'TRADES_ON',
      fromId: asset.id,
      toId: exchange.id,
    });
    assert.equal(asset.type, 'asset');
    assert.equal(edge.type, 'TRADES_ON');

  });

  it('does not require changes to claim or confidence engines', async () => {
    const { knowledge } = createKnowledgeRuntime();
    const asset = await knowledge.createNode({
      tenantId: '10',
      type: MARKET_ENTITY_TYPES.ASSET,
      id: assetId('ETH'),
      name: 'Ethereum',
    });
    const evidence = await knowledge.evidence.createEvidence({
      tenantId: '10',
      sourceType: 'market_adapter',
      summary: 'Momentum building',
    });
    const claim = await knowledge.claims.createClaim({
      tenantId: '10',
      statement: 'Momentum Continuation',
      subjectId: asset.id,
      evidenceIds: [evidence.id],
      metadata: { claimType: 'momentum_continuation' },
    });
    assert.equal(claim.type, NODE_TYPES.CLAIM);
    assert.ok(claim.confidence >= 0);
  });
});
