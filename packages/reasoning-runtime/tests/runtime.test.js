'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {
  createReasoningRuntime,
  createCRMStrategyPack,
  createCRMContextProvider,
  createNextBestActionProvider,
  assertStrategyPack,
  assertContextProvider,
  assertRecommendationProvider,
  REQUIRED_METHODS,
} = require('..');

const FORBIDDEN_DOMAIN_TERMS = [
  'prospect',
  'company',
  'email',
  'outreach',
  'appointment',
  'btc',
  'kalshi',
  'exchange',
  'market',
];

function readCoreSources() {
  const root = path.join(__dirname, '..');
  const files = [
    'ReasoningRuntime.js',
    'interfaces/StrategyPack.js',
    'interfaces/ContextProvider.js',
    'interfaces/RecommendationProvider.js',
  ];
  return files.map((rel) => ({
    rel,
    text: fs.readFileSync(path.join(root, rel), 'utf8').toLowerCase(),
  }));
}

describe('ReasoningRuntime interfaces', () => {
  it('StrategyPack requires the SPEC-015A method surface', () => {
    assert.deepEqual(REQUIRED_METHODS, [
      'initialize',
      'buildEvidence',
      'buildClaims',
      'findHistoricalAnalogs',
      'rankClaims',
      'generateRecommendations',
      'explain',
    ]);
    assert.throws(() => assertStrategyPack({ id: 'x', domain: 'y' }), /initialize/);
  });

  it('ContextProvider and RecommendationProvider assert build/generate', () => {
    assert.throws(() => assertContextProvider({}), /build/);
    assert.throws(() => assertRecommendationProvider({}), /generate/);
    assertContextProvider({ build() {} });
    assertRecommendationProvider({ generate() {} });
  });

  it('runtime core sources omit domain vocabulary', () => {
    const sources = readCoreSources();
    for (const { rel, text } of sources) {
      for (const term of FORBIDDEN_DOMAIN_TERMS) {
        // allow the word only inside comments that say "never" / "does not" — still fail hard
        assert.equal(
          text.includes(term),
          false,
          `${rel} must not contain domain term "${term}"`
        );
      }
    }
  });
});

describe('ReasoningRuntime orchestration', () => {
  it('invokes pack methods in order and never branches on domain', async () => {
    const order = [];
    const contextProvider = {
      async build(input) {
        order.push('context');
        return { subjectId: input.subjectId, evidence: [{ id: 'e1' }], claims: [] };
      },
    };
    const recommendationProvider = {
      generate(input) {
        return {
          id: 'rec-1',
          subject: { id: input.context.subjectId },
          score: input.aggregated.score,
          confidence: input.aggregated.confidence,
          recommendedAction: 'observe',
        };
      },
    };

    const pack = {
      id: 'demo',
      domain: 'demo',
      initialize() {
        order.push('initialize');
        this._ctx = arguments[0].context;
      },
      buildEvidence() {
        order.push('buildEvidence');
        return this._ctx.evidence;
      },
      buildClaims() {
        order.push('buildClaims');
        return [{ id: 'c1' }];
      },
      findHistoricalAnalogs() {
        order.push('findHistoricalAnalogs');
        return [{ id: 'a1' }];
      },
      rankClaims() {
        order.push('rankClaims');
        return { score: 60, confidence: 70 };
      },
      generateRecommendations() {
        order.push('generateRecommendations');
        return recommendationProvider.generate({
          context: this._ctx,
          aggregated: { score: 60, confidence: 70 },
        });
      },
      explain() {
        order.push('explain');
        return {
          recommendationId: 'rec-1',
          confidenceChanges: [{ field: 'score', value: 60 }],
        };
      },
    };

    const runtime = createReasoningRuntime({
      strategyPack: pack,
      contextProvider,
      recommendationProvider,
    });

    const out = await runtime.evaluate({ subjectId: 'subj-1' });
    assert.deepEqual(order, [
      'context',
      'initialize',
      'buildEvidence',
      'buildClaims',
      'findHistoricalAnalogs',
      'rankClaims',
      'generateRecommendations',
      'explain',
    ]);
    assert.equal(out.recommendation.id, 'rec-1');
    assert.equal(out.analogs.length, 1);
    assert.equal(out.meta.domain, 'demo');
    assert.ok(out.explainability.reasoningTrace.steps.length >= 7);
  });

  it('CRMStrategyPack + providers produce explainable output via DI stubs', async () => {
    const context = {
      tenantId: 't1',
      company: { id: 'c1', name: 'Acme' },
      evidence: [{ id: 'e1', summary: 'signal' }],
      claims: [{ id: 'cl1', statement: 'ready' }],
      people: [],
    };
    const registry = {
      evaluateAll() {
        return {
          results: [
            {
              strategy: 'opportunity',
              scoreDelta: 20,
              confidence: 80,
              supportingEvidence: [],
              contradictingEvidence: [],
              claims: ['cl1'],
              summary: 'ok',
            },
          ],
          timings: { opportunity: 0.1 },
        };
      },
    };
    const aggregator = {
      aggregate() {
        return { score: 55, confidence: 80, normalizedScores: {}, weights: {} };
      },
    };
    const recommendationProvider = createNextBestActionProvider({
      builder: {
        build() {
          return {
            id: 't1:c1',
            subject: { id: 'c1', name: 'Acme', type: 'company' },
            type: 'follow_up',
            priority: 'medium',
            score: 55,
            confidence: 80,
            recommendedAction: 'follow_up_outreach',
            supportingSignals: [],
            opposingSignals: [],
            claims: ['cl1'],
            evidence: ['e1'],
            reasoningSummary: { whyThis: [], whyNow: [], whyNot: [], confidenceBasis: [] },
          };
        },
      },
    });
    const explanationEngine = {
      explain({ recommendation }) {
        return {
          recommendationId: recommendation.id,
          subjectId: recommendation.subject.id,
          score: recommendation.score,
          confidence: recommendation.confidence,
          supportingClaims: [],
          evidence: [],
          contradictions: [],
        };
      },
    };

    const pack = createCRMStrategyPack({
      registry,
      aggregator,
      recommendationProvider,
      explanationEngine,
      analogFinder: async () => [{ id: 'hist-1', summary: 'similar score path' }],
    });
    const contextProvider = createCRMContextProvider({
      builder: { build: async () => context },
    });
    const runtime = createReasoningRuntime({
      strategyPack: pack,
      contextProvider,
      recommendationProvider,
    });

    const out = await runtime.evaluate({ tenantId: 't1', subjectId: 'c1' });
    assert.equal(out.recommendation.recommendedAction, 'follow_up_outreach');
    assert.equal(out.analogs[0].id, 'hist-1');
    assert.ok(out.explanation.historicalAnalogs);
    assert.ok(out.explanation.reasoningTrace);
    assert.equal(out.meta.packId, 'crm');
  });
});
