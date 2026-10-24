'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createReplayEngine,
  createReplayTimeline,
  createReplaySession,
  createReplayComparator,
  REPLAY_RULES,
} = require('..');

const START = '2026-07-26T13:00:00.000Z';
const END = '2026-07-26T16:00:00.000Z';

const BTC_OBSERVATIONS = [
  {
    type: 'economic_release',
    series: 'CPI',
    actual: 3.2,
    forecast: 3.0,
    prior: 3.1,
    timestamp: '2026-07-26T13:30:00.000Z',
  },
  {
    type: 'market_session',
    session: 'us_regular',
    status: 'open',
    timestamp: '2026-07-26T14:30:00.000Z',
  },
  {
    type: 'news_event',
    headline: 'BTC institutional inflows accelerate',
    symbols: ['BTC'],
    sentiment: 0.4,
    timestamp: '2026-07-26T14:55:00.000Z',
  },
  {
    type: 'market_snapshot',
    asset: 'BTC',
    price: 68000,
    volume24h: 30000000000,
    changePct: 2.8,
    timestamp: '2026-07-26T15:00:00.000Z',
  },
  {
    type: 'price_tick',
    asset: 'BTC',
    price: 68000,
    venue: 'coinbase',
    timestamp: '2026-07-26T15:00:01.000Z',
  },
  {
    type: 'volume_update',
    asset: 'BTC',
    volume: 2100000000,
    window: '1h',
    timestamp: '2026-07-26T15:00:02.000Z',
  },
  {
    type: 'volatility_observation',
    asset: 'BTC',
    value: 0.65,
    measure: 'realized_24h',
    timestamp: '2026-07-26T15:00:03.000Z',
  },
];

describe('SPEC-018 ReplayTimeline', () => {
  it('navigates without mutating observations', () => {
    const engine = createReplayEngine();
    return engine
      .run({
        subjectId: 'BTC',
        startTime: START,
        endTime: END,
        ontology: 'market',
        strategyPack: 'market',
        observations: BTC_OBSERVATIONS,
      })
      .then((result) => {
        const timeline = createReplayTimeline(result.observations);
        const first = timeline.next();
        assert.ok(first);
        assert.ok(first.id);
        const frozenId = first.id;
        assert.throws(() => {
          first.id = 'mutated';
        });
        assert.equal(first.id, frozenId);

        const sought = timeline.seek(frozenId);
        assert.equal(sought.id, frozenId);

        const byTime = timeline.seek('2026-07-26T15:00:00.000Z');
        assert.ok(byTime);
        assert.ok(Date.parse(byTime.observedAt) <= Date.parse('2026-07-26T15:00:00.000Z'));
      });
  });
});

describe('SPEC-018 ReplaySession', () => {
  it('is disposable and refuses writes after close', () => {
    const session = createReplaySession({
      subjectId: 'BTC',
      versions: {
        ontology: 'market@1',
        strategyPack: 'market@1',
        runtime: '1.0.0',
      },
    });
    session.applyStep({
      observation: { id: 'o1', observedAt: START, observationType: 'price_tick', subjectId: 'BTC' },
      generatedEvidence: [],
      affectedClaims: { derived: [] },
      confidenceChanges: [],
      recommendation: { recommendedAction: 'observe', score: 50, confidence: 40 },
      reasoningTrace: { steps: [] },
      confidence: 40,
    });
    assert.equal(session.getState().currentConfidence, 40);
    session.close();
    assert.throws(() => {
      session.applyStep({
        observation: null,
        generatedEvidence: [],
        affectedClaims: {},
        confidenceChanges: [],
        recommendation: null,
        reasoningTrace: {},
        confidence: null,
      });
    }, /closed/);
  });
});

describe('SPEC-018 ReplayEngine acceptance', () => {
  it('createReplayEngine().run returns the full explainable surface', async () => {
    const replay = createReplayEngine();
    const result = await replay.run({
      subjectId: 'BTC',
      startTime: START,
      endTime: END,
      ontology: 'market',
      strategyPack: 'market',
      observations: BTC_OBSERVATIONS,
    });

    assert.ok(Array.isArray(result.observations));
    assert.equal(result.observations.length, BTC_OBSERVATIONS.length);
    assert.ok(result.evidence);
    assert.ok(result.claims);
    assert.ok(result.confidence != null);
    assert.ok(Array.isArray(result.recommendations));
    assert.ok(result.recommendations.length > 0);
    assert.ok(result.explanation);
    assert.ok(result.reasoningTrace);
    assert.ok(result.versions.ontology.includes('market'));
    assert.ok(result.versions.strategyPack.includes('market'));
    assert.ok(result.versions.runtime);

    for (const step of result.steps) {
      assert.ok(step.reasoningTrace);
      assert.ok(Array.isArray(step.confidenceChanges) || step.confidenceChanges);
      assert.ok('generatedEvidence' in step);
      assert.ok('affectedClaims' in step);
      assert.ok('recommendation' in step);
    }

    // Deterministic observation ids — not positional indexes
    for (const obs of result.observations) {
      assert.ok(obs.id);
      assert.equal(obs.id.includes('obs:price_tick:0'), false);
    }
  });

  it('running the same replay twice produces identical output', async () => {
    const replay = createReplayEngine();
    const input = {
      subjectId: 'BTC',
      startTime: START,
      endTime: END,
      ontology: 'market',
      strategyPack: 'market',
      observations: BTC_OBSERVATIONS,
    };

    const a = await replay.run(input);
    const b = await replay.run(input);

    assert.equal(a.fingerprint, b.fingerprint);
    assert.deepEqual(
      a.recommendations.map((r) => ({
        action: r.recommendedAction,
        score: r.score,
        confidence: r.confidence,
        id: r.id,
      })),
      b.recommendations.map((r) => ({
        action: r.recommendedAction,
        score: r.score,
        confidence: r.confidence,
        id: r.id,
      }))
    );
    assert.equal(a.confidence, b.confidence);
    assert.deepEqual(
      a.observations.map((o) => o.id),
      b.observations.map((o) => o.id)
    );
  });
});

describe('SPEC-018 temporal queries', () => {
  it('answers belief-at, confidence rises, claim appearance, recommendation history', async () => {
    const result = await createReplayEngine().run({
      subjectId: 'BTC',
      startTime: START,
      endTime: END,
      ontology: 'market',
      strategyPack: 'market',
      observations: BTC_OBSERVATIONS,
    });

    const belief = result.queries.beliefAt('2026-07-26T14:55:00.000Z');
    assert.ok(belief);
    assert.ok(belief.recommendation || belief.claims);

    assert.ok(Array.isArray(result.queries.whyConfidenceIncreased()));
    assert.ok(Array.isArray(result.queries.whichObservationChangedRecommendation()));
    assert.ok(Array.isArray(result.queries.showEveryRecommendation()));
    assert.ok(result.queries.showEveryRecommendation().length >= 1);

    const claimId =
      (result.claims &&
        result.claims.derived &&
        result.claims.derived[0] &&
        result.claims.derived[0].id) ||
      (result.claims &&
        result.claims.results &&
        result.claims.results[0] &&
        result.claims.results[0].strategy);
    if (claimId) {
      const first = result.queries.whenClaimFirstAppeared(claimId);
      assert.ok(first === null || first.claimId === claimId);
      const contradicted = result.queries.whatEvidenceContradictedClaim(claimId);
      assert.equal(contradicted.claimId, claimId);
      assert.ok(Array.isArray(contradicted.contradictingEvidence));
    }
  });
});

describe('SPEC-018 ReplayComparator', () => {
  it('reports identity for identical runs and diffs when confidence changes', async () => {
    const replay = createReplayEngine();
    const input = {
      subjectId: 'BTC',
      startTime: START,
      endTime: END,
      ontology: 'market',
      strategyPack: 'market',
      observations: BTC_OBSERVATIONS,
    };
    const left = await replay.run(input);
    const right = await replay.run(input);
    const comparator = createReplayComparator();
    const same = comparator.compare(left, right);
    assert.equal(same.identical, true);
    assert.equal(same.confidenceDifferences.changed, false);

    const mutated = {
      ...right,
      confidence: Number(right.confidence || 0) + 7,
      recommendations: right.recommendations.map((r, i) =>
        i === right.recommendations.length - 1
          ? { ...r, recommendedAction: 'gather_more_evidence', confidence: r.confidence + 7 }
          : r
      ),
    };
    const diff = comparator.compare(left, mutated);
    assert.equal(diff.identical, false);
    assert.equal(diff.confidenceDifferences.changed, true);
    assert.equal(diff.recommendationDifferences.actionChanged, true);
  });
});

describe('SPEC-018 rules surface', () => {
  it('exports replay rules', () => {
    assert.ok(REPLAY_RULES.DETERMINISTIC);
    assert.ok(REPLAY_RULES.REGENERATE_REASONING);
    assert.ok(REPLAY_RULES.NEVER_MODIFY_HISTORY);
  });
});
