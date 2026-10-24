'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createLearningEngine,
  createBeliefTracker,
  createCalibrationEngine,
  createOutcomeEvaluator,
  createLearningSession,
  blend,
  LEARNING_RULES,
  OUTCOME_VERDICTS,
} = require('..');

describe('SPEC-021 Learning rules', () => {
  it('exports guiding rules and verdicts', () => {
    assert.ok(LEARNING_RULES.EVIDENCE_EARNS_TRUST);
    assert.ok(LEARNING_RULES.OUTCOMES_CALIBRATE_TRUST);
    assert.ok(LEARNING_RULES.NO_HISTORY_MUTATION);
    assert.ok(LEARNING_RULES.NO_REPLAY_MUTATION);
    assert.ok(LEARNING_RULES.NO_RUNTIME_MUTATION);
    assert.ok(LEARNING_RULES.NO_ML);
    assert.equal(OUTCOME_VERDICTS.CORRECT, 'correct');
    assert.equal(OUTCOME_VERDICTS.PARTIALLY_CORRECT, 'partially_correct');
  });
});

describe('SPEC-021 OutcomeEvaluator', () => {
  const evaluator = createOutcomeEvaluator();

  it('classifies correct / incorrect / partial / unresolved', () => {
    const claim = { id: 'momentum_continuation', confidence: 0.82 };

    assert.equal(
      evaluator.evaluate({ claim, outcome: { verdict: 'correct' } }).verdict,
      'correct'
    );
    assert.equal(
      evaluator.evaluate({ claim, outcome: { correct: false } }).verdict,
      'incorrect'
    );
    assert.equal(
      evaluator.evaluate({
        claim,
        outcome: { verdict: 'partially_correct', partialScore: 0.6 },
      }).credit,
      0.6
    );
    assert.equal(
      evaluator.evaluate({ claim, outcome: { id: 'pending-1' } }).verdict,
      'unresolved'
    );
  });

  it('never mutates claim or outcome inputs', () => {
    const claim = { id: 'c1', confidence: 0.9 };
    const outcome = { claimId: 'c1', verdict: 'correct' };
    const before = JSON.stringify({ claim, outcome });
    evaluator.evaluate({ claim, outcome });
    assert.equal(JSON.stringify({ claim, outcome }), before);
  });
});

describe('SPEC-021 BeliefTracker', () => {
  it('tracks occurrences / correct / incorrect / accuracy', () => {
    const tracker = createBeliefTracker();
    const evaluator = createOutcomeEvaluator();
    const claim = {
      id: 'momentum_continuation',
      claimType: 'momentum_continuation',
      confidence: 0.82,
      strategyPack: 'market',
    };

    // 91 correct + 36 incorrect = 127 (spec example)
    for (let i = 0; i < 91; i += 1) {
      tracker.record(
        evaluator.evaluate({
          claim,
          outcome: { id: `ok-${i}`, claimId: 'momentum_continuation', verdict: 'correct' },
        })
      );
    }
    for (let i = 0; i < 36; i += 1) {
      tracker.record(
        evaluator.evaluate({
          claim,
          outcome: { id: `bad-${i}`, claimId: 'momentum_continuation', verdict: 'incorrect' },
        })
      );
    }

    const stats = tracker.statsFor('momentum_continuation');
    assert.equal(stats.occurrences, 127);
    assert.equal(stats.correct, 91);
    assert.equal(stats.incorrect, 36);
    assert.equal(stats.accuracy, 0.7165);
    assert.ok(Math.abs(stats.accuracy - 0.7165) < 1e-9);

    const pack = tracker.statsForStrategy('market');
    assert.equal(pack.occurrences, 127);
    assert.equal(pack.accuracy, 0.7165);
  });
});

describe('SPEC-021 CalibrationEngine', () => {
  it('blends confidence with historical calibration (82% × 67% → ~74%)', () => {
    assert.equal(blend(0.82, 0.67, 0.5), 0.745);

    const engine = createCalibrationEngine({ blendWeight: 0.5 });
    const result = engine.calibrate({
      claimId: 'momentum_continuation',
      confidence: 0.82,
      stats: {
        claimId: 'momentum_continuation',
        claimType: 'momentum_continuation',
        label: 'Momentum Continuation',
        occurrences: 127,
        correct: 85,
        incorrect: 42,
        partiallyCorrect: 0,
        unresolved: 0,
        accuracy: 0.67,
        precision: 0.67,
        recall: null,
        historicalCalibration: 0.67,
      },
      observationsConsidered: [{ id: 'obs-1' }],
      outcome: { verdict: 'correct' },
    });

    assert.equal(result.confidence, 0.82);
    assert.equal(result.historicalCalibration, 0.67);
    assert.equal(result.adjustedConfidence, 0.745);
    assert.equal(result.mutatesHistory, false);
    assert.equal(result.mutatesReplay, false);
    assert.equal(result.mutatesRuntime, false);
    assert.ok(result.explanation.observationsConsidered.length >= 1);
    assert.equal(result.explanation.confidenceBefore, 0.82);
    assert.equal(result.explanation.confidenceAfter, 0.745);
    assert.ok(result.explanation.historicalStatistics);
    assert.ok(result.explanation.narrative.includes('adjusted'));
  });
});

describe('SPEC-021 LearningEngine', () => {
  it('reports accuracy, precision, calibration, and confidence adjustments', () => {
    const engine = createLearningEngine();
    const claims = [
      {
        id: 'momentum_continuation',
        claimType: 'momentum_continuation',
        confidence: 0.82,
        strategyPack: 'market',
        observations: [{ id: 'obs-a' }],
      },
    ];
    const outcomes = [
      ...Array.from({ length: 7 }, (_, i) => ({
        id: `y-${i}`,
        claimId: 'momentum_continuation',
        verdict: 'correct',
        strategyPack: 'market',
      })),
      ...Array.from({ length: 3 }, (_, i) => ({
        id: `n-${i}`,
        claimId: 'momentum_continuation',
        verdict: 'incorrect',
        strategyPack: 'market',
      })),
    ];

    const frozenClaims = JSON.stringify(claims);
    const frozenOutcomes = JSON.stringify(outcomes);

    const result = engine.learn({ claims, outcomes, strategyPack: 'market' });

    assert.equal(JSON.stringify(claims), frozenClaims);
    assert.equal(JSON.stringify(outcomes), frozenOutcomes);

    assert.equal(result.mutatesHistory, false);
    assert.equal(result.mutatesReplay, false);
    assert.equal(result.mutatesRuntime, false);

    const belief = result.beliefs[0];
    assert.equal(belief.occurrences, 10);
    assert.equal(belief.correct, 7);
    assert.equal(belief.incorrect, 3);
    assert.equal(belief.accuracy, 0.7);
    assert.equal(belief.precision, 0.7);
    assert.equal(belief.recall, null);

    assert.ok(result.calibrations.length >= 1);
    const last = result.calibrations[result.calibrations.length - 1];
    assert.equal(last.confidence, 0.82);
    assert.ok(last.historicalCalibration != null);
    assert.ok(last.adjustedConfidence != null);
    assert.ok(last.explanation.narrative);

    const packAccuracy = engine.showAccuracy({
      scope: 'strategy_pack',
      id: 'market',
    });
    assert.equal(packAccuracy.scope, 'strategy_pack');
    assert.equal(packAccuracy.occurrences, 10);
    assert.equal(packAccuracy.accuracy, 0.7);

    const calibration = engine.showCalibration('momentum_continuation', {
      confidence: 0.82,
    });
    assert.equal(calibration.claimId, 'momentum_continuation');
    assert.ok(calibration.adjustedConfidence != null);
  });

  it('LearningSession is isolated and copy-on-write for outcomes', () => {
    const session = createLearningSession({
      name: 'base',
      claims: [{ id: 'c1', confidence: 0.9, strategyPack: 'market' }],
      outcomes: [{ claimId: 'c1', verdict: 'correct', strategyPack: 'market' }],
      strategyPack: 'market',
    });
    const child = session.withOutcomes({
      claimId: 'c1',
      verdict: 'incorrect',
      strategyPack: 'market',
    });
    assert.equal(session.getOutcomes().length, 1);
    assert.equal(child.getOutcomes().length, 2);
    assert.notEqual(session.id, child.id);

    const result = child.run();
    assert.equal(result.beliefs[0].occurrences, 2);
    assert.equal(result.isolated, true);
  });
});
