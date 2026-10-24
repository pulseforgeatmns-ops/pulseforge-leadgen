'use strict';

/**
 * SPEC-166 — Outcome Learning Engine tests.
 * ADR-086 — Every decision must teach.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { createAcquisitionMissionEngine } = require('../Engine');
const {
  capturePrediction,
  evaluatePrediction,
  resolvePrediction,
  buildOutcomeReviewSection,
  summarizeOrganizationalLearning,
  ACCURACY_LABELS,
  PREDICTION_STATUS,
} = require('../OutcomeLearning');

function engine() {
  return createAcquisitionMissionEngine();
}

function lawFirmMission(amoEngine, overrides = {}) {
  return amoEngine.create({
    tenantId: '10',
    objective: 'Acquire commercial cleaning contracts from Manchester law firms',
    targetSegment: 'law_firm',
    confidence: 0.84,
    ...overrides,
  });
}

describe('SPEC-166 Outcome Learning Engine', () => {
  it('Scenario 1: captures prediction, tracks outcome, compares with reality', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine);

    const prediction = amoEngine.capturePrediction(mission.id, {
      recommendation: {
        summary: 'Call ABC Law today — highest immediate opportunity.',
        confidence: 0.72,
        kind: 'outreach',
      },
      expectedOutcome: {
        kind: 'walkthrough',
        label: 'Walkthrough booked',
        probability: 0.72,
      },
      opportunityId: 'opp-abc',
      opportunityName: 'ABC Law',
    });

    assert.equal(prediction.status, PREDICTION_STATUS.PENDING);
    assert.equal(prediction.recommendation.probability, 0.72);
    assert.match(prediction.recommendation.summary, /ABC Law/i);

    amoEngine.recordOutcome(mission.id, { type: 'walkthrough_booked', at: '2026-08-25T14:00:00.000Z' });

    const snapshot = amoEngine.inspect(mission.id);
    assert.equal(snapshot.outcomeLearning.predictions.resolved, 1);
    assert.equal(snapshot.outcomeLearning.accuracy.correct, 1);
    assert.equal(snapshot.outcomeLearning.recentEvaluations[0].accuracy, ACCURACY_LABELS.CORRECT);
    assert.equal(snapshot.outcomeLearning.recentEvaluations[0].actualOutcome, 'Walkthrough booked');
  });

  it('Scenario 2: successful prediction strengthens heuristics', () => {
    const { buildBusinessHeuristic } = require('../../scout/heuristics/types');
    const prediction = capturePrediction({
      tenantId: '10',
      missionId: 'm1',
      recommendation: { summary: 'Pursue growing portfolio operator', confidence: 0.8 },
      contributingHeuristicIds: ['heur-vendor-instability'],
      expectedOutcome: { kind: 'walkthrough', label: 'Walkthrough booked', probability: 0.8 },
    });

    const result = resolvePrediction(prediction, {
      actualOutcome: 'walkthrough_booked',
      at: '2026-08-25T12:00:00.000Z',
    }, {
      heuristicLibrary: [buildBusinessHeuristic({
        id: 'heur-vendor-instability',
        name: 'Vendor instability',
        strength: 1,
      })],
    });

    assert.equal(result.evaluation.accuracy, ACCURACY_LABELS.CORRECT);
    assert.ok(result.heuristicUpdates.length >= 1);
    assert.equal(result.heuristicUpdates[0].outcome, 'won');
    assert.ok(result.heuristicUpdates[0].nextStrength > result.heuristicUpdates[0].previousStrength);
    assert.ok(result.learnings.some((l) => l.kind === 'business_heuristic' && l.direction === 'strengthened'));
    assert.equal(result.autoApplied, false);
  });

  it('Scenario 3: failed prediction records root cause and strategy update', () => {
    const prediction = capturePrediction({
      tenantId: '10',
      missionId: 'm1',
      recommendation: {
        summary: 'High opportunity — vendor instability signals',
        confidence: 0.72,
      },
      contributingHeuristicIds: ['heur-vendor-stability'],
      expectedOutcome: { kind: 'walkthrough', label: 'Walkthrough booked', probability: 0.72 },
    });

    const result = resolvePrediction(prediction, {
      actualOutcome: 'lost',
      notes: 'Already under contract. Decision maker unavailable.',
      primaryCause: 'Already under contract',
      secondaryCause: 'Decision maker unavailable',
      lesson: 'Vendor stability heuristic over-weighted.',
    });

    assert.equal(result.evaluation.accuracy, ACCURACY_LABELS.INCORRECT);
    assert.equal(result.evaluation.rootCause.primaryCause, 'Already under contract');
    assert.equal(result.evaluation.rootCause.secondaryCause, 'Decision maker unavailable');
    assert.match(result.evaluation.rootCause.lesson, /Vendor stability heuristic over-weighted/);
    assert.ok(result.learnings.some((l) => l.kind === 'strategy'));
    assert.equal(result.autoApplied, false);
  });

  it('Scenario 4: summarizes organizational learning for operator question', () => {
    const evaluations = [
      evaluatePrediction(
        capturePrediction({
          tenantId: '10',
          missionId: 'm1',
          recommendation: { summary: 'Call ABC today' },
          expectedOutcome: { kind: 'walkthrough', label: 'Walkthrough', probability: 0.72 },
        }),
        { actualOutcome: 'walkthrough_booked' }
      ),
      evaluatePrediction(
        capturePrediction({
          tenantId: '10',
          missionId: 'm2',
          recommendation: { summary: 'Pursue XYZ portfolio' },
          expectedOutcome: { kind: 'walkthrough', label: 'Walkthrough', probability: 0.65 },
        }),
        {
          actualOutcome: 'lost',
          primaryCause: 'Already under contract',
          lesson: 'Vendor stability heuristic over-weighted.',
        }
      ),
    ];

    const learnings = [
      { kind: 'strategy', statement: 'Vendor stability heuristic over-weighted.', direction: 'updated', at: '2026-08-20T00:00:00.000Z' },
    ];

    const summary = summarizeOrganizationalLearning(evaluations, learnings, { period: 'month' });
    assert.equal(summary.evaluated, 2);
    assert.equal(summary.succeeded, 1);
    assert.equal(summary.failed, 1);
    assert.equal(summary.whatPredictionFailed.length, 1);
    assert.equal(summary.whatPredictionSucceeded.length, 1);
    assert.ok(summary.whatShouldNeverHappenAgain.length >= 1);
    assert.match(summary.summary, /2 predictions evaluated/);
    assert.equal(summary.autoApplied, false);
  });

  it('Scenario 5: Mission Intelligence Report includes Outcome Review section', () => {
    const { buildMissionIntelligenceReport } = require('../../scout/investigation/MissionIntelligenceReport');

    const priorEvaluations = [
      evaluatePrediction(
        capturePrediction({
          tenantId: '10',
          recommendation: { summary: 'Prior outreach to ABC' },
          expectedOutcome: { kind: 'walkthrough', label: 'Walkthrough', probability: 0.7 },
        }),
        { actualOutcome: 'walkthrough_booked' }
      ),
    ];

    const report = buildMissionIntelligenceReport({
      state: {
        marketDefinition: { market: 'Commercial cleaning', geography: 'Manchester NH' },
        businessUnderstandings: [],
        activeHypotheses: [],
        confidence: 0.75,
      },
      priorEvaluations,
      priorOutcomeLearnings: [{
        kind: 'business_heuristic',
        subject: 'Vendor instability',
        direction: 'strengthened',
        statement: 'Heuristic strengthened after correct prediction.',
      }],
    });

    assert.equal(report.outcomeLearningSpec, 'SPEC-166');
    assert.equal(report.outcomeLearningAdr, 'ADR-086');
    assert.ok(report.outcomeReview);
    assert.equal(report.outcomeReview.kind, 'outcome_review');
    assert.equal(report.outcomeReview.accuracy.correct, 1);
    assert.ok(report.outcomeReview.lessons.length >= 1);
    assert.ok(report.outcomeReview.recentEvaluations.length >= 1);
    assert.equal(report.outcomeReview.autoApplied, false);
  });

  it('auto-captures prediction from Scout discovery MIR contribution', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine);

    amoEngine.contribute(mission.id, {
      specialist: 'scout',
      kind: 'discovery',
      payload: {
        confidence: 0.81,
        missionIntelligenceReport: {
          recommendation: {
            summary: 'Prioritize ABC Law — 72% walkthrough probability.',
            confidence: 0.72,
            kind: 'outreach',
          },
          strategicDecision: {
            expectedBusinessOutcome: { label: 'Walkthrough booked', arr: 2800 },
            tradeoff: { confidencePercent: 72 },
          },
          opportunityIntelligence: {
            topOpportunity: { id: 'opp-abc', entity: { name: 'ABC Law' } },
          },
          judgmentResult: {
            activatedHeuristics: [{ id: 'heur-growth', name: 'Growing portfolio' }],
          },
        },
      },
    });

    const predictions = amoEngine.store.listPredictions(mission.id);
    assert.equal(predictions.length, 1);
    assert.match(predictions[0].recommendation.summary, /ABC Law/i);
    assert.equal(predictions[0].status, PREDICTION_STATUS.PENDING);
  });

  it('answers operator question about organizational learning', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine);

    amoEngine.capturePrediction(mission.id, {
      recommendation: { summary: 'Outreach to ABC', confidence: 0.7 },
      expectedOutcome: { kind: 'walkthrough', label: 'Walkthrough', probability: 0.7 },
    });
    amoEngine.recordOutcome(mission.id, { type: 'no_answer' });

    const answered = amoEngine.answerOperator('What have we learned this month?', { tenantId: '10', missionId: mission.id });
    assert.equal(answered.inspection.property, 'outcome_learning');
    assert.match(answered.prose, /Outcome Learning/i);
  });

  it('buildOutcomeReviewSection never auto-applies learnings', () => {
    const section = buildOutcomeReviewSection({
      predictions: [{ id: 'p1', status: PREDICTION_STATUS.RESOLVED }],
      evaluations: [{ accuracy: ACCURACY_LABELS.CORRECT, evaluatedAt: '2026-08-25T00:00:00.000Z', recommendation: { summary: 'Test' }, expectedOutcome: { label: 'Walkthrough' }, actualOutcome: { label: 'Walkthrough booked' }, rootCause: { lesson: 'Matched.' }, confidenceAdjustment: 0.07 }],
      outcomeLearnings: [{ kind: 'strategy', statement: 'Updated reasoning', direction: 'updated' }],
    });
    assert.equal(section.autoApplied, false);
    assert.equal(section.accuracy.label, '100%');
  });
});
