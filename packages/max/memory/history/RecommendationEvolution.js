'use strict';

const { round } = require('../../reasoning/ReasoningTypes');
const { TREND_DIRECTIONS } = require('../snapshots/MemoryTypes');

/**
 * Recommendation evolution — history + trend + reason + forecast.
 * Forecast is a deterministic linear extrapolation, clearly labeled (not LLM).
 */
class RecommendationEvolution {
  /**
   * @param {object} input
   * @param {object[]} input.snapshots - chronological
   * @param {object[]} [input.timeline]
   * @param {object|null} [input.latestDiff]
   * @param {object[]} [input.latestChanges]
   */
  build(input) {
    const snapshots = input.snapshots || [];
    if (snapshots.length === 0) {
      return {
        recommendation: null,
        history: [],
        trend: { score: TREND_DIRECTIONS.INSUFFICIENT, confidence: TREND_DIRECTIONS.INSUFFICIENT },
        reason: [],
        forecast: null,
      };
    }

    const latest = snapshots[snapshots.length - 1];
    const history = snapshots.map((s) => ({
      snapshotId: s.id,
      timestamp: s.timestamp,
      score: s.score,
      confidence: s.confidence,
      type: s.recommendation && s.recommendation.type,
      priority: s.recommendation && s.recommendation.priority,
      recommendedAction: s.recommendation && s.recommendation.recommendedAction,
    }));

    const trend = computeTrend(snapshots);
    const reason = (input.latestChanges || []).map((c) => ({
      type: c.type,
      magnitude: c.magnitude,
      details: c.details,
    }));

    const forecast = computeForecast(snapshots);

    return {
      recommendation: latest.recommendation,
      history,
      trend,
      reason,
      forecast,
      scorePath: history.map((h) => h.score),
      confidencePath: history.map((h) => h.confidence),
    };
  }
}

/**
 * @param {object[]} snapshots
 */
function computeTrend(snapshots) {
  if (snapshots.length < 2) {
    return {
      score: TREND_DIRECTIONS.INSUFFICIENT,
      confidence: TREND_DIRECTIONS.INSUFFICIENT,
      scoreDeltaTotal: 0,
      confidenceDeltaTotal: 0,
      points: snapshots.length,
    };
  }
  const first = snapshots[0];
  const last = snapshots[snapshots.length - 1];
  const scoreDeltaTotal = round(last.score - first.score);
  const confidenceDeltaTotal = round(last.confidence - first.confidence);

  // Use last step for short-term direction, total for overall
  const prev = snapshots[snapshots.length - 2];
  const shortScore = round(last.score - prev.score);
  const shortConf = round(last.confidence - prev.confidence);

  return {
    score: direction(shortScore, 1),
    confidence: direction(shortConf, 1),
    scoreOverall: direction(scoreDeltaTotal, 2),
    confidenceOverall: direction(confidenceDeltaTotal, 2),
    scoreDeltaTotal,
    confidenceDeltaTotal,
    scoreDeltaLast: shortScore,
    confidenceDeltaLast: shortConf,
    points: snapshots.length,
  };
}

function direction(delta, threshold) {
  if (delta > threshold) return TREND_DIRECTIONS.UP;
  if (delta < -threshold) return TREND_DIRECTIONS.DOWN;
  return TREND_DIRECTIONS.FLAT;
}

/**
 * Linear extrapolation of score/confidence (structured, labeled).
 * @param {object[]} snapshots
 */
function computeForecast(snapshots) {
  if (snapshots.length < 2) return null;
  const n = snapshots.length;
  const scores = snapshots.map((s) => s.score);
  const confidences = snapshots.map((s) => s.confidence);
  const scoreSlope = (scores[n - 1] - scores[0]) / (n - 1);
  const confSlope = (confidences[n - 1] - confidences[0]) / (n - 1);

  return {
    kind: 'linear_extrapolation',
    horizonSteps: 1,
    nextScore: round(clamp01to100(scores[n - 1] + scoreSlope)),
    nextConfidence: round(clamp01to100(confidences[n - 1] + confSlope)),
    scoreSlope: round(scoreSlope),
    confidenceSlope: round(confSlope),
    basedOnSnapshots: n,
    disclaimer: 'deterministic_linear_extrapolation_not_a_prediction_model',
  };
}

function clamp01to100(n) {
  if (n < 0) return 0;
  if (n > 100) return 100;
  return n;
}

module.exports = {
  RecommendationEvolution,
  computeTrend,
  computeForecast,
  TREND_DIRECTIONS,
};
