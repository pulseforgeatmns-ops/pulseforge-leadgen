'use strict';

const { round, clamp } = require('../../reasoning/ReasoningTypes');
const {
  PRIORITY_WEIGHTS,
  TREND_SCORE,
} = require('../BriefingTypes');

/**
 * Deterministic prioritizer for briefing queues.
 * Does not invent scores — re-ranks existing recommendation + memory fields.
 */
class Prioritizer {
  /**
   * @param {object} [options]
   * @param {Partial<typeof PRIORITY_WEIGHTS>} [options.weights]
   */
  constructor(options = {}) {
    this.weights = { ...PRIORITY_WEIGHTS, ...(options.weights || {}) };
  }

  /**
   * Compute a deterministic rank score in [0, 100].
   *
   * @param {object} item
   * @param {number} item.score
   * @param {number} item.confidence
   * @param {string} [item.trend] - up|down|flat|insufficient
   * @param {number} [item.urgency] - 0..100
   * @param {number} [item.contradictionSeverity] - 0..100
   */
  rankScore(item) {
    const score = clamp(Number(item.score) || 0, 0, 100);
    const confidence = clamp(Number(item.confidence) || 0, 0, 100);
    const trendKey = String(item.trend || 'insufficient');
    const trend =
      TREND_SCORE[trendKey] != null ? TREND_SCORE[trendKey] : TREND_SCORE.insufficient;
    const urgency = clamp(Number(item.urgency) || 0, 0, 100);
    const contradictionSeverity = clamp(
      Number(item.contradictionSeverity) || 0,
      0,
      100
    );

    const w = this.weights;
    const raw =
      score * w.score +
      confidence * w.confidence +
      trend * w.trend +
      urgency * w.urgency +
      contradictionSeverity * w.contradictionSeverity;

    return round(raw);
  }

  /**
   * Sort items by rankScore desc, then companyId asc (stable).
   * Mutates copies only — returns new array with rankScore + rank attached.
   *
   * @param {object[]} items
   * @param {{ limit?: number }} [options]
   */
  order(items, options = {}) {
    const ranked = (items || []).map((item) => {
      const rankScore = this.rankScore(item);
      return { ...item, rankScore };
    });

    ranked.sort((a, b) => {
      if (b.rankScore !== a.rankScore) return b.rankScore - a.rankScore;
      const ca = String(a.companyId || '');
      const cb = String(b.companyId || '');
      if (ca !== cb) return ca.localeCompare(cb);
      return String(a.id || '').localeCompare(String(b.id || ''));
    });

    const limited =
      options.limit != null ? ranked.slice(0, Number(options.limit)) : ranked;

    return limited.map((item, index) => ({
      ...item,
      rank: index + 1,
    }));
  }
}

/**
 * Derive urgency 0..100 from period changes + watch hits (existing signals only).
 * @param {object} input
 * @param {object[]} [input.changes]
 * @param {number} [input.watchHitCount]
 * @param {number} [input.scoreDelta]
 */
function deriveUrgency(input = {}) {
  const changes = input.changes || [];
  let urgency = 0;

  for (const c of changes) {
    const mag = Number(c.magnitude) || 0;
    if (
      c.type === 'score_decreased' ||
      c.type === 'confidence_decreased' ||
      c.type === 'new_contradiction'
    ) {
      urgency = Math.max(urgency, Math.min(100, 40 + mag * 2));
    } else if (
      c.type === 'new_opportunity_signal' ||
      c.type === 'new_decision_maker' ||
      c.type === 'new_hiring_signal'
    ) {
      urgency = Math.max(urgency, Math.min(100, 30 + mag));
    } else if (c.type === 'priority_changed' || c.type === 'action_changed') {
      urgency = Math.max(urgency, 55);
    }
  }

  const watchHits = Number(input.watchHitCount) || 0;
  if (watchHits > 0) {
    urgency = Math.max(urgency, Math.min(100, 50 + watchHits * 15));
  }

  const scoreDelta = Number(input.scoreDelta);
  if (Number.isFinite(scoreDelta) && Math.abs(scoreDelta) >= 10) {
    urgency = Math.max(urgency, Math.min(100, 35 + Math.abs(scoreDelta)));
  }

  return round(clamp(urgency, 0, 100));
}

/**
 * Contradiction severity from opposing signals / new contradictions.
 * @param {object} input
 * @param {object} [input.recommendation]
 * @param {object[]} [input.changes]
 */
function deriveContradictionSeverity(input = {}) {
  const rec = input.recommendation || {};
  const opposing = (rec.opposingSignals || []).length;
  const supporting = (rec.supportingSignals || []).length;
  const newContradictions = (input.changes || []).filter(
    (c) => c.type === 'new_contradiction'
  ).length;

  let severity = opposing * 12 + newContradictions * 25;
  if (opposing > supporting && opposing > 0) {
    severity += 20;
  }
  return round(clamp(severity, 0, 100));
}

module.exports = {
  Prioritizer,
  deriveUrgency,
  deriveContradictionSeverity,
};
