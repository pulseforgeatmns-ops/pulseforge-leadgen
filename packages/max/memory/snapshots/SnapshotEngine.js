'use strict';

const { round, deepFreeze } = require('../../reasoning/ReasoningTypes');
const { deepClone, snapshotId } = require('./MemoryTypes');
const { assertSnapshotRepository } = require('./SnapshotRepository');

/**
 * Snapshot Engine — persist deterministic reasoning snapshots (append-only).
 * No LLM output. Only structured state.
 */
class SnapshotEngine {
  /**
   * @param {object} deps
   * @param {import('./SnapshotRepository').SnapshotRepository} deps.repository
   */
  constructor(deps) {
    assertSnapshotRepository(deps && deps.repository);
    this._repository = deps.repository;
    /** @type {Map<string, number>} */
    this._seq = new Map();
  }

  /**
   * Build a snapshot object from an evaluate() result (does not persist).
   *
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} input.companyId
   * @param {string} [input.timestamp]
   * @param {object} input.evaluation - result of ReasoningEngine.evaluate()
   * @returns {object}
   */
  build(input) {
    const tenantId = requireString(input.tenantId, 'tenantId');
    const companyId = requireString(input.companyId, 'companyId');
    const evaluation = input.evaluation;
    if (!evaluation || !evaluation.recommendation || !evaluation.report) {
      throw new Error('SnapshotEngine.build requires evaluation with recommendation + report');
    }

    const timestamp = input.timestamp || evaluation.report.context.builtAt || new Date().toISOString();
    const seqKey = `${tenantId}::${companyId}`;
    const seq = (this._seq.get(seqKey) || 0) + 1;
    this._seq.set(seqKey, seq);

    const recommendation = stripRecommendation(evaluation.recommendation);
    const strategyResults = (evaluation.report.strategyResults || [])
      .map(stripStrategyResult)
      .sort((a, b) => String(a.strategy).localeCompare(String(b.strategy)));

    const claims = [
      ...new Set([
        ...(recommendation.claims || []),
        ...strategyResults.flatMap((r) => r.claims || []),
      ]),
    ].sort();

    const evidence = [
      ...new Set([
        ...(recommendation.evidence || []),
        ...collectEvidenceIds(recommendation.supportingSignals),
        ...collectEvidenceIds(recommendation.opposingSignals),
      ]),
    ].sort();

    const snapshot = {
      id: snapshotId(tenantId, companyId, timestamp, seq),
      tenantId,
      companyId,
      timestamp,
      recommendation,
      score: round(Number(recommendation.score)),
      confidence: round(Number(recommendation.confidence)),
      strategyResults,
      claims,
      evidence,
      meta: {
        recommendationId: recommendation.id,
        type: recommendation.type,
        priority: recommendation.priority,
        recommendedAction: recommendation.recommendedAction,
        subjectName: recommendation.subject && recommendation.subject.name != null
          ? recommendation.subject.name
          : null,
      },
    };

    return deepFreeze(deepClone(snapshot));
  }

  /**
   * Build + append snapshot. Returns the persisted copy.
   * @param {Parameters<SnapshotEngine['build']>[0]} input
   */
  async capture(input) {
    const snapshot = this.build(input);
    return this._repository.append(deepClone(snapshot));
  }

  /**
   * Replay snapshots for a company (chronological).
   * @param {string} tenantId
   * @param {string} companyId
   * @param {{ limit?: number }} [options]
   */
  async replay(tenantId, companyId, options) {
    return this._repository.listByCompany(tenantId, companyId, options);
  }
}

/**
 * @param {import('../../reasoning/ReasoningTypes').Recommendation} rec
 */
function stripRecommendation(rec) {
  return deepClone({
    id: rec.id,
    subject: rec.subject,
    type: rec.type,
    priority: rec.priority,
    score: round(rec.score),
    confidence: round(rec.confidence),
    recommendedAction: rec.recommendedAction,
    supportingSignals: (rec.supportingSignals || []).map(stripSignal).sort(compareSignal),
    opposingSignals: (rec.opposingSignals || []).map(stripSignal).sort(compareSignal),
    claims: [...(rec.claims || [])].map(String).sort(),
    evidence: [...(rec.evidence || [])].map(String).sort(),
    reasoningSummary: deepClone(rec.reasoningSummary || {}),
  });
}

/**
 * @param {import('../../reasoning/ReasoningTypes').StrategyResult} r
 */
function stripStrategyResult(r) {
  return {
    strategy: r.strategy,
    scoreDelta: round(r.scoreDelta),
    confidence: round(r.confidence),
    summary: r.summary,
    claims: [...(r.claims || [])].map(String).sort(),
    supportingEvidence: (r.supportingEvidence || []).map(stripSignal).sort(compareSignal),
    contradictingEvidence: (r.contradictingEvidence || []).map(stripSignal).sort(compareSignal),
  };
}

function stripSignal(s) {
  return {
    id: String(s.id),
    kind: String(s.kind),
    summary: String(s.summary),
    sourceId: s.sourceId != null ? String(s.sourceId) : null,
    sourceType: s.sourceType != null ? String(s.sourceType) : null,
    confidence:
      s.confidence == null || !Number.isFinite(Number(s.confidence))
        ? null
        : Number(s.confidence),
  };
}

function compareSignal(a, b) {
  const c = String(a.id).localeCompare(String(b.id));
  if (c !== 0) return c;
  return String(a.summary).localeCompare(String(b.summary));
}

function collectEvidenceIds(signals) {
  return (signals || [])
    .filter((s) => s && s.kind === 'evidence')
    .map((s) => String(s.id));
}

function requireString(value, label) {
  if (value == null || value === '') throw new Error(`${label} is required`);
  return String(value);
}

module.exports = {
  SnapshotEngine,
  stripRecommendation,
  stripStrategyResult,
};
