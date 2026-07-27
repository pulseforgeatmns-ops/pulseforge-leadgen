'use strict';

const { round } = require('../../reasoning/ReasoningTypes');
const { setDiff, stableStringify } = require('../snapshots/MemoryTypes');

/**
 * Diff Engine — deterministic comparison of two ReasoningSnapshots.
 * No mutation. Same inputs → same ReasoningDiff every time.
 */
class DiffEngine {
  /**
   * @param {object|null|undefined} fromSnapshot
   * @param {object} toSnapshot
   * @returns {object}
   */
  diff(fromSnapshot, toSnapshot) {
    if (!toSnapshot) throw new Error('DiffEngine.diff requires toSnapshot');
    if (!fromSnapshot) {
      return emptyDiff(null, toSnapshot, {
        newClaims: [...(toSnapshot.claims || [])].sort(),
        newEvidence: [...(toSnapshot.evidence || [])].sort(),
        strategyChanges: (toSnapshot.strategyResults || []).map((r) => ({
          strategy: r.strategy,
          scoreDeltaBefore: null,
          scoreDeltaAfter: r.scoreDelta,
          confidenceBefore: null,
          confidenceAfter: r.confidence,
          scoreDeltaChange: r.scoreDelta,
          confidenceChange: r.confidence,
        })),
        isInitial: true,
      });
    }

    const scoreDelta = round(Number(toSnapshot.score) - Number(fromSnapshot.score));
    const confidenceDelta = round(
      Number(toSnapshot.confidence) - Number(fromSnapshot.confidence)
    );

    const newClaims = setDiff(toSnapshot.claims, fromSnapshot.claims);
    const removedClaims = setDiff(fromSnapshot.claims, toSnapshot.claims);
    const newEvidence = setDiff(toSnapshot.evidence, fromSnapshot.evidence);
    const removedEvidence = setDiff(fromSnapshot.evidence, toSnapshot.evidence);

    const beforeByStrategy = indexStrategies(fromSnapshot.strategyResults);
    const afterByStrategy = indexStrategies(toSnapshot.strategyResults);
    const strategyIds = [
      ...new Set([...Object.keys(beforeByStrategy), ...Object.keys(afterByStrategy)]),
    ].sort();

    const strategyChanges = strategyIds.map((strategy) => {
      const before = beforeByStrategy[strategy] || null;
      const after = afterByStrategy[strategy] || null;
      const scoreDeltaBefore = before ? before.scoreDelta : null;
      const scoreDeltaAfter = after ? after.scoreDelta : null;
      const confidenceBefore = before ? before.confidence : null;
      const confidenceAfter = after ? after.confidence : null;
      return {
        strategy,
        scoreDeltaBefore,
        scoreDeltaAfter,
        confidenceBefore,
        confidenceAfter,
        scoreDeltaChange: round(
          (scoreDeltaAfter == null ? 0 : scoreDeltaAfter) -
            (scoreDeltaBefore == null ? 0 : scoreDeltaBefore)
        ),
        confidenceChange: round(
          (confidenceAfter == null ? 0 : confidenceAfter) -
            (confidenceBefore == null ? 0 : confidenceBefore)
        ),
        summaryBefore: before ? before.summary : null,
        summaryAfter: after ? after.summary : null,
        changed:
          scoreDeltaBefore !== scoreDeltaAfter ||
          confidenceBefore !== confidenceAfter ||
          (before && after && before.summary !== after.summary) ||
          !before ||
          !after,
      };
    });

    const fromMeta = fromSnapshot.meta || {};
    const toMeta = toSnapshot.meta || fromSnapshot.recommendation || {};
    const toRec = toSnapshot.recommendation || {};
    const fromRec = fromSnapshot.recommendation || {};

    return {
      fromSnapshotId: fromSnapshot.id,
      toSnapshotId: toSnapshot.id,
      fromTimestamp: fromSnapshot.timestamp,
      toTimestamp: toSnapshot.timestamp,
      tenantId: toSnapshot.tenantId,
      companyId: toSnapshot.companyId,
      scoreDelta,
      confidenceDelta,
      scoreBefore: fromSnapshot.score,
      scoreAfter: toSnapshot.score,
      confidenceBefore: fromSnapshot.confidence,
      confidenceAfter: toSnapshot.confidence,
      newClaims,
      removedClaims,
      newEvidence,
      removedEvidence,
      strategyChanges,
      recommendation: {
        typeChanged: (fromRec.type || fromMeta.type) !== (toRec.type || toMeta.type),
        priorityChanged:
          (fromRec.priority || fromMeta.priority) !== (toRec.priority || toMeta.priority),
        actionChanged:
          (fromRec.recommendedAction || fromMeta.recommendedAction) !==
          (toRec.recommendedAction || toMeta.recommendedAction),
        typeBefore: fromRec.type || fromMeta.type || null,
        typeAfter: toRec.type || toMeta.type || null,
        priorityBefore: fromRec.priority || fromMeta.priority || null,
        priorityAfter: toRec.priority || toMeta.priority || null,
        actionBefore: fromRec.recommendedAction || fromMeta.recommendedAction || null,
        actionAfter: toRec.recommendedAction || toMeta.recommendedAction || null,
      },
      isInitial: false,
      fingerprint: null, // filled below
    };
  }

  /**
   * Deterministic fingerprint of a diff (for tests).
   * @param {object} diff
   */
  fingerprint(diff) {
    const { fingerprint: _f, ...rest } = diff;
    return stableStringify(rest);
  }
}

function indexStrategies(results) {
  /** @type {Record<string, object>} */
  const map = {};
  for (const r of results || []) {
    map[r.strategy] = r;
  }
  return map;
}

function emptyDiff(fromSnapshot, toSnapshot, extras) {
  const base = {
    fromSnapshotId: fromSnapshot ? fromSnapshot.id : null,
    toSnapshotId: toSnapshot.id,
    fromTimestamp: fromSnapshot ? fromSnapshot.timestamp : null,
    toTimestamp: toSnapshot.timestamp,
    tenantId: toSnapshot.tenantId,
    companyId: toSnapshot.companyId,
    scoreDelta: toSnapshot.score,
    confidenceDelta: toSnapshot.confidence,
    scoreBefore: null,
    scoreAfter: toSnapshot.score,
    confidenceBefore: null,
    confidenceAfter: toSnapshot.confidence,
    newClaims: extras.newClaims || [],
    removedClaims: [],
    newEvidence: extras.newEvidence || [],
    removedEvidence: [],
    strategyChanges: extras.strategyChanges || [],
    recommendation: {
      typeChanged: false,
      priorityChanged: false,
      actionChanged: false,
      typeBefore: null,
      typeAfter: (toSnapshot.recommendation && toSnapshot.recommendation.type) || null,
      priorityBefore: null,
      priorityAfter: (toSnapshot.recommendation && toSnapshot.recommendation.priority) || null,
      actionBefore: null,
      actionAfter:
        (toSnapshot.recommendation && toSnapshot.recommendation.recommendedAction) || null,
    },
    isInitial: extras.isInitial === true,
    fingerprint: null,
  };
  return base;
}

module.exports = { DiffEngine };
