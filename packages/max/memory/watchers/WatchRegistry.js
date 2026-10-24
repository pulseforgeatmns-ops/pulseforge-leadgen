'use strict';

const { WATCH_OPS } = require('../snapshots/MemoryTypes');

/**
 * Watch Registry — support future subscriptions.
 * Detection only; no notifications in v0.8.1.
 *
 * Examples:
 *   Watch company → notify when |scoreΔ| > 10
 *   Watch claim confidence → notify when value >= 0.80
 */
class WatchRegistry {
  constructor() {
    /** @type {Map<string, object>} */
    this._watches = new Map();
  }

  /**
   * @param {object} watch
   * @param {string} [watch.id]
   * @param {string} watch.tenantId
   * @param {'company'|'claim'|'strategy'} watch.targetType
   * @param {string} watch.targetId - companyId, claimId, or strategy id
   * @param {object} watch.condition
   * @param {string} watch.condition.op
   * @param {string} [watch.condition.field] - score|confidence|strategyScore|claimConfidence|changeType
   * @param {number|string} [watch.condition.value]
   */
  register(watch) {
    if (!watch || !watch.tenantId || !watch.targetType || !watch.targetId) {
      throw new Error('Watch requires tenantId, targetType, targetId');
    }
    if (!watch.condition || !watch.condition.op) {
      throw new Error('Watch requires condition.op');
    }
    if (!Object.values(WATCH_OPS).includes(watch.condition.op)) {
      throw new Error(`Unknown watch op: ${watch.condition.op}`);
    }
    const id =
      watch.id ||
      `watch:${watch.tenantId}:${watch.targetType}:${watch.targetId}:${watch.condition.op}:${
        watch.condition.field || 'default'
      }:${watch.condition.value}`;
    if (this._watches.has(id)) {
      throw new Error(`Watch already registered: ${id}`);
    }
    const row = Object.freeze({
      id,
      tenantId: String(watch.tenantId),
      targetType: String(watch.targetType),
      targetId: String(watch.targetId),
      condition: Object.freeze({ ...watch.condition }),
      createdAt: watch.createdAt || new Date().toISOString(),
    });
    this._watches.set(id, row);
    return row;
  }

  /**
   * @param {string} id
   */
  unregister(id) {
    return this._watches.delete(id);
  }

  /**
   * @param {string} [tenantId]
   */
  list(tenantId) {
    const rows = [...this._watches.values()];
    const filtered = tenantId
      ? rows.filter((w) => w.tenantId === String(tenantId))
      : rows;
    return filtered.sort((a, b) => String(a.id).localeCompare(String(b.id)));
  }

  get(id) {
    return this._watches.get(id) || null;
  }

  clear() {
    this._watches.clear();
  }

  /**
   * Evaluate watches against a transition. Returns triggered watches (no notify).
   *
   * @param {object} input
   * @param {object} input.diff
   * @param {object[]} input.changes
   * @param {object} input.toSnapshot
   * @param {object|null} [input.fromSnapshot]
   * @returns {object[]}
   */
  evaluate(input) {
    const { diff, changes, toSnapshot, fromSnapshot } = input;
    if (!toSnapshot) throw new Error('WatchRegistry.evaluate requires toSnapshot');
    const triggered = [];

    for (const watch of this.list(toSnapshot.tenantId)) {
      if (!matchesTarget(watch, toSnapshot, diff, changes)) continue;
      if (matchesCondition(watch, diff, changes, toSnapshot, fromSnapshot)) {
        triggered.push({
          watchId: watch.id,
          tenantId: watch.tenantId,
          targetType: watch.targetType,
          targetId: watch.targetId,
          condition: watch.condition,
          at: toSnapshot.timestamp,
          snapshotId: toSnapshot.id,
          scoreDelta: diff.scoreDelta,
          confidenceDelta: diff.confidenceDelta,
          matchedChanges: (changes || [])
            .filter((c) => relevantToWatch(watch, c))
            .map((c) => c.type)
            .sort(),
        });
      }
    }

    return triggered.sort((a, b) => String(a.watchId).localeCompare(String(b.watchId)));
  }
}

function matchesTarget(watch, toSnapshot, diff, changes) {
  if (watch.targetType === 'company') {
    return watch.targetId === toSnapshot.companyId;
  }
  if (watch.targetType === 'claim') {
    return (
      (diff.newClaims || []).includes(watch.targetId) ||
      (toSnapshot.claims || []).includes(watch.targetId) ||
      (changes || []).some(
        (c) =>
          (c.type === 'new_claim' || c.type === 'removed_claim') &&
          c.details.claimId === watch.targetId
      )
    );
  }
  if (watch.targetType === 'strategy') {
    return (diff.strategyChanges || []).some((s) => s.strategy === watch.targetId);
  }
  return false;
}

function matchesCondition(watch, diff, changes, toSnapshot, fromSnapshot) {
  const { op, field, value } = watch.condition;
  const numericValue = Number(value);

  if (op === WATCH_OPS.CHANGE_TYPE) {
    return (changes || []).some((c) => c.type === value);
  }

  if (field === 'score' || (!field && watch.targetType === 'company')) {
    if (op === WATCH_OPS.DELTA_ABS_GT) return Math.abs(diff.scoreDelta) > numericValue;
    if (op === WATCH_OPS.DELTA_GT) return diff.scoreDelta > numericValue;
    if (op === WATCH_OPS.DELTA_LT) return diff.scoreDelta < numericValue;
    if (op === WATCH_OPS.VALUE_GTE) return toSnapshot.score >= numericValue;
    if (op === WATCH_OPS.VALUE_LTE) return toSnapshot.score <= numericValue;
    if (op === WATCH_OPS.VALUE_EQ) return toSnapshot.score === numericValue;
  }

  if (field === 'confidence') {
    if (op === WATCH_OPS.DELTA_ABS_GT) return Math.abs(diff.confidenceDelta) > numericValue;
    if (op === WATCH_OPS.DELTA_GT) return diff.confidenceDelta > numericValue;
    if (op === WATCH_OPS.DELTA_LT) return diff.confidenceDelta < numericValue;
    if (op === WATCH_OPS.VALUE_GTE) return toSnapshot.confidence >= numericValue;
    if (op === WATCH_OPS.VALUE_LTE) return toSnapshot.confidence <= numericValue;
    if (op === WATCH_OPS.VALUE_EQ) return toSnapshot.confidence === numericValue;
  }

  if (field === 'claimConfidence' && watch.targetType === 'claim') {
    const claimConf = findClaimConfidence(toSnapshot, watch.targetId);
    if (claimConf == null) return false;
    if (op === WATCH_OPS.VALUE_GTE) return claimConf >= numericValue;
    if (op === WATCH_OPS.VALUE_LTE) return claimConf <= numericValue;
    if (op === WATCH_OPS.VALUE_EQ) return claimConf === numericValue;
  }

  if (field === 'strategyScore' && watch.targetType === 'strategy') {
    const sc = (diff.strategyChanges || []).find((s) => s.strategy === watch.targetId);
    if (!sc) return false;
    if (op === WATCH_OPS.DELTA_ABS_GT) return Math.abs(sc.scoreDeltaChange) > numericValue;
    if (op === WATCH_OPS.DELTA_GT) return sc.scoreDeltaChange > numericValue;
    if (op === WATCH_OPS.VALUE_GTE) return (sc.scoreDeltaAfter || 0) >= numericValue;
  }

  // Unused fromSnapshot kept for future watch fields
  void fromSnapshot;
  return false;
}

function findClaimConfidence(snapshot, claimId) {
  // Prefer recommendation supporting/opposing signal confidence if claim id matches
  const signals = [
    ...((snapshot.recommendation && snapshot.recommendation.supportingSignals) || []),
    ...((snapshot.recommendation && snapshot.recommendation.opposingSignals) || []),
  ];
  const hit = signals.find((s) => s.id === claimId || s.kind === 'claim' && s.id === claimId);
  if (hit && hit.confidence != null) return Number(hit.confidence);
  // Allow meta.claimConfidences map if callers attach it
  if (snapshot.meta && snapshot.meta.claimConfidences) {
    const v = snapshot.meta.claimConfidences[claimId];
    if (v != null) return Number(v);
  }
  return null;
}

function relevantToWatch(watch, change) {
  if (watch.targetType === 'claim') {
    return change.details && change.details.claimId === watch.targetId;
  }
  if (watch.targetType === 'strategy') {
    return change.details && change.details.strategy === watch.targetId;
  }
  return true;
}

module.exports = {
  WatchRegistry,
  WATCH_OPS,
};
