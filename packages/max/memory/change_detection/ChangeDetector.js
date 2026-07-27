'use strict';

const { round } = require('../../reasoning/ReasoningTypes');
const { CHANGE_TYPES } = require('../snapshots/MemoryTypes');

/** Default thresholds for meaningful change. */
const DEFAULT_THRESHOLDS = Object.freeze({
  scoreDelta: 5,
  confidenceDelta: 5,
  strategyScoreDelta: 10,
});

/**
 * Change Detector — classify meaningful transitions between snapshots.
 * Outputs structured ChangeEvent[] only (no notifications, no LLM).
 */
class ChangeDetector {
  /**
   * @param {object} [options]
   * @param {Partial<typeof DEFAULT_THRESHOLDS>} [options.thresholds]
   */
  constructor(options = {}) {
    this.thresholds = { ...DEFAULT_THRESHOLDS, ...(options.thresholds || {}) };
  }

  /**
   * @param {object} diff - ReasoningDiff from DiffEngine
   * @param {object} [fromSnapshot]
   * @param {object} [toSnapshot]
   * @returns {object[]}
   */
  detect(diff, fromSnapshot = null, toSnapshot = null) {
    if (!diff) throw new Error('ChangeDetector.detect requires diff');
    /** @type {object[]} */
    const changes = [];

    if (diff.isInitial) {
      changes.push(
        changeEvent(CHANGE_TYPES.SCORE_INCREASED, {
          magnitude: diff.scoreAfter,
          message: 'initial_snapshot',
          scoreAfter: diff.scoreAfter,
          confidenceAfter: diff.confidenceAfter,
        })
      );
      for (const claimId of diff.newClaims || []) {
        changes.push(changeEvent(CHANGE_TYPES.NEW_CLAIM, { claimId }));
      }
      for (const evidenceId of diff.newEvidence || []) {
        changes.push(changeEvent(CHANGE_TYPES.NEW_EVIDENCE, { evidenceId }));
      }
      return sortChanges(changes);
    }

    if (diff.scoreDelta >= this.thresholds.scoreDelta) {
      changes.push(
        changeEvent(CHANGE_TYPES.SCORE_INCREASED, {
          magnitude: diff.scoreDelta,
          scoreBefore: diff.scoreBefore,
          scoreAfter: diff.scoreAfter,
        })
      );
    } else if (diff.scoreDelta <= -this.thresholds.scoreDelta) {
      changes.push(
        changeEvent(CHANGE_TYPES.SCORE_DECREASED, {
          magnitude: Math.abs(diff.scoreDelta),
          scoreBefore: diff.scoreBefore,
          scoreAfter: diff.scoreAfter,
        })
      );
    }

    if (diff.confidenceDelta >= this.thresholds.confidenceDelta) {
      changes.push(
        changeEvent(CHANGE_TYPES.CONFIDENCE_INCREASED, {
          magnitude: diff.confidenceDelta,
          confidenceBefore: diff.confidenceBefore,
          confidenceAfter: diff.confidenceAfter,
        })
      );
    } else if (diff.confidenceDelta <= -this.thresholds.confidenceDelta) {
      changes.push(
        changeEvent(CHANGE_TYPES.CONFIDENCE_DECREASED, {
          magnitude: Math.abs(diff.confidenceDelta),
          confidenceBefore: diff.confidenceBefore,
          confidenceAfter: diff.confidenceAfter,
        })
      );
    }

    for (const claimId of diff.newClaims || []) {
      changes.push(changeEvent(CHANGE_TYPES.NEW_CLAIM, { claimId }));
    }
    for (const claimId of diff.removedClaims || []) {
      changes.push(changeEvent(CHANGE_TYPES.REMOVED_CLAIM, { claimId }));
    }
    for (const evidenceId of diff.newEvidence || []) {
      changes.push(changeEvent(CHANGE_TYPES.NEW_EVIDENCE, { evidenceId }));
    }
    for (const evidenceId of diff.removedEvidence || []) {
      changes.push(changeEvent(CHANGE_TYPES.REMOVED_EVIDENCE, { evidenceId }));
    }

    for (const sc of diff.strategyChanges || []) {
      if (!sc.changed) continue;
      if (sc.scoreDeltaChange >= this.thresholds.strategyScoreDelta) {
        changes.push(
          changeEvent(CHANGE_TYPES.STRATEGY_SCORE_UP, {
            strategy: sc.strategy,
            magnitude: sc.scoreDeltaChange,
            scoreDeltaBefore: sc.scoreDeltaBefore,
            scoreDeltaAfter: sc.scoreDeltaAfter,
          })
        );
        if (sc.strategy === 'opportunity') {
          changes.push(
            changeEvent(CHANGE_TYPES.NEW_OPPORTUNITY_SIGNAL, {
              strategy: sc.strategy,
              magnitude: sc.scoreDeltaChange,
            })
          );
        }
        if (sc.strategy === 'decision_maker') {
          changes.push(
            changeEvent(CHANGE_TYPES.NEW_DECISION_MAKER, {
              strategy: sc.strategy,
              magnitude: sc.scoreDeltaChange,
            })
          );
        }
        if (sc.strategy === 'opportunity' || sc.strategy === 'overflow') {
          const hiring = detectHiringInStrategy(sc, toSnapshot);
          if (hiring) {
            changes.push(changeEvent(CHANGE_TYPES.NEW_HIRING_SIGNAL, hiring));
          }
        }
      } else if (sc.scoreDeltaChange <= -this.thresholds.strategyScoreDelta) {
        changes.push(
          changeEvent(CHANGE_TYPES.STRATEGY_SCORE_DOWN, {
            strategy: sc.strategy,
            magnitude: Math.abs(sc.scoreDeltaChange),
            scoreDeltaBefore: sc.scoreDeltaBefore,
            scoreDeltaAfter: sc.scoreDeltaAfter,
          })
        );
      }
    }

    // Contradictions are first-class — detect even when strategy score is flat
    if (toSnapshot && fromSnapshot) {
      const strategyIds = [
        ...new Set([
          ...(fromSnapshot.strategyResults || []).map((r) => r.strategy),
          ...(toSnapshot.strategyResults || []).map((r) => r.strategy),
        ]),
      ].sort();
      for (const strategyId of strategyIds) {
        const before = findStrategy(fromSnapshot, strategyId);
        const after = findStrategy(toSnapshot, strategyId);
        if (!after) continue;
        const beforeIds = new Set(
          ((before && before.contradictingEvidence) || []).map((e) => e.id)
        );
        for (const e of after.contradictingEvidence || []) {
          if (!beforeIds.has(e.id)) {
            changes.push(
              changeEvent(CHANGE_TYPES.NEW_CONTRADICTION, {
                strategy: strategyId,
                evidenceId: e.id,
                summary: e.summary,
              })
            );
          }
        }
      }
    }

    if (diff.recommendation && diff.recommendation.priorityChanged) {
      changes.push(
        changeEvent(CHANGE_TYPES.PRIORITY_CHANGED, {
          priorityBefore: diff.recommendation.priorityBefore,
          priorityAfter: diff.recommendation.priorityAfter,
        })
      );
    }
    if (diff.recommendation && diff.recommendation.typeChanged) {
      changes.push(
        changeEvent(CHANGE_TYPES.TYPE_CHANGED, {
          typeBefore: diff.recommendation.typeBefore,
          typeAfter: diff.recommendation.typeAfter,
        })
      );
    }
    if (diff.recommendation && diff.recommendation.actionChanged) {
      changes.push(
        changeEvent(CHANGE_TYPES.ACTION_CHANGED, {
          actionBefore: diff.recommendation.actionBefore,
          actionAfter: diff.recommendation.actionAfter,
        })
      );
    }

    return sortChanges(dedupeChanges(changes));
  }
}

function changeEvent(type, details = {}) {
  return {
    type,
    magnitude: details.magnitude == null ? null : round(Number(details.magnitude)),
    details: { ...details },
  };
}

function sortChanges(changes) {
  return [...changes].sort((a, b) => {
    const c = String(a.type).localeCompare(String(b.type));
    if (c !== 0) return c;
    return JSON.stringify(a.details).localeCompare(JSON.stringify(b.details));
  });
}

function dedupeChanges(changes) {
  const seen = new Set();
  const out = [];
  for (const c of changes) {
    const key = `${c.type}:${JSON.stringify(c.details)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(c);
  }
  return out;
}

function findStrategy(snapshot, strategyId) {
  return (snapshot.strategyResults || []).find((r) => r.strategy === strategyId) || null;
}

function detectHiringInStrategy(sc, toSnapshot) {
  if (!toSnapshot) return null;
  const after = findStrategy(toSnapshot, sc.strategy);
  if (!after) return null;
  const signals = [
    ...(after.supportingEvidence || []),
    ...(after.summary ? [{ id: 'summary', summary: after.summary }] : []),
  ];
  const hit = signals.find((s) => /hiring|operations manager|new hire/i.test(s.summary || ''));
  if (!hit) return null;
  return { strategy: sc.strategy, evidenceId: hit.id, summary: hit.summary };
}

module.exports = {
  ChangeDetector,
  DEFAULT_THRESHOLDS,
  CHANGE_TYPES,
};
