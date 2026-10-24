'use strict';

/**
 * Temporal explainability — Why? → Evidence → History → Change → Reason.
 * Extends current-state explanation with transition context.
 */
class TemporalExplanationEngine {
  /**
   * @param {object} input
   * @param {object} [input.currentExplanation] - from ExplanationEngine.explain()
   * @param {object} input.toSnapshot
   * @param {object|null} [input.fromSnapshot]
   * @param {object} input.diff
   * @param {object[]} input.changes
   * @param {object[]} [input.timeline]
   */
  explain(input) {
    const {
      currentExplanation,
      toSnapshot,
      fromSnapshot,
      diff,
      changes,
      timeline,
    } = input;

    if (!toSnapshot || !diff) {
      throw new Error('TemporalExplanationEngine requires toSnapshot and diff');
    }

    const why = buildWhy(diff, changes);
    const evidence = {
      newEvidence: diff.newEvidence || [],
      removedEvidence: diff.removedEvidence || [],
      newClaims: diff.newClaims || [],
      removedClaims: diff.removedClaims || [],
      supportingSignals: (toSnapshot.recommendation && toSnapshot.recommendation.supportingSignals) || [],
      opposingSignals: (toSnapshot.recommendation && toSnapshot.recommendation.opposingSignals) || [],
    };

    const history = (timeline || []).map((t) => ({
      timestamp: t.timestamp,
      snapshotId: t.snapshotId,
      score: t.score,
      confidence: t.confidence,
      scoreDelta: t.scoreDelta,
      changeTypes: t.changeTypes,
    }));

    const change = {
      score: {
        before: diff.scoreBefore,
        after: diff.scoreAfter,
        delta: diff.scoreDelta,
      },
      confidence: {
        before: diff.confidenceBefore,
        after: diff.confidenceAfter,
        delta: diff.confidenceDelta,
      },
      events: changes || [],
    };

    const reason = (changes || []).map((c) => ({
      type: c.type,
      magnitude: c.magnitude,
      details: c.details,
    }));

    return {
      subjectId: toSnapshot.companyId,
      fromSnapshotId: fromSnapshot ? fromSnapshot.id : null,
      toSnapshotId: toSnapshot.id,
      chain: {
        why,
        evidence,
        history,
        change,
        reason,
      },
      why,
      evidence,
      history,
      change,
      reason,
      current: currentExplanation || null,
    };
  }
}

function buildWhy(diff, changes) {
  const parts = [];
  if (diff.isInitial) {
    parts.push({ code: 'initial_snapshot', score: diff.scoreAfter, confidence: diff.confidenceAfter });
  }
  if (diff.scoreDelta !== 0 && !diff.isInitial) {
    parts.push({
      code: 'score_delta',
      before: diff.scoreBefore,
      after: diff.scoreAfter,
      delta: diff.scoreDelta,
    });
  }
  if (diff.confidenceDelta !== 0 && !diff.isInitial) {
    parts.push({
      code: 'confidence_delta',
      before: diff.confidenceBefore,
      after: diff.confidenceAfter,
      delta: diff.confidenceDelta,
    });
  }
  for (const c of changes || []) {
    parts.push({ code: c.type, magnitude: c.magnitude, details: c.details });
  }
  return parts;
}

module.exports = { TemporalExplanationEngine };
