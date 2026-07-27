'use strict';

const { createHash } = require('crypto');

/**
 * ReplayComparator — compare two replay executions (SPEC-018).
 *
 * Typical uses:
 * - Runtime v1 vs Runtime v2
 * - Market Ontology v1 vs Market Ontology v2
 * - Strategy Pack A vs Strategy Pack B
 */
class ReplayComparator {
  /**
   * @param {import('./types').ReplayRunResult} left
   * @param {import('./types').ReplayRunResult} right
   * @returns {object}
   */
  compare(left, right) {
    if (!left || !right) {
      throw new Error('ReplayComparator.compare requires two replay results');
    }

    return {
      versions: {
        left: left.versions || null,
        right: right.versions || null,
      },
      confidenceDifferences: compareConfidence(left, right),
      recommendationDifferences: compareRecommendations(left, right),
      claimDifferences: compareClaims(left, right),
      reasoningDifferences: compareReasoning(left, right),
      identical: resultsIdentical(left, right),
    };
  }
}

/**
 * @param {import('./types').ReplayRunResult} left
 * @param {import('./types').ReplayRunResult} right
 */
function compareConfidence(left, right) {
  const leftConf = left.confidence;
  const rightConf = right.confidence;
  const delta =
    leftConf != null && rightConf != null
      ? round(Number(rightConf) - Number(leftConf))
      : null;

  const stepDiffs = [];
  const leftSteps = left.steps || [];
  const rightSteps = right.steps || [];
  const n = Math.max(leftSteps.length, rightSteps.length);
  for (let i = 0; i < n; i++) {
    const l = leftSteps[i];
    const r = rightSteps[i];
    const lConf = l ? l.confidence : null;
    const rConf = r ? r.confidence : null;
    if (lConf !== rConf) {
      stepDiffs.push({
        observationId: (r && r.observation && r.observation.id) ||
          (l && l.observation && l.observation.id) ||
          null,
        left: lConf,
        right: rConf,
        delta:
          lConf != null && rConf != null
            ? round(Number(rConf) - Number(lConf))
            : null,
      });
    }
  }

  return {
    left: leftConf,
    right: rightConf,
    delta,
    changed: leftConf !== rightConf,
    steps: stepDiffs,
  };
}

/**
 * @param {import('./types').ReplayRunResult} left
 * @param {import('./types').ReplayRunResult} right
 */
function compareRecommendations(left, right) {
  const leftRecs = left.recommendations || [];
  const rightRecs = right.recommendations || [];
  const leftFinal = leftRecs[leftRecs.length - 1] || null;
  const rightFinal = rightRecs[rightRecs.length - 1] || null;

  return {
    leftCount: leftRecs.length,
    rightCount: rightRecs.length,
    leftAction: leftFinal ? leftFinal.recommendedAction : null,
    rightAction: rightFinal ? rightFinal.recommendedAction : null,
    leftScore: leftFinal ? leftFinal.score : null,
    rightScore: rightFinal ? rightFinal.score : null,
    actionChanged:
      (leftFinal && leftFinal.recommendedAction) !==
      (rightFinal && rightFinal.recommendedAction),
    fingerprintChanged:
      stableFingerprint(leftFinal) !== stableFingerprint(rightFinal),
  };
}

/**
 * @param {import('./types').ReplayRunResult} left
 * @param {import('./types').ReplayRunResult} right
 */
function compareClaims(left, right) {
  const leftIds = claimIds(left.claims);
  const rightIds = claimIds(right.claims);
  const leftSet = new Set(leftIds);
  const rightSet = new Set(rightIds);

  return {
    leftOnly: leftIds.filter((id) => !rightSet.has(id)),
    rightOnly: rightIds.filter((id) => !leftSet.has(id)),
    shared: leftIds.filter((id) => rightSet.has(id)),
    changed:
      leftIds.length !== rightIds.length ||
      leftIds.some((id, i) => id !== rightIds[i]),
  };
}

/**
 * @param {import('./types').ReplayRunResult} left
 * @param {import('./types').ReplayRunResult} right
 */
function compareReasoning(left, right) {
  const leftTrace = canonicalizeTrace(left.reasoningTrace);
  const rightTrace = canonicalizeTrace(right.reasoningTrace);
  const leftHash = hashJson(leftTrace);
  const rightHash = hashJson(rightTrace);

  return {
    leftHash,
    rightHash,
    changed: leftHash !== rightHash,
    leftStepCount: (left.steps || []).length,
    rightStepCount: (right.steps || []).length,
  };
}

/**
 * @param {unknown} claims
 * @returns {string[]}
 */
function claimIds(claims) {
  if (!claims) return [];
  if (Array.isArray(claims)) {
    return claims
      .map((c) => (c && (c.id || c.claimType || c.strategy)) || null)
      .filter(Boolean)
      .map(String)
      .sort();
  }
  if (typeof claims === 'object') {
    const derived = claims.derived || claims.results || claims.observations || [];
    return claimIds(derived);
  }
  return [];
}

/**
 * @param {object|null} recommendation
 */
function stableFingerprint(recommendation) {
  if (!recommendation) return 'null';
  return hashJson({
    action: recommendation.recommendedAction || null,
    score: recommendation.score ?? null,
    confidence: recommendation.confidence ?? null,
    type: recommendation.type || null,
    claims: (recommendation.claims || []).slice().sort(),
  });
}

/**
 * @param {object|null|undefined} trace
 */
function canonicalizeTrace(trace) {
  if (!trace || typeof trace !== 'object') return null;
  const copy = { ...trace };
  delete copy.strategyTimings;
  if (Array.isArray(copy.steps)) {
    copy.steps = copy.steps.map((s) => {
      if (s && typeof s === 'object' && !Array.isArray(s)) {
        const step = { ...s };
        delete step.at;
        return step;
      }
      return s;
    });
  }
  return copy;
}

/**
 * @param {import('./types').ReplayRunResult} left
 * @param {import('./types').ReplayRunResult} right
 */
function resultsIdentical(left, right) {
  return (
    hashJson(canonicalizeResult(left)) === hashJson(canonicalizeResult(right))
  );
}

/**
 * @param {import('./types').ReplayRunResult} result
 */
function canonicalizeResult(result) {
  return {
    subjectId: result.subjectId,
    startTime: result.startTime,
    endTime: result.endTime,
    observations: (result.observations || []).map((o) => o.id),
    confidence: result.confidence,
    claims: claimIds(result.claims),
    recommendations: (result.recommendations || []).map((r) => ({
      action: r.recommendedAction,
      score: r.score,
      confidence: r.confidence,
    })),
    versions: result.versions,
    reasoning: canonicalizeTrace(result.reasoningTrace),
  };
}

/**
 * @param {unknown} value
 */
function hashJson(value) {
  return createHash('sha256')
    .update(stableStringify(value))
    .digest('hex')
    .slice(0, 24);
}

/**
 * @param {unknown} value
 */
function stableStringify(value) {
  return JSON.stringify(sortKeys(value));
}

/**
 * @param {unknown} value
 */
function sortKeys(value) {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value && typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value).sort()) {
      out[key] = sortKeys(value[key]);
    }
    return out;
  }
  return value;
}

function round(n) {
  return Math.round(Number(n) * 1000) / 1000;
}

/**
 * @returns {ReplayComparator}
 */
function createReplayComparator() {
  return new ReplayComparator();
}

module.exports = {
  ReplayComparator,
  createReplayComparator,
  canonicalizeResult,
  hashJson,
  stableStringify,
};
