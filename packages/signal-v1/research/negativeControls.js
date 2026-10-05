'use strict';

const { createHash } = require('crypto');
const { DEFAULT_RESEARCH_CONFIG } = require('../config/defaultConfig');

function seededRng(seed) {
  let h = createHash('sha256').update(String(seed)).digest();
  let idx = 0;
  return () => {
    if (idx >= h.length - 4) {
      h = createHash('sha256').update(h).digest();
      idx = 0;
    }
    const n = h.readUInt32BE(idx);
    idx += 4;
    return n / 0xffffffff;
  };
}

function deterministicPermutation(length, seed) {
  const arr = [...Array(length).keys()];
  const rand = seededRng(seed);
  for (let i = arr.length - 1; i > 0; i -= 1) {
    const j = Math.floor(rand() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

/**
 * Shuffle PASS/FAIL labels onto convergence-triggered rows using a broader cohort pool.
 *
 * @param {object[]} convergenceLabeledRows — rows with { label } for triggered convergence
 * @param {string[]} cohortOutcomePool — resolved PASS/FAIL labels from all cohort tokens (e.g. FIRST_CALLER)
 * @param {string|number} seed
 */
function simulateShuffledOutcomeLabels(
  convergenceLabeledRows,
  cohortOutcomePool = null,
  seed = 'signal-v1-shuffle-outcomes-v1'
) {
  const pool =
    cohortOutcomePool && cohortOutcomePool.length
      ? cohortOutcomePool.filter(l => l === 'PASS' || l === 'FAIL')
      : convergenceLabeledRows.map(r => r.label).filter(l => l === 'PASS' || l === 'FAIL');

  if (!convergenceLabeledRows.length || !pool.length) {
    return { permutations: [], summary: { precisionSamples: [] }, observed: null };
  }

  const labels = pool;
  const seeds = [
    seed,
    `${seed}:1`,
    `${seed}:2`,
    `${seed}:3`,
    `${seed}:4`,
  ];

  const precisionSamples = [];
  const permutations = [];

  const targetN = convergenceLabeledRows.length;
  for (const s of seeds) {
    const perm = deterministicPermutation(labels.length, s);
    const shuffled = perm.slice(0, targetN).map(i => labels[i % labels.length]);
    let pass = 0;
    let fail = 0;
    for (const l of shuffled) {
      if (l === 'PASS') pass += 1;
      if (l === 'FAIL') fail += 1;
    }
    const resolved = pass + fail;
    precisionSamples.push(resolved ? pass / resolved : null);
    permutations.push({ seed: s, pass, fail, resolved, precision: resolved ? pass / resolved : null });
  }

  const convergedLabels = convergenceLabeledRows
    .map(r => r.label)
    .filter(l => l === 'PASS' || l === 'FAIL');
  const observedPass = convergedLabels.filter(l => l === 'PASS').length;
  const observedResolved = convergedLabels.length;
  const observedPrecision = observedResolved ? observedPass / observedResolved : null;

  return {
    observed: {
      pass: observedPass,
      fail: convergedLabels.length - observedPass,
      precision: observedPrecision,
      convergedN: observedResolved,
      poolN: pool.length,
    },
    permutations,
    summary: {
      precisionSamples,
      meanShuffledPrecision:
        precisionSamples.filter(p => p != null).reduce((a, b) => a + b, 0) /
        (precisionSamples.filter(p => p != null).length || 1),
      destroysPerfectRelationship: precisionSamples.some(p => p != null && p < 1),
    },
  };
}

/**
 * Permute second-caller timestamps across tokens (in-memory simulation).
 *
 * @param {object[]} auditRows — from buildConvergenceAuditRows
 * @param {string|number} seed
 */
function simulateShuffledConvergenceSecondCallers(auditRows, seed = 'signal-v1-shuffle-convergence-v1') {
  const converged = auditRows.filter(r => r.independentConvergenceTriggered && r.secondCallerTimestamp);
  if (converged.length < 2) {
    return { degraded: false, note: 'insufficient converged rows' };
  }

  const secondTimes = converged.map(r => new Date(r.secondCallerTimestamp).getTime());
  const perm = deterministicPermutation(secondTimes.length, seed);
  const shuffledTimes = perm.map(i => secondTimes[i]);

  let wouldStillConverge = 0;
  for (let i = 0; i < converged.length; i += 1) {
    const row = converged[i];
    const firstMs = new Date(row.firstCallerTimestamp).getTime();
    const secondMs = shuffledTimes[i];
    const deltaMin = (secondMs - firstMs) / 60000;
    const windowMin = DEFAULT_RESEARCH_CONFIG.independentConvergenceWindowMinutes;
    if (deltaMin >= 0 && deltaMin <= windowMin) wouldStillConverge += 1;
  }

  const originalN = converged.length;
  return {
    originalConvergedN: originalN,
    afterShuffleStillWithinWindowN: wouldStillConverge,
    degraded: wouldStillConverge < originalN,
    retentionRate: originalN ? wouldStillConverge / originalN : null,
    seed,
  };
}

module.exports = {
  simulateShuffledOutcomeLabels,
  simulateShuffledConvergenceSecondCallers,
  deterministicPermutation,
  seededRng,
};
