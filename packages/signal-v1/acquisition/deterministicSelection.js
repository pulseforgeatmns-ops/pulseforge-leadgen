'use strict';

const { createHash } = require('crypto');

/**
 * Documented deterministic selection:
 * 1. Stable sort eligible by tokenAddress ASC, then earliestKnownCallAt ASC
 * 2. Partition by selectionCategory (stronger | failure)
 * 3. Take first perCategory from each partition
 * 4. Tie-break within equal sort keys uses selectionVersion hash (stable)
 *
 * @param {object[]} eligible — persisted candidate rows with selectionCategory
 * @param {{ perCategory?: number, selectionVersion?: string, targetSize?: number }} options
 */
function selectBalancedCohort(eligible, options = {}) {
  const perCategory = options.perCategory ?? 20;
  const selectionVersion = options.selectionVersion || 'validation-001-selection-v1';
  const targetSize = options.targetSize ?? perCategory * 2;

  const sorted = [...eligible].sort((a, b) => {
    const addr = a.tokenAddress.localeCompare(b.tokenAddress);
    if (addr !== 0) return addr;
    const ta = new Date(a.earliestKnownCallAt).getTime();
    const tb = new Date(b.earliestKnownCallAt).getTime();
    if (ta !== tb) return ta - tb;
    return stableHash(`${selectionVersion}:${a.tokenAddress}`).localeCompare(
      stableHash(`${selectionVersion}:${b.tokenAddress}`)
    );
  });

  const stronger = sorted.filter(c => c.selectionCategory === 'stronger');
  const failure = sorted.filter(c => c.selectionCategory === 'failure');

  const selectedStronger = stronger.slice(0, perCategory);
  const selectedFailure = failure.slice(0, perCategory);
  const selected = [...selectedStronger, ...selectedFailure].slice(0, targetSize);

  return {
    selected,
    breakdown: {
      strongerPool: stronger.length,
      failurePool: failure.length,
      selectedStronger: selectedStronger.length,
      selectedFailure: selectedFailure.length,
      targetSize,
      perCategory,
      selectionVersion,
      procedure: 'stable_sort_by_token_then_anchor;first_N_per_category',
    },
  };
}

function stableHash(input) {
  return createHash('sha256').update(String(input)).digest('hex');
}

module.exports = {
  selectBalancedCohort,
  stableHash,
};
