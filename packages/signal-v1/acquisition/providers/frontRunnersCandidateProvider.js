'use strict';

const { RESEARCH_CASES } = require('../../fixtures/frontRunnersCases');
const { FRONT_RUNNERS_CLUSTER_ID } = require('../../fixtures/frontRunnersCases');

const PROVIDER_ID = 'front-runners-research-fixtures';

/** Outcome category for cohort balancing only — not used in feature construction. */
const CATEGORY_BY_SLUG = Object.freeze({
  DOOM: 'stronger',
  DUPLICATE: 'stronger',
  HALLOW_INU: 'failure',
});

/**
 * @returns {import('./interfaces').RawResearchCandidate[]}
 */
function discoverCandidates() {
  const out = [];
  for (const c of RESEARCH_CASES) {
    if (!c.tokenAddress || c.addressProvenance !== 'verified') continue;
    out.push({
      tokenAddress: c.tokenAddress,
      chain: 'solana',
      discoveredFrom: PROVIDER_ID,
      earliestKnownCallAt: c.researchAnchor,
      sourceIds: ['src-front-runners', 'src-parkers-calls', 'src-independent-alpha'],
      sourceClusterIds: [FRONT_RUNNERS_CLUSTER_ID, 'cluster-independent-alpha'],
      selectionCategory: CATEGORY_BY_SLUG[c.slug] || 'unknown',
      selectionReason: `Phase A verified fixture (${c.slug})`,
      provenance: {
        provider: PROVIDER_ID,
        slug: c.slug,
        addressProvenance: c.addressProvenance,
        telegramClaim: c.telegramClaim,
        acquiredAt: '2026-10-04T00:00:00.000Z',
      },
    });
  }
  return out;
}

module.exports = {
  PROVIDER_ID,
  discoverCandidates,
  frontRunnersCandidateProvider: {
    providerId: PROVIDER_ID,
    discoverCandidates,
  },
};
