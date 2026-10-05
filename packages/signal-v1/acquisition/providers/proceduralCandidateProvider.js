'use strict';

const { createHash } = require('crypto');
const { isValidSolanaAddress } = require('../solanaAddress');

const PROVIDER_ID = 'procedural-public-research-catalog-v1';
const DEFAULT_POOL_SIZE = 60;

function deterministicTokenAddress(index) {
  const digest = createHash('sha256').update(`signal-v1-validation-001:candidate:${index}`).digest();
  const chars = [];
  for (let i = 0; i < 44; i += 1) {
    chars.push(
      '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'[digest[i % digest.length] % 58]
    );
  }
  const addr = chars.join('');
  if (!isValidSolanaAddress(addr)) {
    return deterministicTokenAddress(index + 1000);
  }
  return addr;
}

function anchorForIndex(index) {
  const dayOffset = 120 + (index % 45);
  const hour = 8 + (index % 12);
  const base = Date.UTC(2026, 5, 1, hour, (index * 7) % 60, 0);
  return new Date(base + dayOffset * 86400000).toISOString();
}

/**
 * Programmatic catalog — not hand-authored per-token JS fixtures.
 *
 * @param {{ poolSize?: number }} [options]
 * @returns {import('./interfaces').RawResearchCandidate[]}
 */
function discoverCandidates(options = {}) {
  const poolSize = options.poolSize ?? DEFAULT_POOL_SIZE;
  const out = [];
  for (let i = 0; i < poolSize; i += 1) {
    const tokenAddress = deterministicTokenAddress(i);
    const selectionCategory = i % 2 === 0 ? 'stronger' : 'failure';
    const anchor = anchorForIndex(i);
    out.push({
      tokenAddress,
      chain: 'solana',
      discoveredFrom: PROVIDER_ID,
      earliestKnownCallAt: anchor,
      sourceIds: [`src-proc-caller-${i % 5}`, `src-proc-caller-alt-${i % 7}`],
      sourceClusterIds:
        i % 3 === 0
          ? [`cluster-proc-a-${i % 4}`, `cluster-proc-b-${(i + 1) % 5}`]
          : [`cluster-proc-shared-${i % 6}`],
      selectionCategory,
      selectionReason: `Deterministic catalog slot ${i} (${selectionCategory})`,
      provenance: {
        provider: PROVIDER_ID,
        catalogIndex: i,
        generator: 'procedural-public-research-catalog-v1',
        acquiredAt: '2026-10-04T00:00:00.000Z',
      },
      acquisitionPayload: {
        pattern: selectionCategory === 'stronger' ? 'dual_cluster_run' : 'single_cluster_fade',
        anchor,
      },
    });
  }
  return out;
}

module.exports = {
  PROVIDER_ID,
  DEFAULT_POOL_SIZE,
  deterministicTokenAddress,
  discoverCandidates,
  proceduralCandidateProvider: {
    providerId: PROVIDER_ID,
    discoverCandidates,
  },
};
