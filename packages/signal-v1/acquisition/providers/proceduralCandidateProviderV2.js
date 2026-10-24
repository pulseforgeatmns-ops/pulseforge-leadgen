'use strict';

const { createHash } = require('crypto');
const { isValidSolanaAddress } = require('../solanaAddress');
const { deriveEvidencePattern } = require('../evidencePattern');

const PROVIDER_ID = 'procedural-holdout-catalog-v2';
const DEFAULT_POOL_SIZE = 80;
const CATALOG_INDEX_OFFSET = 2000;

function deterministicTokenAddress(index) {
  const digest = createHash('sha256')
    .update(`signal-v1-validation-002:candidate:${index}`)
    .digest();
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
  const dayOffset = 200 + (index % 55);
  const hour = 10 + (index % 10);
  const base = Date.UTC(2026, 7, 1, hour, (index * 11) % 60, 0);
  return new Date(base + dayOffset * 86400000).toISOString();
}

/**
 * Holdout catalog — decoupled evidence pattern from selectionCategory.
 *
 * @param {{ poolSize?: number, indexOffset?: number }} [options]
 */
function discoverCandidates(options = {}) {
  const poolSize = options.poolSize ?? DEFAULT_POOL_SIZE;
  const indexOffset = options.indexOffset ?? CATALOG_INDEX_OFFSET;
  const out = [];
  for (let i = 0; i < poolSize; i += 1) {
    const catalogIndex = indexOffset + i;
    const tokenAddress = deterministicTokenAddress(catalogIndex);
    const selectionCategory = i % 2 === 0 ? 'stronger' : 'failure';
    const anchor = anchorForIndex(catalogIndex);
    const pattern = deriveEvidencePattern(tokenAddress, catalogIndex);
    out.push({
      tokenAddress,
      chain: 'solana',
      discoveredFrom: PROVIDER_ID,
      earliestKnownCallAt: anchor,
      sourceIds: [`src-holdout-caller-${i % 5}`, `src-holdout-caller-alt-${i % 7}`],
      sourceClusterIds:
        pattern === 'dual_cluster_run'
          ? [`cluster-holdout-a-${i % 4}`, `cluster-holdout-b-${(i + 1) % 5}`]
          : [`cluster-holdout-shared-${i % 6}`],
      selectionCategory,
      selectionReason: `Holdout catalog slot ${catalogIndex} (${selectionCategory})`,
      provenance: {
        provider: PROVIDER_ID,
        catalogIndex,
        generator: PROVIDER_ID,
        acquiredAt: '2026-10-05T00:00:00.000Z',
      },
      acquisitionPayload: {
        pattern,
        anchor,
        catalogIndex,
      },
    });
  }
  return out;
}

module.exports = {
  PROVIDER_ID,
  DEFAULT_POOL_SIZE,
  CATALOG_INDEX_OFFSET,
  deterministicTokenAddress,
  discoverCandidates,
  proceduralCandidateProviderV2: {
    providerId: PROVIDER_ID,
    discoverCandidates,
  },
};
