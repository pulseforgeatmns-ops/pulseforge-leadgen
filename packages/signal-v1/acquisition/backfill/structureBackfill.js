'use strict';

const { callStore } = require('../../storage/storeUtils');

/**
 * Historical structure only — snapshots at or before anchor-derived times.
 * Never uses current on-chain holder state.
 */
async function backfillStructureEvidence(store, candidate, raw) {
  const anchor = new Date(candidate.earliestKnownCallAt || raw.earliestKnownCallAt);
  const tokenAddress = candidate.tokenAddress;
  const at = new Date(anchor.getTime() - 5 * 60000);

  const payload = raw?.acquisitionPayload?.structure;
  if (!payload) {
    return { status: 'UNAVAILABLE', inserted: 0 };
  }

  const ev = {
    tokenAddress,
    chain: 'solana',
    eventType: 'HOLDER_SNAPSHOT',
    occurredAt: at,
    observedAt: at,
    sourceType: 'manual',
    payload: {
      top10HolderPct: payload.top10HolderPct ?? null,
      devHoldingPct: payload.devHoldingPct ?? null,
      bundleSupplyPct: payload.bundleSupplyPct ?? null,
      liquidityUsd: payload.liquidityUsd ?? null,
      tokenAgeSeconds: payload.tokenAgeSeconds ?? null,
    },
    provenance: {
      backfill: 'structureBackfill',
      temporalBasis: 'historical_snapshot_at_or_before_anchor',
      acquiredAt: at.toISOString(),
    },
    confidence: payload.confidence ?? 0.6,
  };

  await callStore(store, 'insertEvent', ev);
  return { status: 'AVAILABLE', inserted: 1 };
}

module.exports = {
  backfillStructureEvidence,
};
