'use strict';

const { callStore } = require('../../storage/storeUtils');

/**
 * Wallet evidence when declared in acquisition payload only.
 */
async function backfillWalletEvidence(store, candidate, raw) {
  const walletEvents = raw?.acquisitionPayload?.walletEvents;
  if (!Array.isArray(walletEvents) || !walletEvents.length) {
    return { status: 'UNAVAILABLE', inserted: 0 };
  }

  const tokenAddress = candidate.tokenAddress;
  let inserted = 0;
  for (const w of walletEvents) {
    if (!w.occurredAt || !w.eventType) continue;
    await callStore(store, 'insertEvent', {
      tokenAddress,
      chain: 'solana',
      eventType: w.eventType,
      occurredAt: new Date(w.occurredAt),
      observedAt: new Date(w.occurredAt),
      sourceType: 'wallet',
      walletAddress: w.walletAddress,
      payload: w.payload || {},
      provenance: {
        backfill: 'walletBackfill',
        provider: w.provider || 'acquisition',
        confidence: w.confidence ?? 0.7,
      },
      confidence: w.confidence ?? 0.7,
    });
    inserted += 1;
  }

  return { status: inserted ? 'AVAILABLE' : 'UNAVAILABLE', inserted };
}

module.exports = {
  backfillWalletEvidence,
};
