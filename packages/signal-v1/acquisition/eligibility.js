'use strict';

const { isValidSolanaAddress } = require('./solanaAddress');
const { resolveResearchWindowFromAnchor } = require('../fixtures/researchWindows');

/**
 * @param {import('./providers/interfaces').RawResearchCandidate} raw
 * @param {{ earliestMarketEvaluableAt?: Date, latestMarketEvaluableAt?: Date }} [marketBounds]
 */
function evaluateCandidateEligibility(raw, marketBounds = {}) {
  const reasons = [];

  if (!raw?.tokenAddress) reasons.push('missing_token_address');
  else if (!isValidSolanaAddress(raw.tokenAddress)) reasons.push('unverifiable_ca');

  if (!raw?.earliestKnownCallAt) reasons.push('missing_call_timestamp');
  else if (!Number.isFinite(new Date(raw.earliestKnownCallAt).getTime())) {
    reasons.push('invalid_call_timestamp');
  }

  if (!raw?.discoveredFrom) reasons.push('missing_discovery_source');
  if (!raw?.provenance || typeof raw.provenance !== 'object') {
    reasons.push('missing_provenance');
  }

  const sourceIds = raw?.sourceIds || [];
  if (!sourceIds.length) reasons.push('missing_source_provenance');

  if (reasons.length) {
    return { eligible: false, exclusionReason: reasons.join(';') };
  }

  let window;
  try {
    window = resolveResearchWindowFromAnchor(raw.tokenAddress, raw.earliestKnownCallAt);
  } catch (err) {
    return { eligible: false, exclusionReason: `window_error:${err.message}` };
  }

  const anchorMs = new Date(raw.earliestKnownCallAt).getTime();
  if (marketBounds.earliestMarketEvaluableAt) {
    const min = marketBounds.earliestMarketEvaluableAt.getTime();
    if (anchorMs < min) {
      return { eligible: false, exclusionReason: 'event_outside_supported_historical_range' };
    }
  }
  if (marketBounds.latestMarketEvaluableAt) {
    const max = marketBounds.latestMarketEvaluableAt.getTime();
    if (anchorMs > max) {
      return { eligible: false, exclusionReason: 'event_outside_supported_historical_range' };
    }
  }

  return {
    eligible: true,
    exclusionReason: null,
    researchWindow: window,
  };
}

/**
 * @param {import('./providers/interfaces').RawResearchCandidate[]} discovered
 */
function dedupeCandidates(discovered) {
  const byKey = new Map();
  const rejected = [];

  for (const raw of discovered) {
    const key = `${raw.tokenAddress}|${new Date(raw.earliestKnownCallAt).toISOString()}`;
    if (byKey.has(key)) {
      rejected.push({
        raw,
        exclusionReason: 'duplicate_token_event',
      });
      continue;
    }
    byKey.set(key, raw);
  }

  return { unique: [...byKey.values()], rejectedDuplicates: rejected };
}

module.exports = {
  evaluateCandidateEligibility,
  dedupeCandidates,
};
