'use strict';

const { RESEARCH_CASES } = require('./frontRunnersCases');

/** Default research window padding: 1h before anchor through 24h after. */
const PRE_EVENT_MS = 60 * 60 * 1000;
const POST_EVENT_MS = 24 * 60 * 60 * 1000;

function resolveResearchWindowFromAnchor(tokenAddress, researchAnchor, meta = {}) {
  const anchorMs = new Date(researchAnchor).getTime();
  if (!Number.isFinite(anchorMs)) {
    throw new Error(`Invalid researchAnchor for ${tokenAddress}: ${researchAnchor}`);
  }
  const startTime = new Date(anchorMs - PRE_EVENT_MS);
  const endTime = new Date(anchorMs + POST_EVENT_MS);
  return {
    tokenAddress,
    startTime: startTime.toISOString(),
    endTime: endTime.toISOString(),
    resolutionSeconds: 60,
    anchor: new Date(researchAnchor).toISOString(),
    slug: meta.slug || null,
    ticker: meta.ticker || null,
  };
}

function resolveResearchWindow(tokenAddress) {
  const researchCase = RESEARCH_CASES.find(c => c.tokenAddress === tokenAddress);
  if (!researchCase?.researchAnchor) {
    throw new Error(
      `No researchAnchor configured for token ${tokenAddress}. Cannot infer historical event window.`
    );
  }
  return resolveResearchWindowFromAnchor(tokenAddress, researchCase.researchAnchor, {
    slug: researchCase.slug,
    ticker: researchCase.ticker,
  });
}

/**
 * @param {object} store
 * @param {string} tokenAddress
 */
function resolveResearchWindowForStore(store, tokenAddress) {
  try {
    return resolveResearchWindow(tokenAddress);
  } catch {
    const token =
      store?.tokens?.get?.(tokenAddress) ||
      (store?.getToken && typeof store.getToken === 'function' ? null : null);
    if (token?.metadata?.researchAnchor) {
      return resolveResearchWindowFromAnchor(tokenAddress, token.metadata.researchAnchor, {
        slug: token.metadata.slug,
        ticker: token.ticker,
      });
    }
    throw new Error(
      `No researchAnchor configured for token ${tokenAddress}. Cannot infer historical event window.`
    );
  }
}

function listResearchWindows() {
  return RESEARCH_CASES.filter(c => c.tokenAddress).map(c => resolveResearchWindow(c.tokenAddress));
}

module.exports = {
  resolveResearchWindow,
  resolveResearchWindowFromAnchor,
  resolveResearchWindowForStore,
  listResearchWindows,
  PRE_EVENT_MS,
  POST_EVENT_MS,
};
