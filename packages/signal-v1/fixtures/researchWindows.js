'use strict';

const { RESEARCH_CASES } = require('./frontRunnersCases');

/** Default research window padding: 1h before anchor through 24h after. */
const PRE_EVENT_MS = 60 * 60 * 1000;
const POST_EVENT_MS = 24 * 60 * 60 * 1000;

function resolveResearchWindow(tokenAddress) {
  const researchCase = RESEARCH_CASES.find(c => c.tokenAddress === tokenAddress);
  if (!researchCase?.researchAnchor) {
    throw new Error(
      `No researchAnchor configured for token ${tokenAddress}. Cannot infer historical event window.`
    );
  }
  const anchorMs = new Date(researchCase.researchAnchor).getTime();
  if (!Number.isFinite(anchorMs)) {
    throw new Error(`Invalid researchAnchor for ${researchCase.slug}: ${researchCase.researchAnchor}`);
  }
  const startTime = new Date(anchorMs - PRE_EVENT_MS);
  const endTime = new Date(anchorMs + POST_EVENT_MS);
  return {
    tokenAddress,
    startTime: startTime.toISOString(),
    endTime: endTime.toISOString(),
    resolutionSeconds: 60,
    anchor: researchCase.researchAnchor,
    slug: researchCase.slug,
    ticker: researchCase.ticker,
  };
}

function listResearchWindows() {
  return RESEARCH_CASES.filter(c => c.tokenAddress).map(c => resolveResearchWindow(c.tokenAddress));
}

module.exports = {
  resolveResearchWindow,
  listResearchWindows,
  PRE_EVENT_MS,
  POST_EVENT_MS,
};
