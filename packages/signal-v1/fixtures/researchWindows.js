'use strict';

const { RESEARCH_CASES } = require('./frontRunnersCases');

/**
 * Research ingestion windows — at least 1h before first fixture event through 24h after.
 * DUPLICATE fixture timeline starts 2026-09-29T18:00:00Z.
 */
const DEFAULT_EVENT_ANCHOR = '2026-09-29T18:00:00Z';

function resolveResearchWindow(tokenAddress) {
  const researchCase = RESEARCH_CASES.find(c => c.tokenAddress === tokenAddress);
  const anchor = researchCase?.researchAnchor || DEFAULT_EVENT_ANCHOR;
  const anchorMs = new Date(anchor).getTime();
  const startTime = new Date(anchorMs - 60 * 60 * 1000);
  const endTime = new Date(anchorMs + 24 * 60 * 60 * 1000);
  return {
    tokenAddress,
    startTime: startTime.toISOString(),
    endTime: endTime.toISOString(),
    resolutionSeconds: 60,
    anchor,
    slug: researchCase?.slug || null,
  };
}

module.exports = {
  resolveResearchWindow,
  DEFAULT_EVENT_ANCHOR,
};
