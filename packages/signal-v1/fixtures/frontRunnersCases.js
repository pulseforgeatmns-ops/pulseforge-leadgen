'use strict';

/** Research fixtures — Telegram MC claims are evidence only, not outcome truth. */

const FRONT_RUNNERS_CLUSTER_ID = 'cluster-front-runners-research';

const RESEARCH_CASES = Object.freeze([
  {
    slug: 'DOOM',
    ticker: 'DOOM',
    tokenAddress: 'Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump',
    addressProvenance: 'verified',
    /** Front Runners public call ~2026-08-04 01:27 ET → UTC */
    researchAnchor: '2026-08-04T05:27:00.000Z',
    telegramClaim: { fromMcUsd: 90000, toMcUsd: 8000000, provenance: 'source-claimed' },
  },
  {
    slug: 'DUPLICATE',
    ticker: 'DUPLICATE',
    tokenAddress: '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump',
    addressProvenance: 'verified',
    /** Front Runners public event 2026-09-11 (UTC anchor — window derived in researchWindows) */
    researchAnchor: '2026-09-11T22:43:00.000Z',
    telegramClaim: { fromMcUsd: 200000, toMcUsd: 1000000, provenance: 'source-claimed' },
  },
  {
    slug: 'HALLOW_INU',
    ticker: 'Hallow Inu',
    tokenAddress: '6iAj2oywQMiD9NeyTcW1S7UtG7e3jSK7Ud5ZJDqJpump',
    addressProvenance: 'verified',
    /** Public CA ~2026-09-29 18:43 ET → UTC */
    researchAnchor: '2026-09-29T22:43:00.000Z',
    telegramClaim: { provenance: 'source-claimed' },
  },
  {
    slug: 'WHALE',
    ticker: 'WHALE',
    tokenAddress: null,
    addressProvenance: 'unknown',
    telegramClaim: { fromMcUsd: 38000, toMcUsd: 340000, provenance: 'source-claimed' },
  },
  {
    slug: 'MISO',
    ticker: 'MISO',
    tokenAddress: null,
    addressProvenance: 'unknown',
    telegramClaim: { fromMcUsd: 100000, toMcUsd: 350000, provenance: 'source-claimed' },
  },
  {
    slug: 'BONZI',
    ticker: 'BONZI',
    tokenAddress: null,
    addressProvenance: 'unknown',
    telegramClaim: { fromMcUsd: 80000, toMcUsd: 667000, provenance: 'source-claimed' },
  },
]);

module.exports = {
  FRONT_RUNNERS_CLUSTER_ID,
  RESEARCH_CASES,
};
