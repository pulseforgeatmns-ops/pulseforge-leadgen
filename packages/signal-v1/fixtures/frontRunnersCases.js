'use strict';

/** Research fixtures — Telegram MC claims are evidence only, not outcome truth. */

const FRONT_RUNNERS_CLUSTER_ID = 'cluster-front-runners-research';

const RESEARCH_CASES = Object.freeze([
  {
    slug: 'DOOM',
    ticker: 'DOOM',
    tokenAddress: 'Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump',
    addressProvenance: 'verified',
    telegramClaim: { fromMcUsd: 90000, toMcUsd: 8000000, provenance: 'source-claimed' },
  },
  {
    slug: 'DUPLICATE',
    ticker: 'DUPLICATE',
    tokenAddress: '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump',
    addressProvenance: 'verified',
    telegramClaim: { fromMcUsd: 200000, toMcUsd: 1000000, provenance: 'source-claimed' },
  },
  {
    slug: 'HALLOW_INU',
    ticker: 'Hallow Inu',
    tokenAddress: '6iAj2oywQMiD9NeyTcW1S7UtG7e3jSK7Ud5ZJDqJpump',
    addressProvenance: 'verified',
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
