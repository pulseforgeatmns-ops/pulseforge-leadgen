#!/usr/bin/env node
'use strict';

const { InMemorySignalStore } = require('../packages/signal-v1/storage/InMemorySignalStore');
const { seedFrontRunnersFixtures } = require('../packages/signal-v1/fixtures/seedFixtures');
const { replayToken } = require('../packages/signal-v1/replay/replayEngine');
const { RESEARCH_CASES } = require('../packages/signal-v1/fixtures/frontRunnersCases');

function parseArg(name) {
  const hit = process.argv.find(a => a.startsWith(`--${name}=`));
  return hit ? hit.split('=').slice(1).join('=') : null;
}

const tokenArg = parseArg('token');
if (!tokenArg) {
  console.error('Usage: npm run signal:research-replay -- --token=<CA> [--price-path=json-file]');
  process.exit(1);
}

const matchedCase = RESEARCH_CASES.find(
  c => c.tokenAddress === tokenArg || c.slug.toLowerCase() === tokenArg.toLowerCase()
);
const tokenAddress = matchedCase?.tokenAddress || tokenArg;

const defaultPricePaths = {
  '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump': [
    { occurredAt: '2026-09-29T18:00:00Z', price: 0.00012 },
    { occurredAt: '2026-09-29T18:10:00Z', price: 0.00016 },
    { occurredAt: '2026-09-29T18:30:00Z', price: 0.00024 },
    { occurredAt: '2026-09-29T19:00:00Z', price: 0.0003 },
    { occurredAt: '2026-09-29T20:00:00Z', price: 0.00007 },
  ],
  Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump: [
    { occurredAt: '2026-09-28T14:00:00Z', price: 0.00009 },
    { occurredAt: '2026-09-28T14:05:00Z', price: 0.0001 },
    { occurredAt: '2026-09-28T14:30:00Z', price: 0.00015 },
    { occurredAt: '2026-09-29T14:00:00Z', price: 0.0002 },
  ],
  '6iAj2oywQMiD9NeyTcW1S7UtG7e3jSK7Ud5ZJDqJpump': [
    { occurredAt: '2026-09-27T20:00:00Z', price: 0.00004 },
    { occurredAt: '2026-09-27T20:10:00Z', price: 0.000045 },
    { occurredAt: '2026-09-27T21:00:00Z', price: 0.00003 },
  ],
};

const store = new InMemorySignalStore();
seedFrontRunnersFixtures(store);

const result = replayToken(store, {
  tokenAddress,
  pricePath: defaultPricePaths[tokenAddress] || [],
});

console.log(JSON.stringify({
  tokenAddress,
  slug: matchedCase?.slug || null,
  digest: result.digest,
  finalState: result.finalState,
  researchObservations: result.researchObservations.map(o => ({
    observationType: o.observationType,
    occurredAt: o.occurredAt,
    metadata: o.metadata,
    outcomes: store.researchObservationOutcomes
      .filter(x => x.observationId === o.id)
      .map(x => ({
        executionDelaySeconds: x.executionDelaySeconds,
        dataAvailability: x.dataAvailability,
        label: x.label,
        entryPrice: x.entryPrice,
      })),
  })),
}, null, 2));
