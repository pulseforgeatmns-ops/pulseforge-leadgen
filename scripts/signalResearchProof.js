#!/usr/bin/env node
'use strict';

/**
 * End-to-end proof runner for DOOM / DUPLICATE / HALLOW INU (no Railway DB required).
 */

const { InMemorySignalStore } = require('../packages/signal-v1/storage/InMemorySignalStore');
const { seedFrontRunnersFixtures } = require('../packages/signal-v1/fixtures/seedFixtures');
const { SignalService } = require('../packages/signal-v1/SignalService');
const { RESEARCH_CASES } = require('../packages/signal-v1/fixtures/frontRunnersCases');
const { resolveResearchWindow } = require('../packages/signal-v1/fixtures/researchWindows');

async function main() {
  const tokens = RESEARCH_CASES.filter(c => c.tokenAddress).map(c => ({
    slug: c.slug,
    tokenAddress: c.tokenAddress,
  }));

  const store = new InMemorySignalStore();
  seedFrontRunnersFixtures(store);
  const service = new SignalService(store, { seedFixtures: false });

  const report = [];
  for (const t of tokens) {
    const window = resolveResearchWindow(t.tokenAddress);
    const ingest = await service.ingestResearchToken(t.tokenAddress);
    const replay = await service.replay({
      tokenAddress: t.tokenAddress,
      replaceExisting: false,
    });
    report.push({
      slug: t.slug,
      tokenAddress: t.tokenAddress,
      requestedRange: window,
      actualProviderRange: {
        start: ingest.observationStart,
        end: ingest.observationEnd,
        providerEarliest: ingest.coverage?.providerEarliestObservation,
        providerLatest: ingest.coverage?.providerLatestObservation,
      },
      historicalDataStatus: ingest.historicalDataStatus,
      observationCount: ingest.observationCount,
      replayStatus: replay.replayStatus,
      replaySteps: replay.timeline?.length ?? 0,
      unavailable: ingest.unavailable || replay.skipped || false,
    });
  }

  console.log(JSON.stringify(report, null, 2));
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
