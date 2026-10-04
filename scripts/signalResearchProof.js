#!/usr/bin/env node
'use strict';

/**
 * End-to-end proof runner for DOOM / DUPLICATE / HALLOW INU (no Railway DB required).
 */

const { InMemorySignalStore } = require('../packages/signal-v1/storage/InMemorySignalStore');
const { seedFrontRunnersFixtures } = require('../packages/signal-v1/fixtures/seedFixtures');
const { SignalService } = require('../packages/signal-v1/SignalService');
const { RESEARCH_CASES } = require('../packages/signal-v1/fixtures/frontRunnersCases');

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
    const ingest = await service.ingestResearchToken(t.tokenAddress);
    const replay = await service.replay({
      tokenAddress: t.tokenAddress,
      replaceExisting: false,
    });
    report.push({
      slug: t.slug,
      tokenAddress: t.tokenAddress,
      ingest,
      replaySummary: {
        steps: replay.timeline.length,
        finalState: replay.finalState,
        executionDelayOutcomes: (replay.executionDelayOutcomes || []).map(r => ({
          delay: r.executionDelaySeconds,
          entryPrice: r.entry?.effectivePrice ?? null,
          outcome: r.outcome?.label ?? null,
          mfe: r.outcome?.mfe ?? null,
          mae: r.outcome?.mae ?? null,
        })),
      },
    });
  }

  console.log(JSON.stringify(report, null, 2));
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
