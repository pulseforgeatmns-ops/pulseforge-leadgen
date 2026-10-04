#!/usr/bin/env node
'use strict';

const pool = require('../db.js');
const { createSignalStore } = require('../packages/signal-v1/storage/createSignalStore');
const { SignalService } = require('../packages/signal-v1/SignalService');

function parseArgs(argv) {
  const out = {};
  for (const arg of argv) {
    if (!arg.startsWith('--')) continue;
    const [key, value] = arg.slice(2).split('=');
    out[key] = value ?? true;
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const tokenAddress = args.token;
  if (!tokenAddress) {
    console.error('Usage: npm run signal:ingest-history -- --token=<CA> [--start=ISO] [--end=ISO]');
    process.exit(1);
  }

  const store = await createSignalStore(pool, { seedFixtures: true });
  const service = new SignalService(store, { seedFixtures: false, skipSeed: true });

  const stats = await service.ingestHistory({
    tokenAddress,
    startTime: args.start,
    endTime: args.end,
    resolutionSeconds: args.resolution ? Number(args.resolution) : 60,
  });

  console.log(JSON.stringify(stats, null, 2));
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
