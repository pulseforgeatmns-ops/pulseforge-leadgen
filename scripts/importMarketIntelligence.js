'use strict';

/**
 * SPEC-061 Market Intelligence Ingestion CLI
 *
 *   npm run market:intel:import -- --days=365 --label=MARKET_INTEL --limit=1000 --dry-run
 *   pnpm market:intel:import -- --days=365 --label=MARKET_INTEL --limit=1000 --dry-run
 */

require('dotenv').config();

const {
  formatImportReport,
  importMarketIntelligence,
} = require('../services/marketIntelligenceIngestion');

function parseArgs(argv = process.argv.slice(2)) {
  const options = {
    days: 365,
    label: 'MARKET_INTEL',
    limit: 1000,
    dryRun: false,
    json: false,
  };

  for (const arg of argv) {
    if (arg === '--dry-run') {
      options.dryRun = true;
      continue;
    }
    if (arg === '--json') {
      options.json = true;
      continue;
    }
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg.startsWith('--days=')) {
      options.days = Number(arg.slice('--days='.length));
      continue;
    }
    if (arg.startsWith('--label=')) {
      options.label = arg.slice('--label='.length);
      continue;
    }
    if (arg.startsWith('--limit=')) {
      options.limit = Number(arg.slice('--limit='.length));
      continue;
    }
    throw new Error(`Unknown argument: ${arg}`);
  }

  if (!Number.isFinite(options.days) || options.days < 1) {
    throw new Error('--days must be a positive number');
  }
  if (!Number.isFinite(options.limit) || options.limit < 1) {
    throw new Error('--limit must be a positive number');
  }
  if (!String(options.label || '').trim()) {
    throw new Error('--label is required');
  }

  return options;
}

function printHelp() {
  console.log(`Market Intelligence Ingestion (SPEC-061)

Usage:
  npm run market:intel:import -- [options]
  pnpm market:intel:import -- [options]

Options:
  --days=365              Lookback window (default 365)
  --label=MARKET_INTEL    Gmail label to import (required selection)
  --limit=1000            Max messages to fetch
  --dry-run               Parse and resolve without writing
  --json                  Print full JSON result after the report
  --help                  Show this help
`);
}

async function main(argv = process.argv.slice(2)) {
  const options = parseArgs(argv);
  if (options.help) {
    printHelp();
    return { ok: true, help: true };
  }

  const result = await importMarketIntelligence({
    days: options.days,
    label: options.label,
    limit: options.limit,
    dryRun: options.dryRun,
  });

  console.log(formatImportReport(result));
  if (options.json) {
    console.log(JSON.stringify(result, null, 2));
  }
  return result;
}

if (require.main === module) {
  main().then((result) => {
    process.exit(result.ok === false ? 1 : 0);
  }).catch((err) => {
    console.error(err.message || err);
    process.exit(1);
  });
}

module.exports = { main, parseArgs, printHelp };
