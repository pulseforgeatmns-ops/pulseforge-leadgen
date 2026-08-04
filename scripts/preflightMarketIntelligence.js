'use strict';

/**
 * SPEC-068 Market Intelligence Ingestion Preflight CLI
 *
 *   npm run market:intel:preflight
 *   npm run market:intel:preflight -- --label=MARKET_INTEL --days=365 --require-messages
 */

require('dotenv').config();

const {
  formatPreflightReport,
  preflightMarketIntelIngestion,
} = require('../services/marketIntelligencePreflight');

function parseArgs(argv = process.argv.slice(2)) {
  const options = {
    days: 365,
    label: 'MARKET_INTEL',
    limit: 1000,
    requireMessages: false,
    json: false,
    help: false,
  };

  for (const arg of argv) {
    if (arg === '--json') {
      options.json = true;
      continue;
    }
    if (arg === '--require-messages') {
      options.requireMessages = true;
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
  console.log(`Market Intelligence Ingestion Preflight (SPEC-068)

Usage:
  npm run market:intel:preflight -- [options]

Options:
  --days=365              Lookback window (default 365)
  --label=MARKET_INTEL    Gmail label that must exist
  --limit=1000            Max messages to count for discovery
  --require-messages      Fail when the label has zero messages in-window
  --json                  Print full JSON report
  --help                  Show this help

Read-only. Verifies Gmail credentials, label existence, and discoverability.
Does not write market_* tables or touch CRM.
`);
}

async function main(argv = process.argv.slice(2)) {
  const options = parseArgs(argv);
  if (options.help) {
    printHelp();
    return { ok: true, help: true };
  }

  const report = await preflightMarketIntelIngestion({
    days: options.days,
    label: options.label,
    limit: options.limit,
    requireMessages: options.requireMessages,
  });

  if (options.json) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    console.log(formatPreflightReport(report));
  }

  if (!report.ok) process.exitCode = 1;
  return report;
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err && err.message ? err.message : err);
    process.exitCode = 1;
  });
}

module.exports = { main, parseArgs, printHelp };
