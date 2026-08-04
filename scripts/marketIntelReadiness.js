'use strict';

/**
 * SPEC-067 Market Intelligence Operational Acceptance CLI
 *
 *   npm run market:intel:readiness
 *   npm run market:intel:readiness -- --json
 *   npm run market:intel:readiness -- --check
 */

require('dotenv').config();

const pool = require('../db');
const {
  buildMarketIntelReadinessReport,
  formatReadinessReport,
} = require('../services/marketIntelligenceReadiness');

function parseArgs(argv = process.argv.slice(2)) {
  const options = {
    json: false,
    check: false,
    help: false,
  };

  for (const arg of argv) {
    if (arg === '--json') {
      options.json = true;
      continue;
    }
    if (arg === '--check') {
      options.check = true;
      continue;
    }
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    throw new Error(`Unknown argument: ${arg}`);
  }

  return options;
}

function printHelp() {
  console.log(`Market Intelligence Readiness (SPEC-067)

Usage:
  npm run market:intel:readiness -- [options]

Options:
  --json     Print full JSON report (default: human-readable text)
  --check    Exit 1 when status is not ready
  --help     Show this help

Read-only. No scoring, recommendations, CRM writes, or Max side effects.
`);
}

async function main(argv = process.argv.slice(2), db = pool) {
  const options = parseArgs(argv);
  if (options.help) {
    printHelp();
    return { ok: true, help: true };
  }

  const report = await buildMarketIntelReadinessReport({ pool: db });
  if (options.json) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    console.log(formatReadinessReport(report));
  }

  if (options.check && report.status !== 'ready') {
    process.exitCode = 1;
  }

  return report;
}

if (require.main === module) {
  main()
    .catch((err) => {
      console.error(err && err.message ? err.message : err);
      process.exitCode = 1;
    })
    .finally(() => {
      // Never leave the shared pool open in one-shot CLI.
      if (typeof pool.end === 'function') {
        return pool.end().catch(() => {});
      }
      return undefined;
    });
}

module.exports = { parseArgs, main, printHelp };
