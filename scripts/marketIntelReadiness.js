'use strict';

/**
 * SPEC-067 / SPEC-068 Market Intelligence Operational Acceptance CLI
 *
 *   npm run market:intel:readiness
 *   npm run market:intel:readiness -- --json
 *   npm run market:intel:readiness -- --check
 *   npm run market:intel:readiness -- --probe-gmail
 */

require('dotenv').config();

const pool = require('../db');
const {
  buildMarketIntelReadinessReport,
  formatReadinessReport,
} = require('../services/marketIntelligenceReadiness');
const { preflightMarketIntelIngestion } = require('../services/marketIntelligencePreflight');

function parseArgs(argv = process.argv.slice(2)) {
  const options = {
    json: false,
    check: false,
    probeGmail: false,
    help: false,
    days: 365,
    label: 'MARKET_INTEL',
    limit: 1000,
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
    if (arg === '--probe-gmail') {
      options.probeGmail = true;
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

  if (options.probeGmail) {
    if (!Number.isFinite(options.days) || options.days < 1) {
      throw new Error('--days must be a positive number');
    }
    if (!Number.isFinite(options.limit) || options.limit < 1) {
      throw new Error('--limit must be a positive number');
    }
  }

  return options;
}

function printHelp() {
  console.log(`Market Intelligence Readiness (SPEC-067 / SPEC-068)

Usage:
  npm run market:intel:readiness -- [options]

Options:
  --json          Print full JSON report (default: human-readable text)
  --check         Exit 1 when status is not ready
  --probe-gmail   Also verify Gmail credentials/label/discovery (SPEC-068)
  --days=365      Lookback used with --probe-gmail
  --label=NAME    Label used with --probe-gmail (default MARKET_INTEL)
  --limit=1000    Discovery cap used with --probe-gmail
  --help          Show this help

Read-only. No scoring, recommendations, CRM writes, or Max side effects.
`);
}

async function main(argv = process.argv.slice(2), db = pool) {
  const options = parseArgs(argv);
  if (options.help) {
    printHelp();
    return { ok: true, help: true };
  }

  let gmailPreflight = null;
  if (options.probeGmail) {
    gmailPreflight = await preflightMarketIntelIngestion({
      days: options.days,
      label: options.label,
      limit: options.limit,
      requireMessages: false,
    });
  }

  const report = await buildMarketIntelReadinessReport({
    pool: db,
    gmailPreflight,
  });
  if (options.json) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    console.log(formatReadinessReport(report));
    if (gmailPreflight) {
      console.log('');
      console.log(`Gmail probe: ${gmailPreflight.ok ? 'pass' : 'fail'}`);
      if (gmailPreflight.checks?.discovery) {
        console.log(
          `Gmail discovered: ${Number(gmailPreflight.checks.discovery.discoveredCount || 0).toLocaleString('en-US')}`
        );
      }
    }
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
