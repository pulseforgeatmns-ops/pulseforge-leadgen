'use strict';

/**
 * SPEC-061 / SPEC-068 Market Intelligence Ingestion CLI
 *
 *   npm run market:intel:import -- --days=365 --label=MARKET_INTEL --intent=general_market_messaging --dry-run
 *   npm run market:intel:import -- --intent=competitive_watch --label=COMPETITIVE_WATCH
 */

require('dotenv').config();

const pool = require('../db');
const {
  TOKEN_SOURCES,
  resolveMarketIntelTokenSource,
} = require('../utils/gmailClient');
const {
  DEFAULT_IMPORT_INTENT,
  IMPORT_INTENTS,
  formatImportReport,
  importMarketIntelligence,
  resolveImportIntent,
} = require('../services/marketIntelligenceIngestion');
const {
  formatPreflightReport,
  preflightMarketIntelIngestion,
} = require('../services/marketIntelligencePreflight');

function parseArgs(argv = process.argv.slice(2)) {
  const options = {
    days: 365,
    label: 'MARKET_INTEL',
    limit: 1000,
    dryRun: false,
    json: false,
    preflight: false,
    skipPreflight: false,
    help: false,
    importIntent: null,
    sourceIntent: null,
    tokenSource: null,
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
    if (arg === '--preflight') {
      options.preflight = true;
      continue;
    }
    if (arg === '--skip-preflight') {
      options.skipPreflight = true;
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
    if (arg.startsWith('--intent=')) {
      options.importIntent = arg.slice('--intent='.length);
      continue;
    }
    if (arg.startsWith('--import-intent=')) {
      options.importIntent = arg.slice('--import-intent='.length);
      continue;
    }
    if (arg.startsWith('--source-intent=')) {
      options.sourceIntent = arg.slice('--source-intent='.length);
      continue;
    }
    if (arg.startsWith('--token-source=')) {
      options.tokenSource = arg.slice('--token-source='.length);
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

  options.resolvedIntent = resolveImportIntent({
    importIntent: options.importIntent,
    sourceIntent: options.sourceIntent,
  });
  options.resolvedTokenSource = resolveMarketIntelTokenSource(options.tokenSource);

  return options;
}

function printHelp() {
  console.log(`Market Intelligence Ingestion (SPEC-061 / SPEC-068)

Usage:
  npm run market:intel:import -- [options]
  pnpm market:intel:import -- [options]

Options:
  --days=365              Lookback window (default 365)
  --label=MARKET_INTEL    Gmail label to import (required selection)
  --limit=1000            Max messages to fetch
  --token-source=SOURCE   gmail | riley | auto
                          default: gmail when GMAIL_TOKEN exists, else auto
  --intent=NAME           Import/source intent (default: ${DEFAULT_IMPORT_INTENT})
  --import-intent=NAME    Alias of --intent
  --source-intent=NAME    Alias of --intent (must match if both set)
  --dry-run               Parse and resolve without writing
  --preflight             Run Gmail auth/label/discovery checks only
  --skip-preflight        Skip automatic preflight before import/dry-run
  --json                  Print full JSON result after the report
  --help                  Show this help

Allowed --token-source values: ${TOKEN_SOURCES.join(', ')}

Initial allowed intents (SPEC-068):
  ${IMPORT_INTENTS.GENERAL_MARKET_MESSAGING}
  ${IMPORT_INTENTS.COMPETITIVE_WATCH}
  ${IMPORT_INTENTS.VENDOR_NEWSLETTER}
  ${IMPORT_INTENTS.DIRECT_COMPETITOR}
  ${IMPORT_INTENTS.INDIRECT_COMPETITOR}
  ${IMPORT_INTENTS.UNKNOWN}

Rule:
  import_intent is acquisition context only.
  Never treat it as a factual claim that the sender is a competitor.

Safe behavior:
  - Dry-run writes nothing
  - Re-runs dedupe on gmail_id / message_id (no duplicate emails)
  - Real import only after preflight passes (unless --skip-preflight)
`);
}

async function main(argv = process.argv.slice(2)) {
  const options = parseArgs(argv);
  if (options.help) {
    printHelp();
    return { ok: true, help: true };
  }

  if (options.preflight) {
    const report = await preflightMarketIntelIngestion({
      days: options.days,
      label: options.label,
      limit: options.limit,
      requireMessages: true,
      tokenSource: options.resolvedTokenSource,
      showAccount: true,
    });
    console.log(formatPreflightReport(report));
    console.log(`Import intent (for next import): ${options.resolvedIntent}`);
    if (options.json) {
      console.log(JSON.stringify({ ...report, importIntent: options.resolvedIntent }, null, 2));
    }
    if (!report.ok) {
      process.exitCode = 1;
      return { ok: false, preflight: report, importIntent: options.resolvedIntent };
    }
    return { ok: true, preflight: report, importIntent: options.resolvedIntent };
  }

  if (!options.skipPreflight) {
    const preflight = await preflightMarketIntelIngestion({
      days: options.days,
      label: options.label,
      limit: options.limit,
      requireMessages: true,
      tokenSource: options.resolvedTokenSource,
    });
    if (!preflight.ok) {
      console.log(formatPreflightReport(preflight));
      if (options.json) console.log(JSON.stringify(preflight, null, 2));
      process.exitCode = 1;
      return { ok: false, preflight };
    }
    const discovered = Number(preflight.checks?.discovery?.discoveredCount || 0);
    const account = preflight.authenticatedEmail || '(unavailable)';
    console.log(
      `Preflight OK — account ${account} via ${preflight.tokenSource}; discovered ${discovered.toLocaleString('en-US')} labeled messages`
    );
  }

  const result = await importMarketIntelligence({
    days: options.days,
    label: options.label,
    limit: options.limit,
    dryRun: options.dryRun,
    importIntent: options.resolvedIntent,
    tokenSource: options.resolvedTokenSource,
  });

  console.log(formatImportReport(result));
  if (options.json) {
    console.log(JSON.stringify(result, null, 2));
  }
  return result;
}

if (require.main === module) {
  main()
    .then((result) => {
      if (result && result.ok === false) process.exitCode = 1;
    })
    .catch((err) => {
      console.error(err.message || err);
      process.exitCode = 1;
    })
    .finally(() => {
      if (typeof pool.end === 'function') {
        return pool.end().catch(() => {});
      }
      return undefined;
    });
}

module.exports = { main, parseArgs, printHelp };
