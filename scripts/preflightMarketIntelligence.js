'use strict';

/**
 * SPEC-068 Market Intelligence Ingestion Preflight CLI
 *
 *   npm run market:intel:preflight
 *   npm run market:intel:preflight -- --label=MARKET_INTEL --days=365 --require-messages
 *   npm run market:intel:preflight -- --show-account --token-source=gmail --days=365 --label=MARKET_INTEL --limit=10
 */

require('dotenv').config();

const {
  TOKEN_SOURCES,
  resolveMarketIntelTokenSource,
} = require('../utils/gmailClient');
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
    showAccount: false,
    tokenSource: null,
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
    if (arg === '--show-account') {
      options.showAccount = true;
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

  options.resolvedTokenSource = resolveMarketIntelTokenSource(options.tokenSource);
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
  --token-source=SOURCE   gmail | riley | auto
                          default: gmail when GMAIL_TOKEN exists, else auto
  --require-messages      Fail when the label has zero messages in-window
  --show-account          Print authenticated Gmail address before label discovery
  --json                  Print full JSON report (includes authenticatedEmail)
  --help                  Show this help

Allowed --token-source values: ${TOKEN_SOURCES.join(', ')}

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
    tokenSource: options.resolvedTokenSource,
    showAccount: options.showAccount,
    onAuthenticatedAccount: options.showAccount && !options.json
      ? (email) => {
          console.log(`Authenticated Gmail account: ${email}`);
        }
      : null,
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
