'use strict';

/**
 * SPEC-065 Market Intelligence Extraction CLI
 *
 *   npm run market:intel:extract -- --limit=500 --dry-run
 *   npm run market:intel:extract -- --company-id=<uuid> --rebuild-profiles
 */

require('dotenv').config();

const {
  extractMarketIntelligence,
  formatExtractReport,
} = require('../services/marketIntelligenceExtraction');

function parseArgs(argv = process.argv.slice(2)) {
  const options = {
    limit: 1000,
    dryRun: false,
    json: false,
    rebuildProfiles: true,
    companyId: null,
    emailId: null,
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
    if (arg === '--rebuild-profiles') {
      options.rebuildProfiles = true;
      continue;
    }
    if (arg === '--no-rebuild-profiles') {
      options.rebuildProfiles = false;
      continue;
    }
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg.startsWith('--limit=')) {
      options.limit = Number(arg.slice('--limit='.length));
      continue;
    }
    if (arg.startsWith('--company-id=')) {
      options.companyId = arg.slice('--company-id='.length);
      continue;
    }
    if (arg.startsWith('--email-id=')) {
      options.emailId = arg.slice('--email-id='.length);
      continue;
    }
    throw new Error(`Unknown argument: ${arg}`);
  }

  if (!Number.isFinite(options.limit) || options.limit < 1) {
    throw new Error('--limit must be a positive number');
  }

  return options;
}

function printHelp() {
  console.log(`Market Intelligence Extraction (SPEC-065)

Usage:
  npm run market:intel:extract -- [options]

Options:
  --limit=1000            Max emails to process
  --company-id=<uuid>     Scope to one market company
  --email-id=<uuid>       Process a single email
  --rebuild-profiles      Rebuild company profiles after extract (default)
  --no-rebuild-profiles   Skip profile rebuild
  --dry-run               Extract in memory without writing
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

  const result = await extractMarketIntelligence({
    limit: options.limit,
    dryRun: options.dryRun,
    companyId: options.companyId,
    emailId: options.emailId,
    rebuildProfiles: options.rebuildProfiles,
  });

  console.log(formatExtractReport(result));
  if (options.json) {
    console.log(JSON.stringify(result, null, 2));
  }
  return result;
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err && err.message ? err.message : err);
    process.exitCode = 1;
  });
}

module.exports = { parseArgs, main, printHelp };
