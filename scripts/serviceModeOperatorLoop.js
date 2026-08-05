'use strict';

/**
 * SPEC-075 — Service Mode Operator Loop CLI
 *
 *   npm run operator:service-loop
 *   npm run operator:service-loop -- --json
 *   npm run operator:service-loop -- --days=14 --limit=10
 *   npm run operator:service-loop -- --relationship-interaction-id=...
 */

require('dotenv').config();

const pool = require('../db');
const {
  DEFAULT_DAYS,
  DEFAULT_LIMIT,
  formatOperatorLoopReport,
  getServiceModeOperatorLoop,
} = require('../services/serviceModeOperatorLoop');

function parseArgs(argv = process.argv.slice(2)) {
  const options = {
    json: false,
    help: false,
    days: DEFAULT_DAYS,
    limit: DEFAULT_LIMIT,
    companyId: null,
    prospectId: null,
    opportunityId: null,
    relationshipInteractionId: null,
    includeMarketContext: true,
  };

  for (const arg of argv) {
    if (arg === '--json') {
      options.json = true;
      continue;
    }
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg === '--no-market') {
      options.includeMarketContext = false;
      continue;
    }
    if (arg.startsWith('--days=')) {
      options.days = Number(arg.slice('--days='.length));
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
    if (arg.startsWith('--prospect-id=')) {
      options.prospectId = arg.slice('--prospect-id='.length);
      continue;
    }
    if (arg.startsWith('--opportunity-id=')) {
      options.opportunityId = arg.slice('--opportunity-id='.length);
      continue;
    }
    if (arg.startsWith('--relationship-interaction-id=')) {
      options.relationshipInteractionId = arg.slice(
        '--relationship-interaction-id='.length
      );
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

  return options;
}

function printHelp() {
  console.log(`Service Mode Operator Loop (SPEC-075)

Usage:
  npm run operator:service-loop -- [options]

Options:
  --days=14                            Lookback window for committed interactions
  --limit=10                           Max primary manual actions to return
  --relationship-interaction-id=ID     Focus on one committed interaction
  --company-id=ID                      Focus on soft/CRM company id
  --prospect-id=ID                     Focus on prospect / contact soft id
  --opportunity-id=ID                  Focus on opportunity soft id
  --json                               Print full JSON queue (default: human-readable text)
  --no-market                          Skip market intelligence context in briefs
  --help                               Show this help

Read-only manual action queue for Jake. isEvidence=false. No outbound email,
CRM mutation, Composer generation, or autonomous Max execution.
`);
}

async function main(argv = process.argv.slice(2), db = pool, deps = {}) {
  const options = parseArgs(argv);
  if (options.help) {
    printHelp();
    return { ok: true, help: true };
  }

  const loopFn = deps.getServiceModeOperatorLoop || getServiceModeOperatorLoop;
  const loop = await loopFn({
    pool: db,
    store: deps.store,
    loadCompanySnapshot: deps.loadCompanySnapshot,
    marketBriefingService: deps.marketBriefingService,
    relationshipService: deps.relationshipService,
    getProspectOperatingBrief: deps.getProspectOperatingBrief,
    briefService: deps.briefService,
    companyId: options.companyId || undefined,
    prospectId: options.prospectId || undefined,
    opportunityId: options.opportunityId || undefined,
    relationshipInteractionId: options.relationshipInteractionId || undefined,
    days: options.days,
    limit: options.limit,
    includeMarketContext: options.includeMarketContext,
  });

  if (options.json) {
    console.log(JSON.stringify(loop, null, 2));
  } else {
    console.log(formatOperatorLoopReport(loop));
  }

  return loop;
}

if (require.main === module) {
  main()
    .catch((err) => {
      console.error(err && err.message ? err.message : err);
      process.exitCode = 1;
    })
    .finally(() => {
      if (typeof pool.end === 'function') {
        return pool.end().catch(() => {});
      }
      return undefined;
    });
}

module.exports = { parseArgs, main, printHelp };
