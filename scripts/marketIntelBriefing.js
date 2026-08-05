'use strict';

/**
 * SPEC-071 — Market Intelligence Briefing CLI
 *
 *   npm run market:intel:briefing
 *   npm run market:intel:briefing -- --json
 *   npm run market:intel:briefing -- --days=30 --limit=10
 *   npm run market:intel:briefing -- --intent=general_market_messaging
 */

require('dotenv').config();

const pool = require('../db');
const {
  DEFAULT_DAYS,
  DEFAULT_LIMIT,
  formatBriefingReport,
  getMarketIntelligenceBriefing,
} = require('../services/marketIntelligenceBriefing');

function parseArgs(argv = process.argv.slice(2)) {
  const options = {
    json: false,
    help: false,
    days: DEFAULT_DAYS,
    limit: DEFAULT_LIMIT,
    intent: null,
    companyId: null,
    since: null,
    until: null,
    includeHeadlines: false,
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
    if (arg === '--include-headlines') {
      options.includeHeadlines = true;
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
    if (arg.startsWith('--intent=')) {
      options.intent = arg.slice('--intent='.length);
      continue;
    }
    if (arg.startsWith('--company-id=')) {
      options.companyId = arg.slice('--company-id='.length);
      continue;
    }
    if (arg.startsWith('--since=')) {
      options.since = arg.slice('--since='.length);
      continue;
    }
    if (arg.startsWith('--until=')) {
      options.until = arg.slice('--until='.length);
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
  console.log(`Market Intelligence Briefing (SPEC-071)

Usage:
  npm run market:intel:briefing -- [options]

Options:
  --json                 Print full JSON briefing (default: human-readable text)
  --days=30              Lookback window in days
  --limit=10             Max items per section
  --intent=NAME          Filter by import_intent
  --company-id=UUID      Filter to one market company
  --since=ISO            Explicit window start (overrides days when set with --until)
  --until=ISO            Explicit window end
  --include-headlines    Include raw headline patterns as a separate section
  --help                 Show this help

Read-only synthesis. isEvidence=false. No scoring, recommendations, CRM writes,
or Max side effects. CTA image/social/footer/tracking URLs are filtered in
briefing output only; raw observations are unchanged.
`);
}

async function main(argv = process.argv.slice(2), db = pool) {
  const options = parseArgs(argv);
  if (options.help) {
    printHelp();
    return { ok: true, help: true };
  }

  const briefing = await getMarketIntelligenceBriefing({
    pool: db,
    days: options.days,
    limit: options.limit,
    importIntent: options.intent || undefined,
    companyId: options.companyId || undefined,
    since: options.since || undefined,
    until: options.until || undefined,
    includeHeadlines: options.includeHeadlines,
  });

  if (options.json) {
    console.log(JSON.stringify(briefing, null, 2));
  } else {
    console.log(formatBriefingReport(briefing));
  }

  return briefing;
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
