'use strict';

/**
 * SPEC-064 Relationship Intelligence Readiness / Acceptance CLI
 *
 *   npm run relationship:intel:readiness
 *   npm run relationship:intel:readiness -- --json
 *   npm run relationship:intel:readiness -- --check
 *   npm run relationship:intel:readiness -- --accept
 *   npm run relationship:intel:readiness -- --accept --json --check
 */

require('dotenv').config();

const pool = require('../db');
const {
  buildRelationshipIntelReadinessReport,
  formatReadinessReport,
  runRelationshipIntelligenceAcceptance,
  createPostgresStore,
} = require('../services/relationshipIntelligenceReadiness');

function parseArgs(argv = process.argv.slice(2)) {
  const options = {
    json: false,
    check: false,
    accept: false,
    help: false,
    companyId: 'readiness-demo-company',
    clientId: null,
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
    if (arg === '--accept') {
      options.accept = true;
      continue;
    }
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg.startsWith('--company-id=')) {
      options.companyId = arg.slice('--company-id='.length);
      continue;
    }
    if (arg.startsWith('--client-id=')) {
      options.clientId = arg.slice('--client-id='.length);
      continue;
    }
    throw new Error(`Unknown argument: ${arg}`);
  }

  return options;
}

function printHelp() {
  console.log(`Relationship Intelligence Readiness (SPEC-064)

Usage:
  npm run relationship:intel:readiness -- [options]

Options:
  --json               Print full JSON report
  --check              Exit 1 when status is not ready
  --accept             Create a safe notes-mode fixture, summarize, commit, verify
  --company-id=<id>    Soft company ref for --accept (default readiness-demo-company)
  --client-id=<n>      Tenant client_id for --accept
  --help               Show this help

Statuses:
  blocked  missing tables/migrations or constraint mismatch / CRM mutation
  partial  tables exist but no committed interaction with summary + insights
  ready    at least one committed interaction with structured summary + insights

--accept writes only relationship_* tables (never CRM / opportunities).
`);
}

async function main(argv = process.argv.slice(2), db = pool) {
  const options = parseArgs(argv);
  if (options.help) {
    printHelp();
    return { ok: true, help: true };
  }

  let acceptance = null;
  if (options.accept) {
    acceptance = await runRelationshipIntelligenceAcceptance({
      pool: db,
      store: createPostgresStore(db),
      companyId: options.companyId,
      clientId: options.clientId,
    });
  }

  const report = await buildRelationshipIntelReadinessReport({
    pool: db,
    acceptance,
    crmMutation: acceptance ? acceptance.crmMutation : null,
  });

  if (options.json) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    console.log(formatReadinessReport(report));
  }

  if (options.check && report.status !== 'ready') {
    process.exitCode = 1;
  }
  if (options.accept && acceptance && !acceptance.ok) {
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
      if (typeof pool.end === 'function') {
        return pool.end().catch(() => {});
      }
      return undefined;
    });
}

module.exports = { parseArgs, main, printHelp };
