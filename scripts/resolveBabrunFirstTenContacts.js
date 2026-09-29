#!/usr/bin/env node
'use strict';

/**
 * Babrun tenant 13 — resolve remaining first-ten contacts (7 prospects).
 *
 * CONTACT DISCOVERY AND VERIFICATION ONLY.
 * Does NOT send outreach, schedule sends, or modify Emmett capacity.
 *
 * Usage:
 *   node scripts/resolveBabrunFirstTenContacts.js --confirm-production
 *   node scripts/resolveBabrunFirstTenContacts.js --confirm-production --dry-run
 *   node scripts/resolveBabrunFirstTenContacts.js --confirm-production --ak-id=ak_babrun_prospect_p001
 */

require('dotenv').config();

const pool = require('../db');
const {
  TENANT_ID,
  CLIENT_ID,
  BABRUN_CONTACT_TARGETS,
  CONTACT_FINAL_STATE,
  resolveTarget,
  persistContactResolution,
  loadProspectByAkId,
  formatReportRow,
  summarizeResults,
} = require('./lib/babrunContactResolution');

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    confirmProduction: false,
    dryRun: false,
    akIds: [],
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--confirm-production') args.confirmProduction = true;
    else if (arg === '--dry-run') args.dryRun = true;
    else if (arg === '--help' || arg === '-h') args.help = true;
    else if (arg.startsWith('--ak-id=')) args.akIds.push(arg.split('=')[1]);
    else if (arg === '--ak-id') {
      args.akIds.push(argv[i + 1]);
      i += 1;
    }
  }
  if (!args.akIds.length) args.akIds = BABRUN_CONTACT_TARGETS.map((t) => t.akId);
  return args;
}

function printUsage() {
  console.log(`Babrun first-ten contact resolution (tenant ${TENANT_ID})

Usage:
  node scripts/resolveBabrunFirstTenContacts.js --confirm-production [--dry-run] [--ak-id=<id>]

Safety:
  Refuses without --confirm-production.
  Never sends mail. Never schedules outreach. Never modifies Emmett capacity.
`);
}

function assertRuntimeEnv() {
  if (!process.env.DATABASE_URL) {
    throw Object.assign(new Error('Missing required runtime env: DATABASE_URL'), { code: 'runtime_env_missing' });
  }
}

function printReportTable(rows, totals) {
  const header = [
    'Prospect',
    'Domain',
    'Candidates found',
    'Best email',
    'Discovery source',
    'Verification result',
    'Contact classification',
    'Confidence',
    'Final state',
  ];
  const lines = [header.join(' | ')];
  for (const row of rows) {
    lines.push([
      row.prospect,
      row.domain,
      row.candidatesFound,
      row.bestEmail,
      row.discoverySource,
      row.verificationResult,
      row.contactClassification,
      row.confidence,
      row.finalState,
    ].join(' | '));
  }
  lines.push('');
  lines.push(`VERIFIED_FOUNDER_EMAIL: ${totals.VERIFIED_FOUNDER_EMAIL}/7`);
  lines.push(`VERIFIED_ROLE_EMAIL: ${totals.VERIFIED_ROLE_EMAIL}/7`);
  lines.push(`REVIEW_REQUIRED: ${totals.REVIEW_REQUIRED}/7`);
  lines.push(`UNRESOLVED: ${totals.UNRESOLVED}/7`);
  console.log(lines.join('\n'));
}

async function run(options = {}) {
  if (options.help) {
    printUsage();
    return { help: true };
  }
  if (!options.confirmProduction) {
    throw Object.assign(new Error('Refusing to run without --confirm-production.'), { code: 'confirm_production_required' });
  }
  assertRuntimeEnv();

  const targets = BABRUN_CONTACT_TARGETS.filter((t) => options.akIds.includes(t.akId));
  if (!targets.length) {
    throw Object.assign(new Error('No matching Babrun contact targets for supplied --ak-id values.'), { code: 'no_targets' });
  }

  const results = [];
  for (const target of targets) {
    const prospect = await loadProspectByAkId(pool, target.akId);
    const resolved = await resolveTarget(target, { prospect });
    const persist = await persistContactResolution(pool, prospect, target, resolved, options.dryRun);
    results.push({ ...resolved, prospectId: prospect?.id || null, persist });
  }

  const reportRows = results.map(formatReportRow);
  const totals = summarizeResults(results);
  printReportTable(reportRows, totals);

  const founderDirect = results.filter((r) => r.finalState === CONTACT_FINAL_STATE.VERIFIED_FOUNDER_EMAIL);
  if (founderDirect.length) {
    console.log('\nFounder-direct outreach eligible:');
    for (const row of founderDirect) {
      console.log(`  - ${row.target.founder} (${row.target.company}): ${row.best?.email}`);
    }
  }

  return { tenantId: TENANT_ID, clientId: CLIENT_ID, dryRun: options.dryRun, results, reportRows, totals };
}

if (require.main === module) {
  run(parseArgs())
    .then(() => pool.end())
    .catch(async (err) => {
      console.error(err.stack || err.message);
      await pool.end().catch(() => {});
      process.exit(1);
    });
}

module.exports = { parseArgs, run, printReportTable };
