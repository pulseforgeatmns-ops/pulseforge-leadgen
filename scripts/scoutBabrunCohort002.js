#!/usr/bin/env node
'use strict';

/**
 * Babrun tenant 13 — Scout cohort 002 production runner.
 *
 * Discovery + ICP qualification + domain verification + contact resolution + AK/CRM persistence.
 * Does NOT send outreach, schedule sends, or modify Emmett capacity.
 *
 * Usage:
 *   node scripts/scoutBabrunCohort002.js --confirm-production --dry-run
 *   node scripts/scoutBabrunCohort002.js --confirm-production --apply
 *   node scripts/scoutBabrunCohort002.js --confirm-production --apply --max=6
 */

require('dotenv').config();

const pool = require('../db');
const {
  COHORT_TAG,
  TENANT_ID,
  CLIENT_ID,
  runCohort002,
  formatCohortTableRow,
  strongestProspectsByEvidence,
} = require('./lib/babrunCohort002');

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    confirmProduction: false,
    dryRun: true,
    max: 10,
  };
  for (const arg of argv) {
    if (arg === '--confirm-production') args.confirmProduction = true;
    else if (arg === '--apply') args.dryRun = false;
    else if (arg === '--dry-run') args.dryRun = true;
    else if (arg === '--help' || arg === '-h') args.help = true;
    else if (arg.startsWith('--max=')) args.max = Number(arg.split('=')[1]);
  }
  return args;
}

function printUsage() {
  console.log(`Babrun Scout cohort 002 (tenant ${TENANT_ID})

Usage:
  node scripts/scoutBabrunCohort002.js --confirm-production [--dry-run|--apply] [--max=10]

Required production env:
  DATABASE_URL
  BOUNCER_ENABLED=true
  BOUNCER_API_KEY

Optional discovery env:
  GOOGLE_PLACES_KEY — enables Places-backed discovery in addition to research seeds

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

function printReport(result) {
  console.log('\nSCOUT BABRUN COHORT 002\n');
  const header = [
    'Prospect',
    'Founder',
    'Industry/location',
    'ICP evidence',
    'Official domain',
    'Best email',
    'Bouncer result',
    'Contact classification',
    'Confidence',
    'AK ID',
    'Operational prospect ID',
    'Final state',
  ];
  console.log(header.join(' | '));
  for (const row of result.accepted.map(formatCohortTableRow)) {
    console.log([
      row.prospect,
      row.founder,
      row.industryLocation,
      row.icpEvidence,
      row.officialDomain,
      row.bestEmail,
      row.bouncerResult,
      row.contactClassification,
      row.confidence,
      row.akId,
      row.operationalProspectId,
      row.finalState,
    ].join(' | '));
  }

  const s = result.stats;
  console.log('\nTotals:');
  console.log(`Candidates researched: ${s.candidatesResearched}`);
  console.log(`Accepted prospects: ${s.acceptedProspects}`);
  console.log(`VERIFIED_FOUNDER_EMAIL: ${s.VERIFIED_FOUNDER_EMAIL}`);
  console.log(`VERIFIED_ROLE_EMAIL: ${s.VERIFIED_ROLE_EMAIL}`);
  console.log(`REVIEW_REQUIRED: ${s.REVIEW_REQUIRED}`);
  console.log(`UNRESOLVED: ${s.UNRESOLVED}`);
  console.log(`Rejected by ICP: ${s.rejectedByIcp}`);
  console.log(`Rejected duplicate: ${s.rejectedDuplicate}`);
  console.log(`Rejected contactability: ${s.rejectedContactability}`);

  const strongest = strongestProspectsByEvidence(result.accepted);
  if (strongest.length) {
    console.log('\nStrongest prospects by evidence (not conversion ranking):');
    for (const row of strongest) {
      console.log(`  - ${row.founder} / ${row.company} [${row.classification}]`);
      for (const ev of row.evidenceHighlights) console.log(`      ${ev}`);
    }
  }

  console.log('\nScout learning recorded: prospect completion for email acquisition includes verified contactability.');
}

function verdict(result, applied) {
  if (!applied) return 'B. COHORT DISCOVERED — PRODUCTION PERSISTENCE REQUIRED';
  if (result.stats.acceptedProspects === 0) return 'C. INSUFFICIENT HIGH-QUALITY PROSPECTS';
  return 'A. BABRUN COHORT 002 READY — CONTACTABLE PROSPECTS PERSISTED';
}

async function run(options = {}) {
  if (options.help) {
    printUsage();
    return { help: true };
  }
  if (!options.confirmProduction) {
    throw Object.assign(new Error('Refusing to run without --confirm-production.'), { code: 'confirm_production_required' });
  }

  const hasDb = Boolean(process.env.DATABASE_URL);
  if (!options.dryRun) assertRuntimeEnv();

  const result = await runCohort002({
    db: hasDb ? pool : null,
    apply: !options.dryRun && hasDb,
    max: options.max,
  });

  printReport(result);
  const v = verdict(result, !options.dryRun && hasDb);
  console.log(`\nVERDICT: ${v}`);
  return { ...result, verdict: v, cohort: COHORT_TAG, tenantId: TENANT_ID, clientId: CLIENT_ID };
}

if (require.main === module) {
  run(parseArgs())
    .then(() => pool.end().catch(() => {}))
    .catch(async (err) => {
      console.error(err.stack || err.message);
      await pool.end().catch(() => {});
      process.exit(1);
    });
}

module.exports = { parseArgs, run, printReport, verdict };
