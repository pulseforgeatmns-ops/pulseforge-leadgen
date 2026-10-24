#!/usr/bin/env node
'use strict';

/**
 * SPEC-WEB-001 — WEB-COHORT-001 dry-run (25 businesses, NO SEND).
 *
 * Usage:
 *   node scripts/runWebCohort001.js [--apply] [--live-audit]
 */

require('dotenv').config();

const fs = require('fs');
const path = require('path');
const pool = require('../db');
const { runWebCohort001, COHORT_TAG } = require('./lib/webCohort001');

function parseArgs(argv = process.argv.slice(2)) {
  const args = { dryRun: true, fixtureMode: true, help: false };
  for (const arg of argv) {
    if (arg === '--apply') args.dryRun = false;
    else if (arg === '--live-audit') args.fixtureMode = false;
    else if (arg === '--help' || arg === '-h') args.help = true;
  }
  return args;
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(`WEB-COHORT-001 runner

  node scripts/runWebCohort001.js [--apply] [--live-audit]

Safety: NO email, calls, forms, publication, or paid acquisition.
`);
    process.exit(0);
  }

  if (!process.env.DATABASE_URL) {
    console.error('DATABASE_URL required');
    process.exit(1);
  }

  const result = await runWebCohort001(pool, {
    dryRun: args.dryRun,
    fixtureMode: args.fixtureMode,
  });

  const reportPath = path.join(__dirname, '../artifacts/spec-web-001/cohort-report.json');
  fs.mkdirSync(path.dirname(reportPath), { recursive: true });
  fs.writeFileSync(reportPath, JSON.stringify(result, null, 2));

  console.log(`\n${COHORT_TAG} complete — ${result.rows.length} assessments`);
  console.log('Distribution:', result.distribution);
  console.log('\nTop five:');
  for (const t of result.top_five) {
    console.log(`  - ${t.business} (${t.domain}): ${t.recommended_action} — priority ${t.priority_score}`);
  }
  console.log(`\nReport: ${reportPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
