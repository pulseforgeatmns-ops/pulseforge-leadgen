#!/usr/bin/env node
'use strict';

/**
 * SPEC-WEB-001A — WEB-COHORT-003 live read-only revalidation runner.
 *
 * Usage:
 *   node scripts/runWebCohort003.js --confirm-live
 *   node scripts/runWebCohort003.js --confirm-live --apply
 *   node scripts/runWebCohort003.js --confirm-live --skip-puppeteer
 *
 * Requires GOOGLE_PLACES_KEY and/or SERPAPI_KEY for Scout discovery.
 * Optional GOOGLE_API_KEY for PageSpeed Insights.
 * DATABASE_URL required for --apply persistence.
 */

require('dotenv').config({ quiet: true });

const fs = require('fs');
const path = require('path');
const pool = require('../db');
const { runWebCohort003, COHORT_TAG } = require('./lib/webCohort003');
const { ensureMaynardWebTenant } = require('../utils/maynardWebTenant');

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    confirmLive: false,
    apply: false,
    skipPuppeteer: false,
    help: false,
  };
  for (const arg of argv) {
    if (arg === '--confirm-live') args.confirmLive = true;
    else if (arg === '--apply') args.apply = true;
    else if (arg === '--skip-puppeteer') args.skipPuppeteer = true;
    else if (arg === '--help' || arg === '-h') args.help = true;
  }
  return args;
}

function printHelp() {
  console.log(`WEB-COHORT-003 live revalidation (SPEC-WEB-001A)

Usage:
  node scripts/runWebCohort003.js --confirm-live [--apply] [--skip-puppeteer]

Required for Scout discovery:
  GOOGLE_PLACES_KEY and/or SERPAPI_KEY

Optional:
  GOOGLE_API_KEY — PageSpeed Insights (records UNKNOWN with failure reason if absent/failed)
  DATABASE_URL — required for --apply persistence

Safety:
  Read-only audits only. No outreach. No external state changes except optional DB persistence.
`);
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    printHelp();
    return;
  }
  if (!args.confirmLive) {
    throw Object.assign(new Error('Refusing to run without --confirm-live'), { code: 'confirm_live_required' });
  }
  if (args.apply && !process.env.DATABASE_URL) {
    throw Object.assign(new Error('DATABASE_URL required for --apply'), { code: 'database_required' });
  }

  if (args.apply) {
    await ensureMaynardWebTenant(pool);
  }

  const result = await runWebCohort003(args.apply ? pool : null, {
    persist: args.apply,
    skipPuppeteer: args.skipPuppeteer,
  });

  const reportPath = path.join(__dirname, '../artifacts/spec-web-001/cohort-report-003-live.json');
  fs.mkdirSync(path.dirname(reportPath), { recursive: true });
  fs.writeFileSync(reportPath, JSON.stringify(result, null, 2));

  console.log(`\n${COHORT_TAG} ${result.incomplete ? 'INCOMPLETE' : 'complete'}`);
  if (result.error) console.log('Error:', result.error);
  if (result.cohort_composition) console.log('Cohort composition:', result.cohort_composition);
  if (result.vertical_distribution) console.log('Vertical distribution:', result.vertical_distribution);
  if (result.market_distribution) console.log('Market distribution:', result.market_distribution);
  if (result.distribution) console.log('Action distribution:', result.distribution);
  if (result.diagnosis_distribution) console.log('Diagnosis distribution:', result.diagnosis_distribution);
  if (result.psi_telemetry) console.log('PSI telemetry:', result.psi_telemetry);
  if (result.evidence_completeness) console.log('Evidence completeness:', result.evidence_completeness);
  if (result.max_priority_distribution) console.log('Max priority distribution:', result.max_priority_distribution);
  if (result.top_five?.length) {
    console.log('\nTop five (Max priority):');
    for (const t of result.top_five) {
      console.log(`  - ${t.business.name}: priority ${t.prioritization.max_priority} | ${t.diagnosis_class} | ${t.prioritization.recommended_action}`);
    }
  }
  if (result.rejected_candidates?.length) {
    console.log(`\nRejected candidates: ${result.rejected_candidates.length}`);
  }
  console.log(`\nReport: ${reportPath}`);
}

main()
  .then(() => pool.end().catch(() => {}))
  .catch(async (err) => {
    console.error(err.stack || err.message);
    await pool.end().catch(() => {});
    process.exit(1);
  });
