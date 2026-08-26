#!/usr/bin/env node
'use strict';

/**
 * AUDIT-063 — Places API cost attribution report.
 *
 * Usage:
 *   node scripts/placesCostReport.js
 *   node scripts/placesCostReport.js --since 2026-08-01 --tenant 1
 *   node scripts/placesCostReport.js --json
 */

require('dotenv').config();

const {
  queryPlacesCostReport,
  formatPlacesCostReportMarkdown,
} = require('../utils/placesCostAttribution');

function parseArgs(argv) {
  const opts = { json: false };
  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--json') opts.json = true;
    else if (arg === '--since') opts.since = argv[++i];
    else if (arg === '--until') opts.until = argv[++i];
    else if (arg === '--tenant' || arg === '--client-id') opts.tenantId = Number(argv[++i]);
    else if (arg === '--help' || arg === '-h') opts.help = true;
  }
  return opts;
}

function printHelp() {
  console.log(`Places API cost report (AUDIT-063)

Options:
  --since YYYY-MM-DD   Include requests on/after date (UTC)
  --until YYYY-MM-DD   Include requests before date (UTC)
  --tenant ID          Filter to one client/tenant
  --json               Emit JSON instead of markdown tables
  --help               Show this help
`);
}

async function main() {
  const opts = parseArgs(process.argv);
  if (opts.help) {
    printHelp();
    return;
  }

  const report = await queryPlacesCostReport({
    since: opts.since || null,
    until: opts.until || null,
    tenantId: Number.isFinite(opts.tenantId) ? opts.tenantId : null,
  });

  if (opts.json) {
    console.log(JSON.stringify(report, null, 2));
    return;
  }

  console.log(formatPlacesCostReportMarkdown(report));
}

main().catch((err) => {
  console.error('[placesCostReport] failed:', err.message);
  process.exit(1);
});
