#!/usr/bin/env node
'use strict';

/**
 * AUDIT-063 — Places API cost attribution report (daily + cognitive + efficiency).
 *
 * Usage:
 *   node scripts/placesCostReport.js
 *   node scripts/placesCostReport.js --day 2026-08-25
 *   node scripts/placesCostReport.js --since 2026-08-01 --tenant 1 --json
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
    else if (arg === '--day') opts.day = argv[++i];
    else if (arg === '--tenant' || arg === '--client-id') opts.tenantId = Number(argv[++i]);
    else if (arg === '--mission') opts.missionId = argv[++i];
    else if (arg === '--help' || arg === '-h') opts.help = true;
  }
  return opts;
}

function dayBounds(day) {
  const start = new Date(`${day}T00:00:00.000Z`);
  const end = new Date(start);
  end.setUTCDate(end.getUTCDate() + 1);
  return { since: start.toISOString(), until: end.toISOString() };
}

function printHelp() {
  console.log(`Places API cost report (AUDIT-063)

Options:
  --day YYYY-MM-DD     Report for one UTC calendar day (yesterday if omitted with --daily)
  --since YYYY-MM-DD   Include requests on/after date (UTC)
  --until YYYY-MM-DD   Include requests before date (UTC)
  --tenant ID          Filter to one client/tenant
  --mission ID         Filter to one acquisition mission
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

  let since = opts.since || null;
  let until = opts.until || null;
  if (opts.day) {
    const bounds = dayBounds(opts.day);
    since = bounds.since;
    until = bounds.until;
  }

  const report = await queryPlacesCostReport({
    since,
    until,
    tenantId: Number.isFinite(opts.tenantId) ? opts.tenantId : null,
    missionId: opts.missionId || null,
  });

  if (opts.json) {
    console.log(JSON.stringify(report, null, 2));
    return;
  }

  const header = [];
  if (opts.day) header.push(`Report day (UTC): **${opts.day}**`);
  else if (since || until) header.push(`Window: ${since || '…'} → ${until || '…'}`);

  console.log(formatPlacesCostReportMarkdown(report, { headerLines: header }));
}

main().catch((err) => {
  console.error('[placesCostReport] failed:', err.message);
  process.exit(1);
});
