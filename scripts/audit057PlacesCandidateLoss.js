'use strict';

/**
 * AUDIT-057 — Google Places Candidate Loss
 *
 * Traces all 36 Anchor Cleaning (client_id=10) Places workloads:
 *   6 NH cities × 6 verticals × Google Places
 *
 * Usage:
 *   GOOGLE_PLACES_KEY=... node scripts/audit057PlacesCandidateLoss.js
 *   node scripts/audit057PlacesCandidateLoss.js --json
 *   node scripts/audit057PlacesCandidateLoss.js --markdown > /tmp/audit-057.md
 *
 * Never writes CRM/outreach. Never logs the full API key.
 */

try {
  require('dotenv').config({ quiet: true });
} catch {
  // optional
}

const fs = require('fs');
const path = require('path');
const {
  runPlacesCandidateLossAudit,
  buildClient10PlacesWorkloads,
  formatAuditReport,
} = require('../packages/scout/audit/placesCandidateLossAudit');

function parseArgs(argv = process.argv.slice(2)) {
  const opts = {
    json: false,
    markdown: false,
    out: null,
    limit: 20,
    help: false,
    skipB2B: false,
    skipServiceArea: false,
  };
  for (const arg of argv) {
    if (arg === '--json') opts.json = true;
    else if (arg === '--markdown') opts.markdown = true;
    else if (arg === '--help' || arg === '-h') opts.help = true;
    else if (arg === '--skip-b2b') opts.skipB2B = true;
    else if (arg === '--skip-service-area') opts.skipServiceArea = true;
    else if (arg.startsWith('--out=')) opts.out = arg.slice('--out='.length);
    else if (arg.startsWith('--limit=')) opts.limit = Number(arg.slice('--limit='.length));
  }
  return opts;
}

function printHelp() {
  console.log(`AUDIT-057 — Google Places Candidate Loss

Runs 36 Places workloads for client_id=10 (6 cities × 6 verticals).
Requires GOOGLE_PLACES_KEY. Read-only — no CRM writes.

Options:
  --json                 Print JSON report
  --markdown             Print markdown report (default)
  --out=PATH             Also write report to PATH
  --limit=N              Max text-search hits per query (default 20)
  --skip-b2b             Do not apply B2C classification reject
  --skip-service-area    Do not apply service-area distance reject
  --help
`);
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    printHelp();
    process.exit(0);
  }

  const workloads = buildClient10PlacesWorkloads();
  const report = await runPlacesCandidateLossAudit(workloads, {
    limit: args.limit,
    enforceB2B: !args.skipB2B,
    enforceServiceArea: !args.skipServiceArea,
  });

  const text = args.json
    ? JSON.stringify(report, null, 2)
    : formatAuditReport(report);

  if (args.out) {
    fs.mkdirSync(path.dirname(args.out), { recursive: true });
    fs.writeFileSync(args.out, text, 'utf8');
  }

  console.log(text);

  const blocked = report.workloads.some((row) => row.error === 'GOOGLE_PLACES_KEY_missing');
  process.exit(blocked ? 2 : 0);
}

main().catch((err) => {
  console.error(JSON.stringify({ ok: false, error: err?.message || 'audit_failed' }, null, 2));
  process.exit(1);
});
