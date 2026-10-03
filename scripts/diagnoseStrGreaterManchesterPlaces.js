'use strict';

/**
 * Live production-style diagnostic for Greater Manchester STR Places workloads.
 *
 *   node scripts/diagnoseStrGreaterManchesterPlaces.js
 *   node scripts/diagnoseStrGreaterManchesterPlaces.js --json
 *   npm run scout:places:str-greater-manchester
 */

try {
  require('dotenv').config({ quiet: true });
} catch {
  // optional
}

const {
  diagnoseStrGreaterManchesterPlacesWorkload,
} = require('../services/scoutPlacesWorkloadDiagnostic');

function parseArgs(argv = process.argv.slice(2)) {
  return { json: argv.includes('--json'), help: argv.includes('--help') || argv.includes('-h') };
}

function printHelp() {
  console.log(`Greater Manchester STR Places workload diagnostic

Runs all 36 City×Concept workloads using the exact PlacesProvider query path
(concept + city, Text Search → Details → website filter).

Usage:
  node scripts/diagnoseStrGreaterManchesterPlaces.js
  node scripts/diagnoseStrGreaterManchesterPlaces.js --json
`);
}

function formatReport(report) {
  const lines = [];
  lines.push('Greater Manchester STR Places workload diagnostic');
  lines.push('================================================');
  lines.push(`ok: ${report.ok}`);
  lines.push(`adapterAvailable: ${report.adapterAvailable}`);
  lines.push(`keyPresent: ${report.keyPresent}`);
  lines.push(`keyFingerprint: ${report.keyFingerprint || '(none)'}`);
  lines.push(
    `railway: ${report.railway?.testedLabel || 'local_or_unknown'}`
  );
  lines.push(`workloadsPlanned: ${report.workloadsPlanned}`);
  lines.push(`summary: ${JSON.stringify(report.summary)}`);
  lines.push(`conclusion: ${report.conclusion}`);
  if (report.error) lines.push(`error: ${report.error}`);
  lines.push('');
  lines.push('Per-workload:');
  for (const row of report.workloads || []) {
    lines.push(
      `  ${row.city} | ${row.concept} | q="${row.queryText}" | ` +
        `google=${row.googleStatus} text=${row.textSearchResultCount} ` +
        `website=${row.withWebsiteCount} dropped=${row.droppedNoWebsiteCount}` +
        (row.error ? ` ERR=${row.error}` : '')
    );
  }
  lines.push('guardrails: no CRM writes, no outreach, no placeholders, no full key');
  return lines.join('\n');
}

async function main() {
  const options = parseArgs();
  if (options.help) {
    printHelp();
    process.exit(0);
  }

  const report = await diagnoseStrGreaterManchesterPlacesWorkload();
  if (options.json) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    console.log(formatReport(report));
  }
  process.exit(report.ok ? 0 : 2);
}

main().catch((err) => {
  console.error(JSON.stringify({ ok: false, error: err?.message || 'diagnostic_failed' }, null, 2));
  process.exit(1);
});
