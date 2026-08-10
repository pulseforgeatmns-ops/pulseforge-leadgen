'use strict';

/**
 * Safe Scout Places diagnostic CLI.
 *
 * Uses the exact same legacy Places Text Search path as Scout sourcing.
 * Does not write CRM / outreach / placeholders. Never prints the full API key.
 *
 *   node scripts/diagnoseScoutPlaces.js
 *   node scripts/diagnoseScoutPlaces.js --json
 *   node scripts/diagnoseScoutPlaces.js --skip-places-new
 *   npm run scout:places:diagnostic
 */

try {
  require('dotenv').config({ quiet: true });
} catch {
  // dotenv optional when Railway/runtime already injects env
}

const {
  diagnoseScoutPlaces,
  formatScoutPlacesDiagnostic,
} = require('../services/scoutPlacesDiagnostic');

function parseArgs(argv = process.argv.slice(2)) {
  const options = {
    json: false,
    comparePlacesNew: true,
    query: null,
    help: false,
  };

  for (const arg of argv) {
    if (arg === '--json') {
      options.json = true;
      continue;
    }
    if (arg === '--skip-places-new') {
      options.comparePlacesNew = false;
      continue;
    }
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg.startsWith('--query=')) {
      options.query = arg.slice('--query='.length);
      continue;
    }
  }

  return options;
}

function printHelp() {
  console.log(`Scout Places diagnostic

Probes the same legacy Places Text Search endpoint Scout sourcing uses.
Never logs the full GOOGLE_PLACES_KEY. Never writes CRM or outreach.

Usage:
  node scripts/diagnoseScoutPlaces.js
  node scripts/diagnoseScoutPlaces.js --json
  node scripts/diagnoseScoutPlaces.js --skip-places-new
  node scripts/diagnoseScoutPlaces.js --query="law firm Manchester NH"

Reports:
  endpoint family, request host/path, HTTP status, Google status,
  Google error_message, key fingerprint (first4…last4), key present,
  Railway service/environment.
`);
}

async function main() {
  const options = parseArgs();
  if (options.help) {
    printHelp();
    process.exit(0);
  }

  const report = await diagnoseScoutPlaces({
    comparePlacesNew: options.comparePlacesNew,
    query: options.query || undefined,
  });

  if (options.json) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    console.log(formatScoutPlacesDiagnostic(report));
  }

  process.exit(report.ok ? 0 : 2);
}

main().catch((err) => {
  console.error(
    JSON.stringify(
      {
        ok: false,
        error: err && err.message ? String(err.message) : 'diagnostic_failed',
        crmWritesMade: false,
        outreachCopyGenerated: false,
        placeholdersCreated: false,
        fullKeyLogged: false,
      },
      null,
      2
    )
  );
  process.exit(1);
});
