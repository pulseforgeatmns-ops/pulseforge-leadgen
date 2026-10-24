'use strict';

/**
 * Read-only CIE Blueprint lifecycle audit for a client.
 *
 *   DATABASE_URL=... node scripts/auditCieBlueprintLifecycle.js --client_id 11
 *
 * Prints interview + Blueprint rows and what current resolution returns.
 * Never mutates production data.
 */

require('dotenv').config();

const {
  auditClientBlueprintLifecycle,
} = require('../services/clientIntelligenceInterview');

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx < 0) return null;
  return process.argv[idx + 1] || null;
}

async function main() {
  const clientId = Number(argValue('--client_id') || process.env.AS_CLEANING_CLIENT_ID || 11);
  if (!Number.isFinite(clientId)) {
    console.error('Provide --client_id <n>');
    process.exit(1);
  }
  if (!process.env.DATABASE_URL) {
    console.error('DATABASE_URL is required');
    process.exit(1);
  }

  const report = await auditClientBlueprintLifecycle(clientId);
  console.log(JSON.stringify(report, null, 2));
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
