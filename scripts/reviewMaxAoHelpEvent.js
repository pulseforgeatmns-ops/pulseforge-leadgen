#!/usr/bin/env node
'use strict';

const { SCOPE, previewAoHelpReview, applyAoHelpReview, reconcileAoHelpReview } = require('../services/maxAoHelpReviewService');

function parseArgs(argv) {
  const values = {};
  const allowed = new Set(['client-id', 'prospect-id', 'ao-id', 'activity-id', 'expected-source-version', 'preview-observed-at', 'confirm']);
  let mode = 'preview';
  for (const arg of argv) {
    if (arg === '--apply' || arg === '--reconcile') {
      if (mode !== 'preview') throw new Error('Choose only one mode');
      mode = arg.slice(2); continue;
    }
    const match = /^--([^=]+)=(.+)$/.exec(arg);
    if (!match || !allowed.has(match[1]) || Object.hasOwn(values, match[1])) throw new Error('Unknown or duplicate argument');
    values[match[1]] = match[2];
  }
  const scope = { clientId: values['client-id'], prospectId: values['prospect-id'], aoId: values['ao-id'], activityId: values['activity-id'] };
  for (const key of Object.keys(scope)) if (String(scope[key]) !== String(SCOPE[key])) throw new Error('Explicit approved event scope is required');
  if (mode === 'apply' && values.confirm !== `operator-review-only:${SCOPE.activityId}`) throw new Error('Explicit review-only confirmation is required');
  if (mode !== 'apply' && (values.confirm || values['expected-source-version'] || values['preview-observed-at'])) throw new Error('Apply-only arguments are not allowed in read-only mode');
  return { mode, ...scope, expectedSourceVersion: values['expected-source-version'], previewObservedAt: values['preview-observed-at'] };
}
async function main(argv = process.argv.slice(2)) {
  const options = parseArgs(argv); // Validate before creating any connection.
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required');
  const { Pool } = require('pg');
  const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 2, connectionTimeoutMillis: 5000,
    ssl: process.env.DATABASE_SSL === 'false' ? false : { rejectUnauthorized: false } });
  try {
    const action = options.mode === 'apply' ? applyAoHelpReview : options.mode === 'reconcile' ? reconcileAoHelpReview : previewAoHelpReview;
    const result = await action({ pool, ...options });
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  } finally { await pool.end(); } // This script owns this dedicated pool, never the app's shared pool.
}
if (require.main === module) main().catch(error => {
  // Avoid connection strings, credentials and raw database error details in CLI output.
  process.stderr.write(`${error.code || 'AO_HELP_REVIEW_FAILED'}: ${error.code?.startsWith('AO_HELP_REVIEW_') ? error.message : 'Review failed; inspect securely. No automatic retry.'}\n`);
  process.exitCode = 1;
});
module.exports = { parseArgs, main };
