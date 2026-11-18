#!/usr/bin/env node
'use strict';

/**
 * Governed retention run for max_interpretation_shadow.
 *
 *   node scripts/maxInterpretationShadowPurge.js            # dry run: counts only
 *   node scripts/maxInterpretationShadowPurge.js --apply    # deletes rows older than 90 days
 *
 * Operator-run in the managed environment (DATABASE_URL). Never wired into
 * startup or request paths. Exit 0 ok · 1 unavailable.
 */

process.env.DOTENV_CONFIG_QUIET = 'true';
const { createShadowPool } = require('../packages/decision-service/ShadowEventSink');
const { countExpired, purgeExpired, RETENTION_DAYS } = require('../packages/max/interpretation/shadowRetention');

(async () => {
  if (!process.env.DATABASE_URL) {
    console.error('DATABASE_URL is required in the managed environment.');
    process.exitCode = 1;
    return;
  }
  const db = createShadowPool();
  try {
    const before = await countExpired(db, { retentionDays: RETENTION_DAYS });
    if (!process.argv.includes('--apply')) {
      console.log(JSON.stringify({ mode: 'dry_run', retentionDays: RETENTION_DAYS, ...before }));
      return;
    }
    const result = await purgeExpired(db, { retentionDays: RETENTION_DAYS });
    console.log(JSON.stringify({ mode: 'apply', retentionDays: RETENTION_DAYS, expiredBefore: before.expired, ...result }));
  } catch (_) {
    console.error('Shadow retention unavailable (missing table or database error).');
    process.exitCode = 1;
  } finally {
    await db.end();
  }
})();
