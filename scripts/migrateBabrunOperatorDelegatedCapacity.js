#!/usr/bin/env node
'use strict';

/**
 * Migrate tenant 13 governed grant to operator-delegated maximum capacity (20/day envelope).
 * Uses canonical review + migrateProgramPolicy — no ad-hoc SQL on production policy JSON.
 *
 *   node scripts/migrateBabrunOperatorDelegatedCapacity.js --dry-run
 *   node scripts/migrateBabrunOperatorDelegatedCapacity.js --confirm-production --actor-id=1
 */

require('dotenv').config({ quiet: true });

const pool = require('../db');
const { productionService } = require('../services/governedOutbound');
const { TENANT_ID } = require('./lib/babrunCanonicalOutbound');

async function main() {
  const dryRun = process.argv.includes('--dry-run');
  const confirm = process.argv.includes('--confirm-production');
  const actorArg = process.argv.find(a => a.startsWith('--actor-id='));
  const actorId = actorArg ? actorArg.split('=')[1] : '1';
  if (!dryRun && !confirm) {
    throw Object.assign(new Error('Pass --dry-run or --confirm-production'), { code: 'confirm_required' });
  }
  if (!process.env.DATABASE_URL) throw Object.assign(new Error('DATABASE_URL required'), { code: 'runtime_env_missing' });

  const svc = productionService(pool, { tenantId: TENANT_ID });
  const program = await svc.store.program();
  if (!program) throw Object.assign(new Error('No governed program for tenant 13'), { code: 'no_program' });

  const preview = await svc.migrateOperatorDelegatedCapacity({
    tenantId: TENANT_ID,
    operatorDelegatedMaximumDailyCapacity: 20,
    authorizationNote: 'SPEC Emmett-authoritative dynamic outbound capacity — delegated ramp envelope 20/day',
  }, { id: actorId, role: 'admin' });

  if (preview.reviewRequired) {
    if (dryRun) {
      console.log(JSON.stringify({
        dryRun: true,
        reviewRequired: true,
        reviewHash: preview.reviewHash,
        operatorDelegatedMaximumDailyCapacity: preview.policy.operatorDelegatedMaximumDailyCapacity,
        preservedDailyCap: preview.policy.dailyCap,
        programId: program.id,
      }, null, 2));
      await pool.end();
      return;
    }
    const applied = await svc.migrateOperatorDelegatedCapacity({
      tenantId: TENANT_ID,
      operatorDelegatedMaximumDailyCapacity: 20,
      reviewHash: preview.reviewHash,
      authorizationNote: preview.policy.operatorDelegatedMaximumDailyCapacity === 20
        ? 'SPEC Emmett-authoritative dynamic outbound capacity — delegated ramp envelope 20/day'
        : undefined,
    }, { id: actorId, role: 'admin' });
    console.log(JSON.stringify({
      migrated: true,
      programId: applied.id,
      policyHash: applied.policy_hash,
      operatorDelegatedMaximumDailyCapacity: applied.policy.operatorDelegatedMaximumDailyCapacity,
      dailyCap: applied.policy.dailyCap,
    }, null, 2));
  } else {
    console.log(JSON.stringify({ migrated: false, result: preview }, null, 2));
  }
  await pool.end();
}

if (require.main === module) {
  main().catch((err) => {
    console.error(JSON.stringify({ error: err.code || err.message }));
    process.exit(1);
  });
}

module.exports = { main };
