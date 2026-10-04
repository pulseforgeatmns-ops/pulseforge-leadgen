#!/usr/bin/env node
'use strict';

/**
 * Migrate tenant 13 from activation grant → first bounded autonomous operating grant.
 * Uses canonical review + migrateProgramPolicy — no ad-hoc SQL on production policy JSON.
 *
 *   node scripts/migrateBabrunOperatorDelegatedCapacity.js --dry-run
 *   node scripts/migrateBabrunOperatorDelegatedCapacity.js --confirm-production --actor-id=1
 */

require('dotenv').config({ quiet: true });

const pool = require('../db');
const { productionService } = require('../services/governedOutbound');
const { adapters } = require('../services/governedOutboundAdapters');
const { assessOperatingCapacity } = require('../packages/emmett-outbound/OperatingCapacity');
const { DEFAULT_BOUNDED_GRANT_HORIZON_DAYS } = require('../packages/emmett-outbound/OperatorDelegatedCapacity');
const { TENANT_ID } = require('./lib/babrunCanonicalOutbound');

const OPERATING_GRANT = Object.freeze({
  operatorDelegatedMaximumDailyCapacity: 20,
  totalCap: 100,
  grantHorizonDays: DEFAULT_BOUNDED_GRANT_HORIZON_DAYS,
  authorizationNote: 'Babrun first bounded autonomous operating grant — operator envelope 20/day, program total 100, 30-day horizon',
});

function preservedSafetyFields(policy = {}) {
  return {
    dailyCap: policy.dailyCap,
    spacingMinutes: policy.spacingMinutes,
    startHour: policy.startHour,
    endHour: policy.endHour,
    timeZone: policy.timeZone,
    weekdays: policy.weekdays,
    senderEmail: policy.senderEmail,
    sendingIdentityId: policy.sendingIdentityId,
    inboxIntegrationId: policy.inboxIntegrationId,
    sourceMissionId: policy.sourceMissionId,
    allowedContactClassifications: policy.allowedContactClassifications,
    mode: 'active',
  };
}

function authoritySnapshot(envelope = {}) {
  return {
    dailyCap: envelope.dailyCap ?? null,
    totalCap: envelope.totalCap ?? null,
    operatorDelegatedMaximumDailyCapacity: envelope.operatorDelegatedMaximumDailyCapacity ?? null,
    startsAt: envelope.startsAt ?? null,
    expiresAt: envelope.expiresAt ?? null,
  };
}

function expectedEffectiveDailyCapacity(operating) {
  if (!operating) return null;
  return operating.authorizationLimitedCapacity ?? operating.effectiveDailyCapacity ?? null;
}

async function loadEmmettRecommendation(program) {
  if (!program) return null;
  try {
    const infra = await adapters(pool, { tenantId: TENANT_ID }).infrastructure(program, new Date(), null, { mode: 'planning' });
    const op = infra.operating || {};
    const recommended = op.recommendedSafeDailyCapacity ?? infra.assessed?.capacity?.recommended ?? null;
    const operating = op.recommendedSafeDailyCapacity != null
      ? op
      : assessOperatingCapacity({
        assessed: infra.assessed,
        policy: program.policy,
        schedule: {
          allowedSendWindow: op.allowedSendWindow,
          minSpacingMinutes: op.minSpacingMinutes ?? program.policy?.spacingMinutes,
        },
      });
    return {
      recommendedSafeDailyCapacity: recommended,
      authorizationLimitedCapacity: operating.authorizationLimitedCapacity,
      capacityLimitingAuthority: operating.capacityLimitingAuthority,
      limitingFactor: operating.limitingFactor,
      spacingMinutes: op.minSpacingMinutes ?? program.policy?.spacingMinutes,
    };
  } catch (error) {
    return { unavailable: true, reason: error.code || error.message };
  }
}

function buildDryRunReport({ program, preview, emmett }) {
  const before = preview.authority?.before
    ? authoritySnapshot(preview.authority.before)
    : authoritySnapshot(program.policy);
  const after = preview.authority?.after
    ? authoritySnapshot(preview.authority.after)
    : authoritySnapshot(preview.policy);
  const effective = expectedEffectiveDailyCapacity(
    emmett && !emmett.unavailable
      ? { authorizationLimitedCapacity: emmett.authorizationLimitedCapacity }
      : null,
  );
  return {
    dryRun: true,
    reviewRequired: true,
    reviewHash: preview.reviewHash,
    programId: program.id,
    migration: preview.migration,
    BEFORE: before,
    AFTER: after,
    grantHorizonDays: preview.grantHorizon?.grantHorizonDays ?? OPERATING_GRANT.grantHorizonDays,
    operatorDailyCeiling: preview.authority?.after?.effectiveOperatorDailyCeiling ?? 20,
    operatorProgramCeiling: preview.authority?.after?.effectiveOperatorProgramCeiling ?? 100,
    emmettRecommendation: emmett,
    expectedEffectiveDailyCapacity: effective,
    capacityLimitingAuthority: emmett?.capacityLimitingAuthority ?? null,
    programTotalCapMigration: preview.programTotalCapMigration,
    preservedSafetyFields: preservedSafetyFields(preview.policy),
  };
}

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
    ...OPERATING_GRANT,
  }, { id: actorId, role: 'admin' });

  if (preview.reviewRequired) {
    if (dryRun) {
      const emmett = await loadEmmettRecommendation({
        ...program,
        policy: preview.policy,
      });
      console.log(JSON.stringify(buildDryRunReport({ program, preview, emmett }), null, 2));
      await pool.end();
      return;
    }
    const applied = await svc.migrateOperatorDelegatedCapacity({
      tenantId: TENANT_ID,
      ...OPERATING_GRANT,
      reviewHash: preview.reviewHash,
    }, { id: actorId, role: 'admin' });
    console.log(JSON.stringify({
      migrated: true,
      programId: applied.id,
      policyHash: applied.policy_hash,
      operatorDelegatedMaximumDailyCapacity: applied.policy.operatorDelegatedMaximumDailyCapacity,
      dailyCap: applied.policy.dailyCap,
      totalCap: applied.policy.totalCap,
      startsAt: applied.policy.startsAt,
      expiresAt: applied.policy.expiresAt,
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

module.exports = {
  main,
  buildDryRunReport,
  authoritySnapshot,
  preservedSafetyFields,
  OPERATING_GRANT,
};
