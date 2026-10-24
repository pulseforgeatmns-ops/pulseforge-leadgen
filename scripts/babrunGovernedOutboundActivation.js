#!/usr/bin/env node
'use strict';

/**
 * Babrun tenant 13 — governed outbound activation inspector (phases 1–6).
 * Phase 7 (live send) requires BABRUN_GOVERNED_OUTBOUND_ENABLED=true and an active grant.
 *
 *   node scripts/babrunGovernedOutboundActivation.js --confirm-production
 *   node scripts/babrunGovernedOutboundActivation.js --confirm-production --execute-scout
 */

require('dotenv').config({ quiet: true });

const pool = require('../db');
const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
const { productionService } = require('../services/governedOutbound');
const { adapters } = require('../services/governedOutboundAdapters');
const {
  TENANT_ID,
  CLIENT_ID,
  BABRUN_MAILBOX,
} = require('./lib/babrunCanonicalOutbound');

async function ensureClientSender(client) {
  if (client.sender_email && client.sending_domain) return client;
  await pool.query(
    `UPDATE clients
        SET sender_email = COALESCE(sender_email, $2),
            sender_name = COALESCE(NULLIF(sender_name, ''), $3),
            sending_domain = COALESCE(sending_domain, $4)
      WHERE id = $1`,
    [CLIENT_ID, BABRUN_MAILBOX.senderEmail, BABRUN_MAILBOX.senderDisplayName, 'babrun.com']
  );
  const { rows } = await pool.query('SELECT * FROM clients WHERE id=$1', [CLIENT_ID]);
  return rows[0];
}

async function phaseCanonicalState() {
  const blueprint = (await pool.query(
    `SELECT id, status, canonical_snapshot_tenant_id, canonical_snapshot_id, updated_at
       FROM cie_business_blueprints
      WHERE client_id = $1 AND status = 'approved'
      ORDER BY updated_at DESC LIMIT 1`,
    [CLIENT_ID]
  )).rows[0] || null;
  const missions = (await pool.query(
    `SELECT id, stage, status, objective, target_segment, updated_at
       FROM acquisition_missions
      WHERE tenant_id = $1
      ORDER BY updated_at DESC LIMIT 5`,
    [TENANT_ID]
  )).rows;
  return { blueprint, missions, missionCount: missions.length };
}

async function phaseMaxControl(execute) {
  return runMaxOutboundControlLoop({ pool, tenantId: TENANT_ID, execute, logger: console });
}

async function phaseEmmett(program) {
  if (!program) return { halted: 'no_program' };
  const infra = await adapters(pool, { tenantId: TENANT_ID }).infrastructure(program, new Date(), null, { mode: 'planning' });
  const op = infra.operating || {};
  return {
    recommendedSafeDailyCapacity: op.recommendedSafeDailyCapacity ?? infra.assessed?.capacity?.recommended,
    planningDailyCapacity: op.planningDailyCapacity,
    dispatchCapacityNow: op.dispatchCapacityNow,
    dispatchableDailyCapacity: op.dispatchableDailyCapacity,
    effectiveDailyCapacity: op.effectiveDailyCapacity,
    governor: infra.assessed?.governor,
    healthScore: infra.assessed?.health?.score,
    capacityReason: op.capacityReason,
    limitingFactor: op.limitingFactor,
    sentToday: infra.snapshot?.sentToday,
  };
}

async function phaseAuthorization() {
  const program = (await pool.query(
    `SELECT id, mode, policy, policy_hash, scope_hash, source_mission_id, authorized_at
       FROM acquisition_outbound_programs
      WHERE tenant_id = $1 AND mode <> 'revoked'
      ORDER BY authorized_at DESC LIMIT 1`,
    [TENANT_ID]
  )).rows[0] || null;
  return { program };
}

async function main() {
  const confirm = process.argv.includes('--confirm-production');
  const executeScout = process.argv.includes('--execute-scout');
  if (!confirm) throw Object.assign(new Error('Refusing without --confirm-production'), { code: 'confirm_required' });
  if (!process.env.DATABASE_URL) throw Object.assign(new Error('DATABASE_URL required'), { code: 'runtime_env_missing' });

  const client = await ensureClientSender((await pool.query('SELECT * FROM clients WHERE id=$1', [CLIENT_ID])).rows[0]);
  const canonical = await phaseCanonicalState();
  const auth = await phaseAuthorization();
  const emmett = await phaseEmmett(auth.program);
  const maxControl = auth.program
    ? await phaseMaxControl(executeScout)
    : { halted: 'no_enabled_program' };

  const sends = (await pool.query(
    `SELECT id, status, attempted_at, provider_message_id
       FROM acquisition_outbound_items
      WHERE tenant_id = $1 AND attempted_at IS NOT NULL
      ORDER BY attempted_at DESC LIMIT 5`,
    [TENANT_ID]
  )).rows;

  const report = {
    tenantId: TENANT_ID,
    client: { id: client.id, sender_email: client.sender_email, sending_domain: client.sending_domain, autosend_enabled: client.autosend_enabled },
    phase1_canonical: canonical,
    phase3_emmett: emmett,
    phase4_authorization: auth.program ? {
      id: auth.program.id,
      mode: auth.program.mode,
      sourceMissionId: auth.program.source_mission_id,
      dailyCap: auth.program.policy?.dailyCap,
      totalCap: auth.program.policy?.totalCap,
      spacingMinutes: auth.program.policy?.spacingMinutes,
      senderEmail: auth.program.policy?.senderEmail,
      inboxIntegrationId: auth.program.policy?.inboxIntegrationId,
      sendingIdentityId: auth.program.policy?.sendingIdentityId,
      authorizedAt: auth.program.authorized_at,
    } : null,
    phase5_maxControl: maxControl,
    phase7_recentGovernedItems: sends,
    babrunOutboundEnabled: process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED === 'true',
  };

  console.log(JSON.stringify(report, null, 2));
  await pool.end();
}

if (require.main === module) {
  main().catch((err) => {
    console.error(JSON.stringify({ error: err.code || err.message, stack: err.stack }));
    process.exit(1);
  });
}

module.exports = { main };
