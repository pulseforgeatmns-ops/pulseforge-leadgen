#!/usr/bin/env node
'use strict';

/**
 * Reconcile the 2026-09-29 12:14:39 ET Anchor uncertain governed send.
 *
 * Default is inspect-only. Does not send mail.
 *
 *   node scripts/reconcileAnchorUncertainSend.js --confirm-production
 *   node scripts/reconcileAnchorUncertainSend.js --confirm-production --apply --operator <id>
 */

require('dotenv').config({ quiet: true });

const axios = require('axios');
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');

const INCIDENT = Object.freeze({
  tenantId: '10',
  itemId: 'daily_b6405a6360afa44baaf23c2a_3',
  envelopeId: 'daily_b6405a6360afa44baaf23c2a',
  missionId: 'mission_daily_751fbbc204ae39b6db82b15d',
  prospectId: '03c2e326-39c5-4e29-abda-cdb25f077e0b',
  companyId: 'b22946db-7955-4280-b1ac-d8b2b7670e09',
  email: 'hello@sacramentopmg.com',
  attemptedAt: '2026-09-29T16:14:39.271Z',
  subject: 'Cleaning for Auburn Property Management',
});

const EVIDENCE = 'Brevo SMTP events for 2026-09-29 include the three earlier governed sends and no request, delivery, or open for hello@sacramentopmg.com. No acquisition_mission_outbound_executions row exists for this prospect. Claim at 16:14:39.269Z was followed at 16:14:39.472Z by tick_blocked 23502 (execution_identity NOT NULL) before provider acceptance.';

function parseArgs(argv = process.argv.slice(2)) {
  const options = { confirmProduction: false, apply: false, operator: null };
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === '--confirm-production') options.confirmProduction = true;
    else if (argv[i] === '--apply') options.apply = true;
    else if (argv[i] === '--operator') options.operator = argv[++i];
    else if (argv[i] === '--help' || argv[i] === '-h') options.help = true;
    else throw new Error(`Unknown argument: ${argv[i]}`);
  }
  return options;
}

async function loadItem(pool) {
  const { rows } = await pool.query(
    `SELECT i.*, e.mission_id, e.local_day, e.status AS envelope_status
       FROM acquisition_outbound_items i
       JOIN acquisition_outbound_envelopes e ON e.id = i.envelope_id
      WHERE i.id = $1 AND i.tenant_id = $2`,
    [INCIDENT.itemId, INCIDENT.tenantId],
  );
  return rows[0] || null;
}

async function loadExecutions(pool) {
  const { rows } = await pool.query(
    `SELECT id, status, provider_message_id, prospect_id, payload
       FROM acquisition_mission_outbound_executions
      WHERE tenant_id = $1 AND (prospect_id = $2 OR payload->>'email' = $3)`,
    [INCIDENT.tenantId, INCIDENT.prospectId, INCIDENT.email],
  );
  return rows;
}

async function loadBrevoEvents() {
  if (!process.env.BREVO_API_KEY) return { skipped: true, events: [] };
  const { data } = await axios.get('https://api.brevo.com/v3/smtp/statistics/events', {
    headers: { 'api-key': process.env.BREVO_API_KEY, accept: 'application/json' },
    params: { email: INCIDENT.email, startDate: '2026-09-29', endDate: '2026-09-29', limit: 50, sort: 'desc' },
    timeout: 20000,
  });
  return { skipped: false, events: data?.events || [] };
}

function outcomeFromEvidence({ item, executions, brevo }) {
  if (item?.provider_message_id || executions.some(row => row.provider_message_id) || brevo.events.length) {
    return 'PROVIDER_CONFIRMED_SENT';
  }
  if (!brevo.skipped && brevo.events.length === 0 && executions.length === 0) {
    return 'PROVIDER_CONFIRMED_NOT_SENT';
  }
  return 'PROVIDER_STATE_UNKNOWN';
}

async function persistEvidence(pool, report) {
  const id = hash(['send_provider_evidence', INCIDENT.itemId, report.providerOutcome]);
  await pool.query(
    `INSERT INTO acquisition_outbound_events(id,tenant_id,program_id,envelope_id,item_id,event_type,payload)
     VALUES ($1,$2,$3,$4,$5,'send_provider_evidence',$6) ON CONFLICT DO NOTHING`,
    [id, INCIDENT.tenantId, report.programId || null, INCIDENT.envelopeId, INCIDENT.itemId, {
      itemId: INCIDENT.itemId,
      providerOutcome: report.providerOutcome,
      evidence: EVIDENCE,
      email: INCIDENT.email,
      attemptedAt: INCIDENT.attemptedAt,
      brevoEventCount: report.brevo.events.length,
      executionIds: report.executions.map(row => row.id),
      notNullColumn: 'execution_identity',
      sqlstate: '23502',
    }],
  );
}

async function run(argv = process.argv.slice(2)) {
  const options = parseArgs(argv);
  if (options.help) {
    console.log(`Usage:
  node scripts/reconcileAnchorUncertainSend.js --confirm-production
  node scripts/reconcileAnchorUncertainSend.js --confirm-production --apply --operator <id>
`);
    return { help: true };
  }
  if (!options.confirmProduction) {
    throw Object.assign(new Error('Refusing to run without --confirm-production.'), { code: 'confirm_production_required' });
  }
  const pool = require('../db');
  try {
    const item = await loadItem(pool);
    const executions = await loadExecutions(pool);
    const brevo = await loadBrevoEvents();
    const program = (await pool.query(
      `SELECT id, last_error, authorized_by FROM acquisition_outbound_programs
        WHERE tenant_id = $1 AND mode <> 'revoked'`,
      [INCIDENT.tenantId],
    )).rows[0] || null;
    const providerOutcome = outcomeFromEvidence({ item, executions, brevo });
    const report = {
      itemId: INCIDENT.itemId,
      email: INCIDENT.email,
      attemptedAt: item?.attempted_at || INCIDENT.attemptedAt,
      status: item?.status || null,
      reason: item?.reason || null,
      providerMessageId: item?.provider_message_id || null,
      programId: program?.id || null,
      lastError: program?.last_error || null,
      executions,
      brevo,
      providerOutcome,
      applied: false,
    };
    await persistEvidence(pool, report);
    if (!options.apply) return report;
    if (providerOutcome !== 'PROVIDER_CONFIRMED_NOT_SENT') {
      throw Object.assign(new Error(`Refusing to apply ${providerOutcome}.`), { code: 'provider_outcome_blocks_apply' });
    }
    if (!options.operator) throw new Error('--operator is required for --apply');
    const service = require('../services/governedOutbound').productionService(pool, { tenantId: INCIDENT.tenantId });
    report.applyResult = await service.reconcile(
      INCIDENT.itemId,
      'not_accepted',
      null,
      EVIDENCE,
      { id: options.operator, role: 'admin' },
    );
    report.applied = true;
    report.status = (await loadItem(pool))?.status || null;
    return report;
  } finally {
    await pool.end();
  }
}

if (require.main === module) {
  run().then((result) => {
    console.log(JSON.stringify(result, null, 2));
  }).catch((error) => {
    console.error(JSON.stringify({ error: error.code || error.message }));
    process.exitCode = 1;
  });
}

module.exports = { INCIDENT, EVIDENCE, parseArgs, outcomeFromEvidence, run };
