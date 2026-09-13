#!/usr/bin/env node
'use strict';

/**
 * Anchor tenant 10 — read-only Emmett outbound readiness probe.
 *
 * Intended for Railway production SSH. Never sends mail. Never writes
 * clients, missions, autosend, or enabled_agents.
 *
 * Usage:
 *   node scripts/probeAnchorEmmettOutboundReadiness.js --confirm-production
 */

require('dotenv').config();

const pool = require('../db');
const {
  resolveCanonicalSenderIdentity,
  evaluateCanonicalSenderReadiness,
} = require('../utils/canonicalSenderIdentity');
const { getBrevoState } = require('../utils/sendingReadiness');
const { validateProspectMessageBindings } = require('../packages/acquisition-mission/ExecutionApproval');
const { unwrapContributionPayload } = require('./validateAnchorCanonicalMission');

const TENANT_ID = '10';
const CLIENT_ID = 10;
const KNOWN_READY_MISSION = 'mission_1ddd1acb-6bae-4d51-baa8-ca449bab061a';

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const help = argv.includes('--help') || argv.includes('-h');
  const unknown = argv.filter(
    (arg) => arg !== '--confirm-production' && arg !== '--help' && arg !== '-h'
  );
  if (unknown.length) {
    throw new Error(
      `Unknown argument(s): ${unknown.join(', ')}. Usage: node scripts/probeAnchorEmmettOutboundReadiness.js --confirm-production`
    );
  }
  return { confirmProduction, help };
}

function printUsage() {
  console.log(`Anchor Emmett outbound readiness probe (tenant ${TENANT_ID})

Usage:
  node scripts/probeAnchorEmmettOutboundReadiness.js --confirm-production

Safety:
  Refuses to run without --confirm-production.
  SELECT + Brevo GET only. Never POST /v3/smtp/email.
  Never updates clients, missions, autosend, or enabled_agents.
`);
}

function assertRuntimeEnv() {
  if (!process.env.DATABASE_URL) {
    const err = new Error('Missing required runtime env: DATABASE_URL');
    err.code = 'runtime_env_missing';
    throw err;
  }
}

function summarizeBrevo(brevoState) {
  return {
    apiKeyPresent: Boolean(process.env.BREVO_API_KEY),
    domainVerified: brevoState.domain?.verified === true,
    domainAuthenticated: brevoState.domain?.authenticated === true,
    senderRegistered: Boolean(brevoState.sender),
    senderActive: brevoState.sender?.active === true,
    errors: brevoState.errors || [],
  };
}

function inspectBindings(emmettPayload) {
  const body = unwrapContributionPayload(emmettPayload) || {};
  const queueItems = Array.isArray(body.queue?.items) ? body.queue.items : [];
  const validation = validateProspectMessageBindings(body);
  return {
    queueCount: queueItems.length,
    senderIdentity: body.senderIdentity || null,
    governorOutcome: body.governor?.outcome || null,
    recommendedCapacity: body.capacity?.recommended ?? null,
    samplePaigeKeys: queueItems[0]?.paige ? Object.keys(queueItems[0].paige) : [],
    spec212: {
      valid: validation.valid === true,
      result: validation.result || null,
      blockerReason: validation.blockerReason || null,
      violationReasons: (validation.violations || []).map((row) => row.reason),
    },
  };
}

async function run(options = {}) {
  if (options.help) {
    printUsage();
    return { help: true };
  }
  if (!options.confirmProduction) {
    const err = new Error('Refusing to run without --confirm-production.');
    err.code = 'confirm_production_required';
    throw err;
  }
  assertRuntimeEnv();

  const report = {
    tenantId: TENANT_ID,
    readOnly: true,
    sentMail: false,
    autosendMutated: false,
    enabledAgentsMutated: false,
    startedAt: new Date().toISOString(),
  };

  const clientResult = await pool.query(
    `SELECT id, name, active, sender_email, sender_name, sending_domain,
            enabled_agents, autosend_enabled, warmup_start_date
       FROM clients
      WHERE id = $1`,
    [CLIENT_ID]
  );
  const client = clientResult.rows[0] || null;
  report.client = client
    ? {
      id: client.id,
      name: client.name,
      active: client.active === true,
      sender_email: client.sender_email || null,
      sender_name: client.sender_name || null,
      sending_domain: client.sending_domain || null,
      enabled_agents: client.enabled_agents || [],
      autosend_enabled: client.autosend_enabled === true,
      warmup_start_date: client.warmup_start_date || null,
    }
    : null;

  const resolved = await resolveCanonicalSenderIdentity({
    tenantId: TENANT_ID,
    clientId: CLIENT_ID,
    client,
  });
  report.canonicalSender = {
    ok: resolved.ok === true,
    code: resolved.code || null,
    blockReason: resolved.blockReason || null,
    identity: resolved.identity || null,
  };

  const brevoState = await getBrevoState(client || {
    sender_email: 'jacob@goanchorcleaning.com',
    sending_domain: 'goanchorcleaning.com',
  });
  report.brevo = summarizeBrevo(brevoState);

  const readiness = await evaluateCanonicalSenderReadiness({
    client,
    brevoState,
  });
  report.senderReadiness = {
    ready: readiness.ready === true,
    sendable: readiness.sendable === true,
    code: readiness.code || null,
    blockReason: readiness.blockReason || null,
    failures: (readiness.failures || []).map((row) => ({
      code: row.code,
      message: row.message,
    })),
  };

  const missions = await pool.query(
    `SELECT id, stage, status, objective, updated_at
       FROM acquisition_missions
      WHERE tenant_id = $1
        AND stage = 'ready'
      ORDER BY updated_at DESC
      LIMIT 5`,
    [TENANT_ID]
  );
  report.readyMissions = missions.rows.map((row) => ({
    id: row.id,
    stage: row.stage,
    status: row.status,
    objective: row.objective,
    updatedAt: row.updated_at,
    knownValidationMission: row.id === KNOWN_READY_MISSION,
  }));

  const targetMissionId = missions.rows.find((row) => row.id === KNOWN_READY_MISSION)?.id
    || missions.rows[0]?.id
    || null;
  report.inspectedMissionId = targetMissionId;

  if (targetMissionId) {
    const contrib = await pool.query(
      `SELECT id, specialist, kind, payload, at
         FROM acquisition_mission_contributions
        WHERE mission_id = $1
          AND tenant_id = $2
          AND specialist = 'emmett'
          AND kind = 'capacity'
        ORDER BY at DESC
        LIMIT 1`,
      [targetMissionId, TENANT_ID]
    );
    report.emmettCapacity = contrib.rows[0]
      ? {
        contributionId: contrib.rows[0].id,
        at: contrib.rows[0].at,
        ...inspectBindings(contrib.rows[0].payload),
      }
      : { contributionId: null };
  }

  report.enabledAgentsMustChange = false;
  report.autosendWouldFire = report.client?.autosend_enabled === true
    && ['1', 'true', 'yes', 'on'].includes(String(process.env.EMMETT_AUTOSEND_ENABLED || '').trim().toLowerCase());

  const spec212Blocked = report.emmettCapacity
    && report.emmettCapacity.queueCount > 0
    && report.emmettCapacity.spec212
    && report.emmettCapacity.spec212.valid !== true;
  const brevoBlocked = report.senderReadiness.sendable !== true;

  report.firstBlocker = spec212Blocked
    ? {
      code: 'tme_message_binding_contamination',
      message: report.emmettCapacity.spec212.blockerReason
        || 'SPEC-212 bindings missing on persisted CAPACITY queue.',
    }
    : brevoBlocked
      ? {
        code: report.senderReadiness.code || 'canonical_sender_not_ready',
        message: report.senderReadiness.blockReason || 'Canonical sender is not Brevo-ready.',
      }
      : {
        code: null,
        message: 'No probe-visible blocker. Operator APPROVE_EXECUTION then explicit EXECUTE_OUTBOUND still required to send.',
      };

  report.nextOperatorAction = spec212Blocked
    ? 'Do not execute outbound. Preserve SPEC-212 paige.candidateId / bindingScope / attributableIntelligence through CAPACITY persist, then rerun this probe.'
    : brevoBlocked
      ? 'Do not execute outbound. Finish Brevo domain authentication and/or activate jacob@goanchorcleaning.com, then rerun this probe.'
      : 'Keep autosend_enabled=false. Do not change enabled_agents. Approve execution on the READY mission, then issue an explicit EXECUTE_OUTBOUND CER for one queue item.';

  report.completedAt = new Date().toISOString();
  return report;
}

module.exports = {
  TENANT_ID,
  parseArgs,
  run,
};

if (require.main === module) {
  const options = parseArgs();
  run(options)
    .then((report) => {
      if (report.help) return;
      console.log(JSON.stringify(report, null, 2));
      process.exitCode = report.firstBlocker?.code ? 2 : 0;
    })
    .catch((err) => {
      console.log(JSON.stringify({
        error: { code: err.code || null, message: err.message },
        completedAt: new Date().toISOString(),
      }, null, 2));
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}
