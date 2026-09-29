#!/usr/bin/env node
'use strict';

/**
 * Anchor tenant 10 — read-only Emmett outbound readiness probe.
 *
 * Railway: cd /app && node scripts/probeAnchorEmmettOutboundReadiness.js --confirm-production
 *
 * SELECT + Brevo GET only. Never sends mail, never approves, never executes,
 * never mutates autosend or enabled_agents.
 */

require('dotenv').config();

const pool = require('../db');
const { evaluateCanonicalSenderReadiness } = require('../utils/canonicalSenderIdentity');
const { getBrevoState } = require('../utils/sendingReadiness');
const { validateProspectMessageBindings } = require('../packages/acquisition-mission/ExecutionApproval');
const { unwrapContributionPayload } = require('./validateAnchorCanonicalMission');
const {
  isSupersededContribution,
  activeCapacityPointer,
  selectActiveCapacityContribution,
  loadUsableReadyCapacity,
} = require('./lib/activeCapacitySelection');

const TENANT_ID = '10';
const CLIENT_ID = 10;

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
  Refuses without --confirm-production.
  Read-only. Never APPROVE_EXECUTION, EXECUTE_OUTBOUND, or POST /v3/smtp/email.
`);
}

function assertRuntimeEnv() {
  if (!process.env.DATABASE_URL) {
    const err = new Error('Missing required runtime env: DATABASE_URL');
    err.code = 'runtime_env_missing';
    throw err;
  }
}

function isEmailBearingSendable(item) {
  if (!item) return false;
  if (item.sendable === false || item.dnc === true) return false;
  return Boolean(String(item.email || '').trim());
}

function inspectSpec212(emmettPayload) {
  const body = unwrapContributionPayload(emmettPayload) || {};
  const queueItems = Array.isArray(body.queue?.items) ? body.queue.items : [];
  const validation = validateProspectMessageBindings(body);

  let sendableCount = 0;
  let emailBearingSendableCount = 0;
  let missingRecipientEmailCount = 0;

  for (const item of queueItems) {
    if (!item) continue;
    if (item.sendable === false || item.dnc === true) continue;
    sendableCount += 1;
    if (Boolean(String(item.email || '').trim())) {
      emailBearingSendableCount += 1;
    } else {
      missingRecipientEmailCount += 1;
    }
  }

  return {
    queueCount: queueItems.length,
    sendableCount,
    emailBearingSendableCount,
    missingRecipientEmailCount,
    valid: validation.valid === true,
    blocker: validation.valid === true
      ? null
      : (validation.blockerReason || validation.result || 'message_binding_invalid'),
  };
}

function firstBlockerOf({
  missionId,
  capacityContributionId,
  spec212,
  senderReadiness,
}) {
  if (!missionId) {
    return 'ready_mission_missing';
  }
  if (!capacityContributionId) {
    return 'capacity_missing';
  }
  if (spec212.valid !== true) {
    return 'tme_message_binding_contamination';
  }
  if (!spec212.queueCount) {
    return 'empty_capacity_queue';
  }
  if (!spec212.emailBearingSendableCount) {
    return 'no_sendable_recipient_email';
  }
  if (senderReadiness.sendable !== true) {
    return senderReadiness.code || 'canonical_sender_not_ready';
  }
  return null;
}

function verdictOf(firstBlocker) {
  if (firstBlocker === 'ready_mission_missing') {
    return 'No READY Anchor mission with Emmett CAPACITY. Do not execute outbound.';
  }
  if (firstBlocker === 'capacity_missing') {
    return 'READY mission has no persisted CAPACITY contribution. Re-run GENERATE_CAPACITY.';
  }
  if (firstBlocker === 'tme_message_binding_contamination') {
    return 'Persisted CAPACITY failed SPEC-212. Do not execute outbound.';
  }
  if (firstBlocker === 'empty_capacity_queue') {
    return 'CAPACITY queue is empty. Do not execute outbound.';
  }
  if (firstBlocker === 'no_sendable_recipient_email') {
    return 'CAPACITY has no email-bearing sendable queue item. Run mission-bound enrichment and regenerate CAPACITY. Do not execute outbound.';
  }
  if (firstBlocker) {
    return 'Canonical sender is not ready. Do not execute outbound.';
  }
  return 'SPEC-212 and sender readiness passed. Operator APPROVE_EXECUTION then EXECUTE_OUTBOUND still required. Autosend stays off.';
}

function buildReport({
  missionId = null,
  capacityContributionId = null,
  spec212 = {
    valid: false,
    blocker: 'not_inspected',
    queueCount: 0,
    sendableCount: 0,
    emailBearingSendableCount: 0,
    missingRecipientEmailCount: 0,
  },
  senderReadiness = { sendable: false, blocker: 'not_inspected', code: null },
  brevo = {
    keyPresent: false,
    domainVerified: false,
    domainAuthenticated: false,
    senderActive: false,
  },
  autosendEnabled = false,
  enabledAgents = [],
} = {}) {
  const firstBlocker = firstBlockerOf({
    missionId,
    capacityContributionId,
    spec212,
    senderReadiness,
  });
  return {
    missionId,
    capacityContributionId,
    spec212: {
      valid: spec212.valid === true,
      blocker: spec212.valid === true ? null : (spec212.blocker || null),
      queueCount: spec212.queueCount ?? 0,
      sendableCount: spec212.sendableCount ?? 0,
      emailBearingSendableCount: spec212.emailBearingSendableCount ?? 0,
      missingRecipientEmailCount: spec212.missingRecipientEmailCount ?? 0,
    },
    senderReadiness: {
      sendable: senderReadiness.sendable === true,
      blocker: senderReadiness.sendable === true ? null : (senderReadiness.blocker || null),
    },
    brevo: {
      keyPresent: brevo.keyPresent === true,
      domainVerified: brevo.domainVerified === true,
      domainAuthenticated: brevo.domainAuthenticated === true,
      senderActive: brevo.senderActive === true,
    },
    autosendEnabled: autosendEnabled === true,
    enabledAgents: Array.isArray(enabledAgents) ? enabledAgents : [],
    firstBlocker,
    verdict: verdictOf(firstBlocker),
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

  const db = options.pool || pool;
  const clientResult = await db.query(
    `SELECT id, sender_email, sender_name, sending_domain,
            enabled_agents, autosend_enabled, active
       FROM clients
      WHERE id = $1`,
    [CLIENT_ID]
  );
  const client = clientResult.rows[0] || null;

  const brevoState = await getBrevoState(client || {
    sender_email: 'jacob@goanchorcleaning.com',
    sending_domain: 'goanchorcleaning.com',
  }, {
    brevoApiKey: options.brevoApiKey,
    http: options.http,
  });

  const readiness = await evaluateCanonicalSenderReadiness({
    tenantId: TENANT_ID,
    clientId: CLIENT_ID,
    client,
    brevoState,
    brevoApiKey: options.brevoApiKey,
    http: options.http,
  });

  const usable = await loadUsableReadyCapacity(db, TENANT_ID);
  const spec212 = usable
    ? inspectSpec212(usable.payload)
    : {
      valid: false,
      blocker: 'capacity_not_inspected',
      queueCount: 0,
      sendableCount: 0,
      emailBearingSendableCount: 0,
      missingRecipientEmailCount: 0,
    };

  return buildReport({
    missionId: usable?.mission_id || null,
    capacityContributionId: usable?.capacity_id || null,
    spec212,
    senderReadiness: {
      sendable: readiness.sendable === true,
      blocker: readiness.blockReason || null,
      code: readiness.code || null,
    },
    brevo: {
      keyPresent: Boolean(options.brevoApiKey || process.env.BREVO_API_KEY),
      domainVerified: brevoState.domain?.verified === true,
      domainAuthenticated: brevoState.domain?.authenticated === true,
      senderActive: brevoState.sender?.active === true,
    },
    autosendEnabled: client?.autosend_enabled === true,
    enabledAgents: client?.enabled_agents || [],
  });
}

module.exports = {
  TENANT_ID,
  parseArgs,
  isEmailBearingSendable,
  inspectSpec212,
  firstBlockerOf,
  buildReport,
  isSupersededContribution,
  activeCapacityPointer,
  selectActiveCapacityContribution,
  loadUsableReadyCapacity,
  run,
};

if (require.main === module) {
  const options = parseArgs();
  run(options)
    .then((report) => {
      if (report.help) return;
      console.log(JSON.stringify(report, null, 2));
      process.exitCode = report.firstBlocker ? 2 : 0;
    })
    .catch((err) => {
      console.log(JSON.stringify({
        error: { code: err.code || null, message: err.message },
        firstBlocker: err.code || 'probe_failed',
        verdict: err.message,
      }, null, 2));
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}
