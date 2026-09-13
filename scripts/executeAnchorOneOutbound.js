#!/usr/bin/env node
'use strict';

/**
 * Anchor tenant 10 — one controlled production outbound send.
 *
 * Canonical path:
 *   probe (abort if not green)
 *   → APPROVE_EXECUTION
 *   → EXECUTE_OUTBOUND { maxSends: 1 }
 *   → replay EXECUTE_OUTBOUND (must not send again)
 *
 * Railway:
 *   node scripts/executeAnchorOneOutbound.js --confirm-production
 *
 * Never enables autosend. Never changes enabled_agents. Never uses fixtures.
 */

require('dotenv').config();

const amo = require('../packages/acquisition-mission');
const {
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  EXECUTION_RECORD_STATUS,
  createExecutionRequest,
  routeExecutionRequest,
  findValidExecutionApproval,
  validateProspectMessageBindings,
} = amo;
const pool = require('../db');
const { getAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
const {
  listOutboundExecutionsForMission,
} = require('../services/acquisitionMissionOutboundPersistence');
const { unwrapContributionPayload } = require('./validateAnchorCanonicalMission');
const probe = require('./probeAnchorEmmettOutboundReadiness');
const { sendEmail: brevoSendEmail } = require('../packages/providers/brevo/sendEmail');

const TENANT_ID = '10';
const CLIENT_ID = 10;
const MISSION_ID = 'mission_ad7753b0-6def-441d-bb1a-3764656f5750';
const EXPECTED_CAPACITY_ID = 'contrib_55e11312-3837-4485-b95a-58134dcd7601';
const OPERATOR_ID = 'anchor-one-outbound';

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const help = argv.includes('--help') || argv.includes('-h');
  const unknown = argv.filter(
    (arg) => arg !== '--confirm-production' && arg !== '--help' && arg !== '-h'
  );
  if (unknown.length) {
    throw new Error(
      `Unknown argument(s): ${unknown.join(', ')}. Usage: node scripts/executeAnchorOneOutbound.js --confirm-production`
    );
  }
  return { confirmProduction, help };
}

function printUsage() {
  console.log(`Anchor one-item outbound (tenant ${TENANT_ID})

Usage:
  node scripts/executeAnchorOneOutbound.js --confirm-production

Canonical path:
  probe → APPROVE_EXECUTION → EXECUTE_OUTBOUND (maxSends=1) → replay EXECUTE_OUTBOUND

Safety:
  Refuses without --confirm-production.
  Aborts if the readiness probe is not green.
  Sends one queue item only. Never a batch.
  Never ENABLE autosend. Never changes enabled_agents.
  Never ALLOW_FIXTURE_FALLBACK.
`);
}

function assertRuntimeEnv() {
  if (!process.env.DATABASE_URL) {
    const err = new Error('Missing required runtime env: DATABASE_URL');
    err.code = 'runtime_env_missing';
    throw err;
  }
  if (!process.env.BREVO_API_KEY) {
    const err = new Error('Missing required runtime env: BREVO_API_KEY');
    err.code = 'runtime_env_missing';
    throw err;
  }
  if (
    process.env.ALLOW_FIXTURE_FALLBACK === 'true'
    || process.env.allowFixtureFallback === 'true'
  ) {
    const err = new Error('Refusing to run with ALLOW_FIXTURE_FALLBACK enabled.');
    err.code = 'fixture_fallback_env';
    throw err;
  }
}

function abortIfNotGreen(report, label, baseline = null) {
  if (report.firstBlocker) {
    const err = new Error(`Pre-send check changed from green at ${label}: ${report.firstBlocker}`);
    err.code = 'pre_send_not_green';
    err.probe = report;
    throw err;
  }
  if (report.autosendEnabled === true) {
    const err = new Error(`Autosend is enabled at ${label}. Refusing to send.`);
    err.code = 'autosend_enabled';
    throw err;
  }
  if (baseline && baseline.enabledAgentsBefore != null) {
    const current = JSON.stringify(
      Array.isArray(report.enabledAgents) ? [...report.enabledAgents].sort() : []
    );
    if (current !== baseline.enabledAgentsBefore) {
      const err = new Error(`enabled_agents changed at ${label}. Refusing to send.`);
      err.code = 'enabled_agents_changed';
      throw err;
    }
  }
}

function activeCapacityRow(contributions) {
  const rows = (contributions || []).filter(
    (row) => row.specialist === SPECIALISTS.EMMETT
      && row.kind === CONTRIBUTION_KINDS.CAPACITY
      && row.payload?.superseded !== true
  );
  return rows.at(-1) || null;
}

function activePaigeRow(contributions) {
  const rows = (contributions || []).filter(
    (row) => row.specialist === SPECIALISTS.PAIGE
      && row.kind === CONTRIBUTION_KINDS.VARIANTS
      && row.payload?.superseded !== true
  );
  return rows.at(-1) || null;
}

function sendableQueueItems(capacityPayload) {
  const body = unwrapContributionPayload(capacityPayload) || {};
  const items = Array.isArray(body.queue?.items) ? body.queue.items : [];
  return items.filter((item) => {
    if (!item) return false;
    if (item.sendable === false || item.dnc === true) return false;
    const email = String(item.email || '').trim();
    return Boolean(email);
  });
}

function createProviderCounter(inner) {
  const calls = [];
  const sendEmail = async (input) => {
    calls.push({
      toEmail: input.toEmail,
      toName: input.toName,
      subject: input.subject,
      idempotencyKey: input.idempotencyKey,
      at: new Date().toISOString(),
    });
    return inner(input);
  };
  return { sendEmail, calls };
}

async function routeIntent({
  runtime,
  engine,
  missionId,
  intent,
  question,
  sendEmail,
  maxSends,
  prospectId,
}) {
  const mission = engine.get(missionId, TENANT_ID);
  const request = createExecutionRequest({
    source: EXECUTION_SOURCES.API,
    intent,
    missionId,
    mission,
    operatorId: OPERATOR_ID,
    stage: mission.stage,
    question,
    permissions: { canExecute: true, role: 'operator' },
    payload: {
      question,
      ...(maxSends != null ? { maxSends } : {}),
      ...(prospectId ? { prospectId } : {}),
    },
  });
  const routed = await routeExecutionRequest(request, {
    engine,
    tenantId: TENANT_ID,
    operatorId: OPERATOR_ID,
    ...runtime.persistOpts({ persist: true }),
    sendEmail,
    maxSends,
    prospectId: prospectId || null,
    requireProviderReadiness: true,
    allowFixtureFallback: false,
  });
  return { request, routed };
}

async function loadDurableRecords(engine, missionId) {
  const rows = await listOutboundExecutionsForMission(missionId, pool);
  for (const row of rows) {
    if (row && engine.store.addExecutionRecord) engine.store.addExecutionRecord(row);
  }
  return rows;
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

  const probeBefore = await probe.run({ confirmProduction: true, pool });
  abortIfNotGreen(probeBefore, 'initial probe');
  const enabledAgentsBefore = JSON.stringify(
    Array.isArray(probeBefore.enabledAgents) ? [...probeBefore.enabledAgents].sort() : []
  );
  if (probeBefore.missionId !== MISSION_ID) {
    const err = new Error(`Probe selected ${probeBefore.missionId}, expected ${MISSION_ID}.`);
    err.code = 'mission_mismatch';
    throw err;
  }
  if (probeBefore.capacityContributionId !== EXPECTED_CAPACITY_ID) {
    const err = new Error(
      `Probe selected ${probeBefore.capacityContributionId}, expected ${EXPECTED_CAPACITY_ID}.`
    );
    err.code = 'capacity_mismatch';
    throw err;
  }

  const runtime = getAcquisitionMissionRuntime({ production: true, persist: true, pool });
  await runtime.hydrate(TENANT_ID, { pool, production: true });
  const engine = runtime.engine();
  const mission = engine.get(MISSION_ID, TENANT_ID);
  if (!mission) {
    const err = new Error(`Mission ${MISSION_ID} not found for tenant ${TENANT_ID}.`);
    err.code = 'mission_not_found';
    throw err;
  }
  if (mission.stage !== STAGES.READY && mission.stage !== STAGES.EXECUTE) {
    const err = new Error(`Mission ${MISSION_ID} is at stage ${mission.stage}; execute requires READY or EXECUTE.`);
    err.code = 'tme_wrong_stage';
    throw err;
  }

  const snapshot = engine.inspect(MISSION_ID, { tenantId: TENANT_ID });
  const capacity = activeCapacityRow(snapshot.contributions || []);
  const paige = activePaigeRow(snapshot.contributions || []);
  if (!capacity || capacity.id !== EXPECTED_CAPACITY_ID) {
    const err = new Error(
      `Active CAPACITY is ${capacity?.id || 'missing'}, expected ${EXPECTED_CAPACITY_ID}.`
    );
    err.code = 'capacity_mismatch';
    throw err;
  }
  const spec212 = validateProspectMessageBindings(unwrapContributionPayload(capacity.payload) || {});
  if (spec212.valid !== true) {
    const err = new Error(spec212.blockerReason || 'SPEC-212 failed on active CAPACITY.');
    err.code = 'tme_message_binding_contamination';
    throw err;
  }

  const sendable = sendableQueueItems(capacity.payload);
  if (!sendable.length) {
    const err = new Error('Active CAPACITY has no sendable queue item.');
    err.code = 'empty_capacity_queue';
    throw err;
  }
  const selectedItem = sendable[0];
  const selectedProspectId = String(selectedItem.prospectId || selectedItem.id || '').trim();
  if (!selectedProspectId) {
    const err = new Error('First sendable queue item is missing prospectId.');
    err.code = 'empty_capacity_queue';
    throw err;
  }
  const agentsBaseline = { enabledAgentsBefore };

  const durableBefore = await loadDurableRecords(engine, MISSION_ID);
  const alreadySent = durableBefore.filter((row) => row.status === EXECUTION_RECORD_STATUS.SENT);
  if (alreadySent.length) {
    const err = new Error(
      `Mission already has ${alreadySent.length} SENT outbound row(s). Refusing a second production send.`
    );
    err.code = 'already_sent';
    err.sent = alreadySent.map((row) => ({
      id: row.id,
      prospectId: row.prospectId,
      providerMessageId: row.providerMessageId,
    }));
    throw err;
  }

  const probeAfterHydrate = await probe.run({ confirmProduction: true, pool });
  abortIfNotGreen(probeAfterHydrate, 'post-hydrate probe', agentsBaseline);

  const approvalStep = await routeIntent({
    runtime,
    engine,
    missionId: MISSION_ID,
    intent: EXECUTION_INTENTS.APPROVE_EXECUTION,
    question: 'Authorize one controlled outbound send. Do not enable autosend.',
  });
  if (approvalStep.routed.executionResult?.rolledBack === true) {
    const err = new Error(
      approvalStep.routed.executionResult.error?.message || 'APPROVE_EXECUTION rolled back.'
    );
    err.code = approvalStep.routed.executionResult.error?.code || 'tme_persistence';
    throw err;
  }

  const afterApproval = engine.inspect(MISSION_ID, { tenantId: TENANT_ID });
  const approval = findValidExecutionApproval(afterApproval.contributions || [], MISSION_ID);
  if (!approval) {
    const err = new Error('Fresh APPROVE_EXECUTION did not produce a valid execution approval.');
    err.code = 'tme_execution_not_approved';
    throw err;
  }
  if (
    approval.payload?.emmettContributionId
    && approval.payload.emmettContributionId !== EXPECTED_CAPACITY_ID
  ) {
    const err = new Error(
      `Approval bound CAPACITY ${approval.payload.emmettContributionId}, expected ${EXPECTED_CAPACITY_ID}.`
    );
    err.code = 'capacity_mismatch';
    throw err;
  }

  const probeAfterApprove = await probe.run({ confirmProduction: true, pool });
  abortIfNotGreen(probeAfterApprove, 'post-approval probe', agentsBaseline);

  const provider = createProviderCounter(brevoSendEmail);
  const executeStep = await routeIntent({
    runtime,
    engine,
    missionId: MISSION_ID,
    intent: EXECUTION_INTENTS.EXECUTE_OUTBOUND,
    question: 'Send one approved queue item only.',
    sendEmail: provider.sendEmail,
    maxSends: 1,
    prospectId: selectedProspectId,
  });
  if (executeStep.routed.executionResult?.rolledBack === true) {
    const err = new Error(
      executeStep.routed.executionResult.error?.message || 'EXECUTE_OUTBOUND rolled back.'
    );
    err.code = executeStep.routed.executionResult.error?.code || 'tme_persistence';
    throw err;
  }

  const firstProviderCalls = provider.calls.length;
  if (firstProviderCalls !== 1) {
    const err = new Error(`Expected exactly one Brevo send, got ${firstProviderCalls}.`);
    err.code = 'batch_send_refused';
    throw err;
  }

  const executeResult = executeStep.routed.executionResult || {};
  const sentRecords = (executeResult.records || []).filter(
    (row) => row.status === EXECUTION_RECORD_STATUS.SENT
  );
  if (sentRecords.length !== 1) {
    const err = new Error(
      `Expected exactly one SENT record, got ${sentRecords.length} (summary=${JSON.stringify(executeResult.summary || null)}).`
    );
    err.code = 'send_not_persisted';
    throw err;
  }
  const sent = sentRecords[0];
  if (String(sent.prospectId) !== selectedProspectId) {
    const err = new Error(
      `Sent prospect ${sent.prospectId} does not match pinned queue item ${selectedProspectId}.`
    );
    err.code = 'prospect_mismatch';
    throw err;
  }

  const durableAfterSend = await listOutboundExecutionsForMission(MISSION_ID, pool);
  const durableSent = durableAfterSend.filter((row) => row.status === EXECUTION_RECORD_STATUS.SENT);
  if (durableSent.length !== 1 || durableSent[0].id !== sent.id) {
    const err = new Error('SENT record is not durably persisted as a single outbound execution row.');
    err.code = 'tme_persistence_verify';
    throw err;
  }

  await loadDurableRecords(engine, MISSION_ID);
  const replayStep = await routeIntent({
    runtime,
    engine,
    missionId: MISSION_ID,
    intent: EXECUTION_INTENTS.EXECUTE_OUTBOUND,
    question: 'Replay the same outbound intent. Must not send again.',
    sendEmail: provider.sendEmail,
    maxSends: 1,
    prospectId: selectedProspectId,
  });
  const replayProviderCalls = provider.calls.length - firstProviderCalls;
  const replayRecords = replayStep.routed.executionResult?.records || [];
  const replayDeduped = replayRecords.filter((row) => row.deduplicated === true);
  const durableAfterReplay = await listOutboundExecutionsForMission(MISSION_ID, pool);
  const durableSentAfterReplay = durableAfterReplay.filter(
    (row) => row.status === EXECUTION_RECORD_STATUS.SENT
  );

  const paigePayload = unwrapContributionPayload(paige?.payload || {}) || {};
  const sentItem = sendable.find((item) => String(item.prospectId || item.id) === String(sent.prospectId))
    || sendable[0];
  const variant = Array.isArray(paigePayload.variants)
    ? paigePayload.variants.find((row) => String(row.candidateId) === String(sentItem?.paige?.candidateId))
      || paigePayload.variants.find((row) => String(row.label) === String(sentItem?.paige?.variantLabel))
    : null;

  const verdict = (
    firstProviderCalls === 1
    && sent.status === EXECUTION_RECORD_STATUS.SENT
    && sent.providerMessageId
    && durableSent.length === 1
    && replayProviderCalls === 0
    && durableSentAfterReplay.length === 1
    && probeAfterApprove.autosendEnabled !== true
  )
    ? 'one real outbound message sent exactly once; identical replay suppressed'
    : 'controlled send did not meet the success condition';

  return {
    tenantId: TENANT_ID,
    clientId: CLIENT_ID,
    missionId: MISSION_ID,
    capacityContributionId: EXPECTED_CAPACITY_ID,
    approvalId: approval.id,
    executionTransactionId: executeResult.transactionId || executeStep.routed.audit?.transactionId || null,
    executionRequestId: executeStep.request.id,
    prospect: {
      prospectId: sent.prospectId,
      companyId: sentItem?.companyId || sentItem?.company || null,
      company: sentItem?.company || sentItem?.name || null,
    },
    recipient: sent.payload?.email || sentItem?.email || provider.calls[0]?.toEmail || null,
    messageBinding: {
      paigeContributionId: paige?.id || approval.payload?.paigeContributionId || null,
      emmettContributionId: EXPECTED_CAPACITY_ID,
      candidateId: sentItem?.paige?.candidateId || null,
      variantId: sentItem?.paige?.variantId || variant?.id || null,
      variantLabel: sentItem?.paige?.variantLabel || variant?.label || null,
      bindingScope: sentItem?.paige?.bindingScope || null,
      source: sentItem?.paige?.source || 'paige',
    },
    brevoMessageId: sent.providerMessageId,
    persistedSendStatus: durableSent[0].status,
    persistedExecutionId: durableSent[0].id,
    idempotency: {
      executionIdentity: sent.executionIdentity,
      idempotencyKey: sent.idempotencyKey,
      providerCalls: firstProviderCalls,
    },
    duplicateReplay: {
      intent: EXECUTION_INTENTS.EXECUTE_OUTBOUND,
      rolledBack: replayStep.routed.executionResult?.rolledBack === true,
      additionalProviderCalls: replayProviderCalls,
      deduplicatedRecords: replayDeduped.length,
      durableSentCount: durableSentAfterReplay.length,
      suppressed: replayProviderCalls === 0 && durableSentAfterReplay.length === 1,
    },
    autosendEnabled: probeAfterApprove.autosendEnabled === true,
    enabledAgents: probeAfterApprove.enabledAgents || [],
    spec212: {
      valid: spec212.valid === true,
      violationCount: Array.isArray(spec212.violations) ? spec212.violations.length : 0,
    },
    verdict,
    completedAt: new Date().toISOString(),
  };
}

module.exports = {
  TENANT_ID,
  MISSION_ID,
  EXPECTED_CAPACITY_ID,
  parseArgs,
  run,
  sendableQueueItems,
};

if (require.main === module) {
  const options = parseArgs();
  run(options)
    .then((report) => {
      if (report.help) return;
      console.log(JSON.stringify(report, null, 2));
      process.exitCode = report.verdict && report.verdict.startsWith('one real') ? 0 : 2;
    })
    .catch((err) => {
      console.log(JSON.stringify({
        error: { code: err.code || null, message: err.message, sent: err.sent || null },
        completedAt: new Date().toISOString(),
      }, null, 2));
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}
