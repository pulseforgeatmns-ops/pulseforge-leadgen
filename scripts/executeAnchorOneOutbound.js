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
 * Railway / production:
 *   node scripts/executeAnchorOneOutbound.js --confirm-production \
 *     --mission-id mission_82e8102f-249c-4f44-b88e-2de76b13898e
 *
 * Default --mission-id remains the conservative historical id. Production
 * execution must pass the READY STR mission explicitly.
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
  resolvePaigeVariant,
} = amo;
const pool = require('../db');
const { getAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
const {
  listOutboundExecutionsForMission,
} = require('../services/acquisitionMissionOutboundPersistence');
const { unwrapContributionPayload } = require('./validateAnchorCanonicalMission');
const probe = require('./probeAnchorEmmettOutboundReadiness');
const { sendEmail: brevoSendEmail } = require('../packages/providers/brevo/sendEmail');
const {
  loadActiveCapacityForMission,
  selectActiveCapacityContribution,
  loadCapacityRowsForMission,
  unwrapMissionPayload,
} = require('./lib/activeCapacitySelection');

const TENANT_ID = '10';
const CLIENT_ID = 10;
const DEFAULT_MISSION_ID = 'mission_ad7753b0-6def-441d-bb1a-3764656f5750';
const MISSION_ID = DEFAULT_MISSION_ID;
const OPERATOR_ID = 'anchor-one-outbound';

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const help = argv.includes('--help') || argv.includes('-h');
  const missionIdx = argv.indexOf('--mission-id');
  const missionId = missionIdx >= 0
    ? String(argv[missionIdx + 1] || '').trim()
    : DEFAULT_MISSION_ID;
  const unknown = argv.filter(
    (arg, i) =>
      arg !== '--confirm-production'
      && arg !== '--help'
      && arg !== '-h'
      && arg !== '--mission-id'
      && (missionIdx < 0 || i !== missionIdx + 1)
  );
  if (unknown.length) {
    throw new Error(
      `Unknown argument(s): ${unknown.join(', ')}. Usage: node scripts/executeAnchorOneOutbound.js --confirm-production [--mission-id <id>]`
    );
  }
  if (!missionId || missionId.startsWith('--')) {
    const err = new Error('--mission-id requires a mission id value.');
    err.code = 'mission_id_required';
    throw err;
  }
  return { confirmProduction, help, missionId };
}

function printUsage() {
  console.log(`Anchor one-item outbound (tenant ${TENANT_ID})

Usage:
  node scripts/executeAnchorOneOutbound.js --confirm-production [--mission-id <id>]

Production (READY STR mission):
  node scripts/executeAnchorOneOutbound.js --confirm-production \\
    --mission-id mission_82e8102f-249c-4f44-b88e-2de76b13898e

Canonical path:
  probe → APPROVE_EXECUTION → EXECUTE_OUTBOUND (maxSends=1) → replay EXECUTE_OUTBOUND

Safety:
  Refuses without --confirm-production.
  Requires the selected mission to belong to tenant ${TENANT_ID} and stage READY.
  Requires the selected mission to match the readiness probe.
  Aborts if the readiness probe is not green.
  Sends one queue item only. Never a batch.
  Never ENABLE autosend. Never changes enabled_agents.
  Never ALLOW_FIXTURE_FALLBACK.
`);
}

function resolveMissionId(options = {}) {
  const missionId = String(options.missionId || DEFAULT_MISSION_ID).trim();
  if (!missionId || missionId.startsWith('--')) {
    const err = new Error('--mission-id requires a mission id value.');
    err.code = 'mission_id_required';
    throw err;
  }
  return missionId;
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

function assertCapacitySelectionMatch({ expected, actual, context }) {
  if (String(expected) !== String(actual)) {
    const err = new Error(`${context}: expected ${expected}, got ${actual}.`);
    err.code = 'capacity_mismatch';
    throw err;
  }
}

async function loadMissionRow(db, missionId) {
  const result = await db.query(
    `SELECT id, tenant_id, stage, payload
       FROM acquisition_missions
      WHERE id = $1
      LIMIT 1`,
    [missionId]
  );
  return result.rows[0] || null;
}

function assertMissionTenant(row, tenantId = TENANT_ID) {
  if (!row) {
    const err = new Error('Mission not found.');
    err.code = 'mission_not_found';
    throw err;
  }
  if (String(row.tenant_id ?? row.tenantId ?? '') !== String(tenantId)) {
    const err = new Error(
      `Mission ${row.id} belongs to tenant ${row.tenant_id ?? row.tenantId}, expected ${tenantId}.`
    );
    err.code = 'wrong_tenant';
    throw err;
  }
  return row;
}

function assertMissionReady(row) {
  const stage = String(row?.stage || '').toLowerCase();
  if (stage !== STAGES.READY) {
    const err = new Error(
      `Mission ${row?.id} is at stage ${row?.stage}; execute requires READY.`
    );
    err.code = 'tme_wrong_stage';
    throw err;
  }
  return row;
}

function assertProbeMissionMatch(probeReport, missionId) {
  if (String(probeReport?.missionId || '') !== String(missionId)) {
    const err = new Error(
      `Probe selected ${probeReport?.missionId}, expected ${missionId}.`
    );
    err.code = 'mission_mismatch';
    throw err;
  }
  return probeReport;
}

async function resolveCanonicalActiveCapacity(db, tenantId, missionId) {
  const active = await loadActiveCapacityForMission(db, tenantId, missionId);
  if (!active?.capacity_id) {
    const err = new Error(`No active non-superseded CAPACITY for mission ${missionId}.`);
    err.code = 'capacity_not_found';
    throw err;
  }
  return active;
}

async function verifyEngineCapacityMatches(db, engine, tenantId, missionId, expectedCapacityId) {
  const snapshot = engine.inspect(missionId, { tenantId });
  const capacityRows = await loadCapacityRowsForMission(db, tenantId, missionId);
  const missionBody = unwrapMissionPayload(snapshot.mission);
  const selected = selectActiveCapacityContribution(missionBody, capacityRows);
  if (!selected?.capacity_id) {
    const err = new Error('Engine could not select active CAPACITY.');
    err.code = 'capacity_not_found';
    throw err;
  }
  assertCapacitySelectionMatch({
    expected: expectedCapacityId,
    actual: selected.capacity_id,
    context: 'Engine CAPACITY selection vs canonical durable state',
  });
  return selected;
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

function canonicalQueueRank(item) {
  const position = Number(item?.position);
  const total = Number(item?.ranking?.total);
  const priority = Number(item?.maxPriority ?? item?.priority);
  return {
    position: Number.isFinite(position) ? position : Number.POSITIVE_INFINITY,
    total: Number.isFinite(total) ? total : (Number.isFinite(priority) ? priority : 0),
  };
}

function compareCanonicalQueueOrder(a, b) {
  const ra = canonicalQueueRank(a);
  const rb = canonicalQueueRank(b);
  if (ra.position !== rb.position) return ra.position - rb.position;
  if (ra.total !== rb.total) return rb.total - ra.total;
  return 0;
}

function selectHighestRankedSendable(capacityPayload) {
  const sendable = sendableQueueItems(capacityPayload);
  if (!sendable.length) {
    const err = new Error('Active CAPACITY has no sendable email-bearing queue item.');
    err.code = 'empty_capacity_queue';
    throw err;
  }
  const ranked = [...sendable].sort(compareCanonicalQueueOrder);
  return ranked[0];
}

function resolveSelectedCopy(item, paigePayload) {
  const fromItemSubject = String(item?.paige?.subject || '').trim();
  const fromItemBody = String(item?.paige?.body || '').trim();
  if (fromItemSubject && fromItemBody) {
    return {
      variantLabel: item.paige?.variantLabel || 'Primary',
      subject: fromItemSubject,
      body: fromItemBody,
      candidateId: item.paige?.candidateId || item.candidateId || item.id || null,
    };
  }
  return resolvePaigeVariant(paigePayload || {}, {
    variantLabel: item?.paige?.variantLabel || 'Primary',
    candidateId: item?.paige?.candidateId || item?.candidateId || item?.id,
    companyId: item?.companyId,
    placeId: item?.placeId,
    id: item?.id,
  });
}

function buildPreSendPreview(item, paigePayload) {
  const copy = resolveSelectedCopy(item, paigePayload);
  const subject = String(copy?.subject || '').trim();
  const body = String(copy?.body || '').trim();
  if (!subject || !body) {
    const err = new Error('Could not resolve Paige subject/body for the selected queue item.');
    err.code = 'copy_not_resolved';
    throw err;
  }
  return {
    recipient: String(item?.email || '').trim() || null,
    company: item?.company || item?.name || null,
    subject,
    body,
    prospectId: String(item?.prospectId || item?.id || '').trim() || null,
    candidateId: item?.paige?.candidateId || item?.candidateId || item?.id || copy?.candidateId || null,
  };
}

function printPreSendPreview(preview, log = console.log) {
  log(JSON.stringify({ preSend: preview }, null, 2));
  return preview;
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

async function loadDurableRecords(engine, missionId, listExecutions, db = pool) {
  const rows = await listExecutions(missionId, db);
  for (const row of rows) {
    if (row && engine.store.addExecutionRecord) engine.store.addExecutionRecord(row);
  }
  return rows;
}

async function prepareControlledSend(options = {}, deps = {}) {
  const missionId = resolveMissionId(options);
  const db = deps.db || pool;
  const loadRow = deps.loadMissionRow || loadMissionRow;
  const probeRun = deps.probeRun || ((opts) => probe.run(opts));
  const loadCapacity = deps.loadActiveCapacity || resolveCanonicalActiveCapacity;

  const missionRow = await loadRow(db, missionId);
  assertMissionTenant(missionRow, TENANT_ID);
  assertMissionReady(missionRow);

  const probeBefore = await probeRun({ confirmProduction: true, pool: db, missionId });
  abortIfNotGreen(probeBefore, 'initial probe');
  assertProbeMissionMatch(probeBefore, missionId);

  const enabledAgentsBefore = JSON.stringify(
    Array.isArray(probeBefore.enabledAgents) ? [...probeBefore.enabledAgents].sort() : []
  );

  const durableActive = await loadCapacity(db, TENANT_ID, missionId);
  const canonicalCapacityId = durableActive.capacity_id;
  assertCapacitySelectionMatch({
    expected: canonicalCapacityId,
    actual: probeBefore.capacityContributionId,
    context: 'Probe CAPACITY selection vs canonical durable state',
  });

  const spec212 = validateProspectMessageBindings(
    unwrapContributionPayload(durableActive.payload) || {}
  );
  if (spec212.valid !== true) {
    const err = new Error(spec212.blockerReason || 'SPEC-212 failed on active CAPACITY.');
    err.code = 'tme_message_binding_contamination';
    throw err;
  }

  const sendable = sendableQueueItems(durableActive.payload);
  if (!sendable.length) {
    const err = new Error('Active CAPACITY has no sendable email-bearing queue item.');
    err.code = 'empty_capacity_queue';
    throw err;
  }
  const selectedItem = selectHighestRankedSendable(durableActive.payload);
  const selectedProspectId = String(selectedItem.prospectId || selectedItem.id || '').trim();
  if (!selectedProspectId) {
    const err = new Error('Highest-ranked sendable queue item is missing prospectId.');
    err.code = 'empty_capacity_queue';
    throw err;
  }

  return {
    missionId,
    missionRow,
    probeBefore,
    enabledAgentsBefore,
    durableActive,
    canonicalCapacityId,
    spec212,
    sendable,
    selectedItem,
    selectedProspectId,
  };
}

async function run(options = {}, deps = {}) {
  if (options.help) {
    printUsage();
    return { help: true };
  }
  if (!options.confirmProduction) {
    const err = new Error('Refusing to run without --confirm-production.');
    err.code = 'confirm_production_required';
    throw err;
  }

  const checkEnv = deps.assertRuntimeEnv || assertRuntimeEnv;
  checkEnv();

  const prepared = await prepareControlledSend(options, deps);
  if (deps.dryPrepare) return prepared;

  const {
    missionId,
    probeBefore,
    enabledAgentsBefore,
    durableActive,
    canonicalCapacityId,
    spec212,
    sendable,
    selectedItem,
    selectedProspectId,
  } = prepared;
  const db = deps.db || pool;
  const probeRun = deps.probeRun || ((opts) => probe.run(opts));
  const getRuntime = deps.getRuntime || getAcquisitionMissionRuntime;
  const verifyCapacity = deps.verifyEngineCapacityMatches || verifyEngineCapacityMatches;
  const listExecutions = deps.listExecutions || listOutboundExecutionsForMission;
  const findApproval = deps.findValidExecutionApproval || findValidExecutionApproval;
  const route = deps.routeIntent || routeIntent;
  const sendEmailImpl = deps.sendEmail || brevoSendEmail;
  const log = deps.log || console.log;

  const runtime = getRuntime({ production: true, persist: true, pool: db });
  await runtime.hydrate(TENANT_ID, { pool: db, production: true });
  const engine = runtime.engine();
  const mission = engine.get(missionId, TENANT_ID);
  if (!mission) {
    const err = new Error(`Mission ${missionId} not found for tenant ${TENANT_ID}.`);
    err.code = 'mission_not_found';
    throw err;
  }
  assertMissionTenant({ id: mission.id, tenant_id: mission.tenantId }, TENANT_ID);
  assertMissionReady(mission);

  await verifyCapacity(db, engine, TENANT_ID, missionId, canonicalCapacityId);

  const snapshot = engine.inspect(missionId, { tenantId: TENANT_ID });
  const paige = activePaigeRow(snapshot.contributions || []);
  const paigePayload = unwrapContributionPayload(paige?.payload || {}) || {};
  const preSend = printPreSendPreview(buildPreSendPreview(selectedItem, paigePayload), log);
  const agentsBaseline = { enabledAgentsBefore };

  const durableBefore = await loadDurableRecords(engine, missionId, listExecutions, db);
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

  const probeAfterHydrate = await probeRun({ confirmProduction: true, pool: db, missionId });
  abortIfNotGreen(probeAfterHydrate, 'post-hydrate probe', agentsBaseline);
  assertProbeMissionMatch(probeAfterHydrate, missionId);
  assertCapacitySelectionMatch({
    expected: canonicalCapacityId,
    actual: probeAfterHydrate.capacityContributionId,
    context: 'Post-hydrate probe CAPACITY selection vs canonical durable state',
  });

  const approvalStep = await route({
    runtime,
    engine,
    missionId,
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

  const afterApproval = engine.inspect(missionId, { tenantId: TENANT_ID });
  const approval = findApproval(afterApproval.contributions || [], missionId);
  if (!approval) {
    const err = new Error('Fresh APPROVE_EXECUTION did not produce a valid execution approval.');
    err.code = 'tme_execution_not_approved';
    throw err;
  }
  if (approval.payload?.emmettContributionId) {
    assertCapacitySelectionMatch({
      expected: canonicalCapacityId,
      actual: approval.payload.emmettContributionId,
      context: 'Fresh APPROVE_EXECUTION artifact binding vs canonical CAPACITY',
    });
  }

  const probeAfterApprove = await probeRun({ confirmProduction: true, pool: db, missionId });
  abortIfNotGreen(probeAfterApprove, 'post-approval probe', agentsBaseline);
  assertProbeMissionMatch(probeAfterApprove, missionId);
  assertCapacitySelectionMatch({
    expected: canonicalCapacityId,
    actual: probeAfterApprove.capacityContributionId,
    context: 'Post-approval probe CAPACITY selection vs canonical durable state',
  });

  const provider = createProviderCounter(sendEmailImpl);
  const executeStep = await route({
    runtime,
    engine,
    missionId,
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

  const durableAfterSend = await listExecutions(missionId, db);
  const durableSent = durableAfterSend.filter((row) => row.status === EXECUTION_RECORD_STATUS.SENT);
  if (durableSent.length !== 1 || durableSent[0].id !== sent.id) {
    const err = new Error('SENT record is not durably persisted as a single outbound execution row.');
    err.code = 'tme_persistence_verify';
    throw err;
  }

  await loadDurableRecords(engine, missionId, listExecutions, db);
  const replayStep = await route({
    runtime,
    engine,
    missionId,
    intent: EXECUTION_INTENTS.EXECUTE_OUTBOUND,
    question: 'Replay the same outbound intent. Must not send again.',
    sendEmail: provider.sendEmail,
    maxSends: 1,
    prospectId: selectedProspectId,
  });
  const replayProviderCalls = provider.calls.length - firstProviderCalls;
  const replayRecords = replayStep.routed.executionResult?.records || [];
  const replayDeduped = replayRecords.filter((row) => row.deduplicated === true);
  const durableAfterReplay = await listExecutions(missionId, db);
  const durableSentAfterReplay = durableAfterReplay.filter(
    (row) => row.status === EXECUTION_RECORD_STATUS.SENT
  );

  const sentItem = sendable.find((item) => String(item.prospectId || item.id) === String(sent.prospectId))
    || selectedItem;
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
    missionId,
    capacityContributionId: canonicalCapacityId,
    approvalId: approval.id,
    executionTransactionId: executeResult.transactionId || executeStep.routed.audit?.transactionId || null,
    executionRequestId: executeStep.request.id,
    preSend,
    prospect: {
      prospectId: sent.prospectId,
      companyId: sentItem?.companyId || sentItem?.company || null,
      company: sentItem?.company || sentItem?.name || preSend.company,
    },
    recipient: sent.payload?.email || sentItem?.email || provider.calls[0]?.toEmail || preSend.recipient,
    subject: preSend.subject,
    body: preSend.body,
    messageBinding: {
      paigeContributionId: paige?.id || approval.payload?.paigeContributionId || null,
      emmettContributionId: canonicalCapacityId,
      candidateId: sentItem?.paige?.candidateId || preSend.candidateId || null,
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
    probeBefore: {
      missionId: probeBefore.missionId,
      firstBlocker: probeBefore.firstBlocker,
      autosendEnabled: probeBefore.autosendEnabled === true,
    },
    verdict,
    completedAt: new Date().toISOString(),
  };
}

module.exports = {
  TENANT_ID,
  DEFAULT_MISSION_ID,
  MISSION_ID,
  parseArgs,
  resolveMissionId,
  run,
  prepareControlledSend,
  sendableQueueItems,
  selectHighestRankedSendable,
  compareCanonicalQueueOrder,
  buildPreSendPreview,
  assertCapacitySelectionMatch,
  assertMissionTenant,
  assertMissionReady,
  assertProbeMissionMatch,
  resolveCanonicalActiveCapacity,
  verifyEngineCapacityMatches,
  loadMissionRow,
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
