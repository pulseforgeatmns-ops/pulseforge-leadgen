'use strict';

/**
 * Canonical EXECUTE outbound path — mission-bound bundle → provider → evidence.
 */

const {
  STAGES,
  SPECIALISTS,
  EVENT_KINDS,
  asText,
  nowIso,
} = require('./types');
const { executeMissionStage, bumpMissionVersion } = require('./TransactionalExecution');
const { planningError, preconditionError } = require('./ExecutionErrors');
const { specialistContext, canEnter, applyStageTransition } = require('./Lifecycle');
const { createEvent } = require('./Timeline');
const { assertMissionStateConsistent } = require('./PendingOperatorDecision');
const { findValidExecutionApproval } = require('./ExecutionApproval');
const { buildExecutionBundle, assertArtifactRevisionValid } = require('./ExecutionBundle');
const {
  EXECUTION_RECORD_STATUS,
  deriveExecutionIdempotencyKey,
  createExecutionRecord,
} = require('./ExecutionRecords');
const defaultBrevoProvider = require('../providers/brevo/sendEmail');

function validateOutboundPreconditions({ mission, engine, tenantId }) {
  if (!mission) throw planningError('tme_mission_missing', 'Mission does not exist.');
  if (mission.planCancelled === true || /cancelled/i.test(String(mission.status || ''))) {
    throw planningError('tme_mission_inactive', 'Mission is not active.');
  }
  if (mission.stage !== STAGES.READY && mission.stage !== STAGES.EXECUTE) {
    throw preconditionError(
      'tme_wrong_stage',
      `Outbound execution requires stage ${STAGES.READY} or ${STAGES.EXECUTE}.`
    );
  }

  const snapshot = engine.inspect(mission.id, { tenantId });
  const contributions = snapshot.contributions || [];
  const ctx = specialistContext(contributions, { missionId: mission.id });

  if (!ctx.executionApproved) {
    throw preconditionError('amo_execution_not_approved', 'Execution approval is required.');
  }
  if (ctx.deliverabilityPaused) {
    throw preconditionError('amo_deliverability_paused', 'Deliverability risk blocks execution.');
  }

  const executeGate = canEnter(STAGES.EXECUTE, { ...ctx, missionId: mission.id });
  if (!executeGate.ok) {
    throw preconditionError('amo_stage_blocked', executeGate.reason);
  }

  assertArtifactRevisionValid(mission.id, contributions);

  return {
    missionExists: true,
    missionActive: true,
    specialistAvailable: true,
    executionApproved: true,
    deliverabilityPaused: false,
  };
}

async function dispatchSendAttempts(input = {}) {
  const {
    bundle,
    store,
    provider,
    transactionId,
    executionRequestId,
    tenantId,
  } = input;

  const sendFn = provider?.sendEmail || defaultBrevoProvider.sendEmail;
  const results = [];
  const sender = bundle.provider?.senderIdentity || {};

  for (const send of bundle.sends) {
    if (send.status === 'blocked') {
      const blocked = createExecutionRecord({
        missionId: bundle.missionId,
        tenantId,
        prospectId: send.prospectId,
        companyId: send.companyId,
        preparedArtifactRevision: bundle.executionApproval.preparedArtifactRevision,
        executionApprovalContributionId: bundle.executionApproval.contributionId,
        executionRequestId,
        transactionId,
        status: EXECUTION_RECORD_STATUS.BLOCKED,
        attemptedAt: nowIso(),
        providerErrorCode: 'copy_or_recipient_blocked',
        providerErrorMessage: send.blockReason || 'Send blocked before provider.',
        queuePosition: send.queuePosition,
        payload: { email: send.email, blockReason: send.blockReason },
      });
      store.putExecutionRecord(blocked);
      results.push(blocked);
      continue;
    }

    const idempotencyKey = deriveExecutionIdempotencyKey(
      bundle.missionId,
      send.prospectId,
      bundle.executionApproval.preparedArtifactRevision
    );
    const existing = store.getExecutionRecordByKey(idempotencyKey);
    if (existing && existing.status === EXECUTION_RECORD_STATUS.SENT) {
      results.push(existing);
      continue;
    }

    const attemptedAt = nowIso();
    const pending = createExecutionRecord({
      ...(existing || {}),
      idempotencyKey,
      missionId: bundle.missionId,
      tenantId,
      prospectId: send.prospectId,
      companyId: send.companyId,
      preparedArtifactRevision: bundle.executionApproval.preparedArtifactRevision,
      executionApprovalContributionId: bundle.executionApproval.contributionId,
      executionRequestId,
      transactionId,
      status: EXECUTION_RECORD_STATUS.PENDING,
      attemptedAt,
      queuePosition: send.queuePosition,
      subjectArtifactRef: bundle.preparedArtifacts.paigeContributionId,
      queueArtifactRef: bundle.preparedArtifacts.emmettContributionId,
      payload: {
        email: send.email,
        subject: send.message?.subject,
        variantLabel: send.message?.variantLabel,
      },
    });
    store.putExecutionRecord(pending);

    const providerResult = await sendFn({
      toEmail: send.email,
      toName: send.email,
      subject: send.message.subject,
      body: send.message.body,
      tags: [
        bundle.missionId,
        send.message.variantLabel,
        `queue_${send.queuePosition}`,
      ].filter(Boolean),
      sender,
      idempotencyKey,
    });

    const finalRecord = createExecutionRecord({
      ...pending,
      status: providerResult.success
        ? EXECUTION_RECORD_STATUS.SENT
        : EXECUTION_RECORD_STATUS.FAILED,
      providerMessageId: providerResult.providerMessageId || null,
      sentAt: providerResult.sentAt || (providerResult.success ? nowIso() : null),
      attemptedAt: providerResult.attemptedAt || attemptedAt,
      providerErrorCode: providerResult.providerErrorCode || null,
      providerErrorMessage: providerResult.providerErrorMessage || null,
      updatedAt: nowIso(),
      payload: {
        ...pending.payload,
        providerResponse: providerResult.brevoResponse || null,
      },
    });
    store.putExecutionRecord(finalRecord);
    results.push(finalRecord);
  }

  return results;
}

function summarizeExecutionResults(results = []) {
  const sent = results.filter((row) => row.status === EXECUTION_RECORD_STATUS.SENT).length;
  const failed = results.filter((row) => row.status === EXECUTION_RECORD_STATUS.FAILED).length;
  const blocked = results.filter((row) => row.status === EXECUTION_RECORD_STATUS.BLOCKED).length;
  const skipped = results.filter((row) => row.status === EXECUTION_RECORD_STATUS.SKIPPED).length;
  return { sent, failed, blocked, skipped, total: results.length };
}

function commitOutboundExecutionStage({
  engine,
  mission,
  tenantId,
  output,
  transactionId,
  missionVersion,
}) {
  const missionId = mission.id;
  const { bundle, results, summary, fromStage } = output;

  let current = engine.get(missionId, tenantId);
  if (fromStage === STAGES.READY && current.stage === STAGES.READY) {
    const contributions = engine.inspect(missionId, { tenantId }).contributions || [];
    applyStageTransition(current, STAGES.EXECUTE, { contributions });
    engine.store.putMission(current);
    engine.store.addEvent(createEvent({
      missionId,
      kind: EVENT_KINDS.STAGE_TRANSITION,
      specialist: SPECIALISTS.MAX,
      label: `${STAGES.READY} → ${STAGES.EXECUTE}`,
      payload: {
        from: STAGES.READY,
        to: STAGES.EXECUTE,
        trigger: 'execute_outbound',
        transactionId,
      },
    }));
  }

  if (summary.sent > 0) {
    engine.store.addEvent(createEvent({
      missionId,
      kind: EVENT_KINDS.LAUNCHED,
      specialist: SPECIALISTS.OPERATOR,
      label: `${summary.sent} outbound send(s) launched`,
      payload: {
        transactionId,
        sent: summary.sent,
        failed: summary.failed,
        blocked: summary.blocked,
        preparedArtifactRevision: bundle.executionApproval.preparedArtifactRevision,
      },
    }));
  }

  const updated = engine.get(missionId, tenantId);
  bumpMissionVersion(updated, transactionId);
  engine.store.putMission(updated);

  engine.store.addEvent(createEvent({
    missionId,
    kind: EVENT_KINDS.EXECUTION_COMMITTED,
    specialist: SPECIALISTS.OPERATOR,
    label: 'Outbound execution committed',
    payload: {
      transactionId,
      missionVersion: updated.version,
      priorVersion: missionVersion,
      summary,
      executionApprovalContributionId: bundle.executionApproval.contributionId,
    },
  }));

  const snapshot = engine.inspect(missionId, { tenantId });
  assertMissionStateConsistent(snapshot.mission, { contributions: snapshot.contributions });

  return {
    snapshot,
    bundle,
    results,
    summary,
  };
}

/**
 * Canonical EXECUTE outbound mission dispatch.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function executeOutboundMission(input = {}) {
  const {
    engine,
    mission,
    tenantId,
    operatorId,
    executionRequest,
    provider,
    resolveEmail,
    senderIdentity,
  } = input;

  if (!engine || !mission) {
    throw new Error('engine and mission are required');
  }

  const approval = findValidExecutionApproval(
    engine.inspect(mission.id, { tenantId }).contributions || [],
    mission.id
  );

  const fromStage = mission.stage;

  const staged = await executeMissionStage({
    engine,
    missionId: mission.id,
    tenantId,
    pool: input.pool,
    specialist: SPECIALISTS.OPERATOR,
    stage: STAGES.EXECUTE,
    operatorId,
    validatePreconditions: (ctx) => validateOutboundPreconditions(ctx),
    execute: async ({ transactionId }) => {
      const snapshot = engine.inspect(mission.id, { tenantId });
      const bundle = await buildExecutionBundle({
        mission: snapshot.mission,
        contributions: snapshot.contributions,
        approval,
        resolveEmail,
        senderIdentity,
      });

      const results = await dispatchSendAttempts({
        bundle,
        store: engine.store,
        provider,
        transactionId,
        executionRequestId: executionRequest?.id || null,
        tenantId,
      });

      const summary = summarizeExecutionResults(results);
      return {
        bundle,
        results,
        summary,
        fromStage,
        missionId: mission.id,
      };
    },
    validateOutput: (output) => {
      if (!output || !output.bundle) {
        throw planningError('tme_contribution_missing', 'Execution bundle is missing.');
      }
    },
    commit: (ctx) => commitOutboundExecutionStage({
      ...ctx,
      fromStage: ctx.output.fromStage,
    }),
    persistDurable: input.persistDurable,
  });

  return {
    alreadyExecuted: false,
    snapshot: staged.commitResult.snapshot,
    bundle: staged.output.bundle,
    results: staged.output.results,
    summary: staged.output.summary,
    transactionId: staged.transactionId,
    executionOutcome: staged.output.summary.sent > 0 ? 'completed' : 'blocked',
  };
}

module.exports = {
  validateOutboundPreconditions,
  dispatchSendAttempts,
  executeOutboundMission,
  summarizeExecutionResults,
};
