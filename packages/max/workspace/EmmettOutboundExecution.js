'use strict';

/**
 * SPEC-071 — Canonical EXECUTE outbound dispatch via TME.
 * READY + executionApproved → EXECUTE → frozen bundle → Brevo → mission-bound evidence.
 */

const {
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  EVENT_KINDS,
  STAGES,
  nowIso,
} = require('../../acquisition-mission/types');
const {
  executeMissionStage,
  bumpMissionVersion,
  planningError,
  validationError,
  EXECUTION_STATUSES,
} = require('../../acquisition-mission');
const { specialistContext } = require('../../acquisition-mission/Lifecycle');
const { applyStageTransition } = require('../../acquisition-mission/Lifecycle');
const {
  findValidExecutionApproval,
  isExecutionApproved,
} = require('../../acquisition-mission/ExecutionApproval');
const {
  EXECUTION_RECORD_STATUS,
  summarizeExecutionRecords,
} = require('../../acquisition-mission/OutboundExecution');
const { assertMissionStateConsistent } = require('../../acquisition-mission/PendingOperatorDecision');
const { createEvent } = require('../../acquisition-mission/Timeline');
const { executeOutboundBundle } = require('./OutboundExecutionAdapter');
const { persistOutboundExecution } = require('../../../services/acquisitionMissionOutboundPersistence');

function validateExecuteOutboundPreconditions({ mission, engine, tenantId }) {
  if (!mission) throw planningError('tme_mission_missing', 'Mission does not exist.');
  if (mission.planCancelled === true || /cancelled/i.test(String(mission.status || ''))) {
    throw planningError('tme_mission_inactive', 'Mission is not active.');
  }
  const snapshot = engine.inspect(mission.id, { tenantId });
  const contributions = snapshot.contributions || [];
  const ctx = specialistContext(contributions, { missionId: mission.id, ...snapshot });
  if (!ctx.executionApproved) {
    throw planningError('tme_execution_not_approved', 'Execution approval is required before Execute.');
  }
  if (ctx.deliverabilityPaused) {
    throw planningError('tme_deliverability_paused', 'Deliverability risk blocks Execute.');
  }
  if (mission.stage !== STAGES.READY && mission.stage !== STAGES.EXECUTE) {
    throw planningError(
      'tme_wrong_stage',
      `Execute outbound requires stage ${STAGES.READY} or ${STAGES.EXECUTE}.`
    );
  }
  const approval = findValidExecutionApproval(contributions, mission.id);
  if (!approval) {
    throw planningError(
      'tme_execution_approval_stale',
      'Prepared artifacts changed since execution approval. Re-approve before sending.'
    );
  }
  return {
    missionExists: true,
    missionActive: true,
    specialistAvailable: true,
    executionApproved: true,
    approval,
  };
}

function validateExecuteOutboundOutput(output) {
  if (!output || !output.executionResult) {
    throw validationError('tme_execution_result_missing', 'Execute outbound result is missing.');
  }
  if (output.blocked && !output.blockReason) {
    throw validationError('tme_execution_blocked', 'Execute outbound blocked without reason.');
  }
}

function commitExecuteOutboundStage({
  engine,
  mission,
  tenantId,
  output,
  transactionId,
  missionVersion,
}) {
  const missionId = (mission && mission.id) || output.missionId;
  const contributions = engine.inspect(missionId, { tenantId }).contributions || [];
  const current = engine.get(missionId, tenantId);
  applyStageTransition(current, STAGES.EXECUTE, { contributions });
  bumpMissionVersion(current, transactionId);
  current.executionSummary = output.summary || summarizeExecutionRecords(output.records || []);
  current.lastOutboundExecutionAt = nowIso();
  engine.store.putMission(current);

  for (const record of output.records || []) {
    if (record.status === EXECUTION_RECORD_STATUS.SENT) {
      engine.store.addEvent(createEvent({
        missionId,
        kind: EVENT_KINDS.LAUNCHED,
        specialist: SPECIALISTS.EMMETT,
        label: `Sent to ${record.prospectId}`,
        payload: {
          prospectId: record.prospectId,
          providerMessageId: record.providerMessageId,
          preparedArtifactRevision: record.preparedArtifactRevision,
          transactionId,
        },
      }));
    } else if (record.status === EXECUTION_RECORD_STATUS.QUEUED
      || record.status === EXECUTION_RECORD_STATUS.ATTEMPTED) {
      engine.store.addEvent(createEvent({
        missionId,
        kind: EVENT_KINDS.QUEUED,
        specialist: SPECIALISTS.EMMETT,
        label: `Queued ${record.prospectId}`,
        payload: { prospectId: record.prospectId, transactionId },
      }));
    }
  }

  engine.store.addEvent(createEvent({
    missionId,
    kind: EVENT_KINDS.EXECUTION_COMMITTED,
    specialist: SPECIALISTS.EMMETT,
    label: 'Execute outbound committed',
    payload: {
      transactionId,
      missionVersion: current.version,
      priorVersion: missionVersion,
      summary: current.executionSummary,
      recordCount: (output.records || []).length,
    },
  }));

  const snapshot = engine.inspect(missionId, { tenantId });
  assertMissionStateConsistent(snapshot.mission, { contributions: snapshot.contributions });
  return { snapshot, summary: current.executionSummary };
}

async function runExecuteOutboundForAmoMission(input = {}) {
  const {
    mission,
    engine,
    tenantId,
    transactionId,
    executionRequest,
    sendEmail,
    resolveProspectAttributes,
    senderIdentity,
  } = input;

  const snapshot = engine.inspect(mission.id, { tenantId });
  const contributions = snapshot.contributions || [];
  const approval = findValidExecutionApproval(contributions, mission.id);
  const existingRecords = engine.store.listExecutionRecords
    ? engine.store.listExecutionRecords(mission.id)
    : [];

  const persistExecutionRecord = async (record) => {
    if (engine.store.addExecutionRecord) {
      engine.store.addExecutionRecord(record);
    }
    if (input.pool && input.persist !== false) {
      try {
        await persistOutboundExecution(record, input.pool);
      } catch (err) {
        console.error('[execute_outbound] durable execution record persist failed:', err.message);
        throw err;
      }
    }
  };

  return executeOutboundBundle({
    mission,
    contributions,
    approval,
    tenantId,
    transactionId,
    executionRequestId: executionRequest?.id || null,
    sendEmail,
    resolveProspectAttributes,
    senderIdentity,
    existingRecords,
    persistExecutionRecord,
  });
}

/**
 * Canonical EXECUTE outbound — CER EXECUTE_OUTBOUND → TME → adapter → Brevo.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function advanceExecuteOutbound(input = {}) {
  const { engine, mission, tenantId, operatorId, executionRequest } = input;
  if (!engine || !mission) throw new Error('engine and mission are required');

  const staged = await executeMissionStage({
    engine,
    missionId: mission.id,
    tenantId,
    pool: input.pool,
    specialist: SPECIALISTS.EMMETT,
    stage: STAGES.EXECUTE,
    operatorId,
    validatePreconditions: (ctx) => validateExecuteOutboundPreconditions(ctx),
    execute: async ({ mission: current, transactionId }) => {
      const result = await runExecuteOutboundForAmoMission({
        ...input,
        mission: current,
        transactionId,
        executionRequest,
      });

      if (result.blocked) {
        return {
          missionId: current.id,
          blocked: true,
          blockReason: result.blockReason,
          bundle: result.bundle,
          records: result.records || [],
          summary: result.summary,
          executionResult: {
            spec: 'SPEC-132',
            status: EXECUTION_STATUSES.BLOCKED,
            blocked: { reason: result.blockReason },
            confidence: { overall: 0, evidence: 0, fit: 0, completeness: 0 },
            evidence: [],
            contributions: {},
            recommendations: [],
            unknowns: [result.blockReason],
            nextActions: [{ kind: 'operator_review', label: 'Re-approve execution after artifact changes.' }],
          },
        };
      }

      return {
        missionId: current.id,
        blocked: false,
        bundle: result.bundle,
        records: result.records,
        summary: result.summary,
        executionResult: {
          spec: 'SPEC-132',
          status: EXECUTION_STATUSES.SUCCESS,
          confidence: {
            overall: result.summary.sent > 0 ? 0.85 : 0.5,
            evidence: 0.8,
            fit: 0.8,
            completeness: result.summary.complete ? 0.9 : 0.6,
          },
          evidence: (result.records || []).slice(0, 5).map((row) => ({
            id: row.id,
            label: `${row.status}: ${row.prospectId}`,
            source: 'outbound_execution',
            timestamp: row.attemptedAt,
          })),
          contributions: {
            outboundExecution: {
              summary: result.summary,
              recordIds: (result.records || []).map((row) => row.id),
            },
          },
          recommendations: [],
          unknowns: [],
          nextActions: [{ kind: 'observe', label: 'Monitor provider outcomes and webhook correlation.' }],
        },
      };
    },
    validateOutput: (output) => validateExecuteOutboundOutput(output),
    commit: (ctx) => {
      if (ctx.output.blocked) {
        throw validationError('tme_execute_outbound_blocked', ctx.output.blockReason || 'Execute outbound blocked.');
      }
      return commitExecuteOutboundStage(ctx);
    },
    persistDurable: input.persistDurable,
  });

  return {
    alreadyExecuted: false,
    snapshot: staged.commitResult.snapshot,
    bundle: staged.output.bundle,
    records: staged.output.records,
    summary: staged.output.summary,
    executionOutcome: staged.output.blocked ? 'blocked' : 'completed',
    transactionId: staged.transactionId,
    missionVersion: staged.missionVersion,
    executionResult: staged.output.executionResult,
  };
}

module.exports = {
  validateExecuteOutboundPreconditions,
  validateExecuteOutboundOutput,
  commitExecuteOutboundStage,
  runExecuteOutboundForAmoMission,
  advanceExecuteOutbound,
};
