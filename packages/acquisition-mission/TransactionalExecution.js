'use strict';

/**
 * SPEC-131 — Transactional Mission Execution (TME).
 *
 * One logical transaction per stage:
 *   BEGIN → Validate → Execute → Persist → Commit → END
 *
 * No mission state mutation is durable until the stage completes successfully.
 * `executing` exists only inside the transaction working set.
 */

const { clone, asText, nowIso, newId } = require('./types');
const { assertContract } = require('./Contracts');
const { isStructuredMissionApproved } = require('./StructuredMission');
const { recordExecutionAudit, COMMIT_STATUS } = require('./ExecutionAudit');
const {
  TME_CLASSES,
  planningError,
  preconditionError,
  specialistError,
  validationError,
  persistenceError,
  presentationError,
  isTmeError,
  wrapAs,
  classifyError,
  formatRollbackProse,
} = require('./ExecutionErrors');

const RUNTIME_STATES = Object.freeze({
  PLANNED: 'planned',
  APPROVED: 'approved',
  EXECUTING: 'executing',
  COMPLETED: 'completed',
});

function snapshotStore(store) {
  if (!store || typeof store.snapshot !== 'function' || typeof store.restore !== 'function') {
    throw planningError(
      'tme_store_not_transactional',
      'Mission store does not support snapshot/restore.'
    );
  }
  return store.snapshot();
}

function restoreStore(store, snapshot) {
  if (!snapshot) return;
  store.restore(snapshot);
}

function defaultPreconditions(ctx) {
  const { mission, specialist, requireLockedPlan, requireActive, requireSpecialist } = ctx;
  const report = {
    missionExists: Boolean(mission && mission.id),
    missionActive: false,
    missionLocked: false,
    structuredPlanApproved: false,
    specialistAvailable: false,
    requiredEvidencePresent: true,
  };

  if (!report.missionExists) {
    throw planningError('tme_mission_missing', 'Mission does not exist.');
  }

  const cancelled = mission.planCancelled === true || /cancelled/i.test(String(mission.status || ''));
  report.missionActive = !cancelled && mission.stage !== 'improve';
  if (requireActive !== false && !report.missionActive) {
    throw preconditionError('tme_mission_inactive', 'Mission is not active.');
  }

  report.structuredPlanApproved = isStructuredMissionApproved(mission);
  report.missionLocked = report.structuredPlanApproved;
  if (requireLockedPlan !== false && !report.missionLocked) {
    throw planningError('tme_plan_missing', 'Mission Plan missing.');
  }

  const hasSpecialist = typeof ctx.execute === 'function' || Boolean(specialist);
  report.specialistAvailable = hasSpecialist;
  if (requireSpecialist !== false && !hasSpecialist) {
    throw preconditionError('tme_specialist_unavailable', 'Required specialist is not available.');
  }

  return report;
}

function assertConfidenceValid(value, { required = true } = {}) {
  if (value == null || value === '') {
    if (required) throw validationError('tme_confidence_missing', 'Contribution confidence is required.');
    return;
  }
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0 || n > 1) {
    throw validationError('tme_confidence_invalid', 'Contribution confidence must be a number between 0 and 1.');
  }
}

function assertEvidenceAttached(payload = {}, { required = true } = {}) {
  if (!required) return;
  const evidence = payload.evidence || payload.evidenceRefs || [];
  const signals = payload.buyingSignals || payload.signals || [];
  const hasEvidence = Array.isArray(evidence) ? evidence.length > 0 : Boolean(evidence);
  const hasSignals = Array.isArray(signals) ? signals.length > 0 : Boolean(signals);
  if (!hasEvidence && !hasSignals) {
    throw validationError('tme_evidence_missing', 'Contribution must attach evidence.');
  }
}

function assertContributionContract(specialist, payload) {
  try {
    assertContract(specialist, payload);
  } catch (err) {
    throw wrapAs(TME_CLASSES.VALIDATION, err, 'tme_contract', err.message);
  }
}

function bumpMissionVersion(mission, transactionId) {
  const next = (Number(mission.version) || 0) + 1;
  mission.version = next;
  mission.lastTransactionId = transactionId;
  mission.updatedAt = nowIso();
  return next;
}

/**
 * Execute one mission stage inside a single logical transaction.
 *
 * @param {object} input
 * @param {object} input.engine
 * @param {string} input.missionId
 * @param {string} [input.tenantId]
 * @param {string} [input.specialist]
 * @param {string} [input.stage]
 * @param {Function} [input.validatePreconditions]
 * @param {Function} input.execute  async (ctx) => output — must not rely on durable mission writes
 * @param {Function} [input.validateOutput]
 * @param {Function} input.commit   async (ctx) => commitResult — mutates store; rolled back on throw
 * @param {Function} [input.persistDurable] async (ctx) => void — durable persist after in-memory commit
 * @param {Function} [input.present] async (ctx) => presentation — failures do not rollback
 * @returns {Promise<object>}
 */
async function executeMissionStage(input = {}) {
  const engine = input.engine;
  if (!engine || !engine.store) {
    throw planningError('tme_engine_required', 'Acquisition mission engine is required.');
  }
  const missionId = asText(input.missionId);
  if (!missionId) throw planningError('tme_mission_id_required', 'missionId is required.');

  const tenantId = input.tenantId;
  const specialist = asText(input.specialist) || null;
  const stage = asText(input.stage) || null;
  const transactionId = asText(input.transactionId) || newId('tme');
  const startedAt = Date.now();
  const store = engine.store;

  let snapshot;
  try {
    snapshot = snapshotStore(store);
  } catch (err) {
    throw wrapAs(TME_CLASSES.PLANNING, err, 'tme_store_not_transactional', err.message);
  }

  const prior = engine.get(missionId, tenantId);
  const missionVersion = Number(prior && prior.version) || 0;
  let preconditions = {};
  let rollbackReason = null;
  let errorClass = null;
  let exception = null;
  let output = null;
  let commitResult = null;
  let runtimeState = RUNTIME_STATES.PLANNED;

  const finishAudit = (status, extra = {}) => recordExecutionAudit({
    transactionId,
    missionId,
    tenantId,
    missionVersion,
    specialist,
    stage,
    preconditions,
    durationMs: Date.now() - startedAt,
    commitStatus: status,
    rollbackReason,
    errorClass,
    exception,
    payload: {
      runtimeState,
      ...extra,
    },
  });

  try {
    if (!prior) {
      throw planningError('tme_mission_missing', 'Mission does not exist.');
    }
    runtimeState = isStructuredMissionApproved(prior)
      ? RUNTIME_STATES.APPROVED
      : RUNTIME_STATES.PLANNED;

    const validatePreconditions = typeof input.validatePreconditions === 'function'
      ? input.validatePreconditions
      : defaultPreconditions;

    preconditions = validatePreconditions({
      engine,
      mission: clone(prior),
      tenantId,
      specialist,
      stage,
      execute: input.execute,
      requireLockedPlan: input.requireLockedPlan,
      requireActive: input.requireActive,
      requireSpecialist: input.requireSpecialist,
    }) || {};

    if (typeof input.execute !== 'function') {
      throw preconditionError('tme_specialist_unavailable', 'Required specialist is not available.');
    }

    runtimeState = RUNTIME_STATES.EXECUTING;

    try {
      output = await input.execute({
        engine,
        mission: clone(prior),
        tenantId,
        specialist,
        stage,
        transactionId,
        missionVersion,
        runtimeState,
      });
    } catch (err) {
      throw wrapAs(TME_CLASSES.SPECIALIST, err, 'tme_specialist', err.message || 'Specialist exception.');
    }

    if (typeof input.validateOutput === 'function') {
      try {
        input.validateOutput(output, {
          engine,
          mission: clone(prior),
          tenantId,
          specialist,
          stage,
        });
      } catch (err) {
        throw wrapAs(TME_CLASSES.VALIDATION, err, 'tme_validation', err.message || 'Output validation failed.');
      }
    }

    try {
      commitResult = await input.commit({
        engine,
        mission: engine.get(missionId, tenantId),
        tenantId,
        specialist,
        stage,
        output,
        transactionId,
        missionVersion,
        operatorId: input.operatorId,
      });
    } catch (err) {
      throw wrapAs(TME_CLASSES.PERSISTENCE, err, 'tme_persistence', err.message || 'Persistence failure.');
    }

    runtimeState = RUNTIME_STATES.COMPLETED;
    const auditPreview = {
      id: newId('tme'),
      transactionId,
      missionId,
      tenantId,
      missionVersion,
      specialist,
      stage,
      preconditions,
      durationMs: Date.now() - startedAt,
      commitStatus: COMMIT_STATUS.COMMITTED,
      rollbackReason: null,
      errorClass: null,
      exception: null,
      payload: { runtimeState, ...(commitResult && commitResult.auditPayload) },
    };

    if (typeof input.persistDurable === 'function') {
      try {
        await input.persistDurable({
          engine,
          missionId,
          tenantId,
          transactionId,
          missionVersion,
          output,
          commitResult,
          audit: auditPreview,
        });
      } catch (err) {
        throw wrapAs(TME_CLASSES.PERSISTENCE, err, 'tme_persistence', err.message || 'Persistence failure.');
      }
    }

    const audit = recordExecutionAudit(auditPreview);

    let presentation = null;
    if (typeof input.present === 'function') {
      try {
        presentation = await input.present({
          engine,
          mission: engine.get(missionId, tenantId),
          tenantId,
          output,
          commitResult,
          transactionId,
          audit,
        });
      } catch (err) {
        presentation = {
          retryable: true,
          error: isTmeError(err)
            ? err
            : presentationError('tme_presentation', err.message || 'Presentation failed.', { cause: err }),
        };
      }
    }

    return {
      committed: true,
      rolledBack: false,
      transactionId,
      missionVersion: Number((engine.get(missionId, tenantId) || {}).version) || missionVersion + 1,
      priorVersion: missionVersion,
      specialist,
      stage,
      runtimeState,
      output,
      commitResult,
      presentation,
      audit,
    };
  } catch (err) {
    const classified = isTmeError(err) ? err.tmeClass : classifyError(err);
    errorClass = classified;
    exception = err && err.message ? err.message : String(err);
    rollbackReason = (err && err.rollbackReason) || exception;
    try {
      restoreStore(store, snapshot);
    } catch (restoreErr) {
      exception = `${exception}; restore failed: ${restoreErr.message}`;
    }
    runtimeState = isStructuredMissionApproved(engine.get(missionId, tenantId) || prior)
      ? RUNTIME_STATES.APPROVED
      : RUNTIME_STATES.PLANNED;
    const audit = finishAudit(COMMIT_STATUS.ROLLED_BACK);
    const wrapped = isTmeError(err)
      ? err
      : wrapAs(classified, err, err && err.code, exception);
    wrapped.transactionId = transactionId;
    wrapped.missionId = missionId;
    wrapped.missionVersion = missionVersion;
    wrapped.commitStatus = COMMIT_STATUS.ROLLED_BACK;
    wrapped.rollback = true;
    wrapped.audit = audit;
    throw wrapped;
  }
}

module.exports = {
  RUNTIME_STATES,
  executeMissionStage,
  defaultPreconditions,
  assertConfidenceValid,
  assertEvidenceAttached,
  assertContributionContract,
  bumpMissionVersion,
  snapshotStore,
  restoreStore,
  formatRollbackProse,
};
