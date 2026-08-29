'use strict';

/**
 * SPEC-170 — Native Acquisition Mission Specialist Execution.
 *
 * Specialists execute against runtime contracts, not runtime implementations.
 * The runtime supplies context; specialists supply intelligence.
 */

const { buildSharedContext } = require('./Context');
const { buildWorkspace } = require('./Workspace');

const RUNTIME_KINDS = Object.freeze({
  ACQUISITION_MISSION: 'acquisition_mission',
  LEGACY_MISSION_ENGINE: 'legacy_mission_engine',
});

function isNativeAmoExecution(executionContext) {
  if (!executionContext) return false;
  if (executionContext.runtime === RUNTIME_KINDS.ACQUISITION_MISSION) return true;
  return executionContext.persistence?.runtime === 'amo';
}

function shouldSuppressSpecialistSideEffects(executionContext) {
  if (!executionContext) return false;
  if (executionContext.persistence?.suppressSideEffects === true) return true;
  return isNativeAmoExecution(executionContext);
}

/**
 * Runtime handles travel separately from serializable mission execution context.
 * Never clone, persist, or expose these as canonical mission data.
 *
 * @param {object} input
 * @param {import('./Engine').AcquisitionMissionEngine} [input.engine]
 * @param {object} [input.pool]
 * @returns {{ pool: object|null, engine: object|null }}
 */
function buildRuntimeDependencies(input = {}) {
  return {
    pool: input.pool || null,
    engine: input.engine || null,
  };
}

/**
 * Build the serializable execution contract for specialist runs.
 * Runtime dependencies (pool, engine) must be passed separately via buildRuntimeDependencies.
 *
 * @param {object} input
 * @param {import('./Engine').AcquisitionMissionEngine} [input.engine]
 * @param {object} input.mission
 * @param {string|number} [input.tenantId]
 * @param {string} [input.transactionId]
 * @param {object} [input.pool] — used only to resolve snapshot via engine; not stored on context
 * @returns {object}
 */
function buildMissionExecutionContext(input = {}) {
  const { engine, mission, tenantId, transactionId } = input;
  if (!mission) {
    throw new Error('buildMissionExecutionContext requires mission');
  }

  const resolvedTenantId = String(tenantId || mission.tenantId || mission.clientId || '');
  const snapshot = engine && typeof engine.inspect === 'function'
    ? engine.inspect(mission.id, { tenantId: resolvedTenantId })
    : { mission, contributions: [] };
  const resolvedMission = snapshot.mission || mission;
  const contributions = snapshot.contributions || [];
  const intelligence = buildSharedContext(resolvedMission, contributions);
  const workspace = buildWorkspace(resolvedMission, intelligence);

  return {
    spec: 'SPEC-170',
    runtime: RUNTIME_KINDS.ACQUISITION_MISSION,
    mission: resolvedMission,
    transaction: {
      id: transactionId || null,
      tenantId: resolvedTenantId,
    },
    workspace,
    intelligence,
    persistence: {
      runtime: 'amo',
      suppressSideEffects: true,
    },
  };
}

module.exports = {
  RUNTIME_KINDS,
  buildMissionExecutionContext,
  buildRuntimeDependencies,
  isNativeAmoExecution,
  shouldSuppressSpecialistSideEffects,
};
