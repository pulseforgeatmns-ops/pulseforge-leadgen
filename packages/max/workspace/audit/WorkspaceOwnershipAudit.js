'use strict';

/**
 * SPEC-125 — Workspace Ownership-First Runtime audit events.
 */

/** @type {object[]} */
const _auditLog = [];

function logWorkspaceOwnershipEvent(event, payload = {}) {
  const row = {
    event,
    timestamp: payload.timestamp || new Date().toISOString(),
    ...payload,
  };
  _auditLog.push(row);
  if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'test') {
    console.info(`[${event}]`, JSON.stringify(row));
  }
  return row;
}

/**
 * @param {object} payload
 * @param {string} payload.owner
 * @param {string} payload.reason
 * @param {number} [payload.confidence]
 * @param {string} [payload.question]
 */
/**
 * SPEC-125 / SPEC-126 — map pipeline owner to audit owner kind.
 * @param {string|null} owner
 * @returns {string|null}
 */
function normalizeWorkspaceOwnerKind(owner) {
  if (owner === 'mission_creation') return 'MissionCreation';
  if (owner === 'objective_persistence') return 'ObjectivePersistence';
  return null;
}

function logWorkspaceOwnerSelected(payload = {}) {
  const ownerKind = normalizeWorkspaceOwnerKind(payload.owner);
  const row = logWorkspaceOwnershipEvent('WORKSPACE_OWNER_SELECTED', {
    owner: payload.owner || null,
    reason: payload.reason || null,
    confidence: payload.confidence != null ? payload.confidence : 1,
    question: payload.question || null,
    specialist: payload.specialist || null,
    ...payload,
  });
  if (ownerKind) {
    logWorkspaceOwnershipEvent('WORKSPACE_OWNER', {
      owner: ownerKind,
      pipelineOwner: payload.owner || null,
      reason: payload.reason || null,
      confidence: payload.confidence != null ? payload.confidence : 1,
      question: payload.question || null,
    });
  }
  return row;
}

/**
 * @param {object} payload
 * @param {string} payload.claimedOwner
 * @param {string} payload.fallbackOwner
 * @param {string} payload.reason
 */
function logWorkspaceOwnerFallback(payload = {}) {
  return logWorkspaceOwnershipEvent('WORKSPACE_OWNER_FALLBACK', {
    claimedOwner: payload.claimedOwner || null,
    fallbackOwner: payload.fallbackOwner || 'reasoning',
    reason: payload.reason || null,
    question: payload.question || null,
    ...payload,
  });
}

/**
 * SPEC-127 — active mission guard blocked GC / briefing domain switch.
 * @param {object} payload
 */
function logActiveMissionGuard(payload = {}) {
  return logWorkspaceOwnershipEvent('ACTIVE_MISSION_GUARD', {
    missionId: payload.missionId || null,
    source: payload.source || null,
    blockedDomain: payload.blockedDomain || null,
    executionCommand: payload.executionCommand === true,
    question: payload.question || null,
    reason: payload.reason || null,
    ...payload,
  });
}

/**
 * SPEC-127 — mission pipeline selected as owner.
 * @param {object} payload
 */
function logMissionOwnerSelected(payload = {}) {
  return logWorkspaceOwnershipEvent('MISSION_OWNER_SELECTED', {
    owner: payload.owner || null,
    reason: payload.reason || null,
    missionId: payload.missionId || null,
    source: payload.source || null,
    question: payload.question || null,
    ...payload,
  });
}

/**
 * SPEC-127 — error when GC/briefing claimed a mission execution turn.
 * @param {object} payload
 */
function logMissionOwnerBypassed(payload = {}) {
  return logWorkspaceOwnershipEvent('MISSION_OWNER_BYPASSED', {
    attemptedOwner: payload.attemptedOwner || null,
    missionId: payload.missionId || null,
    question: payload.question || null,
    reason: payload.reason || null,
    ...payload,
  });
}

function listWorkspaceOwnershipAuditLog() {
  return _auditLog.map((row) => ({ ...row }));
}

function clearWorkspaceOwnershipAuditLog() {
  _auditLog.length = 0;
}

function createWorkspaceOwnershipAudit() {
  const localLog = [];
  return {
    log: localLog,
    logOwnerSelected(payload) {
      const row = logWorkspaceOwnerSelected(payload);
      localLog.push(row);
      const ownerKind = normalizeWorkspaceOwnerKind(payload.owner);
      if (ownerKind) {
        localLog.push({
          event: 'WORKSPACE_OWNER',
          timestamp: row.timestamp,
          owner: ownerKind,
          pipelineOwner: payload.owner || null,
          reason: payload.reason || null,
          confidence: payload.confidence != null ? payload.confidence : 1,
          question: payload.question || null,
        });
      }
      return row;
    },
    logOwnerFallback(payload) {
      const row = logWorkspaceOwnerFallback(payload);
      localLog.push(row);
      return row;
    },
    logActiveMissionGuard(payload) {
      const row = logActiveMissionGuard(payload);
      localLog.push(row);
      return row;
    },
    logMissionOwnerSelected(payload) {
      const row = logMissionOwnerSelected(payload);
      localLog.push(row);
      return row;
    },
    logMissionOwnerBypassed(payload) {
      const row = logMissionOwnerBypassed(payload);
      localLog.push(row);
      return row;
    },
  };
}

module.exports = {
  logWorkspaceOwnershipEvent,
  logWorkspaceOwnerSelected,
  logWorkspaceOwnerFallback,
  logActiveMissionGuard,
  logMissionOwnerSelected,
  logMissionOwnerBypassed,
  normalizeWorkspaceOwnerKind,
  listWorkspaceOwnershipAuditLog,
  clearWorkspaceOwnershipAuditLog,
  createWorkspaceOwnershipAudit,
};
