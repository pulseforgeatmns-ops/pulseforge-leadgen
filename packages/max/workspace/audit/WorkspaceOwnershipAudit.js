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
function logWorkspaceOwnerSelected(payload = {}) {
  return logWorkspaceOwnershipEvent('WORKSPACE_OWNER_SELECTED', {
    owner: payload.owner || null,
    reason: payload.reason || null,
    confidence: payload.confidence != null ? payload.confidence : 1,
    question: payload.question || null,
    specialist: payload.specialist || null,
    ...payload,
  });
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
      return row;
    },
    logOwnerFallback(payload) {
      const row = logWorkspaceOwnerFallback(payload);
      localLog.push(row);
      return row;
    },
  };
}

module.exports = {
  logWorkspaceOwnershipEvent,
  logWorkspaceOwnerSelected,
  logWorkspaceOwnerFallback,
  listWorkspaceOwnershipAuditLog,
  clearWorkspaceOwnershipAuditLog,
  createWorkspaceOwnershipAudit,
};
