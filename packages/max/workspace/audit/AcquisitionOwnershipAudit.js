'use strict';

/**
 * SPEC-124 — Acquisition Ownership Convergence audit events.
 */

const OWNER = 'AMO';

/** @type {object[]} */
const _auditLog = [];

function logAcquisitionOwnershipEvent(event, payload = {}) {
  const row = {
    event,
    owner: payload.owner || OWNER,
    timestamp: payload.timestamp || new Date().toISOString(),
    ...payload,
  };
  _auditLog.push(row);
  if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'test') {
    console.info(`[${event}]`, JSON.stringify(row));
  }
  return row;
}

function logAcquisitionOwner(payload = {}) {
  return logAcquisitionOwnershipEvent('ACQUISITION_OWNER', {
    owner: payload.owner || OWNER,
    missionId: payload.missionId || null,
    action: payload.action || null,
    objective: payload.objective || null,
    tenantId: payload.tenantId || null,
    ...payload,
  });
}

function logClientIntelligenceContribution(payload = {}) {
  return logAcquisitionOwnershipEvent('CLIENT_INTELLIGENCE_CONTRIBUTION', {
    missionId: payload.missionId || null,
    blueprintId: payload.blueprintId || null,
    sectionsAttached: payload.sectionsAttached || [],
    strategicEvidence: payload.strategicEvidence || null,
    attached: payload.attached === true,
    ...payload,
  });
}

function buildAcquisitionOwnershipTrace(events = _auditLog) {
  const steps = [];
  for (const row of events) {
    if (row.event === 'ACQUISITION_OWNER') {
      steps.push(row.action === 'resumed' ? 'Mission Resumed' : 'Mission Created');
    } else if (row.event === 'CLIENT_INTELLIGENCE_CONTRIBUTION' && row.attached) {
      steps.push('Blueprint Attached');
    }
  }
  if (steps.length) steps.push('Mission Response');
  return steps;
}

function listAcquisitionOwnershipAuditLog() {
  return _auditLog.map((row) => ({ ...row }));
}

function clearAcquisitionOwnershipAuditLog() {
  _auditLog.length = 0;
}

function createAcquisitionOwnershipAudit() {
  const localLog = [];
  return {
    log: localLog,
    logAcquisitionOwner(payload) {
      const row = logAcquisitionOwner(payload);
      localLog.push(row);
      return row;
    },
    logClientIntelligenceContribution(payload) {
      const row = logClientIntelligenceContribution(payload);
      localLog.push(row);
      return row;
    },
    buildOwnershipTrace() {
      return buildAcquisitionOwnershipTrace(localLog);
    },
  };
}

module.exports = {
  OWNER,
  logAcquisitionOwnershipEvent,
  logAcquisitionOwner,
  logClientIntelligenceContribution,
  buildAcquisitionOwnershipTrace,
  listAcquisitionOwnershipAuditLog,
  clearAcquisitionOwnershipAuditLog,
  createAcquisitionOwnershipAudit,
};
