'use strict';

/**
 * In-memory Direct Mail Execution store (SPEC-035).
 * Append-only snapshots; audit log is immutable once written.
 */

function newId() {
  return `dmx_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

class InMemoryDirectMailExecutionStore {
  constructor() {
    /** @type {Map<string, object>} */
    this._byId = new Map();
    /** @type {Map<string, string[]>} campaign/mission key → snapshot ids */
    this._byCampaign = new Map();
  }

  /**
   * @param {object} input
   * @returns {object}
   */
  create(input) {
    const now = new Date().toISOString();
    const campaignId =
      input.campaignId != null ? String(input.campaignId) : null;
    const missionId = input.missionId != null ? String(input.missionId) : null;
    const key = campaignId || missionId || newId();
    const existing = this.listForCampaign(key);
    const snapshot = existing.length + 1;
    const row = {
      id: input.id || newId(),
      campaignId,
      missionId,
      clientId: input.clientId != null ? input.clientId : null,
      tenantId: String(input.tenantId || ''),
      snapshot,
      status: input.status || 'draft',
      campaignName: input.campaignName || 'Campaign',
      revision:
        input.revision != null
          ? Number(input.revision)
          : input.summary && input.summary.revision != null
            ? Number(input.summary.revision)
            : null,
      execution: input.execution || null,
      summary: input.summary || null,
      prospects: Array.isArray(input.prospects) ? input.prospects : [],
      auditLog: Array.isArray(input.auditLog) ? input.auditLog : [],
      printSessions: Array.isArray(input.printSessions)
        ? input.printSessions
        : [],
      missionEvents: Array.isArray(input.missionEvents)
        ? input.missionEvents
        : [],
      timeline: Array.isArray(input.timeline) ? input.timeline : [],
      lock: input.lock || null,
      pinnedArtifacts: input.pinnedArtifacts || null,
      changeSummary: input.changeSummary || '',
      operator: input.operator || 'system',
      createdAt: now,
      updatedAt: now,
    };
    this._byId.set(row.id, freezeAudit(row));
    const list = this._byCampaign.get(key) || [];
    list.push(row.id);
    this._byCampaign.set(key, list);
    return clone(row);
  }

  /**
   * @param {string} id
   * @returns {object|null}
   */
  get(id) {
    const row = this._byId.get(String(id));
    return row ? clone(row) : null;
  }

  /**
   * @param {string|null} campaignOrMissionId
   * @returns {object[]}
   */
  listForCampaign(campaignOrMissionId) {
    if (!campaignOrMissionId) return [];
    const ids = this._byCampaign.get(String(campaignOrMissionId)) || [];
    return ids.map((id) => this.get(id)).filter(Boolean);
  }

  /**
   * @param {string|null} campaignOrMissionId
   * @returns {object|null}
   */
  getLatest(campaignOrMissionId) {
    const list = this.listForCampaign(campaignOrMissionId);
    if (!list.length) return null;
    return list[list.length - 1];
  }
}

/**
 * Ensure audit entries are frozen (immutable).
 * @param {object} row
 * @returns {object}
 */
function freezeAudit(row) {
  if (Array.isArray(row.auditLog)) {
    row.auditLog = row.auditLog.map((e) => Object.freeze({ ...e }));
    Object.freeze(row.auditLog);
  }
  return row;
}

function clone(row) {
  return {
    ...row,
    prospects: Array.isArray(row.prospects)
      ? row.prospects.map((p) => ({
          ...p,
          assembly: p.assembly ? { ...p.assembly } : p.assembly,
        }))
      : [],
    auditLog: Array.isArray(row.auditLog)
      ? row.auditLog.map((e) => ({ ...e }))
      : [],
    printSessions: Array.isArray(row.printSessions)
      ? row.printSessions.map((s) => ({ ...s }))
      : [],
    missionEvents: Array.isArray(row.missionEvents)
      ? row.missionEvents.map((e) => ({ ...e }))
      : [],
    timeline: Array.isArray(row.timeline)
      ? row.timeline.map((t) => ({ ...t }))
      : [],
    summary: row.summary
      ? {
          ...row.summary,
          metrics: row.summary.metrics ? { ...row.summary.metrics } : null,
        }
      : null,
    lock: row.lock ? { ...row.lock } : null,
    pinnedArtifacts: row.pinnedArtifacts
      ? {
          ...row.pinnedArtifacts,
          mailBatch: row.pinnedArtifacts.mailBatch
            ? { ...row.pinnedArtifacts.mailBatch }
            : null,
          executionPackage: row.pinnedArtifacts.executionPackage
            ? { ...row.pinnedArtifacts.executionPackage }
            : null,
        }
      : null,
    execution: row.execution
      ? {
          ...row.execution,
          prospects: Array.isArray(row.execution.prospects)
            ? row.execution.prospects.map((p) => ({
                ...p,
                assembly: p.assembly ? { ...p.assembly } : p.assembly,
              }))
            : [],
          auditLog: Array.isArray(row.execution.auditLog)
            ? row.execution.auditLog.map((e) => ({ ...e }))
            : [],
          summary: row.execution.summary
            ? {
                ...row.execution.summary,
                metrics: row.execution.summary.metrics
                  ? { ...row.execution.summary.metrics }
                  : null,
              }
            : null,
          lock: row.execution.lock ? { ...row.execution.lock } : null,
        }
      : null,
  };
}

function createInMemoryDirectMailExecutionStore() {
  return new InMemoryDirectMailExecutionStore();
}

module.exports = {
  InMemoryDirectMailExecutionStore,
  createInMemoryDirectMailExecutionStore,
};
