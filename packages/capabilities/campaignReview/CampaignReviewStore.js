'use strict';

/**
 * In-memory Campaign Review revision store (SPEC-034).
 * Append-only revisions; prior revisions remain available for compare / restore.
 */

function newId() {
  return `crev_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

class InMemoryCampaignReviewStore {
  constructor() {
    /** @type {Map<string, object>} */
    this._byId = new Map();
    /** @type {Map<string, string[]>} campaign/mission key → revision ids */
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
    const revision = existing.length + 1;
    const row = {
      id: input.id || newId(),
      campaignId,
      missionId,
      clientId: input.clientId != null ? input.clientId : null,
      tenantId: String(input.tenantId || ''),
      revision,
      status: input.status || 'in_review',
      campaignName: input.campaignName || 'Campaign',
      workspace: input.workspace || null,
      summary: input.summary || null,
      queue: Array.isArray(input.queue) ? input.queue : [],
      decisions: Array.isArray(input.decisions) ? input.decisions : [],
      missionRevisions: Array.isArray(input.missionRevisions)
        ? input.missionRevisions
        : [],
      executionPackage: input.executionPackage || null,
      changeSummary: input.changeSummary || '',
      operator: input.operator || 'system',
      createdAt: now,
      updatedAt: now,
    };
    this._byId.set(row.id, row);
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

  /**
   * @param {string|null} campaignOrMissionId
   * @param {number} revision
   * @returns {object|null}
   */
  getRevision(campaignOrMissionId, revision) {
    const list = this.listForCampaign(campaignOrMissionId);
    return list.find((r) => Number(r.revision) === Number(revision)) || null;
  }

  /**
   * Duplicate latest (or specified) revision as a new append.
   * @param {string|null} campaignOrMissionId
   * @param {number} [fromRevision]
   * @param {object} [meta]
   * @returns {object|null}
   */
  duplicate(campaignOrMissionId, fromRevision, meta = {}) {
    const source =
      fromRevision != null
        ? this.getRevision(campaignOrMissionId, fromRevision)
        : this.getLatest(campaignOrMissionId);
    if (!source) return null;
    return this.create({
      campaignId: source.campaignId,
      missionId: source.missionId,
      clientId: source.clientId,
      tenantId: source.tenantId,
      status: source.status,
      campaignName: source.campaignName,
      workspace: source.workspace,
      summary: source.summary,
      queue: source.queue,
      decisions: [],
      missionRevisions: [],
      executionPackage: null,
      changeSummary: meta.changeSummary || `Duplicated from revision ${source.revision}`,
      operator: meta.operator || 'system',
    });
  }

  /**
   * Compare two revisions (shallow field diff).
   * @param {string|null} campaignOrMissionId
   * @param {number} revisionA
   * @param {number} revisionB
   * @returns {object|null}
   */
  compare(campaignOrMissionId, revisionA, revisionB) {
    const a = this.getRevision(campaignOrMissionId, revisionA);
    const b = this.getRevision(campaignOrMissionId, revisionB);
    if (!a || !b) return null;
    return {
      revisionA: a.revision,
      revisionB: b.revision,
      statusChanged: a.status !== b.status,
      changeSummaryA: a.changeSummary,
      changeSummaryB: b.changeSummary,
      queueCountA: (a.queue || []).length,
      queueCountB: (b.queue || []).length,
      approvedA: (a.queue || []).filter((r) => r.status === 'approved').length,
      approvedB: (b.queue || []).filter((r) => r.status === 'approved').length,
      timestampA: a.createdAt,
      timestampB: b.createdAt,
      operatorA: a.operator,
      operatorB: b.operator,
    };
  }

  /**
   * Restore: append a new revision cloned from an older one (history preserved).
   * @param {string|null} campaignOrMissionId
   * @param {number} revision
   * @param {object} [meta]
   * @returns {object|null}
   */
  restore(campaignOrMissionId, revision, meta = {}) {
    const source = this.getRevision(campaignOrMissionId, revision);
    if (!source) return null;
    return this.create({
      campaignId: source.campaignId,
      missionId: source.missionId,
      clientId: source.clientId,
      tenantId: source.tenantId,
      status: source.status,
      campaignName: source.campaignName,
      workspace: source.workspace,
      summary: {
        ...(source.summary || {}),
        activeRevision: undefined, // stamped by caller after create
      },
      queue: source.queue,
      decisions: [],
      missionRevisions: [],
      executionPackage: source.executionPackage,
      changeSummary:
        meta.changeSummary || `Restored from revision ${source.revision}`,
      operator: meta.operator || 'system',
    });
  }
}

function clone(row) {
  return {
    ...row,
    queue: Array.isArray(row.queue) ? row.queue.map((q) => ({ ...q })) : [],
    decisions: Array.isArray(row.decisions) ? row.decisions.slice() : [],
    missionRevisions: Array.isArray(row.missionRevisions)
      ? row.missionRevisions.slice()
      : [],
    summary: row.summary ? { ...row.summary } : null,
    workspace: row.workspace
      ? {
          ...row.workspace,
          queue: Array.isArray(row.workspace.queue)
            ? row.workspace.queue.map((q) => ({ ...q }))
            : [],
          summary: row.workspace.summary
            ? { ...row.workspace.summary }
            : null,
        }
      : null,
    executionPackage: row.executionPackage
      ? { ...row.executionPackage }
      : null,
  };
}

function createInMemoryCampaignReviewStore() {
  return new InMemoryCampaignReviewStore();
}

module.exports = {
  InMemoryCampaignReviewStore,
  createInMemoryCampaignReviewStore,
};
