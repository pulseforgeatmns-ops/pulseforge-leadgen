'use strict';

/**
 * In-memory Mail Package revision store (SPEC-033).
 * Regeneration appends a revision; prior revisions remain available.
 */

const { PACKAGE_STATUS } = require('./types');

function newId() {
  return `mail_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

class InMemoryMailPackageStore {
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
    const campaignId = input.campaignId != null ? String(input.campaignId) : null;
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
      status: input.status || 'review',
      campaignName: input.campaignName || 'Campaign Mail Packages',
      packages: Array.isArray(input.packages) ? input.packages : [],
      campaignSummary: input.campaignSummary || null,
      campaignHtml: input.campaignHtml || null,
      mailMergeCsv: input.mailMergeCsv || null,
      addressLabelCsv: input.addressLabelCsv || null,
      campaignDocxHtml: input.campaignDocxHtml || null,
      playbookId: input.playbookId || null,
      playbookVersion: input.playbookVersion || null,
      confidenceThreshold: input.confidenceThreshold != null ? input.confidenceThreshold : null,
      createdAt: now,
      updatedAt: now,
    };
    this._byId.set(row.id, row);
    const list = this._byCampaign.get(key) || [];
    list.push(row.id);
    this._byCampaign.set(key, list);
    return { ...row };
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
   * Latest revision for a campaign / mission key.
   * @param {string|null} campaignOrMissionId
   * @returns {object|null}
   */
  getLatest(campaignOrMissionId) {
    const list = this.listForCampaign(campaignOrMissionId);
    if (!list.length) return null;
    return list[list.length - 1];
  }

  /**
   * @param {string} id
   * @param {object} patch
   * @returns {object|null}
   */
  update(id, patch) {
    const row = this._byId.get(String(id));
    if (!row) return null;
    const next = {
      ...row,
      ...patch,
      id: row.id,
      revision: row.revision,
      updatedAt: new Date().toISOString(),
    };
    this._byId.set(row.id, next);
    return clone(next);
  }
}

function clone(row) {
  return {
    ...row,
    packages: Array.isArray(row.packages) ? row.packages.map((p) => ({ ...p })) : [],
  };
}

function createInMemoryMailPackageStore() {
  return new InMemoryMailPackageStore();
}

module.exports = {
  InMemoryMailPackageStore,
  createInMemoryMailPackageStore,
  PACKAGE_STATUS,
};
