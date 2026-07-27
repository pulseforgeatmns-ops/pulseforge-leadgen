'use strict';

/**
 * In-memory ProposalStore (SPEC-027B).
 */

const { PROPOSAL_STATUS } = require('./types');

function newId() {
  return `prop_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

class InMemoryProposalStore {
  constructor() {
    /** @type {Map<string, object>} */
    this._byId = new Map();
    /** @type {Map<string, string[]>} opportunity/mission key → ids */
    this._byOpportunity = new Map();
  }

  /**
   * @param {object} input
   * @returns {object}
   */
  create(input) {
    const now = new Date().toISOString();
    const opportunityId = input.opportunityId != null ? String(input.opportunityId) : null;
    const missionId = input.missionId != null ? String(input.missionId) : null;
    const existing = this.listForOpportunity(opportunityId || missionId);
    const version = existing.length + 1;
    const row = {
      id: input.id || newId(),
      opportunityId,
      missionId,
      clientId: input.clientId != null ? input.clientId : null,
      tenantId: String(input.tenantId || ''),
      version,
      status: input.status || PROPOSAL_STATUS.REVIEW,
      discoverySummary: input.discoverySummary,
      discoveryProfileId: input.discoveryProfileId || null,
      pricingPackageId: input.pricingPackageId || 'setup_monthly',
      document: input.document,
      html: input.html || null,
      acceptedChanges: Array.isArray(input.acceptedChanges) ? input.acceptedChanges : [],
      clientOutcome: input.clientOutcome || null,
      winLoss: input.winLoss || null,
      feedback: input.feedback || null,
      createdAt: now,
      updatedAt: now,
    };
    this._byId.set(row.id, row);
    const key = opportunityId || missionId || row.id;
    const list = this._byOpportunity.get(key) || [];
    list.push(row.id);
    this._byOpportunity.set(key, list);
    return { ...row };
  }

  /**
   * @param {string} id
   * @returns {object|null}
   */
  get(id) {
    const row = this._byId.get(String(id));
    return row ? { ...row } : null;
  }

  /**
   * @param {string|null} opportunityOrMissionId
   * @returns {object[]}
   */
  listForOpportunity(opportunityOrMissionId) {
    if (!opportunityOrMissionId) return [];
    const ids = this._byOpportunity.get(String(opportunityOrMissionId)) || [];
    return ids.map((id) => this.get(id)).filter(Boolean);
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
      version: row.version,
      updatedAt: new Date().toISOString(),
    };
    this._byId.set(row.id, next);
    return { ...next };
  }
}

function createInMemoryProposalStore() {
  return new InMemoryProposalStore();
}

module.exports = {
  InMemoryProposalStore,
  createInMemoryProposalStore,
};
