'use strict';

/**
 * In-memory Outcome Intelligence store (SPEC-036).
 * Append-only snapshots; recommendations mutate only via new snapshots + actions.
 */

function newId() {
  return `out_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

class InMemoryOutcomeIntelligenceStore {
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
      campaignName: input.campaignName || 'Campaign',
      package: input.package || null,
      outcomes: Array.isArray(input.outcomes) ? input.outcomes : [],
      learnings: Array.isArray(input.learnings) ? input.learnings : [],
      recommendations: Array.isArray(input.recommendations)
        ? input.recommendations
        : [],
      rankingFeedback: Array.isArray(input.rankingFeedback)
        ? input.rankingFeedback
        : [],
      analytics: input.analytics || null,
      outcomeSummary: input.outcomeSummary || null,
      missionEvents: Array.isArray(input.missionEvents)
        ? input.missionEvents
        : [],
      timeline: Array.isArray(input.timeline) ? input.timeline : [],
      summary: input.summary || null,
      changeSummary: input.changeSummary || '',
      operator: input.operator || 'system',
      createdAt: now,
      updatedAt: now,
    };
    this._byId.set(row.id, deepFreeze(clone(row)));
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
 * @param {object} [seed]
 * @returns {InMemoryOutcomeIntelligenceStore}
 */
function createInMemoryOutcomeIntelligenceStore(seed) {
  const store = new InMemoryOutcomeIntelligenceStore();
  if (seed && typeof seed === 'object') {
    store.create(seed);
  }
  return store;
}

function clone(row) {
  return JSON.parse(JSON.stringify(row));
}

function deepFreeze(obj) {
  if (obj && typeof obj === 'object' && !Object.isFrozen(obj)) {
    Object.freeze(obj);
    for (const key of Object.keys(obj)) {
      deepFreeze(obj[key]);
    }
  }
  return obj;
}

module.exports = {
  InMemoryOutcomeIntelligenceStore,
  createInMemoryOutcomeIntelligenceStore,
};
