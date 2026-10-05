'use strict';

const { ATTENTION_STATUS, UNRESOLVED_STATUSES } = require('../types');

class MemoryAttentionStore {
  constructor({ clientId } = {}) {
    this.clientId = clientId;
    this.items = [];
    this.runs = [];
    this.heartbeat = {
      id: 'global',
      last_successful_cycle_at: null,
      last_run_id: null,
      consecutive_failures: 0,
    };
  }

  async init() {
    return this;
  }

  async findByFingerprint(clientId, fingerprint) {
    return this.items.find(i =>
      i.client_id === clientId
      && i.dedup_fingerprint === fingerprint
      && UNRESOLVED_STATUSES.includes(i.status)
    ) || null;
  }

  async findById(id) {
    return this.items.find(i => i.id === id) || null;
  }

  async createAttention(item) {
    this.items.push({ ...item });
    return item;
  }

  async updateAttention(id, patch) {
    const idx = this.items.findIndex(i => i.id === id);
    if (idx < 0) return null;
    this.items[idx] = { ...this.items[idx], ...patch, id };
    return this.items[idx];
  }

  async resolveAttention(id, patch) {
    return this.updateAttention(id, {
      ...patch,
      status: patch.status || ATTENTION_STATUS.RESOLVED,
    });
  }

  async listUnresolved({ clientId, subjectType, subjectId } = {}) {
    return this.items.filter(i =>
      UNRESOLVED_STATUSES.includes(i.status)
      && (clientId == null || i.client_id === clientId)
      && (subjectType == null || i.subject_type === subjectType)
      && (subjectId == null || i.subject_id === subjectId)
    );
  }

  async listDueForReview({ clientId, now = new Date(), limit = 50 }) {
    const ts = now.getTime();
    return this.items
      .filter(i =>
        i.client_id === clientId
        && UNRESOLVED_STATUSES.includes(i.status)
        && i.next_review_at
        && new Date(i.next_review_at).getTime() <= ts
        && !i.claim_token
      )
      .slice(0, limit);
  }

  async claimDueItems({ clientId, now, limit, claimToken }) {
    const due = await this.listDueForReview({ clientId, now, limit });
    const claimed = [];
    for (const item of due) {
      const idx = this.items.findIndex(i => i.id === item.id);
      if (idx < 0 || this.items[idx].claim_token) continue;
      this.items[idx].claim_token = claimToken;
      this.items[idx].claimed_at = now.toISOString();
      if (new Date(item.next_review_at).getTime() < now.getTime()) {
        this.items[idx].status = ATTENTION_STATUS.OVERDUE;
      }
      claimed.push({ ...this.items[idx] });
    }
    return claimed;
  }

  async releaseClaim(id, { claimToken, patch = {} } = {}) {
    const idx = this.items.findIndex(i => i.id === id && i.claim_token === claimToken);
    if (idx < 0) return null;
    const failures = patch.execution_failures_increment
      ? (this.items[idx].execution_failures || 0) + 1
      : this.items[idx].execution_failures;
    const { execution_failures_increment, ...rest } = patch;
    this.items[idx] = {
      ...this.items[idx],
      ...rest,
      execution_failures: failures,
      claim_token: null,
      claimed_at: null,
    };
    return this.items[idx];
  }

  async recordSchedulerRun(run) {
    this.runs.push({ ...run });
    return run;
  }

  async updateHeartbeat(patch) {
    this.heartbeat = { ...this.heartbeat, ...patch, updated_at: new Date().toISOString() };
    return this.heartbeat;
  }

  async getHeartbeat() {
    return { ...this.heartbeat };
  }
}

module.exports = {
  MemoryAttentionStore,
};
