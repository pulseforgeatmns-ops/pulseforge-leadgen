'use strict';

const { ATTENTION_STATUS, UNRESOLVED_STATUSES } = require('../types');
const { isManualReviewOnlyAttention } = require('../manualReviewOnly');
const { DEFAULT_LEASE_MS, validateLeaseMs, requireClaimToken, claimLost } = require('../claimPolicy');

class MemoryAttentionStore {
  constructor({ clientId, leaseMs = DEFAULT_LEASE_MS, clock = Date.now } = {}) {
    this.clientId = clientId;
    this.leaseMs = validateLeaseMs(leaseMs);
    this.clock = clock;
    this.executing = new Set();
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

  async findByFingerprint(clientId, fingerprint, { includeResolved = false } = {}) {
    return this.items.find(i =>
      i.client_id === clientId
      && i.dedup_fingerprint === fingerprint
      && (includeResolved || UNRESOLVED_STATUSES.includes(i.status))
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
        && !isManualReviewOnlyAttention(i)
        && UNRESOLVED_STATUSES.includes(i.status)
        && i.next_review_at
        && new Date(i.next_review_at).getTime() <= ts
        && !this.executing.has(i.id)
        && (!i.claim_token || !this._claimIsFresh(i))
      )
      .slice(0, limit);
  }

  async claimDueItems({ clientId, now = new Date(), limit = 20, claimToken }) {
    requireClaimToken(claimToken);
    const due = await this.listDueForReview({ clientId, now, limit });
    const claimed = [];
    for (const item of due) {
      const idx = this.items.findIndex(i => i.id === item.id);
      if (idx < 0 || this.executing.has(item.id) || (this.items[idx].claim_token && this._claimIsFresh(this.items[idx]))) continue;
      this.items[idx].claim_token = claimToken;
      this.items[idx].claimed_at = new Date(this.clock()).toISOString();
      if (new Date(item.next_review_at).getTime() < now.getTime()) {
        this.items[idx].status = ATTENTION_STATUS.OVERDUE;
      }
      claimed.push({ ...this.items[idx] });
    }
    return claimed;
  }

  _claimIsFresh(item) {
    return item.claimed_at && new Date(item.claimed_at).getTime() > this.clock() - this.leaseMs;
  }

  async withClaim(id, { claimToken, clientId }, callback) {
    requireClaimToken(claimToken);
    const item = await this.findById(id);
    if (!item || item.client_id !== clientId || item.claim_token !== claimToken
      || !this._claimIsFresh(item) || this.executing.has(id)) throw claimLost();
    this.executing.add(id);
    try {
      return await callback({ attentionStore: this, item: { ...item }, db: null });
    } finally {
      this.executing.delete(id);
    }
  }

  async releaseClaim(id, { claimToken, patch = {} } = {}) {
    requireClaimToken(claimToken);
    const idx = this.items.findIndex(i => i.id === id && i.claim_token === claimToken && this._claimIsFresh(i));
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
    const existing = this.runs.find(r => r.id === run.id);
    if (existing && (existing.client_id !== run.client_id || existing.started_at !== run.started_at)) {
      throw new Error('Scheduler run identity does not match');
    }
    if (existing?.status !== 'running' && existing) return { ...existing };
    if (existing) Object.assign(existing, run);
    else this.runs.push({ ...run });
    return { ...run };
  }

  async finishSchedulerRun(run) {
    if (!['completed', 'failed'].includes(run.status)) throw new Error('Terminal scheduler status required');
    const existing = this.runs.find(r => r.id === run.id);
    if (!existing) throw new Error('Scheduler run has not started');
    const wasRunning = existing.status === 'running';
    const saved = await this.recordSchedulerRun(run);
    if (wasRunning) {
      await this.updateHeartbeat({
        last_run_id: run.id,
        ...(run.status === 'completed' ? { last_successful_cycle_at: run.completed_at } : {}),
        consecutive_failures: run.status === 'failed' ? this.heartbeat.consecutive_failures + 1 : 0,
      });
    }
    return saved;
  }

  async updateHeartbeat(patch) {
    this.heartbeat = { ...this.heartbeat, ...Object.fromEntries(Object.entries(patch).filter(([, value]) => value !== undefined)), updated_at: new Date().toISOString() };
    return this.heartbeat;
  }

  async getHeartbeat() {
    return { ...this.heartbeat };
  }
}

module.exports = {
  MemoryAttentionStore,
};
