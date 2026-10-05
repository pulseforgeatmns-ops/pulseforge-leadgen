'use strict';

const { EXECUTION_STATUS } = require('../types');

class MemoryDecisionStore {
  constructor(seed = {}) {
    this.clientId = seed.clientId || 1;
    this.decisions = seed.decisions || [];
    this.actionIntents = seed.actionIntents || [];
    this.aoTasks = seed.aoTasks || [];
    this.delegations = seed.delegations || [];
    this.escalations = seed.escalations || [];
    this.telemetry = seed.telemetry || null;
    this.simulateVerificationFailure = Boolean(seed.simulateVerificationFailure);
  }

  async findDecisionByIdempotency(clientId, idempotencyKey) {
    return this.decisions.find(d =>
      d.client_id === clientId && d.idempotency_key === idempotencyKey
    ) || null;
  }

  async findActiveDecision({ clientId, subjectType, subjectId, actionType }) {
    return this.decisions.find(d =>
      d.client_id === clientId
      && d.subject_type === subjectType
      && d.subject_id === subjectId
      && d.selected_action?.action_type === actionType
      && !['SUPERSEDED', 'EXECUTION_FAILURE'].includes(d.execution_status)
      && d.execution_status !== EXECUTION_STATUS.SUPERSEDED
    ) || null;
  }

  async persistDecision(decision) {
    const existing = this.decisions.findIndex(d => d.id === decision.id);
    if (existing >= 0) {
      this.decisions[existing] = decision;
    } else {
      this.decisions.push(decision);
    }
    return decision;
  }

  async persistActionIntent(intent) {
    const idx = this.actionIntents.findIndex(i => i.id === intent.id);
    if (idx >= 0) this.actionIntents[idx] = intent;
    else this.actionIntents.push(intent);
    return intent;
  }

  async createAoTask(task) {
    const dup = this.aoTasks.find(t =>
      t.decision_id === task.decision_id && t.status === 'open'
    );
    if (dup) return dup;
    this.aoTasks.push(task);
    return task;
  }

  async findAoTask(id) {
    return this.aoTasks.find(t => t.id === id) || null;
  }

  async supersedeActiveDecisions({ clientId, subjectType, subjectId, supersededBy }) {
    const superseded = [];
    for (const d of this.decisions) {
      if (d.client_id !== clientId) continue;
      if (d.subject_type !== subjectType || d.subject_id !== subjectId) continue;
      if (d.execution_status === EXECUTION_STATUS.SUPERSEDED) continue;
      if (d.id === supersededBy) continue;
      d.execution_status = EXECUTION_STATUS.SUPERSEDED;
      d.superseded_by = supersededBy;
      d.resolved_at = new Date().toISOString();
      superseded.push(d);
      for (const task of this.aoTasks) {
        if (task.decision_id === d.id && task.status === 'open') {
          task.status = 'superseded';
        }
      }
    }
    return superseded;
  }

  async createDelegation(entry) {
    const row = { id: `del-${this.delegations.length + 1}`, ...entry };
    this.delegations.push(row);
    return row;
  }

  async createEscalation(entry) {
    this.escalations.push(entry);
    return entry;
  }

  async listDecisions({ clientId = this.clientId } = {}) {
    return this.decisions.filter(d => d.client_id === clientId);
  }
}

module.exports = {
  MemoryDecisionStore,
};
