'use strict';

/**
 * Knowledge event type constants.
 * Producers (Scout, CRM, Max) emit these — they never write to GraphRepository.
 */
const KNOWLEDGE_EVENTS = Object.freeze({
  COMPANY_OBSERVED: 'knowledge.company_observed',
  PERSON_OBSERVED: 'knowledge.person_observed',
  INTERACTION_RECORDED: 'knowledge.interaction_recorded',
  EVIDENCE_RECORDED: 'knowledge.evidence_recorded',
  CLAIM_PROPOSED: 'knowledge.claim_proposed',
  EDGE_REQUESTED: 'knowledge.edge_requested',
});

/**
 * Minimal in-process event bus for knowledge ingestion.
 */
class KnowledgeEventBus {
  constructor() {
    /** @type {Map<string, Set<Function>>} */
    this._handlers = new Map();
    /** @type {Array<object>} */
    this._history = [];
  }

  /**
   * @param {string} type
   * @param {(event: object) => Promise<void>|void} handler
   * @returns {() => void} unsubscribe
   */
  subscribe(type, handler) {
    if (typeof handler !== 'function') {
      throw new Error('handler must be a function');
    }
    if (!this._handlers.has(type)) {
      this._handlers.set(type, new Set());
    }
    this._handlers.get(type).add(handler);
    return () => this._handlers.get(type)?.delete(handler);
  }

  /**
   * @param {object} event
   * @param {string} event.type
   * @param {string} event.tenantId
   * @param {object} [event.payload]
   */
  async publish(event) {
    if (!event || !event.type) {
      throw new Error('event.type is required');
    }
    if (event.tenantId == null || event.tenantId === '') {
      throw new Error('event.tenantId is required');
    }
    const envelope = {
      id: event.id || require('crypto').randomUUID(),
      type: event.type,
      tenantId: String(event.tenantId),
      payload: event.payload || {},
      occurredAt: event.occurredAt || new Date().toISOString(),
    };
    this._history.push(envelope);

    const handlers = [
      ...(this._handlers.get(envelope.type) || []),
      ...(this._handlers.get('*') || []),
    ];
    const results = [];
    for (const handler of handlers) {
      results.push(await handler(envelope));
    }
    return { envelope, results };
  }

  /** Test helper */
  history() {
    return [...this._history];
  }

  clearHistory() {
    this._history = [];
  }
}

module.exports = {
  KNOWLEDGE_EVENTS,
  KnowledgeEventBus,
};
