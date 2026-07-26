'use strict';

const { KNOWLEDGE_EVENTS } = require('./KnowledgeEventBus');
const { NODE_TYPES } = require('../types/nodeTypes');
const { EDGE_TYPES } = require('../edges/edgeTypes');

/**
 * Wires knowledge events → KnowledgeService.
 * This is the only path producers should use to mutate the graph.
 */
class KnowledgeIngestor {
  /**
   * @param {object} deps
   * @param {import('../services/KnowledgeService').KnowledgeService} deps.knowledge
   * @param {import('./KnowledgeEventBus').KnowledgeEventBus} deps.bus
   */
  constructor(deps) {
    this.knowledge = deps.knowledge;
    this.bus = deps.bus;
    this._unsubscribers = [];
  }

  start() {
    this._unsubscribers.push(
      this.bus.subscribe(KNOWLEDGE_EVENTS.COMPANY_OBSERVED, (e) => this.onCompanyObserved(e)),
      this.bus.subscribe(KNOWLEDGE_EVENTS.PERSON_OBSERVED, (e) => this.onPersonObserved(e)),
      this.bus.subscribe(KNOWLEDGE_EVENTS.INTERACTION_RECORDED, (e) => this.onInteractionRecorded(e)),
      this.bus.subscribe(KNOWLEDGE_EVENTS.EVIDENCE_RECORDED, (e) => this.onEvidenceRecorded(e)),
      this.bus.subscribe(KNOWLEDGE_EVENTS.CLAIM_PROPOSED, (e) => this.onClaimProposed(e)),
      this.bus.subscribe(KNOWLEDGE_EVENTS.EDGE_REQUESTED, (e) => this.onEdgeRequested(e))
    );
    return this;
  }

  stop() {
    for (const unsub of this._unsubscribers) unsub();
    this._unsubscribers = [];
  }

  async onCompanyObserved(event) {
    const company = await this.knowledge.createNode({
      tenantId: event.tenantId,
      type: NODE_TYPES.COMPANY,
      name: event.payload.name,
      metadata: {
        ...(event.payload.metadata || {}),
        ingestedFrom: event.type,
        sourceEventId: event.id,
      },
      id: event.payload.nodeId,
    });

    if (event.payload.emitEvidence !== false) {
      const evidence = await this.knowledge.evidence.createEvidence({
        tenantId: event.tenantId,
        sourceType: event.payload.sourceType || 'crm_or_scout',
        sourceId: event.payload.sourceId || event.id,
        summary: event.payload.evidenceSummary || `Observed company ${company.name || company.id}`,
        confidence: event.payload.confidence,
        payload: { companyId: company.id },
      });
      await this.knowledge.evidence.attachEvidence(event.tenantId, evidence.id, company.id);
    }

    return company;
  }

  async onPersonObserved(event) {
    const person = await this.knowledge.createNode({
      tenantId: event.tenantId,
      type: NODE_TYPES.PERSON,
      name: event.payload.name,
      email: event.payload.email,
      title: event.payload.title,
      metadata: {
        ...(event.payload.metadata || {}),
        ingestedFrom: event.type,
        sourceEventId: event.id,
      },
      id: event.payload.nodeId,
    });

    if (event.payload.companyId) {
      await this.knowledge.createEdge({
        tenantId: event.tenantId,
        type: EDGE_TYPES.WORKS_FOR,
        fromId: person.id,
        toId: event.payload.companyId,
      });
      await this.knowledge.createEdge({
        tenantId: event.tenantId,
        type: EDGE_TYPES.HAS_CONTACT,
        fromId: event.payload.companyId,
        toId: person.id,
      });
    }

    if (event.payload.emitEvidence !== false) {
      const evidence = await this.knowledge.evidence.createEvidence({
        tenantId: event.tenantId,
        sourceType: event.payload.sourceType || 'crm_or_scout',
        sourceId: event.payload.sourceId || event.id,
        summary: event.payload.evidenceSummary || `Observed person ${person.name || person.id}`,
        confidence: event.payload.confidence,
        payload: { personId: person.id },
      });
      await this.knowledge.evidence.attachEvidence(event.tenantId, evidence.id, person.id);
    }

    return person;
  }

  async onInteractionRecorded(event) {
    const interaction = await this.knowledge.createNode({
      tenantId: event.tenantId,
      type: NODE_TYPES.INTERACTION,
      channel: event.payload.channel,
      actionType: event.payload.actionType,
      summary: event.payload.summary,
      occurredAt: event.payload.occurredAt || event.occurredAt,
      metadata: {
        ...(event.payload.metadata || {}),
        ingestedFrom: event.type,
        sourceEventId: event.id,
      },
      id: event.payload.nodeId,
    });

    if (event.payload.participantId) {
      await this.knowledge.createEdge({
        tenantId: event.tenantId,
        type: EDGE_TYPES.PARTICIPATED_IN,
        fromId: event.payload.participantId,
        toId: interaction.id,
      });
    }

    return interaction;
  }

  async onEvidenceRecorded(event) {
    const evidence = await this.knowledge.evidence.createEvidence({
      tenantId: event.tenantId,
      sourceType: event.payload.sourceType,
      sourceId: event.payload.sourceId || event.id,
      summary: event.payload.summary,
      confidence: event.payload.confidence,
      payload: event.payload.payload,
      metadata: event.payload.metadata,
      id: event.payload.nodeId,
    });
    if (event.payload.subjectId) {
      await this.knowledge.evidence.attachEvidence(
        event.tenantId,
        evidence.id,
        event.payload.subjectId
      );
    }
    return evidence;
  }

  async onClaimProposed(event) {
    return this.knowledge.claims.createClaim({
      tenantId: event.tenantId,
      statement: event.payload.statement,
      subjectId: event.payload.subjectId,
      evidenceIds: event.payload.evidenceIds || [],
      reason: event.payload.reason,
      metadata: {
        ...(event.payload.metadata || {}),
        ingestedFrom: event.type,
        sourceEventId: event.id,
      },
    });
  }

  async onEdgeRequested(event) {
    return this.knowledge.createEdge({
      tenantId: event.tenantId,
      type: event.payload.type,
      fromId: event.payload.fromId,
      toId: event.payload.toId,
      metadata: event.payload.metadata,
    });
  }
}

module.exports = {
  KnowledgeIngestor,
};
