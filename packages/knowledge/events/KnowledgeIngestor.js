'use strict';

const { KNOWLEDGE_EVENTS } = require('./KnowledgeEventBus');
const { NODE_TYPES } = require('../types/nodeTypes');
const { EDGE_TYPES } = require('../edges/edgeTypes');
const { stableEvidenceId } = require('../sync/stableIds');

/**
 * Wires knowledge events → KnowledgeService.
 * This is the only path producers should use to mutate the graph.
 * Handlers are idempotent via ensureNode / ensureEdge / ensureEvidence.
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
    if (!event.payload.nodeId) {
      throw new Error('COMPANY_OBSERVED requires payload.nodeId for idempotent ingest');
    }
    const { node: company } = await this.knowledge.ensureNode({
      tenantId: event.tenantId,
      type: NODE_TYPES.COMPANY,
      id: event.payload.nodeId,
      name: event.payload.name,
      metadata: {
        ...(event.payload.metadata || {}),
        ingestedFrom: event.type,
        sourceEventId: event.id,
      },
    });

    if (event.payload.emitEvidence !== false) {
      const sourceType = event.payload.sourceType || 'crm_or_scout';
      const sourceId = String(event.payload.sourceId || event.id);
      const evidenceId =
        event.payload.evidenceId ||
        stableEvidenceId(event.tenantId, sourceType, sourceId);
      const { node: evidence } = await this.knowledge.evidence.ensureEvidence({
        tenantId: event.tenantId,
        id: evidenceId,
        sourceType,
        sourceId,
        summary: event.payload.evidenceSummary || `Observed company ${company.name || company.id}`,
        confidence: event.payload.confidence,
        payload: { companyId: company.id },
      });
      await this._ensureAbout(event.tenantId, evidence.id, company.id);
    }

    return company;
  }

  async onPersonObserved(event) {
    if (!event.payload.nodeId) {
      throw new Error('PERSON_OBSERVED requires payload.nodeId for idempotent ingest');
    }
    const { node: person } = await this.knowledge.ensureNode({
      tenantId: event.tenantId,
      type: NODE_TYPES.PERSON,
      id: event.payload.nodeId,
      name: event.payload.name,
      email: event.payload.email,
      title: event.payload.title,
      metadata: {
        ...(event.payload.metadata || {}),
        ingestedFrom: event.type,
        sourceEventId: event.id,
      },
    });

    if (event.payload.companyId) {
      await this.knowledge.ensureEdge({
        tenantId: event.tenantId,
        type: EDGE_TYPES.WORKS_FOR,
        fromId: person.id,
        toId: event.payload.companyId,
      });
      await this.knowledge.ensureEdge({
        tenantId: event.tenantId,
        type: EDGE_TYPES.HAS_CONTACT,
        fromId: event.payload.companyId,
        toId: person.id,
      });
    }

    if (event.payload.emitEvidence !== false) {
      const sourceType = event.payload.sourceType || 'crm_or_scout';
      const sourceId = String(event.payload.sourceId || event.id);
      const evidenceId =
        event.payload.evidenceId ||
        stableEvidenceId(event.tenantId, sourceType, sourceId);
      const { node: evidence } = await this.knowledge.evidence.ensureEvidence({
        tenantId: event.tenantId,
        id: evidenceId,
        sourceType,
        sourceId,
        summary: event.payload.evidenceSummary || `Observed person ${person.name || person.id}`,
        confidence: event.payload.confidence,
        payload: { personId: person.id },
      });
      await this._ensureAbout(event.tenantId, evidence.id, person.id);
    }

    return person;
  }

  async onInteractionRecorded(event) {
    if (!event.payload.nodeId) {
      throw new Error('INTERACTION_RECORDED requires payload.nodeId for idempotent ingest');
    }
    const { node: interaction } = await this.knowledge.ensureNode({
      tenantId: event.tenantId,
      type: NODE_TYPES.INTERACTION,
      id: event.payload.nodeId,
      channel: event.payload.channel,
      actionType: event.payload.actionType,
      summary: event.payload.summary,
      occurredAt: event.payload.occurredAt || event.occurredAt,
      metadata: {
        ...(event.payload.metadata || {}),
        ingestedFrom: event.type,
        sourceEventId: event.id,
      },
    });

    if (event.payload.participantId) {
      await this.knowledge.ensureEdge({
        tenantId: event.tenantId,
        type: EDGE_TYPES.PARTICIPATED_IN,
        fromId: event.payload.participantId,
        toId: interaction.id,
      });
    }

    return interaction;
  }

  async onEvidenceRecorded(event) {
    const sourceType = event.payload.sourceType;
    const sourceId = String(event.payload.sourceId || event.id);
    const evidenceId =
      event.payload.nodeId ||
      stableEvidenceId(event.tenantId, sourceType, sourceId);
    const { node: evidence } = await this.knowledge.evidence.ensureEvidence({
      tenantId: event.tenantId,
      id: evidenceId,
      sourceType,
      sourceId,
      summary: event.payload.summary,
      confidence: event.payload.confidence,
      payload: event.payload.payload,
      metadata: event.payload.metadata,
    });
    if (event.payload.subjectId) {
      await this._ensureAbout(event.tenantId, evidence.id, event.payload.subjectId);
    }
    return evidence;
  }

  async onClaimProposed(event) {
    // Claims are not yet ensure-stable by statement; sync engine uses ledger to avoid re-propose.
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
    const { edge } = await this.knowledge.ensureEdge({
      tenantId: event.tenantId,
      type: event.payload.type,
      fromId: event.payload.fromId,
      toId: event.payload.toId,
      metadata: event.payload.metadata,
    });
    return edge;
  }

  async _ensureAbout(tenantId, evidenceId, subjectId) {
    await this.knowledge.ensureEdge({
      tenantId,
      type: EDGE_TYPES.ABOUT,
      fromId: evidenceId,
      toId: subjectId,
      metadata: { role: 'subject' },
    });
    await this.knowledge.ensureEdge({
      tenantId,
      type: EDGE_TYPES.GENERATED,
      fromId: subjectId,
      toId: evidenceId,
      metadata: { role: 'produced_evidence' },
    });
  }
}

module.exports = {
  KnowledgeIngestor,
};
