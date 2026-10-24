'use strict';

const { createEvidenceNode, updateEvidenceNode } = require('../nodes/Evidence');
const { calculateConfidenceFromEvidence, combineConfidences } = require('../confidence/calculateConfidence');
const { EDGE_TYPES } = require('../edges/edgeTypes');
const { NODE_TYPES } = require('../types/nodeTypes');

/**
 * EvidenceEngine — create, attach, merge evidence; calculate confidence.
 * Mutates graph only through a GraphRepository.
 */
class EvidenceEngine {
  /**
   * @param {import('../repositories/GraphRepository').GraphRepository} repository
   */
  constructor(repository) {
    this.repository = repository;
  }

  /**
   * @param {object} input
   */
  async createEvidence(input) {
    const node = createEvidenceNode(input);
    return this.repository.createNode(node);
  }

  /**
   * Idempotent evidence upsert by stable id.
   *
   * @param {object} input
   * @returns {Promise<{ node: object, created: boolean }>}
   */
  async ensureEvidence(input) {
    if (!input.id) {
      throw new Error('ensureEvidence requires a stable id');
    }
    const existingRows = await this.repository.find(input.tenantId, { id: input.id });
    const existing = existingRows[0];
    if (!existing) {
      const created = await this.createEvidence(input);
      return { node: created, created: true };
    }
    if (existing.type !== NODE_TYPES.EVIDENCE) {
      throw new Error(`ensureEvidence id conflict: ${input.id} is not evidence`);
    }
    const updated = updateEvidenceNode(existing, {
      sourceType: input.sourceType,
      sourceId: input.sourceId,
      summary: input.summary,
      confidence: input.confidence,
      payload: input.payload,
      metadata: input.metadata,
    });
    const saved = await this.repository.updateNode(input.tenantId, input.id, updated);
    return { node: saved, created: false };
  }

  /**
   * Attach evidence to a subject via GENERATED (subject ← evidence) or ABOUT (evidence → subject).
   * Default: Evidence -ABOUT-> subject, and optional subject -GENERATED-> evidence for provenance walk.
   *
   * @param {string} tenantId
   * @param {string} evidenceId
   * @param {string} subjectId
   * @param {{ alsoGenerated?: boolean }} [options]
   */
  async attachEvidence(tenantId, evidenceId, subjectId, options = {}) {
    const found = await this.repository.find(tenantId, { ids: [evidenceId, subjectId] });
    const byId = new Map(found.map((n) => [n.id, n]));
    if (!byId.has(evidenceId) || byId.get(evidenceId).type !== NODE_TYPES.EVIDENCE) {
      throw new Error('attachEvidence: evidenceId must reference an Evidence node');
    }
    if (!byId.has(subjectId)) {
      throw new Error('attachEvidence: subjectId not found in tenant');
    }

    const about = await this.repository.createEdge({
      tenantId,
      type: EDGE_TYPES.ABOUT,
      fromId: evidenceId,
      toId: subjectId,
      metadata: { role: 'subject' },
    });

    let generated = null;
    if (options.alsoGenerated !== false) {
      generated = await this.repository.createEdge({
        tenantId,
        type: EDGE_TYPES.GENERATED,
        fromId: subjectId,
        toId: evidenceId,
        metadata: { role: 'produced_evidence' },
      });
    }

    return { about, generated };
  }

  /**
   * @param {Array<object>} evidenceNodes
   */
  calculateConfidence(evidenceNodes) {
    return calculateConfidenceFromEvidence(evidenceNodes);
  }

  /**
   * Merge two evidence nodes about the same subject into a survivor with combined confidence.
   *
   * @param {string} tenantId
   * @param {string} primaryId
   * @param {string} secondaryId
   */
  async mergeEvidence(tenantId, primaryId, secondaryId) {
    const nodes = await this.repository.find(tenantId, { ids: [primaryId, secondaryId] });
    const byId = new Map(nodes.map((n) => [n.id, n]));
    const primary = byId.get(primaryId);
    const secondary = byId.get(secondaryId);
    if (!primary || primary.type !== NODE_TYPES.EVIDENCE) {
      throw new Error('mergeEvidence: primary must be Evidence');
    }
    if (!secondary || secondary.type !== NODE_TYPES.EVIDENCE) {
      throw new Error('mergeEvidence: secondary must be Evidence');
    }

    const confidence = combineConfidences([primary.confidence, secondary.confidence]);
    const merged = updateEvidenceNode(primary, {
      confidence,
      summary: primary.summary || secondary.summary,
      payload: {
        ...(secondary.payload || {}),
        ...(primary.payload || {}),
        mergedFrom: [...(primary.payload?.mergedFrom || []), secondaryId],
      },
      metadata: {
        ...(secondary.metadata || {}),
        ...(primary.metadata || {}),
        mergedAt: new Date().toISOString(),
      },
    });

    const saved = await this.repository.updateNode(tenantId, primaryId, merged);

    // Re-point secondary ABOUT edges to keep subject links; then delete secondary node.
    const secondaryNeighbors = await this.repository.neighbors(tenantId, secondaryId, {
      direction: 'out',
      edgeType: EDGE_TYPES.ABOUT,
    });
    for (const { node: subject } of secondaryNeighbors) {
      const primaryAbout = await this.repository.neighbors(tenantId, primaryId, {
        direction: 'out',
        edgeType: EDGE_TYPES.ABOUT,
      });
      const already = primaryAbout.some((n) => n.node.id === subject.id);
      if (!already) {
        await this.repository.createEdge({
          tenantId,
          type: EDGE_TYPES.ABOUT,
          fromId: primaryId,
          toId: subject.id,
        });
      }
    }

    await this.repository.deleteNode(tenantId, secondaryId);
    return saved;
  }
}

module.exports = {
  EvidenceEngine,
};
