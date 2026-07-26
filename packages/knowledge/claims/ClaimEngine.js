'use strict';

const { createClaimNode, updateClaimNode } = require('../nodes/Claim');
const { calculateConfidenceFromEvidence } = require('../confidence/calculateConfidence');
const { EDGE_TYPES } = require('../edges/edgeTypes');
const { NODE_TYPES } = require('../types/nodeTypes');

/**
 * ClaimEngine — create, evaluate, invalidate, merge claims.
 */
class ClaimEngine {
  /**
   * @param {import('../repositories/GraphRepository').GraphRepository} repository
   */
  constructor(repository) {
    this.repository = repository;
  }

  /**
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} input.statement
   * @param {string} [input.subjectId] - node the claim is ABOUT
   * @param {string[]} [input.evidenceIds] - evidence that SUPPORTS the claim
   * @param {string} [input.reason]
   * @param {Record<string, unknown>} [input.metadata]
   */
  async createClaim(input) {
    const evidenceNodes = input.evidenceIds?.length
      ? await this.repository.find(input.tenantId, {
          ids: input.evidenceIds,
          type: NODE_TYPES.EVIDENCE,
        })
      : [];
    const scored = calculateConfidenceFromEvidence(evidenceNodes);
    const claim = createClaimNode({
      tenantId: input.tenantId,
      statement: input.statement,
      confidence: scored.confidence,
      reason: input.reason || scored.reason,
      metadata: input.metadata,
      status: 'active',
    });
    const saved = await this.repository.createNode(claim);

    if (input.subjectId) {
      await this.repository.createEdge({
        tenantId: input.tenantId,
        type: EDGE_TYPES.ABOUT,
        fromId: saved.id,
        toId: input.subjectId,
      });
    }

    for (const evidence of evidenceNodes) {
      await this.repository.createEdge({
        tenantId: input.tenantId,
        type: EDGE_TYPES.SUPPORTS,
        fromId: evidence.id,
        toId: saved.id,
      });
    }

    return saved;
  }

  /**
   * Recompute confidence/reason from current SUPPORTS evidence.
   *
   * @param {string} tenantId
   * @param {string} claimId
   */
  async evaluateClaim(tenantId, claimId) {
    const found = await this.repository.find(tenantId, { id: claimId, type: NODE_TYPES.CLAIM });
    const claim = found[0];
    if (!claim) {
      throw new Error(`Claim not found: ${claimId}`);
    }
    if (claim.status === 'invalidated') {
      return claim;
    }

    const supporting = await this.repository.neighbors(tenantId, claimId, {
      direction: 'in',
      edgeType: EDGE_TYPES.SUPPORTS,
    });
    const evidenceNodes = supporting
      .map((n) => n.node)
      .filter((n) => n.type === NODE_TYPES.EVIDENCE);
    const scored = calculateConfidenceFromEvidence(evidenceNodes);
    const updated = updateClaimNode(claim, {
      confidence: scored.confidence,
      reason: scored.reason,
      status: 'active',
    });
    return this.repository.updateNode(tenantId, claimId, updated);
  }

  /**
   * @param {string} tenantId
   * @param {string} claimId
   * @param {string} [reason]
   */
  async invalidateClaim(tenantId, claimId, reason) {
    const found = await this.repository.find(tenantId, { id: claimId, type: NODE_TYPES.CLAIM });
    const claim = found[0];
    if (!claim) {
      throw new Error(`Claim not found: ${claimId}`);
    }
    const updated = updateClaimNode(claim, {
      status: 'invalidated',
      reason: reason || claim.reason || 'Invalidated',
      confidence: 0,
    });
    return this.repository.updateNode(tenantId, claimId, updated);
  }

  /**
   * Keep primary claim; fold secondary evidence SUPPORTS onto primary; invalidate secondary.
   *
   * @param {string} tenantId
   * @param {string} primaryId
   * @param {string} secondaryId
   */
  async mergeClaims(tenantId, primaryId, secondaryId) {
    const claims = await this.repository.find(tenantId, {
      ids: [primaryId, secondaryId],
      type: NODE_TYPES.CLAIM,
    });
    const byId = new Map(claims.map((c) => [c.id, c]));
    const primary = byId.get(primaryId);
    const secondary = byId.get(secondaryId);
    if (!primary || !secondary) {
      throw new Error('mergeClaims: both claims must exist');
    }

    const secondarySupport = await this.repository.neighbors(tenantId, secondaryId, {
      direction: 'in',
      edgeType: EDGE_TYPES.SUPPORTS,
    });
    const primarySupport = await this.repository.neighbors(tenantId, primaryId, {
      direction: 'in',
      edgeType: EDGE_TYPES.SUPPORTS,
    });
    const primaryEvidenceIds = new Set(primarySupport.map((n) => n.node.id));

    for (const { node: evidence } of secondarySupport) {
      if (primaryEvidenceIds.has(evidence.id)) continue;
      await this.repository.createEdge({
        tenantId,
        type: EDGE_TYPES.SUPPORTS,
        fromId: evidence.id,
        toId: primaryId,
      });
    }

    await this.invalidateClaim(
      tenantId,
      secondaryId,
      `Merged into claim ${primaryId}`
    );
    const invalidated = updateClaimNode(
      (await this.repository.find(tenantId, { id: secondaryId }))[0],
      { status: 'merged' }
    );
    await this.repository.updateNode(tenantId, secondaryId, invalidated);

    return this.evaluateClaim(tenantId, primaryId);
  }
}

module.exports = {
  ClaimEngine,
};
