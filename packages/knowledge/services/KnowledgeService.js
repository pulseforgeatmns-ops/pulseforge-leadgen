'use strict';

const { createNodeByType, updateNodeByType } = require('../nodes');
const { NODE_TYPES, isNodeType } = require('../types/nodeTypes');
const { isEdgeType } = require('../edges/edgeTypes');
const { EDGE_TYPES } = require('../edges/edgeTypes');
const { assertGraphRepository } = require('../repositories/GraphRepository');
const { EvidenceEngine } = require('../evidence/EvidenceEngine');
const { ClaimEngine } = require('../claims/ClaimEngine');

/**
 * KnowledgeService — the only public API for graph operations.
 * No storage implementation details are exposed.
 */
class KnowledgeService {
  /**
   * @param {object} deps
   * @param {import('../repositories/GraphRepository').GraphRepository} deps.repository
   * @param {EvidenceEngine} [deps.evidenceEngine]
   * @param {ClaimEngine} [deps.claimEngine]
   */
  constructor(deps) {
    assertGraphRepository(deps.repository);
    this._repository = deps.repository;
    this.evidence = deps.evidenceEngine || new EvidenceEngine(deps.repository);
    this.claims = deps.claimEngine || new ClaimEngine(deps.repository);
  }

  /**
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} input.type
   * @param {Record<string, unknown>} [input.metadata]
   */
  async createNode(input) {
    requireTenant(input.tenantId);
    if (!isNodeType(input.type)) {
      throw new Error(`Unknown node type: ${input.type}`);
    }
    const node = createNodeByType(input.type, input);
    return this._repository.createNode(node);
  }

  /**
   * @param {string} tenantId
   * @param {string} nodeId
   * @param {object} patch
   */
  async updateNode(tenantId, nodeId, patch) {
    requireTenant(tenantId);
    const existing = await this.findNode(tenantId, nodeId);
    if (!existing) return null;
    const updated = updateNodeByType(existing, patch || {});
    return this._repository.updateNode(tenantId, nodeId, updated);
  }

  /**
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} input.type
   * @param {string} input.fromId
   * @param {string} input.toId
   * @param {Record<string, unknown>} [input.metadata]
   */
  async createEdge(input) {
    requireTenant(input.tenantId);
    if (!isEdgeType(input.type)) {
      throw new Error(`Unknown edge type: ${input.type}`);
    }
    return this._repository.createEdge(input);
  }

  /**
   * @param {string} tenantId
   * @param {string} nodeId
   */
  async findNode(tenantId, nodeId) {
    requireTenant(tenantId);
    const rows = await this._repository.find(tenantId, { id: nodeId });
    return rows[0] || null;
  }

  /**
   * @param {string} tenantId
   * @param {string} nodeId
   * @param {object} [options]
   */
  async findNeighbors(tenantId, nodeId, options) {
    requireTenant(tenantId);
    return this._repository.neighbors(tenantId, nodeId, options);
  }

  /**
   * @param {string} tenantId
   * @param {object} [query]
   */
  async findEvidence(tenantId, query = {}) {
    requireTenant(tenantId);
    return this._repository.find(tenantId, { ...query, type: NODE_TYPES.EVIDENCE });
  }

  /**
   * @param {string} tenantId
   * @param {object} [query]
   */
  async findClaims(tenantId, query = {}) {
    requireTenant(tenantId);
    return this._repository.find(tenantId, { ...query, type: NODE_TYPES.CLAIM });
  }

  /**
   * Explainability chain for Max:
   * Claim → Evidence → Original Source → Confidence → Reason
   *
   * @param {string} tenantId
   * @param {string} nodeId
   */
  async explain(tenantId, nodeId) {
    requireTenant(tenantId);
    const node = await this.findNode(tenantId, nodeId);
    if (!node) {
      return null;
    }

    if (node.type === NODE_TYPES.CLAIM) {
      return this._explainClaim(tenantId, node);
    }
    if (node.type === NODE_TYPES.EVIDENCE) {
      return this._explainEvidence(node);
    }

    // Subject node: gather claims ABOUT it, else evidence ABOUT it.
    const claimLinks = await this.findNeighbors(tenantId, nodeId, {
      direction: 'in',
      edgeType: EDGE_TYPES.ABOUT,
    });
    const claims = claimLinks
      .map((l) => l.node)
      .filter((n) => n.type === NODE_TYPES.CLAIM && n.status === 'active');

    if (claims.length > 0) {
      const claimExplanations = [];
      for (const claim of claims) {
        claimExplanations.push(await this._explainClaim(tenantId, claim));
      }
      return {
        subject: summarizeNode(node),
        claims: claimExplanations,
        confidence: maxConfidence(claimExplanations.map((c) => c.confidence)),
        reason: `Subject has ${claims.length} active claim(s)`,
      };
    }

    const evidence = claimLinks
      .map((l) => l.node)
      .filter((n) => n.type === NODE_TYPES.EVIDENCE);

    return {
      subject: summarizeNode(node),
      claims: [],
      evidence: evidence.map((e) => this._explainEvidence(e)),
      confidence: maxConfidence(evidence.map((e) => e.confidence)),
      reason:
        evidence.length > 0
          ? `Subject has ${evidence.length} attached evidence node(s) and no claims`
          : 'No claims or evidence attached',
    };
  }

  /**
   * Lightweight search across node fields for a tenant.
   *
   * @param {string} tenantId
   * @param {string} text
   * @param {{ types?: string[], limit?: number }} [options]
   */
  async search(tenantId, text, options = {}) {
    requireTenant(tenantId);
    const q = String(text || '').trim().toLowerCase();
    if (!q) return [];
    const types = options.types || null;
    const limit = options.limit || 50;
    const all = await this._repository.find(tenantId, {
      predicate: (node) => {
        if (types && !types.includes(node.type)) return false;
        return nodeMatchesQuery(node, q);
      },
    });
    return all.slice(0, limit);
  }

  async _explainClaim(tenantId, claim) {
    const supporting = await this.findNeighbors(tenantId, claim.id, {
      direction: 'in',
      edgeType: EDGE_TYPES.SUPPORTS,
    });
    const evidence = supporting
      .map((l) => l.node)
      .filter((n) => n.type === NODE_TYPES.EVIDENCE)
      .map((e) => this._explainEvidence(e));

    return {
      claim: {
        id: claim.id,
        statement: claim.statement,
        status: claim.status,
      },
      evidence,
      originalSources: evidence.map((e) => e.originalSource),
      confidence: claim.confidence,
      reason: claim.reason,
    };
  }

  _explainEvidence(evidence) {
    return {
      evidence: {
        id: evidence.id,
        summary: evidence.summary,
        confidence: evidence.confidence,
      },
      originalSource: {
        sourceType: evidence.sourceType,
        sourceId: evidence.sourceId,
      },
      confidence: evidence.confidence,
      reason: evidence.summary || `Evidence from ${evidence.sourceType}`,
    };
  }
}

function requireTenant(tenantId) {
  if (tenantId == null || tenantId === '') {
    throw new Error('tenantId is required');
  }
}

function summarizeNode(node) {
  return {
    id: node.id,
    type: node.type,
    name: node.name || null,
    statement: node.statement || null,
  };
}

function maxConfidence(values) {
  if (!values || values.length === 0) return 0;
  return Math.max(...values.map((v) => Number(v) || 0));
}

function nodeMatchesQuery(node, q) {
  const haystacks = [
    node.name,
    node.email,
    node.title,
    node.summary,
    node.statement,
    node.sourceType,
    node.sourceId,
    node.channel,
    node.actionType,
    JSON.stringify(node.metadata || {}),
  ];
  return haystacks.some((h) => h && String(h).toLowerCase().includes(q));
}

module.exports = {
  KnowledgeService,
};
