'use strict';

const { createNodeByType, updateNodeByType } = require('../nodes');
const { isNodeType } = require('../types/nodeTypes');
const { isEdgeType } = require('../edges/edgeTypes');
const { assertGraphRepository } = require('../repositories/GraphRepository');
const { EvidenceEngine } = require('../evidence/EvidenceEngine');
const { ClaimEngine } = require('../claims/ClaimEngine');
const { QueryEngine } = require('../query/QueryEngine');

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
   * @param {QueryEngine} [deps.queryEngine]
   * @param {(m: object) => void} [deps.onQueryMetrics]
   */
  constructor(deps) {
    assertGraphRepository(deps.repository);
    this._repository = deps.repository;
    this.evidence = deps.evidenceEngine || new EvidenceEngine(deps.repository);
    this.claims = deps.claimEngine || new ClaimEngine(deps.repository);
    this._query =
      deps.queryEngine ||
      new QueryEngine({
        repository: deps.repository,
        onMetrics: deps.onQueryMetrics,
      });
  }

  /** @returns {import('../query/QueryTypes').QueryMetrics|null} */
  getLastQueryMetrics() {
    return this._query.getLastMetrics();
  }

  /** @returns {import('../query/QueryTypes').QueryMetrics[]} */
  getQueryMetricsHistory() {
    return this._query.getMetricsHistory();
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
   * Idempotent create-or-update by stable node id.
   * Used by the sync engine and event replay — never touches storage directly outside this service.
   *
   * @param {object} input
   * @returns {Promise<{ node: object, created: boolean }>}
   */
  async ensureNode(input) {
    requireTenant(input.tenantId);
    if (!isNodeType(input.type)) {
      throw new Error(`Unknown node type: ${input.type}`);
    }
    if (!input.id) {
      throw new Error('ensureNode requires a stable id');
    }
    const existing = await this.findNode(input.tenantId, input.id);
    if (!existing) {
      const created = await this.createNode(input);
      return { node: created, created: true };
    }
    if (existing.type !== input.type) {
      throw new Error(
        `ensureNode type conflict for ${input.id}: existing=${existing.type} requested=${input.type}`
      );
    }
    const { id: _id, tenantId: _tenantId, type: _type, ...patch } = input;
    const updated = await this.updateNode(input.tenantId, input.id, patch);
    return { node: updated, created: false };
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
   * Idempotent edge ensure: returns existing same-type edge between endpoints if present.
   *
   * @param {object} input
   * @returns {Promise<{ edge: object, created: boolean }>}
   */
  async ensureEdge(input) {
    requireTenant(input.tenantId);
    if (!isEdgeType(input.type)) {
      throw new Error(`Unknown edge type: ${input.type}`);
    }
    const outbound = await this.findNeighbors(input.tenantId, input.fromId, {
      direction: 'out',
      edgeType: input.type,
    });
    const existing = outbound.find((n) => n.node.id === input.toId);
    if (existing) {
      return { edge: existing.edge, created: false };
    }
    const edge = await this.createEdge(input);
    return { edge, created: true };
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

  // ─── Query Engine API (SPEC-001C) ───────────────────────────────────────────

  /** @param {import('../query/QueryTypes').CompanyQuery} filter */
  async findCompanies(filter) {
    return this._query.findCompanies(filter);
  }

  /** @param {import('../query/QueryTypes').PersonQuery} filter */
  async findPeople(filter) {
    return this._query.findPeople(filter);
  }

  /** @param {import('../query/QueryTypes').InteractionQuery} filter */
  async findInteractions(filter) {
    return this._query.findInteractions(filter);
  }

  /**
   * Evidence query. Accepts EvidenceQuery object, or legacy (tenantId, query).
   * @param {import('../query/QueryTypes').EvidenceQuery|string} filterOrTenantId
   * @param {object} [maybeQuery]
   */
  async findEvidence(filterOrTenantId, maybeQuery) {
    const filter = normalizeTenantQuery(filterOrTenantId, maybeQuery, 'EvidenceQuery');
    return this._query.findEvidence(filter);
  }

  /**
   * Claim query. Accepts ClaimQuery object, or legacy (tenantId, query).
   * @param {import('../query/QueryTypes').ClaimQuery|string} filterOrTenantId
   * @param {object} [maybeQuery]
   */
  async findClaims(filterOrTenantId, maybeQuery) {
    const filter = normalizeTenantQuery(filterOrTenantId, maybeQuery, 'ClaimQuery');
    return this._query.findClaims(filter);
  }

  /**
   * Neighbor traversal via query object.
   * @param {import('../query/QueryTypes').NeighborQuery} query
   */
  async neighbors(query) {
    return this._query.neighbors(query);
  }

  /**
   * Multi-hop related nodes (depth-limited, deterministic).
   * @param {import('../query/QueryTypes').RelatedQuery} query
   */
  async related(query) {
    return this._query.related(query);
  }

  /**
   * Chronological history for a node.
   * @param {import('../query/QueryTypes').TimelineQuery} query
   */
  async timeline(query) {
    return this._query.timeline(query);
  }

  /**
   * Shortest path between two nodes.
   * @param {import('../query/QueryTypes').PathQuery} query
   */
  async path(query) {
    return this._query.path(query);
  }

  /**
   * Explainability chain for Max:
   * Claim → Evidence → Original Source → Confidence → Timeline Position → Reason
   *
   * Accepts ExplainQuery `{ tenantId, nodeId }` or legacy `(tenantId, nodeId)`.
   *
   * @param {import('../query/QueryTypes').ExplainQuery|string} queryOrTenantId
   * @param {string} [maybeNodeId]
   */
  async explain(queryOrTenantId, maybeNodeId) {
    const query =
      typeof queryOrTenantId === 'string'
        ? { tenantId: queryOrTenantId, nodeId: maybeNodeId }
        : queryOrTenantId;
    requireTenant(query.tenantId);
    if (!query.nodeId) throw new Error('explain requires nodeId');
    return this._query.explain(query);
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
}

function requireTenant(tenantId) {
  if (tenantId == null || tenantId === '') {
    throw new Error('tenantId is required');
  }
}

/**
 * @param {object|string} filterOrTenantId
 * @param {object} [maybeQuery]
 * @param {string} label
 */
function normalizeTenantQuery(filterOrTenantId, maybeQuery, label) {
  if (typeof filterOrTenantId === 'string') {
    requireTenant(filterOrTenantId);
    return { ...(maybeQuery || {}), tenantId: filterOrTenantId };
  }
  if (!filterOrTenantId || typeof filterOrTenantId !== 'object') {
    throw new Error(`${label} requires a query object with tenantId`);
  }
  requireTenant(filterOrTenantId.tenantId);
  return filterOrTenantId;
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
