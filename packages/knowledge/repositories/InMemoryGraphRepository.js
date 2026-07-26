'use strict';

const { randomUUID } = require('crypto');
const { isEdgeType } = require('../edges/edgeTypes');

/**
 * In-memory GraphRepository — SPEC-001A only storage backend.
 * Tenant isolation is enforced on every operation.
 */
class InMemoryGraphRepository {
  constructor() {
    /** @type {Map<string, { nodes: Map<string, object>, edges: Map<string, object> }>} */
    this._tenants = new Map();
  }

  _bucket(tenantId) {
    const key = String(tenantId);
    if (!this._tenants.has(key)) {
      this._tenants.set(key, {
        nodes: new Map(),
        edges: new Map(),
      });
    }
    return this._tenants.get(key);
  }

  async createNode(node) {
    if (!node || !node.tenantId || !node.id) {
      throw new Error('createNode requires node.id and node.tenantId');
    }
    const bucket = this._bucket(node.tenantId);
    if (bucket.nodes.has(node.id)) {
      throw new Error(`Node already exists: ${node.id}`);
    }
    const stored = structuredClone(node);
    bucket.nodes.set(node.id, stored);
    return structuredClone(stored);
  }

  async updateNode(tenantId, nodeId, patch) {
    const bucket = this._bucket(tenantId);
    const existing = bucket.nodes.get(nodeId);
    if (!existing || existing.tenantId !== String(tenantId)) {
      return null;
    }
    // Caller supplies fully updated node (KnowledgeService applies domain updaters).
    const next = structuredClone({ ...patch, id: existing.id, tenantId: existing.tenantId, type: existing.type });
    bucket.nodes.set(nodeId, next);
    return structuredClone(next);
  }

  async deleteNode(tenantId, nodeId) {
    const bucket = this._bucket(tenantId);
    const existing = bucket.nodes.get(nodeId);
    if (!existing || existing.tenantId !== String(tenantId)) {
      return false;
    }
    bucket.nodes.delete(nodeId);
    for (const [edgeId, edge] of [...bucket.edges.entries()]) {
      if (edge.fromId === nodeId || edge.toId === nodeId) {
        bucket.edges.delete(edgeId);
      }
    }
    return true;
  }

  async createEdge(edge) {
    if (!edge || !edge.tenantId || !edge.fromId || !edge.toId || !edge.type) {
      throw new Error('createEdge requires tenantId, fromId, toId, type');
    }
    if (!isEdgeType(edge.type)) {
      throw new Error(`Unknown edge type: ${edge.type}`);
    }
    const bucket = this._bucket(edge.tenantId);
    if (!bucket.nodes.has(edge.fromId) || !bucket.nodes.has(edge.toId)) {
      throw new Error('createEdge requires both endpoints to exist in tenant');
    }
    const id = edge.id || randomUUID();
    const stored = {
      id,
      tenantId: String(edge.tenantId),
      type: edge.type,
      fromId: edge.fromId,
      toId: edge.toId,
      metadata: edge.metadata && typeof edge.metadata === 'object' ? { ...edge.metadata } : {},
      createdAt: edge.createdAt || new Date().toISOString(),
    };
    bucket.edges.set(id, stored);
    return structuredClone(stored);
  }

  async deleteEdge(tenantId, edgeId) {
    const bucket = this._bucket(tenantId);
    const existing = bucket.edges.get(edgeId);
    if (!existing || existing.tenantId !== String(tenantId)) {
      return false;
    }
    bucket.edges.delete(edgeId);
    return true;
  }

  /**
   * @param {string} tenantId
   * @param {string} nodeId
   * @param {{ direction?: 'out'|'in'|'both', edgeType?: string }} [options]
   */
  async neighbors(tenantId, nodeId, options = {}) {
    const bucket = this._bucket(tenantId);
    const node = bucket.nodes.get(nodeId);
    if (!node || node.tenantId !== String(tenantId)) {
      return [];
    }
    const direction = options.direction || 'both';
    const edgeType = options.edgeType || null;
    const results = [];

    for (const edge of bucket.edges.values()) {
      if (edgeType && edge.type !== edgeType) continue;
      let neighborId = null;
      let dir = null;
      if ((direction === 'out' || direction === 'both') && edge.fromId === nodeId) {
        neighborId = edge.toId;
        dir = 'out';
      } else if ((direction === 'in' || direction === 'both') && edge.toId === nodeId) {
        neighborId = edge.fromId;
        dir = 'in';
      }
      if (!neighborId) continue;
      const neighbor = bucket.nodes.get(neighborId);
      if (!neighbor) continue;
      results.push({
        direction: dir,
        edge: structuredClone(edge),
        node: structuredClone(neighbor),
      });
    }
    return results;
  }

  /**
   * @param {string} tenantId
   * @param {{ id?: string, type?: string, ids?: string[], predicate?: (node: object) => boolean }} query
   */
  async find(tenantId, query = {}) {
    const bucket = this._bucket(tenantId);
    const out = [];
    for (const node of bucket.nodes.values()) {
      if (node.tenantId !== String(tenantId)) continue;
      if (query.id && node.id !== query.id) continue;
      if (query.ids && !query.ids.includes(node.id)) continue;
      if (query.type && node.type !== query.type) continue;
      if (typeof query.predicate === 'function' && !query.predicate(node)) continue;
      out.push(structuredClone(node));
    }
    return out;
  }

  /** Test helper */
  clear() {
    this._tenants.clear();
  }
}

module.exports = {
  InMemoryGraphRepository,
};
