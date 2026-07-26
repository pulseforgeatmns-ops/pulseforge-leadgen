'use strict';

const { randomUUID } = require('crypto');
const { isEdgeType } = require('../edges/edgeTypes');
const { NODE_TYPES } = require('../types/nodeTypes');

const CORE_NODE_KEYS = new Set([
  'id',
  'tenantId',
  'type',
  'createdAt',
  'updatedAt',
  'metadata',
]);

/**
 * Postgres-backed GraphRepository (SPEC-001).
 * Implements the exact GraphRepository contract — no extra methods required by KnowledgeService.
 */
class PersistentGraphRepository {
  /**
   * @param {{ query: Function }} pool
   */
  constructor(pool) {
    if (!pool || typeof pool.query !== 'function') {
      throw new Error('PersistentGraphRepository requires a pg pool');
    }
    this.pool = pool;
  }

  async createNode(node) {
    if (!node || !node.tenantId || !node.id) {
      throw new Error('createNode requires node.id and node.tenantId');
    }
    const table = tableForType(node.type);
    const row = serializeNode(node);
    try {
      await this.pool.query(
        `INSERT INTO ${table}
          (tenant_id, id, ${table === 'knowledge_nodes' ? 'type, ' : ''}created_at, updated_at, metadata, body)
         VALUES ($1, $2, ${table === 'knowledge_nodes' ? '$3, $4, $5, $6, $7' : '$3, $4, $5, $6'})`,
        table === 'knowledge_nodes'
          ? [
              row.tenant_id,
              row.id,
              row.type,
              row.created_at,
              row.updated_at,
              row.metadata,
              row.body,
            ]
          : [
              row.tenant_id,
              row.id,
              row.created_at,
              row.updated_at,
              row.metadata,
              row.body,
            ]
      );
    } catch (err) {
      if (err && err.code === '23505') {
        throw new Error(`Node already exists: ${node.id}`);
      }
      throw err;
    }
    return deserializeNode(row);
  }

  async updateNode(tenantId, nodeId, patch) {
    const existing = await this._getNode(tenantId, nodeId);
    if (!existing) return null;
    const next = {
      ...patch,
      id: existing.id,
      tenantId: existing.tenantId,
      type: existing.type,
    };
    const row = serializeNode(next);
    const table = tableForType(existing.type);
    const result = await this.pool.query(
      `UPDATE ${table}
       SET created_at = $3,
           updated_at = $4,
           metadata = $5::jsonb,
           body = $6::jsonb
           ${table === 'knowledge_nodes' ? ', type = type' : ''}
       WHERE tenant_id = $1 AND id = $2
       RETURNING tenant_id, id, ${table === 'knowledge_nodes' ? 'type,' : ''} created_at, updated_at, metadata, body`,
      [
        String(tenantId),
        nodeId,
        row.created_at,
        row.updated_at,
        row.metadata,
        row.body,
      ]
    );
    if (!result.rows[0]) return null;
    return deserializeNode(normalizeRow(result.rows[0], existing.type));
  }

  async deleteNode(tenantId, nodeId) {
    const existing = await this._getNode(tenantId, nodeId);
    if (!existing) return false;
    const table = tableForType(existing.type);
    await this.pool.query(`DELETE FROM ${table} WHERE tenant_id = $1 AND id = $2`, [
      String(tenantId),
      nodeId,
    ]);
    await this.pool.query(
      `DELETE FROM knowledge_edges
       WHERE tenant_id = $1 AND (from_id = $2 OR to_id = $2)`,
      [String(tenantId), nodeId]
    );
    return true;
  }

  async createEdge(edge) {
    if (!edge || !edge.tenantId || !edge.fromId || !edge.toId || !edge.type) {
      throw new Error('createEdge requires tenantId, fromId, toId, type');
    }
    if (!isEdgeType(edge.type)) {
      throw new Error(`Unknown edge type: ${edge.type}`);
    }
    const from = await this._getNode(edge.tenantId, edge.fromId);
    const to = await this._getNode(edge.tenantId, edge.toId);
    if (!from || !to) {
      throw new Error('createEdge requires both endpoints to exist in tenant');
    }
    const id = edge.id || randomUUID();
    const createdAt = edge.createdAt || new Date().toISOString();
    const metadata =
      edge.metadata && typeof edge.metadata === 'object' ? edge.metadata : {};
    await this.pool.query(
      `INSERT INTO knowledge_edges
        (tenant_id, id, type, from_id, to_id, metadata, created_at)
       VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)`,
      [
        String(edge.tenantId),
        id,
        edge.type,
        edge.fromId,
        edge.toId,
        JSON.stringify(metadata),
        createdAt,
      ]
    );
    return {
      id,
      tenantId: String(edge.tenantId),
      type: edge.type,
      fromId: edge.fromId,
      toId: edge.toId,
      metadata: { ...metadata },
      createdAt: toIso(createdAt),
    };
  }

  async deleteEdge(tenantId, edgeId) {
    const result = await this.pool.query(
      `DELETE FROM knowledge_edges WHERE tenant_id = $1 AND id = $2 RETURNING id`,
      [String(tenantId), edgeId]
    );
    return result.rowCount > 0;
  }

  async neighbors(tenantId, nodeId, options = {}) {
    const node = await this._getNode(tenantId, nodeId);
    if (!node) return [];

    const direction = options.direction || 'both';
    const params = [String(tenantId), nodeId];
    const clauses = ['tenant_id = $1'];
    if (direction === 'out') {
      clauses.push('from_id = $2');
    } else if (direction === 'in') {
      clauses.push('to_id = $2');
    } else {
      clauses.push('(from_id = $2 OR to_id = $2)');
    }
    if (options.edgeType) {
      params.push(options.edgeType);
      clauses.push(`type = $${params.length}`);
    }

    const { rows } = await this.pool.query(
      `SELECT tenant_id, id, type, from_id, to_id, metadata, created_at
       FROM knowledge_edges
       WHERE ${clauses.join(' AND ')}`,
      params
    );

    const results = [];
    for (const row of rows) {
      let neighborId = null;
      let dir = null;
      if (row.from_id === nodeId) {
        neighborId = row.to_id;
        dir = 'out';
      } else if (row.to_id === nodeId) {
        neighborId = row.from_id;
        dir = 'in';
      }
      if (!neighborId) continue;
      if (direction === 'out' && dir !== 'out') continue;
      if (direction === 'in' && dir !== 'in') continue;
      const neighbor = await this._getNode(tenantId, neighborId);
      if (!neighbor) continue;
      results.push({
        direction: dir,
        edge: deserializeEdge(row),
        node: neighbor,
      });
    }
    return results;
  }

  async find(tenantId, query = {}) {
    const tenant = String(tenantId);
    let candidates = [];

    if (query.id) {
      const node = await this._getNode(tenant, query.id);
      candidates = node ? [node] : [];
    } else if (query.ids && query.ids.length) {
      for (const id of query.ids) {
        const node = await this._getNode(tenant, id);
        if (node) candidates.push(node);
      }
    } else if (query.type) {
      candidates = await this._listByType(tenant, query.type);
    } else {
      candidates = [
        ...(await this._listByType(tenant, NODE_TYPES.COMPANY)),
        ...(await this._listByType(tenant, NODE_TYPES.PERSON)),
        ...(await this._listByType(tenant, NODE_TYPES.INTERACTION)),
        ...(await this._listByType(tenant, NODE_TYPES.EVIDENCE)),
        ...(await this._listByType(tenant, NODE_TYPES.CLAIM)),
      ];
    }

    return candidates.filter((node) => {
      if (query.type && node.type !== query.type) return false;
      if (query.ids && !query.ids.includes(node.id)) return false;
      if (typeof query.predicate === 'function' && !query.predicate(node)) return false;
      return true;
    });
  }

  async _getNode(tenantId, nodeId) {
    const tenant = String(tenantId);
    for (const type of [
      NODE_TYPES.COMPANY,
      NODE_TYPES.PERSON,
      NODE_TYPES.INTERACTION,
      NODE_TYPES.EVIDENCE,
      NODE_TYPES.CLAIM,
    ]) {
      const table = tableForType(type);
      const { rows } = await this.pool.query(
        `SELECT tenant_id, id, ${table === 'knowledge_nodes' ? 'type,' : ''} created_at, updated_at, metadata, body
         FROM ${table}
         WHERE tenant_id = $1 AND id = $2
         LIMIT 1`,
        [tenant, nodeId]
      );
      if (rows[0]) {
        return deserializeNode(normalizeRow(rows[0], type));
      }
    }
    return null;
  }

  async _listByType(tenantId, type) {
    const table = tableForType(type);
    const { rows } = await this.pool.query(
      `SELECT tenant_id, id, ${table === 'knowledge_nodes' ? 'type,' : ''} created_at, updated_at, metadata, body
       FROM ${table}
       WHERE tenant_id = $1
         ${table === 'knowledge_nodes' ? 'AND type = $2' : ''}`,
      table === 'knowledge_nodes' ? [String(tenantId), type] : [String(tenantId)]
    );
    return rows.map((row) => deserializeNode(normalizeRow(row, type)));
  }
}

function tableForType(type) {
  if (type === NODE_TYPES.EVIDENCE) return 'knowledge_evidence';
  if (type === NODE_TYPES.CLAIM) return 'knowledge_claims';
  if (
    type === NODE_TYPES.COMPANY ||
    type === NODE_TYPES.PERSON ||
    type === NODE_TYPES.INTERACTION
  ) {
    return 'knowledge_nodes';
  }
  throw new Error(`Unknown node type: ${type}`);
}

function serializeNode(node) {
  const body = {};
  for (const [key, value] of Object.entries(node)) {
    if (CORE_NODE_KEYS.has(key)) continue;
    body[key] = value;
  }
  return {
    tenant_id: String(node.tenantId),
    id: node.id,
    type: node.type,
    created_at: node.createdAt || new Date().toISOString(),
    updated_at: node.updatedAt || node.createdAt || new Date().toISOString(),
    metadata: JSON.stringify(node.metadata && typeof node.metadata === 'object' ? node.metadata : {}),
    body: JSON.stringify(body),
  };
}

function normalizeRow(row, fallbackType) {
  return {
    tenant_id: row.tenant_id,
    id: row.id,
    type: row.type || fallbackType,
    created_at: row.created_at,
    updated_at: row.updated_at,
    metadata: row.metadata,
    body: row.body,
  };
}

function deserializeNode(row) {
  const metadata =
    typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata || {};
  const body = typeof row.body === 'string' ? JSON.parse(row.body) : row.body || {};
  return {
    id: row.id,
    tenantId: String(row.tenant_id),
    type: row.type,
    createdAt: toIso(row.created_at),
    updatedAt: toIso(row.updated_at),
    metadata,
    ...body,
  };
}

function deserializeEdge(row) {
  const metadata =
    typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata || {};
  return {
    id: row.id,
    tenantId: String(row.tenant_id),
    type: row.type,
    fromId: row.from_id,
    toId: row.to_id,
    metadata,
    createdAt: toIso(row.created_at),
  };
}

function toIso(value) {
  if (!value) return new Date().toISOString();
  if (value instanceof Date) return value.toISOString();
  return new Date(value).toISOString();
}

module.exports = {
  PersistentGraphRepository,
};
