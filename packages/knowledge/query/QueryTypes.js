'use strict';

/**
 * Strongly typed query objects for the Knowledge Query Engine (SPEC-001C).
 * Every query requires tenantId — tenant-safe by default.
 */

/**
 * @typedef {object} CompanyQuery
 * @property {string} tenantId
 * @property {string} [industry]
 * @property {string} [technology]
 * @property {string} [location]
 * @property {number} [confidenceMin]
 * @property {string} [createdAfter] ISO timestamp
 * @property {number} [limit]
 */

/**
 * @typedef {object} PersonQuery
 * @property {string} tenantId
 * @property {string} [companyId]
 * @property {string} [email]
 * @property {string} [title]
 * @property {string} [name]
 * @property {number} [confidenceMin]
 * @property {string} [createdAfter]
 * @property {number} [limit]
 */

/**
 * @typedef {object} InteractionQuery
 * @property {string} tenantId
 * @property {string} [channel]
 * @property {string} [actionType]
 * @property {string} [relatedNodeId]
 * @property {string} [createdAfter]
 * @property {string} [occurredAfter]
 * @property {number} [limit]
 */

/**
 * @typedef {object} EvidenceQuery
 * @property {string} tenantId
 * @property {string} [sourceType]
 * @property {string} [sourceId]
 * @property {string} [aboutNodeId]
 * @property {number} [confidenceMin]
 * @property {string} [createdAfter]
 * @property {number} [limit]
 */

/**
 * @typedef {object} ClaimQuery
 * @property {string} tenantId
 * @property {string} [subjectId]
 * @property {string} [status]
 * @property {number} [confidenceMin]
 * @property {string} [createdAfter]
 * @property {number} [limit]
 */

/**
 * @typedef {object} NeighborQuery
 * @property {string} tenantId
 * @property {string} nodeId
 * @property {'out'|'in'|'both'} [direction]
 * @property {string} [edgeType]
 * @property {string[]} [edgeTypes]
 * @property {number} [limit]
 */

/**
 * @typedef {object} RelatedQuery
 * @property {string} tenantId
 * @property {string} nodeId
 * @property {number} [depth] 1..3 (default 2)
 * @property {'out'|'in'|'both'} [direction]
 * @property {string} [edgeType]
 * @property {string[]} [edgeTypes]
 * @property {number} [limit]
 */

/**
 * @typedef {object} TimelineQuery
 * @property {string} tenantId
 * @property {string} nodeId
 * @property {number} [limit]
 */

/**
 * @typedef {object} PathQuery
 * @property {string} tenantId
 * @property {string} fromId
 * @property {string} toId
 * @property {number} [maxDepth] default 6, max 8
 * @property {'out'|'in'|'both'} [direction]
 * @property {string} [edgeType]
 * @property {string[]} [edgeTypes]
 */

/**
 * @typedef {object} ExplainQuery
 * @property {string} tenantId
 * @property {string} nodeId
 */

/**
 * @typedef {object} NeighborResult
 * @property {'out'|'in'} direction
 * @property {object} edge
 * @property {object} node
 */

/**
 * @typedef {object} RelatedNode
 * @property {object} node
 * @property {number} depth
 * @property {object|null} viaEdge
 * @property {string|null} parentId
 */

/**
 * @typedef {object} TimelineEvent
 * @property {string} at ISO timestamp
 * @property {string} kind node type or 'edge'
 * @property {object} node
 * @property {object|null} [edge]
 * @property {string} [label]
 */

/**
 * @typedef {object} PathHop
 * @property {object} node
 * @property {object|null} edge edge into this node from previous (null for start)
 */

/**
 * @typedef {object} PathResult
 * @property {PathHop[]} hops
 * @property {number} length edge count
 */

/**
 * @typedef {object} QueryMetrics
 * @property {string} queryName
 * @property {number} executionTimeMs
 * @property {number} nodesVisited
 * @property {number} edgesTraversed
 * @property {number} resultsReturned
 * @property {string} repositoryType
 */

const DEFAULT_LIMIT = 100;
const DEFAULT_RELATED_DEPTH = 2;
const MAX_RELATED_DEPTH = 3;
const DEFAULT_PATH_DEPTH = 6;
const MAX_PATH_DEPTH = 8;

/**
 * @param {object} query
 * @param {string} [label]
 */
function requireTenantId(query, label = 'query') {
  if (!query || query.tenantId == null || query.tenantId === '') {
    throw new Error(`${label} requires tenantId`);
  }
  return String(query.tenantId);
}

/**
 * @param {number|undefined|null} value
 * @param {number} fallback
 * @param {number} max
 */
function clampDepth(value, fallback, max) {
  const n = value == null ? fallback : Number(value);
  if (!Number.isFinite(n) || n < 1) return fallback;
  return Math.min(Math.floor(n), max);
}

/**
 * @param {number|undefined|null} value
 * @param {number} fallback
 */
function resolveLimit(value, fallback = DEFAULT_LIMIT) {
  if (value == null) return fallback;
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return fallback;
  return Math.floor(n);
}

/**
 * Normalize edge type filters into a Set or null (no filter).
 * @param {{ edgeType?: string, edgeTypes?: string[] }} options
 * @returns {Set<string>|null}
 */
function resolveEdgeTypeSet(options = {}) {
  const list = [];
  if (options.edgeType) list.push(options.edgeType);
  if (Array.isArray(options.edgeTypes)) list.push(...options.edgeTypes);
  if (list.length === 0) return null;
  return new Set(list);
}

module.exports = {
  DEFAULT_LIMIT,
  DEFAULT_RELATED_DEPTH,
  MAX_RELATED_DEPTH,
  DEFAULT_PATH_DEPTH,
  MAX_PATH_DEPTH,
  requireTenantId,
  clampDepth,
  resolveLimit,
  resolveEdgeTypeSet,
};
