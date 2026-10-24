'use strict';

/**
 * @typedef {object} GraphEdge
 * @property {string} id
 * @property {string} tenantId
 * @property {string} type
 * @property {string} fromId
 * @property {string} toId
 * @property {Record<string, unknown>} metadata
 * @property {string} createdAt
 */

/**
 * Storage interface only. Implementations must not leak into KnowledgeService callers.
 *
 * Contract methods:
 * - createNode(node)
 * - updateNode(tenantId, nodeId, patchOrNode)
 * - deleteNode(tenantId, nodeId)
 * - createEdge(edge)
 * - deleteEdge(tenantId, edgeId)
 * - neighbors(tenantId, nodeId, options?)
 * - find(tenantId, query)
 *
 * @typedef {object} GraphRepository
 * @property {(node: object) => Promise<object>|object} createNode
 * @property {(tenantId: string, nodeId: string, patch: object) => Promise<object|null>|object|null} updateNode
 * @property {(tenantId: string, nodeId: string) => Promise<boolean>|boolean} deleteNode
 * @property {(edge: GraphEdge) => Promise<GraphEdge>|GraphEdge} createEdge
 * @property {(tenantId: string, edgeId: string) => Promise<boolean>|boolean} deleteEdge
 * @property {(tenantId: string, nodeId: string, options?: object) => Promise<object[]>|object[]} neighbors
 * @property {(tenantId: string, query: object) => Promise<object[]>|object[]} find
 */

const GRAPH_REPOSITORY_METHODS = Object.freeze([
  'createNode',
  'updateNode',
  'deleteNode',
  'createEdge',
  'deleteEdge',
  'neighbors',
  'find',
]);

/**
 * @param {object} candidate
 * @returns {boolean}
 */
function isGraphRepository(candidate) {
  if (!candidate || typeof candidate !== 'object') return false;
  return GRAPH_REPOSITORY_METHODS.every((method) => typeof candidate[method] === 'function');
}

/**
 * @param {object} candidate
 */
function assertGraphRepository(candidate) {
  if (!isGraphRepository(candidate)) {
    throw new Error(
      `GraphRepository contract violation: missing methods [${GRAPH_REPOSITORY_METHODS.join(', ')}]`
    );
  }
}

module.exports = {
  GRAPH_REPOSITORY_METHODS,
  isGraphRepository,
  assertGraphRepository,
};
