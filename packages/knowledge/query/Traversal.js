'use strict';

const {
  clampDepth,
  resolveLimit,
  resolveEdgeTypeSet,
  DEFAULT_RELATED_DEPTH,
  MAX_RELATED_DEPTH,
  DEFAULT_PATH_DEPTH,
  MAX_PATH_DEPTH,
} = require('./QueryTypes');

/**
 * Deterministic graph traversal helpers (SPEC-001C).
 */

/**
 * Stable neighbor ordering: edge.id then node.id.
 * @param {import('./QueryTypes').NeighborResult[]} neighbors
 */
function sortNeighbors(neighbors) {
  return [...neighbors].sort((a, b) => {
    const edgeCmp = String(a.edge.id).localeCompare(String(b.edge.id));
    if (edgeCmp !== 0) return edgeCmp;
    return String(a.node.id).localeCompare(String(b.node.id));
  });
}

/**
 * @param {object} repository
 * @param {string} tenantId
 * @param {string} nodeId
 * @param {{ direction?: string, edgeType?: string, edgeTypes?: string[] }} options
 * @param {import('./Metrics').MetricsCollector} [metrics]
 * @returns {Promise<import('./QueryTypes').NeighborResult[]>}
 */
async function fetchNeighbors(repository, tenantId, nodeId, options = {}, metrics) {
  const edgeTypes = resolveEdgeTypeSet(options);
  const repoOptions = {
    direction: options.direction || 'both',
  };
  // Repository contract supports a single edgeType; multi-type filtered in-engine.
  if (!edgeTypes && options.edgeType) {
    repoOptions.edgeType = options.edgeType;
  } else if (edgeTypes && edgeTypes.size === 1) {
    repoOptions.edgeType = [...edgeTypes][0];
  }

  const raw = await repository.neighbors(tenantId, nodeId, repoOptions);
  let filtered = raw;
  if (edgeTypes && edgeTypes.size > 1) {
    filtered = raw.filter((n) => edgeTypes.has(n.edge.type));
  } else if (edgeTypes && edgeTypes.size === 1 && !repoOptions.edgeType) {
    filtered = raw.filter((n) => edgeTypes.has(n.edge.type));
  }

  if (metrics) {
    metrics.traverseEdges(filtered.length);
    metrics.visitNodes(filtered.length);
  }

  return sortNeighbors(filtered);
}

/**
 * Depth-1 neighbors for a node.
 * @param {object} repository
 * @param {import('./QueryTypes').NeighborQuery} query
 * @param {import('./Metrics').MetricsCollector} [metrics]
 */
async function neighbors(repository, query, metrics) {
  const limit = resolveLimit(query.limit);
  const list = await fetchNeighbors(
    repository,
    query.tenantId,
    query.nodeId,
    query,
    metrics
  );
  if (metrics) metrics.visitNodes(1);
  return list.slice(0, limit);
}

/**
 * Multi-hop BFS. Deterministic expansion order.
 * Prevents graph explosions via depth + optional limit.
 *
 * @param {object} repository
 * @param {import('./QueryTypes').RelatedQuery} query
 * @param {import('./Metrics').MetricsCollector} [metrics]
 * @returns {Promise<import('./QueryTypes').RelatedNode[]>}
 */
async function related(repository, query, metrics) {
  const depth = clampDepth(query.depth, DEFAULT_RELATED_DEPTH, MAX_RELATED_DEPTH);
  const limit = resolveLimit(query.limit, 500);
  const startId = query.nodeId;
  const tenantId = query.tenantId;

  /** @type {Map<string, import('./QueryTypes').RelatedNode>} */
  const seen = new Map();
  seen.set(startId, {
    node: null,
    depth: 0,
    viaEdge: null,
    parentId: null,
  });

  /** @type {string[]} */
  let frontier = [startId];
  const results = [];

  // Resolve start node
  const startNodes = await repository.find(tenantId, { id: startId });
  const startNode = startNodes[0] || null;
  if (!startNode) {
    if (metrics) metrics.setResults(0);
    return [];
  }
  seen.get(startId).node = startNode;
  if (metrics) metrics.visitNodes(1);

  for (let d = 1; d <= depth; d++) {
    const nextFrontier = [];
    // Deterministic frontier order
    frontier = [...frontier].sort((a, b) => String(a).localeCompare(String(b)));

    for (const currentId of frontier) {
      const neigh = await fetchNeighbors(
        repository,
        tenantId,
        currentId,
        query,
        metrics
      );
      for (const link of neigh) {
        if (seen.has(link.node.id)) continue;
        seen.set(link.node.id, {
          node: link.node,
          depth: d,
          viaEdge: link.edge,
          parentId: currentId,
        });
        nextFrontier.push(link.node.id);
        results.push(seen.get(link.node.id));
        if (results.length >= limit) {
          if (metrics) metrics.setResults(results.length);
          return results;
        }
      }
    }
    frontier = nextFrontier;
    if (frontier.length === 0) break;
  }

  if (metrics) metrics.setResults(results.length);
  return results;
}

/**
 * Shortest path (BFS). Returns node/edge alternating hops.
 * Deterministic: when multiple equal-length paths exist, prefers lower edge.id then node.id.
 *
 * @param {object} repository
 * @param {import('./QueryTypes').PathQuery} query
 * @param {import('./Metrics').MetricsCollector} [metrics]
 * @returns {Promise<import('./QueryTypes').PathResult|null>}
 */
async function findPath(repository, query, metrics) {
  const maxDepth = clampDepth(query.maxDepth, DEFAULT_PATH_DEPTH, MAX_PATH_DEPTH);
  const tenantId = query.tenantId;
  const { fromId, toId } = query;

  if (fromId === toId) {
    const nodes = await repository.find(tenantId, { id: fromId });
    const node = nodes[0] || null;
    if (!node) return null;
    if (metrics) {
      metrics.visitNodes(1);
      metrics.setResults(1);
    }
    return { hops: [{ node, edge: null }], length: 0 };
  }

  const startNodes = await repository.find(tenantId, { id: fromId });
  const start = startNodes[0];
  if (!start) return null;
  if (metrics) metrics.visitNodes(1);

  /** @type {Map<string, { node: object, edge: object|null, parentId: string|null }>} */
  const cameFrom = new Map();
  cameFrom.set(fromId, { node: start, edge: null, parentId: null });

  let frontier = [fromId];

  for (let d = 0; d < maxDepth; d++) {
    frontier = [...frontier].sort((a, b) => String(a).localeCompare(String(b)));
    const nextFrontier = [];

    for (const currentId of frontier) {
      const neigh = await fetchNeighbors(
        repository,
        tenantId,
        currentId,
        query,
        metrics
      );
      for (const link of neigh) {
        if (cameFrom.has(link.node.id)) continue;
        cameFrom.set(link.node.id, {
          node: link.node,
          edge: link.edge,
          parentId: currentId,
        });
        if (link.node.id === toId) {
          const hops = reconstructPath(cameFrom, fromId, toId);
          if (metrics) metrics.setResults(hops.length);
          return { hops, length: Math.max(0, hops.length - 1) };
        }
        nextFrontier.push(link.node.id);
      }
    }
    frontier = nextFrontier;
    if (frontier.length === 0) break;
  }

  if (metrics) metrics.setResults(0);
  return null;
}

/**
 * @param {Map<string, { node: object, edge: object|null, parentId: string|null }>} cameFrom
 * @param {string} fromId
 * @param {string} toId
 * @returns {import('./QueryTypes').PathHop[]}
 */
function reconstructPath(cameFrom, fromId, toId) {
  const stack = [];
  let cur = toId;
  while (cur != null) {
    const entry = cameFrom.get(cur);
    if (!entry) break;
    stack.push({ node: entry.node, edge: entry.edge });
    if (cur === fromId) break;
    cur = entry.parentId;
  }
  stack.reverse();
  return stack;
}

module.exports = {
  sortNeighbors,
  fetchNeighbors,
  neighbors,
  related,
  findPath,
};
