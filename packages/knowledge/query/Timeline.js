'use strict';

const { resolveLimit } = require('./QueryTypes');
const { fetchNeighbors } = require('./Traversal');
const { NODE_TYPES } = require('../types/nodeTypes');

/**
 * Chronological history for any graph node (SPEC-001C).
 * Basis for future "What changed?" capabilities.
 */

/**
 * @param {object} node
 * @returns {string}
 */
function eventTimestamp(node) {
  return (
    node.occurredAt ||
    node.updatedAt ||
    node.createdAt ||
    new Date(0).toISOString()
  );
}

/**
 * @param {object} node
 * @param {object|null} [edge]
 * @returns {import('./QueryTypes').TimelineEvent}
 */
function toTimelineEvent(node, edge = null) {
  return {
    at: eventTimestamp(node),
    kind: node.type,
    node,
    edge,
    label: timelineLabel(node),
  };
}

/**
 * @param {object} node
 * @returns {string}
 */
function timelineLabel(node) {
  if (node.type === NODE_TYPES.COMPANY) return node.name || 'Company';
  if (node.type === NODE_TYPES.PERSON) return node.name || 'Person';
  if (node.type === NODE_TYPES.INTERACTION) {
    return [node.channel, node.actionType, node.summary].filter(Boolean).join(' ') || 'Interaction';
  }
  if (node.type === NODE_TYPES.EVIDENCE) {
    return node.summary || `Evidence from ${node.sourceType}`;
  }
  if (node.type === NODE_TYPES.CLAIM) {
    return node.statement || 'Claim';
  }
  return node.type || 'node';
}

/**
 * Collect chronological events around a node:
 * self + 1-hop neighbors (people, interactions, evidence, claims) + claim confidence updates via updatedAt.
 *
 * @param {object} repository
 * @param {import('./QueryTypes').TimelineQuery} query
 * @param {import('./Metrics').MetricsCollector} [metrics]
 * @returns {Promise<import('./QueryTypes').TimelineEvent[]>}
 */
async function buildTimeline(repository, query, metrics) {
  const limit = resolveLimit(query.limit, 200);
  const tenantId = query.tenantId;
  const nodeId = query.nodeId;

  const roots = await repository.find(tenantId, { id: nodeId });
  const root = roots[0];
  if (!root) {
    if (metrics) metrics.setResults(0);
    return [];
  }
  if (metrics) metrics.visitNodes(1);

  /** @type {Map<string, import('./QueryTypes').TimelineEvent>} */
  const byKey = new Map();
  const add = (node, edge = null) => {
    const key = `${node.id}:${eventTimestamp(node)}`;
    if (byKey.has(key)) return;
    byKey.set(key, toTimelineEvent(node, edge));
  };

  add(root);

  const direct = await fetchNeighbors(
    repository,
    tenantId,
    nodeId,
    { direction: 'both' },
    metrics
  );

  for (const link of direct) {
    add(link.node, link.edge);

    // One extra hop for Company → Person → Interaction (common narrative chain)
    if (
      root.type === NODE_TYPES.COMPANY &&
      link.node.type === NODE_TYPES.PERSON
    ) {
      const personLinks = await fetchNeighbors(
        repository,
        tenantId,
        link.node.id,
        { direction: 'both' },
        metrics
      );
      for (const pl of personLinks) {
        if (
          pl.node.type === NODE_TYPES.INTERACTION ||
          pl.node.type === NODE_TYPES.EVIDENCE ||
          pl.node.type === NODE_TYPES.CLAIM
        ) {
          add(pl.node, pl.edge);
        }
      }
    }
  }

  const events = [...byKey.values()].sort((a, b) => {
    const t = String(a.at).localeCompare(String(b.at));
    if (t !== 0) return t;
    return String(a.node.id).localeCompare(String(b.node.id));
  });

  const sliced = events.slice(0, limit);
  if (metrics) metrics.setResults(sliced.length);
  return sliced;
}

/**
 * Index of a node within its own timeline (0-based), or -1 if missing.
 * Used by explainability.
 *
 * @param {import('./QueryTypes').TimelineEvent[]} timeline
 * @param {string} nodeId
 * @returns {{ index: number, at: string|null, total: number }}
 */
function timelinePosition(timeline, nodeId) {
  const index = timeline.findIndex((e) => e.node && e.node.id === nodeId);
  if (index < 0) {
    return { index: -1, at: null, total: timeline.length };
  }
  return {
    index,
    at: timeline[index].at,
    total: timeline.length,
  };
}

module.exports = {
  eventTimestamp,
  toTimelineEvent,
  timelineLabel,
  buildTimeline,
  timelinePosition,
};
