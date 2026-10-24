'use strict';

/**
 * SPEC-143 — Memory graph connecting market → companies → people → claims → evidence.
 */

const { MEMORY_TYPES } = require('./types');

function createMemoryGraph(seed = {}) {
  return {
    tenantId: seed.tenantId || null,
    nodes: new Map(),
    edges: [],
    createdAt: seed.createdAt || new Date().toISOString(),
  };
}

function addMemoryNode(graph, memory) {
  if (!memory || !memory.id) return null;
  graph.nodes.set(memory.id, {
    id: memory.id,
    type: memory.type,
    label: memory.label || memory.text || memory.name || memory.id,
    data: { ...memory },
  });
  return memory.id;
}

function addMemoryEdge(graph, fromId, toId, relation) {
  if (!fromId || !toId) return;
  graph.edges.push({ from: fromId, to: toId, relation: relation || 'related' });
}

function buildMemoryGraphFromKnowledge(knowledge = {}) {
  const tenantId =
    (knowledge.market && knowledge.market.tenantId) ||
    (knowledge.companies && knowledge.companies[0] && knowledge.companies[0].tenantId) ||
    null;
  const graph = createMemoryGraph({ tenantId });

  if (knowledge.market) {
    addMemoryNode(graph, knowledge.market);
  }

  for (const company of knowledge.companies || []) {
    addMemoryNode(graph, company);
    if (knowledge.market) {
      addMemoryEdge(graph, knowledge.market.id, company.id, 'contains');
    }
  }

  for (const person of knowledge.people || []) {
    addMemoryNode(graph, person);
    const companyNode = [...graph.nodes.values()].find(
      (n) => n.type === MEMORY_TYPES.COMPANY && n.data.companyId === person.companyId
    );
    if (companyNode) addMemoryEdge(graph, companyNode.id, person.id, 'employs');
  }

  for (const claim of knowledge.claims || []) {
    addMemoryNode(graph, claim);
    const parent =
      claim.entityType === 'person'
        ? [...graph.nodes.values()].find(
            (n) => n.type === MEMORY_TYPES.PERSON && n.data.personId === claim.entityId
          )
        : [...graph.nodes.values()].find(
            (n) => n.type === MEMORY_TYPES.COMPANY && n.data.companyId === claim.entityId
          );
    if (parent) addMemoryEdge(graph, parent.id, claim.id, 'has_claim');
    for (const source of claim.verificationSources || []) {
      const sourceId = `source:${String(source).toLowerCase().replace(/[\s-]+/g, '_')}`;
      if (!graph.nodes.has(sourceId)) {
        addMemoryNode(graph, {
          id: sourceId,
          type: 'source',
          label: String(source),
        });
      }
      addMemoryEdge(graph, claim.id, sourceId, 'supported_by');
    }
  }

  if (knowledge.investigation) {
    addMemoryNode(graph, knowledge.investigation);
    if (knowledge.market) {
      addMemoryEdge(graph, knowledge.market.id, knowledge.investigation.id, 'investigated_by');
    }
  }

  return graph;
}

function mergeMemoryGraphs(existingGraph, newKnowledge) {
  const base =
    existingGraph && existingGraph.nodes instanceof Map
      ? existingGraph
      : createMemoryGraph({ tenantId: existingGraph?.tenantId });
  const addition = buildMemoryGraphFromKnowledge(newKnowledge);

  for (const [id, node] of addition.nodes) {
    base.nodes.set(id, node);
  }
  for (const edge of addition.edges) {
    const exists = base.edges.some(
      (e) => e.from === edge.from && e.to === edge.to && e.relation === edge.relation
    );
    if (!exists) base.edges.push(edge);
  }

  return base;
}

function serializeMemoryGraph(graph) {
  return {
    tenantId: graph.tenantId,
    createdAt: graph.createdAt,
    nodes: [...graph.nodes.values()],
    edges: graph.edges.slice(),
    summary: {
      markets: [...graph.nodes.values()].filter((n) => n.type === MEMORY_TYPES.MARKET).length,
      companies: [...graph.nodes.values()].filter((n) => n.type === MEMORY_TYPES.COMPANY).length,
      people: [...graph.nodes.values()].filter((n) => n.type === MEMORY_TYPES.PERSON).length,
      claims: [...graph.nodes.values()].filter((n) => n.type === MEMORY_TYPES.CLAIM).length,
      investigations: [...graph.nodes.values()]
        .filter((n) => n.type === MEMORY_TYPES.INVESTIGATION)
        .length,
      edges: graph.edges.length,
    },
  };
}

module.exports = {
  createMemoryGraph,
  addMemoryNode,
  addMemoryEdge,
  buildMemoryGraphFromKnowledge,
  mergeMemoryGraphs,
  serializeMemoryGraph,
};
