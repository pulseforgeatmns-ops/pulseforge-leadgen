'use strict';

/**
 * SPEC-152 — Concept Graph.
 * Stores concepts and relationships — not answers.
 */

const RELATION_TYPES = Object.freeze([
  'owns',
  'depends_on',
  'coordinates',
  'specializes_in',
  'delegates_to',
  'reports_to',
  'cannot_override',
  'supports',
  'requires',
  'explains',
  'retains_authority',
  'balances',
]);

const CONCEPT_CATEGORIES = Object.freeze([
  'identity',
  'specialist',
  'principle',
  'authority',
  'boundary',
  'process',
  'mission',
  'business',
]);

function normalizeId(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '_');
}

function deepFreeze(value) {
  if (!value || typeof value !== 'object') return value;
  Object.freeze(value);
  for (const key of Object.keys(value)) {
    deepFreeze(value[key]);
  }
  return value;
}

class ConceptGraph {
  /**
   * @param {object} input
   * @param {Array<{id:string,label:string,category:string,description:string}>} input.concepts
   * @param {Array<{from:string,to:string,relation:string}>} input.relationships
   */
  constructor(input = {}) {
    this.concepts = new Map();
    this.relationships = [];

    for (const concept of input.concepts || []) {
      this.addConcept(concept);
    }
    for (const relationship of input.relationships || []) {
      this.addRelationship(relationship);
    }
  }

  addConcept(concept) {
    const id = normalizeId(concept.id);
    if (!id) return null;
    const entry = deepFreeze({
      id,
      label: concept.label || id,
      category: concept.category || 'business',
      description: concept.description || '',
    });
    this.concepts.set(id, entry);
    return entry;
  }

  addRelationship(relationship) {
    const from = normalizeId(relationship.from);
    const to = normalizeId(relationship.to);
    const relation = String(relationship.relation || '').trim();
    if (!from || !to || !RELATION_TYPES.includes(relation)) return null;
    if (!this.concepts.has(from) || !this.concepts.has(to)) return null;
    const entry = deepFreeze({ from, to, relation });
    this.relationships.push(entry);
    return entry;
  }

  getConcept(id) {
    return this.concepts.get(normalizeId(id)) || null;
  }

  listConcepts() {
    return Array.from(this.concepts.values());
  }

  getRelationships(filter = {}) {
    const from = filter.from ? normalizeId(filter.from) : null;
    const to = filter.to ? normalizeId(filter.to) : null;
    const relation = filter.relation || null;

    return this.relationships.filter((edge) => {
      if (from && edge.from !== from) return false;
      if (to && edge.to !== to) return false;
      if (relation && edge.relation !== relation) return false;
      return true;
    });
  }

  getOutgoing(conceptId, relation = null) {
    return this.getRelationships({ from: conceptId, relation: relation || undefined });
  }

  getIncoming(conceptId, relation = null) {
    return this.getRelationships({ to: conceptId, relation: relation || undefined });
  }

  getNeighbors(conceptId, options = {}) {
    const id = normalizeId(conceptId);
    const depth = Math.max(1, options.depth || 1);
    const visited = new Set([id]);
    const frontier = [id];
    const collected = [];

    for (let hop = 0; hop < depth; hop += 1) {
      const nextFrontier = [];
      for (const current of frontier) {
        for (const edge of this.relationships) {
          let neighbor = null;
          if (edge.from === current) neighbor = edge.to;
          if (edge.to === current) neighbor = edge.from;
          if (!neighbor || visited.has(neighbor)) continue;
          visited.add(neighbor);
          nextFrontier.push(neighbor);
          collected.push({
            concept: this.getConcept(neighbor),
            edge,
            hop: hop + 1,
          });
        }
      }
      frontier.splice(0, frontier.length, ...nextFrontier);
    }

    return collected.filter((entry) => entry.concept);
  }

  /**
   * Breadth-first traversal from seed concepts toward optional target concepts.
   * @param {string[]} startConceptIds
   * @param {object} options
   * @returns {{ path: Array<{concept, edge|null}>, hops: number, edges: object[] }}
   */
  traverse(startConceptIds = [], options = {}) {
    const starts = startConceptIds.map(normalizeId).filter((id) => this.concepts.has(id));
    const targets = new Set((options.targetConcepts || []).map(normalizeId));
    const maxHops = options.maxHops || 4;
    const expandNeighbors = options.expandNeighbors !== false;

    if (!starts.length) {
      return { path: [], hops: 0, edges: [] };
    }

    const queue = starts.map((id) => ({ id, path: [{ concept: this.getConcept(id), edge: null }], hops: 0 }));
    const visited = new Set(starts);
    const edges = [];
    let bestPath = queue[0].path;

    while (queue.length) {
      const current = queue.shift();
      if (targets.size && targets.has(current.id)) {
        bestPath = current.path;
        break;
      }
      if (current.hops >= maxHops) continue;

      const outgoing = this.getOutgoing(current.id);
      const incoming = expandNeighbors ? this.getIncoming(current.id) : [];
      const adjacent = [...outgoing, ...incoming];

      for (const edge of adjacent) {
        const neighborId = edge.from === current.id ? edge.to : edge.from;
        if (visited.has(neighborId)) continue;
        visited.add(neighborId);
        edges.push(edge);
        const nextPath = current.path.concat({
          concept: this.getConcept(neighborId),
          edge,
        });
        queue.push({ id: neighborId, path: nextPath, hops: current.hops + 1 });
        if (!targets.size && nextPath.length > bestPath.length) {
          bestPath = nextPath;
        }
      }
    }

    return {
      path: bestPath,
      hops: Math.max(0, bestPath.length - 1),
      edges,
    };
  }
}

module.exports = {
  ConceptGraph,
  RELATION_TYPES,
  CONCEPT_CATEGORIES,
  normalizeId,
};
