'use strict';

/**
 * SPEC-113 Stage 3 — Ontology construction.
 * Concepts become relationships. The compiler constructs a reasoning graph.
 */

const { RELATIONS, CONCEPT_TYPES, asText, newId, slugify } = require('./types');

function buildEdge(partial = {}) {
  return {
    id: asText(partial.id) || newId('edge'),
    from: asText(partial.from || partial.fromId),
    to: asText(partial.to || partial.toId),
    relation: asText(partial.relation) || RELATIONS.MAPS_TO,
    label: asText(partial.label) || asText(partial.relation),
  };
}

function findByLabel(concepts, label) {
  const needle = slugify(label);
  return concepts.find((c) => slugify(c.label) === needle || slugify(c.id) === needle);
}

function constructOntology(concepts = [], pendingEdges = []) {
  const edges = [];
  const seen = new Set();

  function addEdge(partial) {
    const edge = buildEdge(partial);
    if (!edge.from || !edge.to || edge.from === edge.to) return;
    const key = `${edge.from}|${edge.relation}|${edge.to}`;
    if (seen.has(key)) return;
    seen.add(key);
    edges.push(edge);
  }

  for (const pending of pendingEdges) addEdge(pending);

  const pains = concepts.filter((c) => c.type === CONCEPT_TYPES.PAIN);
  const signals = concepts.filter((c) => c.type === CONCEPT_TYPES.OBSERVABLE_SIGNAL);
  const buying = concepts.filter((c) => c.type === CONCEPT_TYPES.BUYING_TRIGGER);
  const disqualifiers = concepts.filter((c) => c.type === CONCEPT_TYPES.DISQUALIFIER);
  const icp = concepts.find((c) => c.type === CONCEPT_TYPES.ICP);
  const confidence = concepts.filter((c) => c.type === CONCEPT_TYPES.CONFIDENCE_RULE);
  const language = concepts.filter((c) => c.type === CONCEPT_TYPES.LANGUAGE);
  const objections = concepts.filter((c) => c.type === CONCEPT_TYPES.OBJECTION);
  const evidence = concepts.filter((c) => c.type === CONCEPT_TYPES.EVIDENCE);

  for (const signal of signals) {
    const owner = pains.find((p) =>
      String(signal.statement || '').toLowerCase().includes(String(p.label || '').toLowerCase())
    );
    if (owner) addEdge({ from: owner.id, to: signal.id, relation: RELATIONS.OBSERVED_THROUGH });
  }

  for (const trigger of buying) {
    const owner = pains.find((p) =>
      String(trigger.statement || '').toLowerCase().includes(String(p.label || '').toLowerCase())
    ) || pains[0];
    if (owner) addEdge({ from: owner.id, to: trigger.id, relation: RELATIONS.MAPS_TO });
  }

  if (icp) {
    for (const row of disqualifiers) {
      addEdge({ from: icp.id, to: row.id, relation: RELATIONS.EXCLUDES });
    }
  }

  const founder = findByLabel(pains, 'Founder Dependency') || pains[0];
  if (founder) {
    for (const rule of confidence) {
      addEdge({ from: founder.id, to: rule.id, relation: RELATIONS.INCREASES_CERTAINTY });
    }
    for (const row of language) {
      addEdge({ from: founder.id, to: row.id, relation: RELATIONS.BELONGS_TO });
    }
    for (const row of objections) {
      addEdge({ from: founder.id, to: row.id, relation: RELATIONS.BELONGS_TO });
    }
    for (const row of evidence) {
      addEdge({ from: founder.id, to: row.id, relation: RELATIONS.SUPPORTED_BY });
    }
  }

  return { concepts, edges };
}

function neighbors(ontology, conceptId, relation) {
  return (ontology.edges || [])
    .filter((e) => e.from === conceptId && (!relation || e.relation === relation))
    .map((e) => (ontology.concepts || []).find((c) => c.id === e.to))
    .filter(Boolean);
}

function explainConcept(ontology, conceptId) {
  const concept = (ontology.concepts || []).find((c) => c.id === conceptId);
  if (!concept) return null;
  const outgoing = (ontology.edges || []).filter((e) => e.from === conceptId);
  return {
    concept,
    provenance: concept.provenance,
    relations: outgoing.map((edge) => ({
      relation: edge.relation,
      target: (ontology.concepts || []).find((c) => c.id === edge.to) || { id: edge.to },
    })),
  };
}

module.exports = {
  RELATIONS,
  buildEdge,
  constructOntology,
  neighbors,
  explainConcept,
};
