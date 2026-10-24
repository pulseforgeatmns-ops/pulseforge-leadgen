'use strict';

/**
 * SPEC-142 — Investigation Graph.
 * Everything connects: Mission → Market → Candidate → Claims → Evidence → Sources.
 */

const { GRAPH_NODE_TYPES } = require('./types');

function createInvestigationGraph(seed = {}) {
  const graph = {
    nodes: new Map(),
    edges: [],
    missionId: seed.missionId || null,
    createdAt: seed.createdAt || new Date().toISOString(),
  };

  if (seed.mission) addMissionNode(graph, seed.mission);
  if (seed.market) addMarketNode(graph, seed.market);
  if (Array.isArray(seed.candidates)) {
    for (const candidate of seed.candidates) addCandidateNode(graph, candidate);
  }

  return graph;
}

function addNode(graph, node) {
  if (!node || !node.id) return null;
  graph.nodes.set(node.id, { ...node });
  return node.id;
}

function addEdge(graph, fromId, toId, relation) {
  if (!fromId || !toId) return;
  graph.edges.push({ from: fromId, to: toId, relation: relation || 'related' });
}

function addMissionNode(graph, mission) {
  const id = `mission:${mission.id || 'unknown'}`;
  addNode(graph, {
    id,
    type: GRAPH_NODE_TYPES.MISSION,
    label: mission.objectiveText || mission.title || 'Investigation mission',
    data: {
      missionId: mission.id,
      objective: mission.objectiveText || mission.objective,
    },
  });
  return id;
}

function addMarketNode(graph, marketDefinition) {
  const id = `market:${marketDefinition.geography || 'unknown'}-${marketDefinition.segment || 'general'}`;
  addNode(graph, {
    id,
    type: GRAPH_NODE_TYPES.MARKET,
    label: [marketDefinition.geography, marketDefinition.segment].filter(Boolean).join(' — '),
    data: {
      geography: marketDefinition.geography,
      segment: marketDefinition.segment,
      segments: marketDefinition.segments,
      buyer: marketDefinition.buyer,
    },
  });

  const missionNodes = [...graph.nodes.values()].filter((n) => n.type === GRAPH_NODE_TYPES.MISSION);
  for (const m of missionNodes) addEdge(graph, m.id, id, 'investigates');

  return id;
}

function addCandidateNode(graph, candidate) {
  const id = `candidate:${candidate.id}`;
  addNode(graph, {
    id,
    type: GRAPH_NODE_TYPES.CANDIDATE,
    label: candidate.name || candidate.id,
    data: {
      candidateId: candidate.id,
      name: candidate.name,
      industry: candidate.industry,
      location: candidate.location,
      website: candidate.website,
    },
  });

  const marketNodes = [...graph.nodes.values()].filter((n) => n.type === GRAPH_NODE_TYPES.MARKET);
  for (const m of marketNodes) addEdge(graph, m.id, id, 'contains');

  for (const person of candidate.people || []) {
    addDecisionMakerNode(graph, person, id);
  }

  return id;
}

function addDecisionMakerNode(graph, person, candidateNodeId) {
  const id = `dm:${candidateNodeId}:${person.name || person.email || 'unknown'}`;
  addNode(graph, {
    id,
    type: GRAPH_NODE_TYPES.DECISION_MAKER,
    label: person.name || person.jobTitle || 'Decision maker',
    data: {
      name: person.name,
      jobTitle: person.jobTitle,
      email: person.email,
      phone: person.phone,
    },
  });
  addEdge(graph, candidateNodeId, id, 'has_decision_maker');
  return id;
}

function addEvidenceNode(graph, evidence, candidateNodeId) {
  const id = evidence.id || `evidence:${candidateNodeId}:${evidence.source}:${Date.now()}`;
  addNode(graph, {
    id,
    type: GRAPH_NODE_TYPES.EVIDENCE,
    label: evidence.label || evidence.source || 'Evidence',
    data: {
      source: evidence.source,
      kind: evidence.kind,
      label: evidence.label,
      observedAt: evidence.observedAt,
      weight: evidence.weight,
    },
  });
  addEdge(graph, candidateNodeId, id, 'has_evidence');

  const sourceId = addSourceNode(graph, evidence.source);
  if (sourceId) addEdge(graph, id, sourceId, 'from_source');

  return id;
}

function addSourceNode(graph, sourceName) {
  if (!sourceName) return null;
  const id = `source:${String(sourceName).toLowerCase().replace(/[\s-]+/g, '_')}`;
  if (!graph.nodes.has(id)) {
    addNode(graph, {
      id,
      type: GRAPH_NODE_TYPES.SOURCE,
      label: String(sourceName).replace(/_/g, ' '),
      data: { source: sourceName },
    });
  }
  return id;
}

function addClaimNode(graph, claim, candidateNodeId) {
  const id = claim.id || `claim:${candidateNodeId}:${Date.now()}`;
  addNode(graph, {
    id,
    type: GRAPH_NODE_TYPES.CLAIM,
    label: claim.text,
    data: {
      text: claim.text,
      confidence: claim.confidence,
      hypothesisId: claim.hypothesisId,
      missingEvidence: claim.missingEvidence,
      contradictions: claim.contradictions,
    },
  });
  addEdge(graph, candidateNodeId, id, 'has_claim');

  const confidenceId = `confidence:${id}`;
  addNode(graph, {
    id: confidenceId,
    type: GRAPH_NODE_TYPES.CONFIDENCE,
    label: `Confidence ${claim.confidence}`,
    data: { score: claim.confidence, claimId: id },
  });
  addEdge(graph, id, confidenceId, 'has_confidence');

  for (const support of claim.supportedBy || []) {
    const evidenceId = typeof support === 'string' ? support : support.evidenceId || support.source;
    if (evidenceId && graph.nodes.has(evidenceId)) {
      addEdge(graph, evidenceId, id, 'supports');
    }
  }

  return id;
}

function addHypothesisNode(graph, hypothesis) {
  const id = hypothesis.id;
  addNode(graph, {
    id,
    type: GRAPH_NODE_TYPES.HYPOTHESIS,
    label: hypothesis.text,
    data: {
      text: hypothesis.text,
      confidence: hypothesis.confidence,
      status: hypothesis.status,
      requiredEvidence: hypothesis.requiredEvidence,
      missingEvidence: hypothesis.missingEvidence,
    },
  });

  if (hypothesis.entityId) {
    const candidateId = `candidate:${hypothesis.entityId}`;
    if (graph.nodes.has(candidateId)) addEdge(graph, candidateId, id, 'evaluates');
  }

  return id;
}

function getNodesByType(graph, type) {
  return [...graph.nodes.values()].filter((n) => n.type === type);
}

function getClaimsForCandidate(graph, candidateId) {
  const nodeId = `candidate:${candidateId}`;
  const claimIds = graph.edges
    .filter((e) => e.from === nodeId && e.relation === 'has_claim')
    .map((e) => e.to);
  return claimIds.map((id) => graph.nodes.get(id)).filter(Boolean);
}

function serializeGraph(graph) {
  return {
    missionId: graph.missionId,
    createdAt: graph.createdAt,
    nodes: [...graph.nodes.values()],
    edges: graph.edges.slice(),
    summary: {
      candidates: getNodesByType(graph, GRAPH_NODE_TYPES.CANDIDATE).length,
      claims: getNodesByType(graph, GRAPH_NODE_TYPES.CLAIM).length,
      evidence: getNodesByType(graph, GRAPH_NODE_TYPES.EVIDENCE).length,
      hypotheses: getNodesByType(graph, GRAPH_NODE_TYPES.HYPOTHESIS).length,
      sources: getNodesByType(graph, GRAPH_NODE_TYPES.SOURCE).length,
      conflicts: getNodesByType(graph, GRAPH_NODE_TYPES.CLAIM).filter(
        (c) => (c.data.contradictions || []).length > 0
      ).length,
    },
  };
}

module.exports = {
  createInvestigationGraph,
  addNode,
  addEdge,
  addMissionNode,
  addMarketNode,
  addCandidateNode,
  addDecisionMakerNode,
  addEvidenceNode,
  addSourceNode,
  addClaimNode,
  addHypothesisNode,
  getNodesByType,
  getClaimsForCandidate,
  serializeGraph,
};
