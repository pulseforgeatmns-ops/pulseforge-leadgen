'use strict';

/**
 * SPEC-159 — Investigation State.
 * Canonical understanding container. No search terms — only understanding.
 *
 * Invariant (ADR-079): search results never flow directly into conclusions.
 * They flow into understanding first.
 */

const { reviseMarketDefinition } = require('../intelligence/MarketDefinition');
const {
  reviseCandidateUniverseEstimate,
  extractExpectedValue,
} = require('../universe/CandidateUniverseEstimate');
const { createInvestigationGraph, serializeGraph } = require('./InvestigationGraph');
const {
  HYPOTHESIS_LIFECYCLE,
  buildHypothesisLifecycleRecord,
} = require('./HypothesisLifecycle');

function nowIso() {
  return new Date().toISOString();
}

function cloneList(value) {
  return Array.isArray(value) ? value.slice() : [];
}

/**
 * @typedef {object} InvestigationState
 * @property {object} marketDefinition
 * @property {object|null} universeEstimate
 * @property {object[]} activeHypotheses
 * @property {object[]} rejectedHypotheses
 * @property {object|null} evidenceGraph
 * @property {object|null} coverage
 * @property {object} uncertainty
 * @property {number} confidence
 * @property {object[]} nextQuestions
 * @property {object[]} [archivedHypotheses]
 * @property {object[]} [confidenceEvolution]
 * @property {object[]} [understandingRevisions]
 * @property {object[]} [businessUnderstandings]
 * @property {object|null} [synthesisSummary]
 * @property {object|null} [priorUnderstanding]
 */

function buildInvestigationState(partial = {}) {
  return {
    missionId: partial.missionId || null,
    tenantId: partial.tenantId || null,
    phase: partial.phase || 'observe',
    marketDefinition: partial.marketDefinition || null,
    universeEstimate: partial.universeEstimate != null ? partial.universeEstimate : null,
    activeHypotheses: cloneList(partial.activeHypotheses),
    rejectedHypotheses: cloneList(partial.rejectedHypotheses),
    archivedHypotheses: cloneList(partial.archivedHypotheses),
    evidenceGraph: partial.evidenceGraph || null,
    coverage: partial.coverage || null,
    uncertainty: partial.uncertainty || { open: [], persistent: [], resolved: [] },
    confidence: partial.confidence != null ? Number(partial.confidence) : 0,
    nextQuestions: cloneList(partial.nextQuestions),
    confidenceEvolution: cloneList(partial.confidenceEvolution),
    understandingRevisions: cloneList(partial.understandingRevisions),
    businessUnderstandings: cloneList(partial.businessUnderstandings),
    synthesisSummary: partial.synthesisSummary || null,
    priorUnderstanding: partial.priorUnderstanding || null,
    seededFromMemory: partial.seededFromMemory === true,
    createdAt: partial.createdAt || nowIso(),
    updatedAt: partial.updatedAt || nowIso(),
  };
}

function seedFromPriorMemory(state, memory = {}) {
  if (!memory || !memory.loaded) return state;

  const marketMemory = memory.market || null;
  const investigationMemory = memory.investigation || null;
  const priorClaims = memory.claims || [];

  const priorUnderstanding = {
    marketMemory,
    investigationMemory,
    claimCount: priorClaims.length,
    overallConfidence: investigationMemory?.overallConfidence || 0,
    remainingGaps: investigationMemory?.remainingGaps || [],
    loadedAt: nowIso(),
  };

  let next = { ...state, priorUnderstanding, seededFromMemory: true };

  if (marketMemory && next.marketDefinition) {
    const terminology = cloneList(marketMemory.knownTerminology || marketMemory.terminology);
    const dominantTerminology =
      marketMemory.marketUnderstanding?.dominantTerminology || terminology;
    if (dominantTerminology.length) {
      next = {
        ...next,
        marketDefinition: {
          ...next.marketDefinition,
          terminology: [...new Set([...dominantTerminology, ...(next.marketDefinition.terminology || [])])],
          priorMarketMemory: true,
        },
      };
    }

    if (Array.isArray(marketMemory.entities) && marketMemory.entities.length) {
      const entityNames = marketMemory.entities.map((e) => e.name).filter(Boolean);
      next.uncertainty = {
        ...next.uncertainty,
        resolved: [
          ...new Set([...(next.uncertainty.resolved || []), ...entityNames.slice(0, 10)]),
        ],
      };
    }
  }

  if (investigationMemory?.overallConfidence) {
    next.confidence = Math.max(next.confidence, investigationMemory.overallConfidence);
    next.confidenceEvolution.push({
      at: nowIso(),
      confidence: next.confidence,
      reason: 'Prior investigation memory loaded',
      source: 'memory',
    });
  }

  if (priorClaims.length) {
    const openUnknowns = priorClaims
      .filter((c) => (c.missingEvidence || []).length > 0)
      .map((c) => c.text || c.id);
    next.uncertainty = {
      ...next.uncertainty,
      open: [...new Set([...(next.uncertainty.open || []), ...openUnknowns])],
    };
    next.nextQuestions = openUnknowns.slice(0, 5).map((text) => ({
      question: text,
      source: 'prior_memory',
      priority: 'medium',
    }));
  }

  return next;
}

function createInvestigationState(input = {}) {
  const mission = input.mission || {};
  const marketDefinition = input.marketDefinition || null;
  const universeEstimate = input.universeEstimate || null;
  const memory = input.memory || input.investigationMemory || {};

  const graph = createInvestigationGraph({
    missionId: mission.id,
    mission,
    market: marketDefinition,
    candidates: input.candidates || [],
  });

  const initialHypotheses = (input.hypotheses || input.searchHypotheses || []).map((h) =>
    buildHypothesisLifecycleRecord(h, { lifecycle: HYPOTHESIS_LIFECYCLE.GENERATED })
  );

  let state = buildInvestigationState({
    missionId: mission.id || null,
    tenantId: input.tenantId || mission.tenantId || mission.clientId || null,
    marketDefinition,
    universeEstimate,
    activeHypotheses: initialHypotheses.filter(
      (h) => h.lifecycle !== HYPOTHESIS_LIFECYCLE.REJECTED && h.lifecycle !== HYPOTHESIS_LIFECYCLE.ARCHIVED
    ),
    rejectedHypotheses: initialHypotheses.filter((h) => h.lifecycle === HYPOTHESIS_LIFECYCLE.REJECTED),
    evidenceGraph: serializeGraph(graph),
    coverage: input.coverage || null,
    uncertainty: {
      open: cloneList(input.initialUnknowns),
      persistent: [],
      resolved: [],
    },
    confidence: input.initialConfidence != null ? input.initialConfidence : 0.35,
    nextQuestions: cloneList(input.initialQuestions),
    confidenceEvolution: [
      {
        at: nowIso(),
        confidence: input.initialConfidence != null ? input.initialConfidence : 0.35,
        reason: 'Investigation beginning',
        source: 'initial',
      },
    ],
  });

  return seedFromPriorMemory(state, memory);
}

function recordConfidenceStep(state, partial = {}) {
  const confidence = partial.confidence != null ? Number(partial.confidence) : state.confidence;
  const entry = {
    at: nowIso(),
    confidence,
    reason: partial.reason || 'Evidence fusion updated confidence',
    source: partial.source || 'evidence_fusion',
    delta: confidence - (state.confidenceEvolution[state.confidenceEvolution.length - 1]?.confidence || 0),
  };
  return {
    ...state,
    confidence,
    confidenceEvolution: [...state.confidenceEvolution, entry],
    updatedAt: nowIso(),
  };
}

function recordUnderstandingRevision(state, partial = {}) {
  const revision = {
    at: nowIso(),
    kind: partial.kind || 'market_definition',
    reason: partial.reason || 'Evidence changed understanding',
    before: partial.before || null,
    after: partial.after || null,
  };
  return {
    ...state,
    understandingRevisions: [...state.understandingRevisions, revision],
    updatedAt: nowIso(),
  };
}

function applyMarketDefinitionRevision(state, revisionContext = {}) {
  if (!state.marketDefinition) return state;
  const before = { terminology: cloneList(state.marketDefinition.terminology) };
  const revised = reviseMarketDefinition(state.marketDefinition, revisionContext);
  const after = { terminology: cloneList(revised.terminology) };

  return recordUnderstandingRevision(
    { ...state, marketDefinition: revised, phase: 'update_understanding' },
    {
      kind: 'market_definition',
      reason: revisionContext.reason || 'Evidence contradicted original market definition',
      before,
      after,
    }
  );
}

function applyUniverseEstimateRevision(state, context = {}) {
  if (!state.universeEstimate) return state;
  const before = extractExpectedValue(state.universeEstimate);
  const revised = reviseCandidateUniverseEstimate(state.universeEstimate, context);
  const after = extractExpectedValue(revised);

  return recordUnderstandingRevision(
    { ...state, universeEstimate: revised, phase: 'update_understanding' },
    {
      kind: 'universe_estimate',
      reason: context.reason || `Coverage increased: investigated ${context.investigated || 0}, discovered ${context.discovered || 0}`,
      before,
      after,
    }
  );
}

function updateHypothesisBuckets(state, hypothesisUpdates = []) {
  const active = [];
  const rejected = cloneList(state.rejectedHypotheses);
  const archived = cloneList(state.archivedHypotheses);

  for (const hyp of hypothesisUpdates) {
    const record = buildHypothesisLifecycleRecord(hyp, { lifecycle: hyp.lifecycle });
    if (record.lifecycle === HYPOTHESIS_LIFECYCLE.REJECTED) {
      rejected.push(record);
    } else if (record.lifecycle === HYPOTHESIS_LIFECYCLE.ARCHIVED) {
      archived.push(record);
    } else {
      active.push(record);
    }
  }

  for (const existing of state.activeHypotheses) {
    if (!hypothesisUpdates.some((h) => h.id === existing.id)) {
      active.push(existing);
    }
  }

  return {
    ...state,
    activeHypotheses: active,
    rejectedHypotheses: rejected,
    archivedHypotheses: archived,
    updatedAt: nowIso(),
  };
}

function addEvidenceToGraph(state, evidenceItems = [], parentId = null) {
  if (!evidenceItems.length) return state;

  const graph = state.evidenceGraph || { nodes: [], edges: [], summary: {} };
  const nodes = [...(graph.nodes || [])];
  const edges = [...(graph.edges || [])];

  for (const item of evidenceItems) {
    const nodeId = item.id || `evidence:${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
    nodes.push({
      id: nodeId,
      type: 'evidence',
      label: item.label || item.source || 'Evidence',
      data: item,
    });
    if (parentId) {
      edges.push({
        from: parentId,
        to: nodeId,
        relation: item.relation || 'supports',
      });
    }
    if (item.relatedTo) {
      edges.push({
        from: nodeId,
        to: item.relatedTo,
        relation: item.relationshipType || 'related',
      });
    }
  }

  return {
    ...state,
    evidenceGraph: {
      ...graph,
      nodes,
      edges,
      summary: {
        ...(graph.summary || {}),
        evidence: nodes.filter((n) => n.type === 'evidence').length,
        relationships: edges.length,
      },
    },
    updatedAt: nowIso(),
  };
}

function setNextQuestions(state, questions = []) {
  return {
    ...state,
    nextQuestions: questions.map((q) =>
      typeof q === 'string'
        ? { question: q, source: 'generated', priority: 'medium' }
        : q
    ),
    phase: 'decide',
    updatedAt: nowIso(),
  };
}

function updateUncertainty(state, partial = {}) {
  return {
    ...state,
    uncertainty: {
      open: cloneList(partial.open != null ? partial.open : state.uncertainty.open),
      persistent: cloneList(partial.persistent != null ? partial.persistent : state.uncertainty.persistent),
      resolved: cloneList(partial.resolved != null ? partial.resolved : state.uncertainty.resolved),
    },
    updatedAt: nowIso(),
  };
}

function applyBusinessUnderstandingSynthesis(state, synthesisResult = {}) {
  const understandings = synthesisResult.understandings || [];
  if (!understandings.length && !(synthesisResult.revisions || []).length) {
    return state;
  }

  const before = {
    count: (state.businessUnderstandings || []).length,
    confidence: state.confidence,
  };

  let next = {
    ...state,
    businessUnderstandings: understandings,
    synthesisSummary: synthesisResult.summary || null,
    phase: 'update_understanding',
    updatedAt: nowIso(),
  };

  const avgConfidence = synthesisResult.summary?.averageConfidence;
  if (avgConfidence != null && avgConfidence > next.confidence) {
    next = recordConfidenceStep(next, {
      confidence: avgConfidence,
      reason: 'Business understanding synthesized from multi-source evidence',
      source: 'evidence_synthesis',
    });
  }

  return recordUnderstandingRevision(next, {
    kind: 'business_understanding',
    reason: 'Evidence synthesis produced business understanding',
    before,
    after: {
      count: understandings.length,
      confidence: next.confidence,
      contradictions: synthesisResult.summary?.contradictions || 0,
    },
  });
}

function applyBusinessJudgment(state, judgmentResult = {}) {
  if (!judgmentResult?.activatedHeuristics?.length) {
    return state;
  }

  const before = {
    heuristicCount: state.businessJudgment?.activatedHeuristics?.length || 0,
    confidence: state.confidence,
  };

  let next = {
    ...state,
    businessJudgment: judgmentResult,
    phase: 'apply_judgment',
    updatedAt: nowIso(),
  };

  const judgmentConfidence = judgmentResult.overallJudgment?.confidence;
  if (judgmentConfidence != null && judgmentConfidence > next.confidence) {
    next = recordConfidenceStep(next, {
      confidence: judgmentConfidence,
      reason: 'Business heuristics activated from synthesized understanding',
      source: 'business_heuristics',
    });
  }

  return recordUnderstandingRevision(next, {
    kind: 'business_judgment',
    reason: 'Business heuristics engine produced judgment from understanding',
    before,
    after: {
      heuristicCount: judgmentResult.activatedHeuristics.length,
      confidence: next.confidence,
      contradictions: (judgmentResult.contradictions || []).length,
    },
  });
}

function serializeInvestigationState(state) {
  return {
    missionId: state.missionId,
    tenantId: state.tenantId,
    phase: state.phase,
    marketDefinition: state.marketDefinition,
    universeEstimate: state.universeEstimate,
    activeHypotheses: state.activeHypotheses,
    rejectedHypotheses: state.rejectedHypotheses,
    archivedHypotheses: state.archivedHypotheses,
    evidenceGraph: state.evidenceGraph,
    coverage: state.coverage,
    uncertainty: state.uncertainty,
    confidence: state.confidence,
    nextQuestions: state.nextQuestions,
    confidenceEvolution: state.confidenceEvolution,
    understandingRevisions: state.understandingRevisions,
    businessUnderstandings: state.businessUnderstandings,
    synthesisSummary: state.synthesisSummary,
    businessJudgment: state.businessJudgment,
    priorUnderstanding: state.priorUnderstanding,
    seededFromMemory: state.seededFromMemory,
    createdAt: state.createdAt,
    updatedAt: state.updatedAt,
  };
}

module.exports = {
  buildInvestigationState,
  createInvestigationState,
  seedFromPriorMemory,
  recordConfidenceStep,
  recordUnderstandingRevision,
  applyMarketDefinitionRevision,
  applyUniverseEstimateRevision,
  applyBusinessUnderstandingSynthesis,
  applyBusinessJudgment,
  updateHypothesisBuckets,
  addEvidenceToGraph,
  setNextQuestions,
  updateUncertainty,
  serializeInvestigationState,
};
