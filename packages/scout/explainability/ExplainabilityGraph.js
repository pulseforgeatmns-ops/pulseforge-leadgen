'use strict';

/**
 * SPEC-183 — Cognitive Explainability (ADR-098).
 * Canonical graph tracing every recommendation through the full cognitive chain.
 *
 * Chain: Mission Objective → Market Definition → Hypotheses → Plan → Evidence →
 *        Understanding → Judgment → Recommendation
 *
 * Providers are implementation details — never terminal reasoning nodes.
 */

const { buildOperatorExplanations } = require('../coverage/HypothesisInvestigationPlanner');
const { explainJudgment } = require('../heuristics/BusinessHeuristicsEngine');
const { containsForbiddenReasoningKeys } = require('../investigation/MissionIntelligenceReport');

const EXPLAINABILITY_SPEC = 'SPEC-183';
const EXPLAINABILITY_ADR = 'ADR-098';

const NODE_KINDS = Object.freeze({
  OBJECTIVE: 'objective',
  MARKET_DEFINITION: 'market_definition',
  HYPOTHESIS: 'hypothesis',
  PLAN: 'plan',
  EVIDENCE: 'evidence',
  UNDERSTANDING: 'understanding',
  JUDGMENT: 'judgment',
  RECOMMENDATION: 'recommendation',
});

/** Provider labels that must never be terminal reasoning nodes. */
const PROVIDER_TERMINALS = Object.freeze([
  'google places',
  'google_maps',
  'google maps',
  'linkedin',
  'website',
  'company website',
]);

function nowIso() {
  return new Date().toISOString();
}

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function stableId(prefix, seed) {
  const slug = asText(seed)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_|_$/g, '')
    .slice(0, 48);
  return `${prefix}:${slug || 'unknown'}`;
}

function createExplainabilityGraph(seed = {}) {
  return {
    spec: EXPLAINABILITY_SPEC,
    adr: EXPLAINABILITY_ADR,
    missionId: seed.missionId || null,
    nodes: new Map(),
    recommendationId: seed.recommendationId || null,
    createdAt: seed.createdAt || nowIso(),
  };
}

function finalizeExplainabilityGraph(graph) {
  return Object.freeze({
    spec: graph.spec,
    adr: graph.adr,
    missionId: graph.missionId,
    nodes: graph.nodes,
    recommendationId: graph.recommendationId,
    createdAt: graph.createdAt,
  });
}

function addNode(graph, node) {
  if (!node || !node.id) return null;
  const frozen = Object.freeze({
    id: node.id,
    kind: node.kind,
    label: node.label || node.kind,
    rationale: node.rationale || '',
    parentIds: Object.freeze([...(node.parentIds || [])]),
    parentReasoningNode: node.parentReasoningNode || (node.parentIds || [])[0] || null,
    source: node.source || null,
    reasoning: node.reasoning || node.rationale || '',
    confidence: node.confidence != null ? Number(node.confidence) : null,
    supportingEvidence: Object.freeze([...(node.supportingEvidence || [])]),
    contradictoryEvidence: Object.freeze([...(node.contradictoryEvidence || [])]),
    metadata: Object.freeze({ ...(node.metadata || {}) }),
    timestamp: node.timestamp || nowIso(),
  });
  graph.nodes.set(frozen.id, frozen);
  return frozen.id;
}

function getNode(graph, nodeId) {
  return graph.nodes.get(nodeId) || null;
}

function getNodesByKind(graph, kind) {
  return [...graph.nodes.values()].filter((node) => node.kind === kind);
}

function ancestorIds(graph, nodeId, acc = new Set()) {
  const node = getNode(graph, nodeId);
  if (!node) return acc;
  for (const parentId of node.parentIds) {
    if (acc.has(parentId)) continue;
    acc.add(parentId);
    ancestorIds(graph, parentId, acc);
  }
  return acc;
}

function tracePath(graph, nodeId) {
  const path = [];
  const visited = new Set();
  let currentId = nodeId;

  while (currentId && !visited.has(currentId)) {
    visited.add(currentId);
    const node = getNode(graph, currentId);
    if (!node) break;
    path.unshift(node);
    currentId = node.parentIds[0] || null;
  }

  return path;
}

/**
 * Build the canonical explainability graph from pipeline outputs.
 * @param {object} input
 * @returns {object}
 */
function buildExplainabilityGraph(input = {}) {
  const mission = input.mission || {};
  const investigationState = input.investigationState || {};
  const plan = input.plan || investigationState.investigationPlan || null;
  const mir = input.missionIntelligenceReport || input.mir || null;
  const synthesis = input.synthesis || investigationState.synthesisSummary || null;
  const judgment =
    input.judgment ||
    mir?.judgmentResult ||
    mir?.businessJudgment ||
    null;

  const graph = createExplainabilityGraph({
    missionId: mission.id || investigationState.missionId || null,
  });

  const objectiveText =
    asText(mission.objectiveText) ||
    asText(mission.objective) ||
    asText(plan?.objective) ||
    'Mission objective';

  const objectiveId = addNode(graph, {
    id: stableId('objective', objectiveText),
    kind: NODE_KINDS.OBJECTIVE,
    label: objectiveText,
    rationale: 'Root mission objective — all recommendations must trace here.',
    parentIds: [],
    source: 'mission',
    reasoning: objectiveText,
    confidence: 1,
    metadata: { missionId: mission.id || null },
  });

  const market = investigationState.marketDefinition || mir?.finalMarketDefinition || {};
  const marketLabel = [market.geography, market.segment || (market.segments || [])[0]]
    .filter(Boolean)
    .join(' — ') || 'Market definition';
  const marketId = addNode(graph, {
    id: stableId('market', marketLabel),
    kind: NODE_KINDS.MARKET_DEFINITION,
    label: marketLabel,
    rationale:
      asText(market.rationale) ||
      asText(market.revisionReason) ||
      `Canonical market definition for ${marketLabel}.`,
    parentIds: [objectiveId],
    source: 'market_definition_engine',
    reasoning: `Segment: ${(market.segments || []).join(', ') || market.segment || 'general'}. Geography: ${market.geography || 'unspecified'}.`,
    confidence: market.confidence != null ? Number(market.confidence) : null,
    metadata: {
      terminology: market.terminology || [],
      buyer: market.buyer || null,
      revised: market.revised === true,
    },
  });

  const hypotheses = [
    ...(investigationState.activeHypotheses || []),
    ...(investigationState.rejectedHypotheses || []),
    ...(plan?.hypotheses || []),
    ...(mir?.hypothesisHistory || []),
  ];
  const seenHyp = new Set();
  const hypothesisIds = [];

  for (const hyp of hypotheses) {
    const key = hyp.id || hyp.text;
    if (!key || seenHyp.has(key)) continue;
    seenHyp.add(key);
    const hypText = asText(hyp.text) || asText(hyp.statement) || 'Hypothesis';
    const hypId = addNode(graph, {
      id: stableId('hypothesis', key),
      kind: NODE_KINDS.HYPOTHESIS,
      label: hypText,
      rationale:
        asText(hyp.rationale) ||
        `Testing whether ${hypText}`,
      parentIds: [marketId],
      source: 'canonical_hypothesis_engine',
      reasoning: hypText,
      confidence: hyp.confidence != null ? Number(hyp.confidence) : null,
      supportingEvidence: (hyp.supportingEvidence || []).map(formatEvidenceRef),
      contradictoryEvidence: (hyp.contradictoryEvidence || []).map(formatEvidenceRef),
      metadata: {
        lifecycle: hyp.lifecycle || hyp.status || null,
        kind: hyp.kind || null,
      },
    });
    hypothesisIds.push(hypId);
  }

  if (!hypothesisIds.length) {
    const fallbackHypId = addNode(graph, {
      id: 'hypothesis:investigation',
      kind: NODE_KINDS.HYPOTHESIS,
      label: asText(plan?.objective) || 'Investigation hypotheses',
      rationale: 'Hypotheses derived from mission objective and market definition.',
      parentIds: [marketId],
      source: 'canonical_hypothesis_engine',
      reasoning: asText(plan?.objective) || objectiveText,
      metadata: { fallback: true },
    });
    hypothesisIds.push(fallbackHypId);
  }

  const operatorExplanations = plan ? buildOperatorExplanations(plan) : [];
  const planParentIds = hypothesisIds.length ? hypothesisIds : [marketId];
  const planId = addNode(graph, {
    id: stableId('plan', plan?.objective || objectiveText),
    kind: NODE_KINDS.PLAN,
    label: asText(plan?.objective) || 'Investigation plan',
    rationale: asText(plan?.rationale) || 'Evidence requirements and provider assignments derived from hypotheses.',
    parentIds: planParentIds,
    source: 'hypothesis_investigation_planner',
    reasoning: asText(plan?.rationale) || 'Single investigation planner (SPEC-180).',
    metadata: {
      tasks: (plan?.tasks || []).map((task) => ({
        id: task.id || null,
        evidenceType: task.evidenceType || null,
        providerId: task.providerId || null,
        phase: task.phase || null,
      })),
      evidenceRequirements: (plan?.evidenceRequirements || investigationState.evidenceRequirements || []).map(
        (req) => ({
          evidenceType: req.evidenceType || req.type || null,
          rationale: req.rationale || null,
        })
      ),
      providerAssignments: operatorExplanations.map((row) => ({
        providerId: row.providerId,
        evidenceType: row.evidenceType,
        explanation: row.explanation,
      })),
    },
  });

  const evidenceGraph = investigationState.evidenceGraph || {};
  const evidenceNodes = evidenceGraph.nodes || [];
  const satisfiedEvidence = investigationState.satisfiedEvidence || [];
  const evidenceIds = [];
  const seenEvidence = new Set();

  for (const ev of [...evidenceNodes, ...satisfiedEvidence]) {
    const evKey = ev.id || `${ev.source}:${ev.label || ev.claim}`;
    if (!evKey || seenEvidence.has(evKey)) continue;
    seenEvidence.add(evKey);
    const data = ev.data || ev;
    const provider = asText(data.providerId || data.provider || data.source);
    const label = asText(data.label || data.claim || data.source || 'Evidence');
    const evId = addNode(graph, {
      id: stableId('evidence', evKey),
      kind: NODE_KINDS.EVIDENCE,
      label,
      rationale: `Collected via ${provider || 'investigation'} to satisfy evidence requirements.`,
      parentIds: [planId],
      source: provider || 'investigation',
      reasoning: label,
      confidence: data.weight != null ? Number(data.weight) : data.confidence != null ? Number(data.confidence) : null,
      supportingEvidence: [{ source: provider, observation: label }],
      metadata: {
        providerId: data.providerId || null,
        evidenceType: data.evidenceType || data.kind || null,
        observedAt: data.observedAt || null,
      },
    });
    evidenceIds.push(evId);
  }

  if (!evidenceIds.length) {
    const placeholderId = addNode(graph, {
      id: 'evidence:pending',
      kind: NODE_KINDS.EVIDENCE,
      label: 'Evidence collection pending or insufficient',
      rationale: 'No exportable evidence nodes yet; judgment proceeds from available understanding.',
      parentIds: [planId],
      source: 'investigation',
      reasoning: 'Outstanding evidence requirements remain on the plan.',
      confidence: 0,
      metadata: { placeholder: true },
    });
    evidenceIds.push(placeholderId);
  }

  const businessUnderstanding =
    mir?.businessUnderstanding ||
    (investigationState.businessUnderstandings?.length
      ? {
          items: investigationState.businessUnderstandings,
          synthesizedNotRaw: true,
        }
      : null);

  const understandingSummary =
    asText(businessUnderstanding?.summary) ||
    (businessUnderstanding?.items || [])
      .slice(0, 3)
      .map((item) => `${item.entity}: ${(item.assertions || []).join('; ')}`)
      .join(' | ') ||
    asText(synthesis?.summary) ||
    'Synthesized business understanding from collected evidence.';

  const understandingId = addNode(graph, {
    id: stableId('understanding', understandingSummary),
    kind: NODE_KINDS.UNDERSTANDING,
    label: 'Business understanding',
    rationale: understandingSummary,
    parentIds: evidenceIds,
    source: 'evidence_synthesis_engine',
    reasoning: understandingSummary,
    confidence:
      businessUnderstanding?.items?.[0]?.confidence != null
        ? Number(businessUnderstanding.items[0].confidence)
        : synthesis?.confidence != null
          ? Number(synthesis.confidence)
          : null,
    supportingEvidence: (businessUnderstanding?.items || []).flatMap((item) =>
      (item.assertions || []).map((assertion) => ({
        source: item.entity,
        observation: assertion,
      }))
    ),
    contradictoryEvidence: (synthesis?.contradictions || []).map((row) => ({
      source: row.source || 'synthesis',
      observation: row.description || row.text || String(row),
    })),
    metadata: {
      itemCount: (businessUnderstanding?.items || []).length,
      synthesizedNotRaw: businessUnderstanding?.synthesizedNotRaw === true,
    },
  });

  const judgmentExplanation = explainJudgment(judgment || {});
  const judgmentSummary =
    asText(judgmentExplanation.overallJudgment?.summary) ||
    asText(judgment?.overallJudgment?.summary) ||
    'Business judgment from activated heuristics.';

  const judgmentId = addNode(graph, {
    id: stableId('judgment', judgmentSummary),
    kind: NODE_KINDS.JUDGMENT,
    label: 'Business judgment',
    rationale: judgmentSummary,
    parentIds: [understandingId],
    source: 'business_heuristics_engine',
    reasoning: judgmentSummary,
    confidence:
      judgmentExplanation.overallJudgment?.confidence != null
        ? Number(judgmentExplanation.overallJudgment.confidence)
        : judgment?.overallJudgment?.confidence != null
          ? Number(judgment.overallJudgment.confidence)
          : null,
    supportingEvidence: (judgmentExplanation.activatedHeuristics || []).flatMap((h) =>
      (h.evidence || []).map((e) => ({
        source: e.source,
        observation: e.observation,
      }))
    ),
    contradictoryEvidence: (judgmentExplanation.activatedHeuristics || []).flatMap((h) =>
      (h.contradictoryEvidence || []).map((e) => ({
        source: e.source,
        observation: e.observation,
      }))
    ),
    metadata: {
      activatedHeuristics: (judgmentExplanation.activatedHeuristics || []).map((h) => ({
        name: h.name,
        heuristicId: h.heuristicId,
        score: h.score,
      })),
      heuristicCount: judgmentExplanation.heuristicCount || 0,
    },
  });

  const recommendation = mir?.recommendation || input.recommendation || null;
  const recommendationSummary =
    asText(recommendation?.summary) ||
    asText(recommendation?.recommendationHint) ||
    'No recommendation recorded';

  const recommendationId = addNode(graph, {
    id: stableId('recommendation', recommendationSummary),
    kind: NODE_KINDS.RECOMMENDATION,
    label: recommendationSummary,
    rationale:
      asText(recommendation?.rationale) ||
      `Recommendation derived from business judgment, not directly from provider output.`,
    parentIds: [judgmentId],
    source: 'mission_intelligence_report',
    reasoning: recommendationSummary,
    confidence: recommendation?.confidence != null ? Number(recommendation.confidence) : null,
    metadata: {
      kind: recommendation?.kind || null,
      basedOnUnderstanding: recommendation?.basedOnUnderstanding === true,
      basedOnHeuristics: recommendation?.basedOnHeuristics === true,
      notDirectFromEvidence: recommendation?.notDirectFromEvidence === true,
    },
  });

  graph.recommendationId = recommendationId;
  validateExplainabilityGraph(graph);
  return finalizeExplainabilityGraph(graph);
}

function formatEvidenceRef(ev) {
  if (typeof ev === 'string') return { source: 'evidence', observation: ev };
  return {
    source: ev.source || ev.providerId || 'evidence',
    observation: ev.observation || ev.label || ev.claim || String(ev),
  };
}

/**
 * Enforce cognitive chain invariants (ADR-098).
 * @param {object} graph
 */
function validateExplainabilityGraph(graph) {
  const recommendationId = graph.recommendationId;
  if (!recommendationId) {
    throw explainabilityError('Explainability graph missing recommendation node.');
  }

  const recommendation = getNode(graph, recommendationId);
  if (!recommendation || recommendation.kind !== NODE_KINDS.RECOMMENDATION) {
    throw explainabilityError('Recommendation node is invalid.');
  }

  const ancestors = ancestorIds(graph, recommendationId);
  const ancestorNodes = [...ancestors].map((id) => getNode(graph, id)).filter(Boolean);
  const kinds = new Set(ancestorNodes.map((n) => n.kind));

  const requiredKinds = [
    NODE_KINDS.OBJECTIVE,
    NODE_KINDS.MARKET_DEFINITION,
    NODE_KINDS.JUDGMENT,
    NODE_KINDS.UNDERSTANDING,
    NODE_KINDS.PLAN,
  ];

  for (const kind of requiredKinds) {
    if (!kinds.has(kind)) {
      throw explainabilityError(`Recommendation trace missing required ancestor kind: ${kind}.`);
    }
  }

  if (recommendation.parentIds.length !== 1 || recommendation.parentIds[0] !== getNodesByKind(graph, NODE_KINDS.JUDGMENT)[0]?.id) {
    const judgmentNode = getNodesByKind(graph, NODE_KINDS.JUDGMENT)[0];
    if (!judgmentNode || !recommendation.parentIds.includes(judgmentNode.id)) {
      throw explainabilityError('Recommendation must parent directly to judgment node.');
    }
  }

  for (const node of [recommendation, ...ancestorNodes]) {
    if (isProviderTerminalNode(node)) {
      throw explainabilityError(
        `Provider "${node.label}" cannot be a terminal reasoning node — trace must reach mission objective.`
      );
    }
  }

  const directParents = recommendation.parentIds.map((id) => getNode(graph, id)).filter(Boolean);
  for (const parent of directParents) {
    if (PROVIDER_TERMINALS.some((p) => asText(parent.label).toLowerCase().includes(p))) {
      throw explainabilityError('Recommendation cannot trace directly to a provider.');
    }
  }
}

function isProviderTerminalNode(node) {
  if (!node || node.kind === NODE_KINDS.OBJECTIVE) return false;
  if (node.kind === NODE_KINDS.RECOMMENDATION) return false;
  const label = asText(node.label).toLowerCase();
  const source = asText(node.source).toLowerCase();
  if (node.kind === NODE_KINDS.EVIDENCE) return false;
  return PROVIDER_TERMINALS.some((p) => label === p || source === p);
}

function explainabilityError(message) {
  const err = new Error(message);
  err.code = 'EXPLAINABILITY_GRAPH_INVALID';
  err.spec = EXPLAINABILITY_SPEC;
  return err;
}

/**
 * Trace a recommendation back through the cognitive chain.
 * @param {object} graph
 * @param {string} [recommendationId]
 * @returns {object}
 */
function traceRecommendation(graph, recommendationId = null) {
  const recId = recommendationId || graph.recommendationId;
  const path = tracePath(graph, recId);
  return Object.freeze({
    spec: EXPLAINABILITY_SPEC,
    recommendationId: recId,
    path,
    terminatesAtObjective: path.some((node) => node.kind === NODE_KINDS.OBJECTIVE),
    chain: path.map((node) => ({
      kind: node.kind,
      label: node.label,
      rationale: node.rationale,
      confidence: node.confidence,
    })),
  });
}

/**
 * Human-readable cognitive chain for operators.
 * @param {object} graph
 * @returns {string[]}
 */
function serializeForOperator(graph) {
  const trace = traceRecommendation(graph);
  const lines = ['Cognitive reasoning chain (SPEC-183):', ''];

  for (const node of trace.path) {
    const header = `${kindLabel(node.kind)}: ${node.label}`;
    lines.push(header);
    if (node.rationale && node.rationale !== node.label) {
      lines.push(`  Rationale: ${node.rationale}`);
    }
    if (node.confidence != null) {
      lines.push(`  Confidence: ${Math.round(node.confidence * 100)}%`);
    }
    if (node.supportingEvidence.length) {
      lines.push(`  Supporting: ${node.supportingEvidence.slice(0, 3).map(formatEvidenceLine).join('; ')}`);
    }
    if (node.contradictoryEvidence.length) {
      lines.push(`  Contradictory: ${node.contradictoryEvidence.slice(0, 2).map(formatEvidenceLine).join('; ')}`);
    }
    lines.push('');
  }

  return lines;
}

function kindLabel(kind) {
  const labels = {
    objective: 'Mission Objective',
    market_definition: 'Market Definition',
    hypothesis: 'Hypothesis',
    plan: 'Investigation Plan',
    evidence: 'Evidence',
    understanding: 'Understanding',
    judgment: 'Judgment',
    recommendation: 'Recommendation',
  };
  return labels[kind] || kind;
}

function formatEvidenceLine(ev) {
  return `${ev.source}: ${ev.observation}`;
}

/**
 * SPEC-173-safe projection for AMO boundary.
 * @param {object} graph
 * @returns {object}
 */
function serializeForAmo(graph) {
  const trace = traceRecommendation(graph);
  const projection = {
    spec: EXPLAINABILITY_SPEC,
    adr: EXPLAINABILITY_ADR,
    recommendationId: graph.recommendationId,
    terminatesAtObjective: trace.terminatesAtObjective,
    chain: trace.chain,
    summary: trace.path.map((node) => `${kindLabel(node.kind)}: ${node.label}`).join(' → '),
    recommendation: trace.path.find((node) => node.kind === NODE_KINDS.RECOMMENDATION) || null,
    businessUnderstanding: trace.path.find((node) => node.kind === NODE_KINDS.UNDERSTANDING)?.rationale || null,
    businessJudgment: trace.path.find((node) => node.kind === NODE_KINDS.JUDGMENT)?.rationale || null,
    boundaryProjected: true,
  };

  if (containsForbiddenReasoningKeys(projection)) {
    throw explainabilityError('AMO explainability projection contains forbidden reasoning keys.');
  }

  return Object.freeze(projection);
}

function serializeGraph(graph) {
  return Object.freeze({
    spec: graph.spec,
    adr: graph.adr,
    missionId: graph.missionId,
    recommendationId: graph.recommendationId,
    createdAt: graph.createdAt,
    nodes: [...graph.nodes.values()],
  });
}

function deserializeGraph(data = {}) {
  const graph = createExplainabilityGraph({
    missionId: data.missionId,
    recommendationId: data.recommendationId,
    createdAt: data.createdAt,
  });
  for (const node of data.nodes || []) {
    addNode(graph, node);
  }
  return graph;
}

/**
 * Answer operator explainability questions via graph traversal.
 * @param {object} graph
 * @param {string} question
 * @returns {string|null}
 */
function answerOperatorQuestion(graph, question = '') {
  const q = asText(question).toLowerCase();
  if (!q) return null;

  if (/linkedin|why.*(?:search|use|provider)|why this provider/.test(q)) {
    return answerProviderQuestion(graph, q);
  }

  if (/reject|why not|eliminated|passed over/.test(q)) {
    return answerRejectionQuestion(graph);
  }

  if (/terminology|what terms|what did we test/.test(q)) {
    return answerTerminologyQuestion(graph);
  }

  if (/why this recommendation|why recommend/.test(q)) {
    const trace = traceRecommendation(graph);
    const rec = trace.path.find((node) => node.kind === NODE_KINDS.RECOMMENDATION);
    const judgment = trace.path.find((node) => node.kind === NODE_KINDS.JUDGMENT);
    return (
      `${rec?.label || 'This recommendation'} ` +
      `because ${judgment?.rationale || 'business judgment evaluated synthesized understanding'}. ` +
      `The full chain traces to mission objective — not directly to any provider.`
    );
  }

  if (/what would change|change the recommendation/.test(q)) {
    return answerWhatWouldChange(graph);
  }

  if (/why this evidence/.test(q)) {
    const evidenceNodes = getNodesByKind(graph, NODE_KINDS.EVIDENCE).filter((n) => !n.metadata?.placeholder);
    if (!evidenceNodes.length) return 'No collected evidence nodes on the current trace.';
    const plan = getNodesByKind(graph, NODE_KINDS.PLAN)[0];
    return (
      `Evidence was collected to satisfy plan requirements: ${plan?.rationale || 'investigation plan'}. ` +
      `Examples: ${evidenceNodes.slice(0, 3).map((n) => n.label).join('; ')}.`
    );
  }

  if (/why not another company|another company|compare/.test(q)) {
    return (
      'Company ranking flows through understanding and judgment nodes — ask about the recommendation ' +
      'or rejection rationale for a specific comparison.'
    );
  }

  return serializeForOperator(graph).slice(0, 8).join('\n');
}

function answerProviderQuestion(graph, question) {
  const plan = getNodesByKind(graph, NODE_KINDS.PLAN)[0];
  const assignments = plan?.metadata?.providerAssignments || [];
  const providerMatch = question.match(/\b(linkedin|google|website|maps|places)\b/);
  const providerHint = providerMatch ? providerMatch[1] : null;

  const match = providerHint
    ? assignments.find((row) => asText(row.providerId).includes(providerHint))
    : assignments[0];

  if (match?.explanation) {
    return `${match.explanation} (Plan → evidence requirement → provider assignment — not a direct recommendation driver.)`;
  }

  const hypNodes = getNodesByKind(graph, NODE_KINDS.HYPOTHESIS);
  const hypText = hypNodes[0]?.label || 'the active hypothesis';
  return (
    `Provider selection serves investigation plan evidence requirements for: ${hypText}. ` +
    `Trace: recommendation → judgment → understanding → evidence → plan → provider assignment.`
  );
}

function answerRejectionQuestion(graph) {
  const judgment = getNodesByKind(graph, NODE_KINDS.JUDGMENT)[0];
  const understanding = getNodesByKind(graph, NODE_KINDS.UNDERSTANDING)[0];
  const rejected = getNodesByKind(graph, NODE_KINDS.HYPOTHESIS).filter(
    (node) => node.metadata?.lifecycle === 'rejected' || node.metadata?.lifecycle === 'refuted'
  );

  const parts = [
    judgment?.rationale || 'Business judgment evaluated synthesized understanding.',
  ];

  if (understanding?.contradictoryEvidence?.length) {
    parts.push(
      `Contradictory evidence: ${understanding.contradictoryEvidence.slice(0, 2).map(formatEvidenceLine).join('; ')}.`
    );
  }

  if (rejected.length) {
    parts.push(`Rejected hypotheses: ${rejected.map((h) => h.label).join('; ')}.`);
  }

  return parts.join(' ');
}

function answerTerminologyQuestion(graph) {
  const market = getNodesByKind(graph, NODE_KINDS.MARKET_DEFINITION)[0];
  const terminology = market?.metadata?.terminology || [];
  const hypNodes = getNodesByKind(graph, NODE_KINDS.HYPOTHESIS);
  const terminologyHyps = hypNodes.filter(
    (node) => node.metadata?.kind === 'terminology' || /terminolog|term|call themselves/i.test(node.label)
  );

  if (terminologyHyps.length) {
    return `Terminology hypotheses tested: ${terminologyHyps.map((h) => h.label).join('; ')}.`;
  }

  if (terminology.length) {
    return `Market terminology under investigation: ${terminology.join(', ')}.`;
  }

  return `Terminology derives from market definition: ${market?.label || 'see market definition node'}.`;
}

function answerWhatWouldChange(graph) {
  const understanding = getNodesByKind(graph, NODE_KINDS.UNDERSTANDING)[0];
  const plan = getNodesByKind(graph, NODE_KINDS.PLAN)[0];
  const outstanding = (plan?.metadata?.evidenceRequirements || []).length;
  const gaps = [];

  if (understanding?.contradictoryEvidence?.length) {
    gaps.push('resolving contradictory evidence');
  }
  if (outstanding) {
    gaps.push('satisfying outstanding evidence requirements on the investigation plan');
  }
  gaps.push('new business understanding that shifts heuristic activation');

  return `The recommendation would change with ${gaps.join(', ')} — not merely additional provider output without synthesis and judgment.`;
}

module.exports = {
  EXPLAINABILITY_SPEC,
  EXPLAINABILITY_ADR,
  NODE_KINDS,
  PROVIDER_TERMINALS,
  finalizeExplainabilityGraph,
  createExplainabilityGraph,
  buildExplainabilityGraph,
  validateExplainabilityGraph,
  traceRecommendation,
  serializeForOperator,
  serializeForAmo,
  serializeGraph,
  deserializeGraph,
  answerOperatorQuestion,
  getNode,
  getNodesByKind,
  ancestorIds,
  tracePath,
};
