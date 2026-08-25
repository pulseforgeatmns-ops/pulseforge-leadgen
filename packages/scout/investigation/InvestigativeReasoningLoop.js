'use strict';

/**
 * SPEC-159 — Investigative Reasoning Loop.
 *
 * Observe → Think → Update Understanding → Decide → Investigate → Repeat
 *
 * Scout is no longer executing a plan. Scout is reducing uncertainty.
 * Invariant (ADR-079): search results never flow directly into conclusions.
 */

const { asText } = require('../../max/scoutAcquisition/Types');
const { inferTerminologyRevision } = require('./SearchHypothesisEngine');
const {
  createInvestigationState,
  applyMarketDefinitionRevision,
  applyUniverseEstimateRevision,
  applyBusinessUnderstandingSynthesis,
  applyBusinessJudgment,
  updateHypothesisBuckets,
  addEvidenceToGraph,
  setNextQuestions,
  updateUncertainty,
  recordConfidenceStep,
  serializeInvestigationState,
} = require('./InvestigationState');
const {
  HYPOTHESIS_LIFECYCLE,
  applySearchHypothesisEvaluation,
  generateReplacementHypotheses,
  buildHypothesisLifecycleRecord,
  markHypothesisTesting,
} = require('./HypothesisLifecycle');
const { buildMissionIntelligenceReport } = require('./MissionIntelligenceReport');
const { synthesizeFromCandidates } = require('../synthesis/EvidenceSynthesisEngine');
const { activateHeuristics } = require('../heuristics/BusinessHeuristicsEngine');
const { COMPLETION_REASONS } = require('./types');

const DEFAULT_UNCERTAINTY_THRESHOLD = 0.15;
const DEFAULT_CONFIDENCE_TARGET = 0.85;

function extractEvidenceFromCandidate(candidate) {
  const items = [];
  const candidateId = `candidate:${candidate.id}`;

  for (const signal of candidate.signals || []) {
    items.push({
      id: `signal:${candidate.id}:${signal.type || items.length}`,
      source: signal.source || 'signal',
      label: signal.label || signal.type,
      kind: signal.type,
      relation: 'supports',
      relatedTo: candidateId,
      relationshipType: signal.type === 'expansion' ? 'recently_expanded' : 'buying_signal',
    });
  }

  for (const row of candidate.evidence || []) {
    items.push({
      id: row.id || `evidence:${candidate.id}:${items.length}`,
      source: row.source || 'collected',
      label: row.label || row.kind,
      kind: row.kind,
      relation: 'supports',
      relatedTo: candidateId,
    });
  }

  if (candidate.discoveryConcept) {
    items.push({
      id: `terminology:${candidate.id}`,
      source: 'search',
      label: `Discovered via "${candidate.discoveryConcept}" terminology`,
      kind: 'terminology_evidence',
      relation: 'discovered_via',
      relatedTo: candidateId,
      relationshipType: 'terminology_match',
    });
  }

  return items;
}

function fuseEvidenceBatch(state, evidenceBatch = []) {
  let next = state;
  let understandingChanged = false;

  for (const item of evidenceBatch) {
    const parentId = item.relatedTo || null;
    next = addEvidenceToGraph(next, [item], parentId);
  }

  if (evidenceBatch.length > 0) {
    understandingChanged = true;
    next = { ...next, phase: 'think' };
  }

  return { state: next, understandingChanged, evidenceCount: evidenceBatch.length };
}

function processHypothesisEvidence(state, searchHypotheses = []) {
  let next = state;
  let understandingChanged = false;
  const evaluated = [];

  for (const hyp of searchHypotheses) {
    const record = applySearchHypothesisEvaluation(hyp, {
      status: hyp.status,
      confidence: hyp.confidence,
      resultCount: hyp.evidence?.resultCount,
      reason: hyp.evidence?.reason,
    });
    evaluated.push(record);

    if (record.lifecycle === HYPOTHESIS_LIFECYCLE.SUPPORTED || record.lifecycle === HYPOTHESIS_LIFECYCLE.REJECTED) {
      understandingChanged = true;
    }
  }

  next = updateHypothesisBuckets(next, evaluated);

  const revision = inferTerminologyRevision(searchHypotheses);
  if (revision) {
    next = applyMarketDefinitionRevision(next, revision);
    understandingChanged = true;
  }

  const rejected = evaluated.filter((h) => h.lifecycle === HYPOTHESIS_LIFECYCLE.REJECTED);
  if (rejected.length) {
    const replacements = generateReplacementHypotheses(rejected, next.marketDefinition, {
      generateFollowUp: true,
    });
    if (replacements.length) {
      next = updateHypothesisBuckets(next, [...evaluated, ...replacements]);
      understandingChanged = true;
    }
  }

  return { state: next, understandingChanged, evaluated };
}

function computeConfidenceFromEvidence(state, context = {}) {
  let confidence = state.confidence || 0.35;
  const coverage = context.coverageMetrics || state.coverage;
  const candidateCount = context.investigatedCount || 0;
  const hypothesisCount = (state.activeHypotheses || []).filter(
    (h) => h.lifecycle === HYPOTHESIS_LIFECYCLE.SUPPORTED
  ).length;

  if (context.existingIntelligence?.companyCount) {
    confidence = Math.max(confidence, 0.41);
  }
  if (candidateCount > 0) {
    confidence = Math.max(confidence, 0.56);
  }
  if (coverage?.complete) {
    confidence = Math.max(confidence, 0.67);
  }
  if (hypothesisCount > 0) {
    const avgHypConf =
      state.activeHypotheses
        .filter((h) => h.lifecycle === HYPOTHESIS_LIFECYCLE.SUPPORTED)
        .reduce((s, h) => s + (h.confidence || 0), 0) / hypothesisCount;
    confidence = Math.max(confidence, Math.min(0.89, avgHypConf));
  }
  if (coverage?.complete && state.uncertainty.open.length === 0) {
    confidence = Math.max(confidence, 0.89);
  }

  return Number(confidence.toFixed(2));
}

function generateQuestionsFromUncertainty(state) {
  const questions = [];
  const open = state.uncertainty?.open || [];

  for (const unknown of open.slice(0, 5)) {
    questions.push({
      question: typeof unknown === 'string' ? unknown : unknown.label || unknown.question,
      source: 'uncertainty_tracking',
      priority: 'high',
    });
  }

  const defaultUnknowns = [
    'Do operators advertise separately from platform listings?',
    'Are Facebook groups producing evidence?',
    'How many operators self-manage versus use agencies?',
  ];

  for (const text of defaultUnknowns) {
    if (questions.length >= 5) break;
    if (!questions.some((q) => q.question === text)) {
      questions.push({ question: text, source: 'market_unknown', priority: 'medium' });
    }
  }

  for (const hyp of state.activeHypotheses.filter((h) => h.lifecycle === HYPOTHESIS_LIFECYCLE.GENERATED)) {
    questions.push({
      question: `Test hypothesis: ${hyp.text}`,
      source: 'hypothesis',
      priority: 'high',
      hypothesisId: hyp.id,
    });
  }

  return questions.slice(0, 8);
}

function deriveOpenUnknowns(state, context = {}) {
  const open = [];
  const persistent = [];

  for (const hyp of state.rejectedHypotheses || []) {
    open.push(`Rejected: ${hyp.text} — ${hyp.archiveReason || 'insufficient evidence'}`);
  }

  if (context.coverageMetrics && !context.coverageMetrics.complete) {
    open.push('Coverage incomplete — universe estimate may be understated.');
  }

  const fbEvidence = (state.evidenceGraph?.nodes || []).some((n) =>
    /facebook/i.test(n.label || n.data?.source || '')
  );
  if (!fbEvidence) {
    persistent.push('Are Facebook groups producing evidence? — Unknown.');
  }

  return { open: [...new Set(open)], persistent: [...new Set(persistent)] };
}

function shouldStopInvestigation(state, opts = {}) {
  const uncertaintyThreshold = opts.uncertaintyThreshold ?? DEFAULT_UNCERTAINTY_THRESHOLD;
  const confidenceTarget = opts.confidenceTarget ?? DEFAULT_CONFIDENCE_TARGET;
  const coverageThreshold = opts.coverageThreshold ?? 0.8;

  const coveragePct = state.coverage?.searches?.ratio ?? state.coverage?.complete ? 1 : 0;
  const openCount = (state.uncertainty?.open || []).length;
  const persistentCount = (state.uncertainty?.persistent || []).length;
  const remainingUncertainty = openCount / Math.max(openCount + persistentCount + 1, 1);

  const coverageSufficient = state.coverage?.complete === true || coveragePct >= coverageThreshold;
  const uncertaintyLow = remainingUncertainty <= uncertaintyThreshold;
  const noHighValueBranch =
    !state.nextQuestions.some((q) => q.priority === 'high' && q.source === 'hypothesis');

  if (coverageSufficient && uncertaintyLow && noHighValueBranch) {
    return {
      stop: true,
      reason: COMPLETION_REASONS.COVERAGE_COMPLETE,
      explanation: `Coverage sufficient, uncertainty ${Math.round(remainingUncertainty * 100)}% below threshold, no higher-value branches remain.`,
    };
  }

  if (state.confidence >= confidenceTarget && coverageSufficient) {
    return {
      stop: true,
      reason: COMPLETION_REASONS.CONFIDENCE_THRESHOLD,
      explanation: `Confidence ${state.confidence} reached with sufficient coverage.`,
    };
  }

  if (opts.forceComplete) {
    return {
      stop: true,
      reason: COMPLETION_REASONS.NO_HIGHER_VALUE_EVIDENCE,
      explanation: 'Investigation cycle complete for this evidence batch.',
    };
  }

  return { stop: false, reason: null, explanation: null };
}

/**
 * Run one reasoning cycle: collect → fuse → update understanding → decide.
 * @param {object} state — current InvestigationState
 * @param {object} evidenceBatch — new evidence to process
 * @param {object} context — coverage metrics, candidates, etc.
 * @returns {{ state: object, understandingChanged: boolean, stop: object }}
 */
function runReasoningCycle(state, evidenceBatch = [], context = {}) {
  let next = { ...state, phase: 'collect_evidence' };

  const fusion = fuseEvidenceBatch(next, evidenceBatch);
  next = fusion.state;

  if (context.searchHypotheses?.length) {
    const hypResult = processHypothesisEvidence(next, context.searchHypotheses);
    next = hypResult.state;
    fusion.understandingChanged = fusion.understandingChanged || hypResult.understandingChanged;
  }

  if (context.investigatedCount != null && next.universeEstimate) {
    const beforeExpected = next.universeEstimate.expected;
    next = applyUniverseEstimateRevision(next, {
      investigated: context.investigatedCount,
      discovered: context.investigatedCount,
      coverageMetrics: context.coverageMetrics,
      coverageComplete: context.coverageMetrics?.complete,
      reason: `Coverage increased to ${context.investigatedCount} investigated candidates`,
    });
    if (next.universeEstimate?.expected !== beforeExpected) {
      fusion.understandingChanged = true;
    }
  }

  const newConfidence = computeConfidenceFromEvidence(next, context);
  if (newConfidence !== next.confidence) {
    next = recordConfidenceStep(next, {
      confidence: newConfidence,
      reason: context.confidenceReason || 'Evidence fusion updated confidence',
      source: context.confidenceSource || 'evidence_fusion',
    });
    fusion.understandingChanged = true;
  }

  const unknowns = deriveOpenUnknowns(next, context);
  next = updateUncertainty(next, unknowns);

  if (fusion.understandingChanged) {
    next = { ...next, phase: 'update_understanding' };
    next = setNextQuestions(next, generateQuestionsFromUncertainty(next));
  }

  next = { ...next, coverage: context.coverageMetrics || next.coverage, phase: 'investigate' };

  const stop = shouldStopInvestigation(next, context.opts || {});

  return {
    state: next,
    understandingChanged: fusion.understandingChanged,
    stop,
  };
}

/**
 * Run the full investigative reasoning loop over coverage execution results.
 * Processes evidence incrementally — understanding updates before conclusions.
 *
 * @param {object} input
 * @returns {Promise<object>}
 */
async function runInvestigativeReasoningLoop(input = {}) {
  const mission = input.mission || {};
  const opts = input.opts || {};
  const coverageResult = input.coverageResult || {};
  const candidates = coverageResult.candidates || input.candidates || [];
  const searchHypotheses = coverageResult.searchHypotheses || input.searchHypotheses || [];
  const coverageMetrics = coverageResult.coverage || input.coverageMetrics || null;
  const revisedMarketDefinition = coverageResult.revisedMarketDefinition || input.marketDefinition;

  let state = createInvestigationState({
    mission,
    tenantId: input.tenantId || mission.tenantId || mission.clientId,
    marketDefinition: input.marketDefinition,
    universeEstimate: input.universeEstimate,
    hypotheses: searchHypotheses.map((h) => markHypothesisTesting(h, 'Coverage branch executing')),
    candidates,
    coverage: coverageMetrics,
    memory: input.memory || opts.memory,
    initialConfidence: input.initialConfidence != null ? input.initialConfidence : 0.35,
    initialUnknowns: input.initialUnknowns || [],
  });

  const cycles = [];
  const batchSize = opts.evidenceBatchSize || 5;

  for (let i = 0; i < candidates.length; i += batchSize) {
    const batch = candidates.slice(i, i + batchSize);
    const evidenceBatch = batch.flatMap((c) => extractEvidenceFromCandidate(c));

    const cycleResult = runReasoningCycle(state, evidenceBatch, {
      coverageMetrics,
      investigatedCount: Math.min(i + batch.length, candidates.length),
      searchHypotheses: i === 0 ? searchHypotheses : [],
      existingIntelligence: input.existingIntelligence,
      confidenceSource: i === 0 ? 'crm_evidence' : 'places_evidence',
      confidenceReason:
        i === 0
          ? 'CRM / existing intelligence evidence integrated'
          : `Places / discovery evidence batch ${Math.floor(i / batchSize) + 1}`,
      opts,
    });

    state = cycleResult.state;
    cycles.push({
      cycle: cycles.length + 1,
      evidenceProcessed: evidenceBatch.length,
      understandingChanged: cycleResult.understandingChanged,
      confidence: state.confidence,
      phase: state.phase,
    });

    if (cycleResult.stop.stop && i + batchSize >= candidates.length) {
      state = { ...state, phase: 'complete' };
      break;
    }
  }

  if (searchHypotheses.length && candidates.length === 0) {
    const hypResult = processHypothesisEvidence(state, searchHypotheses);
    state = hypResult.state;
    if (revisedMarketDefinition && revisedMarketDefinition.revised) {
      state = { ...state, marketDefinition: revisedMarketDefinition };
    }
    cycles.push({
      cycle: cycles.length + 1,
      evidenceProcessed: 0,
      understandingChanged: hypResult.understandingChanged,
      confidence: state.confidence,
      phase: 'hypothesis_only',
    });
  }

  if (revisedMarketDefinition?.revised && state.marketDefinition?.source !== 'evidence_revision') {
    state = applyMarketDefinitionRevision(state, {
      dominantTerminology: revisedMarketDefinition.terminology?.[0],
      reason: 'Terminology revised from hypothesis-driven discovery',
    });
  }

  state = setNextQuestions(state, generateQuestionsFromUncertainty(state));

  const synthesisResult = synthesizeFromCandidates({
    candidates,
    priorUnderstandings: state.businessUnderstandings || [],
  });
  if (synthesisResult.understandings.length) {
    state = applyBusinessUnderstandingSynthesis(state, synthesisResult);
  }

  const judgmentResult = activateHeuristics({
    businessUnderstandings: state.businessUnderstandings || [],
    heuristicLibrary: opts.heuristicLibrary,
  });
  if (judgmentResult.activatedHeuristics.length) {
    state = applyBusinessJudgment(state, judgmentResult);
  }

  const stop = shouldStopInvestigation(state, { ...opts, forceComplete: true });
  state = { ...state, phase: stop.stop ? 'complete' : 'investigate' };

  const report = buildMissionIntelligenceReport({
    state,
    mission,
    cycles,
    stop,
    candidates,
    coverageMetrics,
    synthesisResult,
    judgmentResult,
  });

  return {
    state: serializeInvestigationState(state),
    report,
    cycles,
    stop,
    understandingFirst: true,
    completionReason: stop.reason,
    stopExplanation: stop.explanation,
  };
}

module.exports = {
  runInvestigativeReasoningLoop,
  runReasoningCycle,
  shouldStopInvestigation,
  fuseEvidenceBatch,
  processHypothesisEvidence,
  computeConfidenceFromEvidence,
  generateQuestionsFromUncertainty,
  extractEvidenceFromCandidate,
  DEFAULT_UNCERTAINTY_THRESHOLD,
  DEFAULT_CONFIDENCE_TARGET,
};
