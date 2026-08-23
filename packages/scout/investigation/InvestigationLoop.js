'use strict';

/**
 * SPEC-142 — Investigation Loop orchestrator.
 * Hypothesis → Evidence Required → Collect → Fuse → Update Confidence → Repeat
 */

const { buildMarketDefinition } = require('../intelligence/MarketUnderstanding');
const { discoverCandidateUniverse } = require('../intelligence/CandidateDiscovery');
const { qualifyCandidates } = require('../intelligence/Qualification');
const { rankOpportunities } = require('../intelligence/OpportunityRanking');
const { collectEvidence } = require('../intelligence/EvidenceCollection');
const { buildProviderStrategy } = require('../intelligence/ProviderStrategy');
const { buildEvidencePlan } = require('../intelligence/EvidencePlanning');

const {
  buildInvestigationResult,
  COMPLETION_REASONS,
  DEFAULT_CONFIDENCE_THRESHOLD,
  DEFAULT_MAX_ITERATIONS,
  DEFAULT_MAX_COST_BUDGET,
} = require('./types');
const { createInvestigationGraph, addEvidenceNode, addClaimNode, addHypothesisNode, serializeGraph } = require('./InvestigationGraph');
const { generateHypotheses, generateCandidateHypotheses } = require('./HypothesisGeneration');
const { determineMissingEvidence, evidenceSatisfiesGap } = require('./MissingEvidence');
const { selectNextInvestigation } = require('./InvestigationPlanner');
const { fuseAndUpdateClaims } = require('./ClaimConfidence');
const { detectContradictions } = require('./ContradictionDetection');
const { executeInvestigationStep } = require('./EvidenceExecutor');
const { buildInvestigationReport } = require('./InvestigationReport');
const {
  emitInvestigationStarted,
  emitInvestigationIteration,
  emitInvestigationStep,
  emitInvestigationConflict,
  emitInvestigationCompleted,
} = require('./observability');
const {
  prepareInvestigationWithMemory,
  persistInvestigationKnowledge,
  emitMemoryEvent,
  MEMORY_EVENTS,
} = require('../memory');

function mergeCandidateEvidence(candidate, collected) {
  const existing = candidate.evidence || [];
  const merged = [...existing];
  for (const item of collected) {
    if (!merged.some((e) => e.id === item.id)) merged.push(item);
  }
  return { ...candidate, evidence: merged };
}

function computeOverallConfidence(claims) {
  if (!claims.length) return 0;
  return Number((claims.reduce((s, c) => s + (c.confidence || 0), 0) / claims.length).toFixed(2));
}

function isInvestigationComplete(state, opts) {
  const threshold = opts.confidenceThreshold || DEFAULT_CONFIDENCE_THRESHOLD;
  const maxIterations = opts.maxIterations || DEFAULT_MAX_ITERATIONS;
  const maxCost = opts.maxCostBudget || DEFAULT_MAX_COST_BUDGET;

  if (state.iteration >= maxIterations) {
    return { complete: true, reason: COMPLETION_REASONS.NO_HIGHER_VALUE_EVIDENCE };
  }
  if (state.totalCost >= maxCost) {
    return { complete: true, reason: COMPLETION_REASONS.COST_EXCEEDS_BENEFIT };
  }
  if (state.overallConfidence >= threshold && (state.missingEvidence.missing || []).length === 0) {
    return { complete: true, reason: COMPLETION_REASONS.CONFIDENCE_THRESHOLD };
  }
  if (state.coverageFinished) {
    return { complete: true, reason: COMPLETION_REASONS.COVERAGE_COMPLETE };
  }
  if (!state.nextStep && state.iteration > 0) {
    return { complete: true, reason: COMPLETION_REASONS.NO_HIGHER_VALUE_EVIDENCE };
  }
  return { complete: false, reason: null };
}

/**
 * Run the hypothesis-driven investigation engine.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function runInvestigationEngine(input = {}) {
  const opts = input.opts || {};
  const mission = input.mission || {};
  const missionId = mission.id;

  emitInvestigationStarted({ missionId, tenantId: mission.tenantId });

  const marketDefinition = buildMarketDefinition(input);
  if (!marketDefinition.valid) {
    emitInvestigationCompleted({ missionId, outcome: 'blocked' });
    return buildInvestigationResult({
      outcome: 'blocked',
      completionReason: COMPLETION_REASONS.BLOCKED,
      marketDefinition,
    });
  }

  const baseHypotheses = generateHypotheses(marketDefinition, mission, opts);

  const memoryPrep = await prepareInvestigationWithMemory({
    tenantId: mission.tenantId || mission.clientId,
    mission,
    marketDefinition,
    opts: { ...opts, store: opts.memoryStore },
  });
  const startingPoint = memoryPrep.startingPoint;
  if (memoryPrep.hasPriorKnowledge) {
    emitMemoryEvent(MEMORY_EVENTS.LOADED, {
      tenantId: mission.tenantId,
      missionId,
      counts: startingPoint.counts,
    });
  }

  const candidateUniverse = await discoverCandidateUniverse({
    marketDefinition,
    delegation: input.delegation,
    mission,
    scoutPayload: input.scoutPayload,
    opts,
  });

  let candidates = candidateUniverse.candidates || candidateUniverse.resolved || [];

  if ((startingPoint.inheritedEvidence || []).length) {
    const evidenceByEntity = new Map();
    for (const item of startingPoint.inheritedEvidence) {
      const key = item.entityId;
      if (!evidenceByEntity.has(key)) evidenceByEntity.set(key, []);
      evidenceByEntity.get(key).push(item);
    }
    candidates = candidates.map((c) => {
      const inherited = evidenceByEntity.get(c.id) || [];
      if (!inherited.length) return c;
      const merged = [...(c.evidence || [])];
      for (const item of inherited) {
        if (!merged.some((e) => e.source === item.source && e.label === item.label)) {
          merged.push(item);
        }
      }
      return { ...c, evidence: merged };
    });
  }

  const graph = createInvestigationGraph({
    missionId: mission.id,
    mission,
    market: marketDefinition,
    candidates,
  });

  let allHypotheses = [];
  for (const candidate of candidates) {
    const candidateHyps = generateCandidateHypotheses(baseHypotheses, candidate);
    for (const hyp of candidateHyps) addHypothesisNode(graph, hyp);
    allHypotheses = allHypotheses.concat(candidateHyps);
  }

  const evidencePlan = buildEvidencePlan(marketDefinition, opts);
  const providerStrategy = buildProviderStrategy(evidencePlan, opts);

  const attempted = new Set();
  for (const step of startingPoint.skippedSteps || []) {
    const stepKey = `${step.entityId || 'global'}:${step.gap}:${step.providerId}:${step.capability}`;
    attempted.add(stepKey);
  }
  const resolvedGaps = new Set(
    (memoryPrep.memory?.investigation?.resolvedGaps || []).map(String)
  );
  const iterations = [];
  let totalCost = 0;
  let allClaims = (startingPoint.preloadedClaims || []).map((c) => ({ ...c }));
  let workingCandidates = candidates.map((c) => ({ ...c, evidence: c.evidence || [] }));
  let allConflicts = [];

  for (let iteration = 0; iteration < (opts.maxIterations || DEFAULT_MAX_ITERATIONS); iteration += 1) {
    const iterationClaims = [];
    const iterationConflicts = [];

    for (const candidate of workingCandidates) {
      const candidateHyps = allHypotheses.filter((h) => h.entityId === candidate.id);
      const conflicts = detectContradictions(candidate, candidate.evidence || []);
      if (conflicts.length) {
        for (const c of conflicts) emitInvestigationConflict({ missionId, conflict: c });
        iterationConflicts.push(...conflicts);
      }

      const { claims, hypotheses: updatedHyps, fused } = fuseAndUpdateClaims(
        candidate,
        candidateHyps,
        [],
        conflicts
      );

      allHypotheses = allHypotheses.map((h) => updatedHyps.find((u) => u.id === h.id) || h);

      for (const item of fused.evidence || []) {
        addEvidenceNode(graph, item, `candidate:${candidate.id}`);
      }
      for (const claim of claims) {
        addClaimNode(graph, claim, `candidate:${candidate.id}`);
      }

      iterationClaims.push(...claims);
    }

    allClaims = iterationClaims;
    allConflicts = iterationConflicts;
    const missingEvidence = determineMissingEvidence({ hypotheses: allHypotheses, claims: allClaims });
    const overallConfidence = computeOverallConfidence(allClaims);

    const nextStep = selectNextInvestigation({
      missing: missingEvidence.missing,
      attempted: [...attempted],
      resolvedGaps: [...resolvedGaps],
      entityId: workingCandidates[0] && workingCandidates[0].id,
      registry: opts.registry,
    });

    const completion = isInvestigationComplete(
      {
        iteration,
        totalCost,
        overallConfidence,
        missingEvidence,
        nextStep,
        coverageFinished: false,
      },
      opts
    );

    iterations.push({
      iteration: iteration + 1,
      phase: completion.complete ? 'complete' : 'investigate',
      overallConfidence,
      missing: missingEvidence.missing,
      claimsCount: allClaims.length,
      conflictsCount: allConflicts.length,
      nextStep: nextStep || null,
    });

    emitInvestigationIteration({
      missionId,
      iteration: iteration + 1,
      overallConfidence,
      missingCount: missingEvidence.missing.length,
    });

    if (completion.complete) break;
    if (!nextStep) break;

    const targetCandidate =
      workingCandidates.find((c) => c.id === nextStep.entityId) || workingCandidates[0];
    if (!targetCandidate) break;

    const stepKey = `${nextStep.entityId || 'global'}:${nextStep.gap}:${nextStep.providerId}:${nextStep.capability}`;
    attempted.add(stepKey);

    const result = await executeInvestigationStep(nextStep, targetCandidate, opts);
    emitInvestigationStep({ missionId, step: nextStep, collected: result.collected.length });

    totalCost += result.cost || 0;
    for (const gap of result.resolvedGaps || []) resolvedGaps.add(gap);

    workingCandidates = workingCandidates.map((c) =>
      c.id === targetCandidate.id ? mergeCandidateEvidence(c, result.collected) : c
    );

    if (result.resolvedGaps && result.resolvedGaps.length > 0) {
      for (const hyp of allHypotheses.filter((h) => h.entityId === targetCandidate.id)) {
        const gapEvidence = (hyp.requiredEvidence || []).filter((req) =>
          evidenceSatisfiesGap(nextStep.gap, [req, ...(result.collected || []).map((e) => e.evidenceType)])
        );
        if (gapEvidence.length > 0 || result.resolvedGaps.includes(nextStep.gap)) {
          hyp.collectedEvidence = [...(hyp.collectedEvidence || []), ...(result.collected || [])];
        }
      }
    }
  }

  const evidenceCollection = collectEvidence({
    candidateUniverse: { candidates: workingCandidates },
    providerStrategy,
  });

  const qualification = await qualifyCandidates({
    marketDefinition,
    candidateUniverse: { candidates: workingCandidates },
    evidenceCollection,
    opts,
  });

  const ranking = rankOpportunities({
    qualification,
    evidenceCollection,
    candidateUniverse: { candidates: workingCandidates },
  });

  const finalMissing = determineMissingEvidence({ hypotheses: allHypotheses, claims: allClaims });
  const overallConfidence = computeOverallConfidence(allClaims);
  const serializedGraph = serializeGraph(graph);

  const report = buildInvestigationReport({
    mission,
    marketDefinition,
    graph: serializedGraph,
    hypotheses: allHypotheses,
    claims: allClaims,
    missingEvidence: finalMissing,
    overallConfidence,
    qualification,
    ranking,
    candidateUniverse,
    conflicts: allConflicts,
    iterations,
    providerStrategy,
  });

  const lastIteration = iterations[iterations.length - 1];
  const completionReason =
    lastIteration && lastIteration.phase === 'complete'
      ? COMPLETION_REASONS.CONFIDENCE_THRESHOLD
      : overallConfidence >= (opts.confidenceThreshold || DEFAULT_CONFIDENCE_THRESHOLD)
        ? COMPLETION_REASONS.CONFIDENCE_THRESHOLD
        : COMPLETION_REASONS.NO_HIGHER_VALUE_EVIDENCE;

  emitInvestigationCompleted({
    missionId,
    outcome: 'completed',
    overallConfidence,
    claims: allClaims.length,
  });

  const investigationResult = buildInvestigationResult({
    outcome: candidates.length > 0 ? 'completed' : 'partial',
    completionReason,
    iterations,
    graph: serializedGraph,
    hypotheses: allHypotheses,
    claims: allClaims,
    missingEvidence: finalMissing,
    report,
    marketDefinition,
    candidateUniverse: { ...candidateUniverse, candidates: workingCandidates },
    overallConfidence,
    totalCost,
    qualification,
    ranking,
    evidenceCollection,
    providerStrategy,
    evidencePlan,
  });

  let memoryPersist = null;
  if (opts.persistMemory !== false) {
    memoryPersist = await persistInvestigationKnowledge(investigationResult, {
      tenantId: mission.tenantId || mission.clientId,
      missionId,
      mission,
      store: opts.memoryStore,
      completedAt: new Date().toISOString(),
      opts,
    });
  }

  return {
    ...investigationResult,
    startingPoint,
    memoryLoaded: memoryPrep.hasPriorKnowledge,
    memoryPersist,
  };
}

module.exports = {
  runInvestigationEngine,
  computeOverallConfidence,
  isInvestigationComplete,
};
