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
const { selectNextInvestigation, explainStepSelection, DEFAULT_MIN_EXPECTED_GAIN } = require('./InvestigationPlanner');
const { fuseAndUpdateClaims } = require('./ClaimConfidence');
const { detectContradictions } = require('./ContradictionDetection');
const { executeInvestigationStep } = require('./EvidenceExecutor');
const { buildInvestigationReport } = require('./InvestigationReport');
const {
  createInvestigationBoard,
  updateBoardAfterStep,
  summarizeBoard,
  computeCoverage,
  DEFAULT_COVERAGE_THRESHOLD,
} = require('./InvestigationBoard');
const {
  createInvestigationJournal,
  recordJournalStart,
  recordJournalStep,
  recordJournalStop,
  serializeJournal,
} = require('./InvestigationJournal');
const {
  createProviderLearningStore,
  loadLearningFromMemory,
  exportLearningForMemory,
} = require('./ProviderLearning');
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
  const coverageThreshold = opts.coverageThreshold || DEFAULT_COVERAGE_THRESHOLD;
  const minGain = opts.minExpectedGain != null ? opts.minExpectedGain : DEFAULT_MIN_EXPECTED_GAIN;

  if (state.iteration >= maxIterations) {
    return {
      complete: true,
      reason: COMPLETION_REASONS.NO_HIGHER_VALUE_EVIDENCE,
      explanation: `Reached maximum ${maxIterations} investigation iterations`,
    };
  }
  if (state.totalCost >= maxCost) {
    return {
      complete: true,
      reason: COMPLETION_REASONS.COST_EXCEEDS_BENEFIT,
      explanation: `Cost budget ${maxCost} exceeded (spent ${state.totalCost})`,
    };
  }
  if (state.nextStep && state.nextStep.belowGainThreshold) {
    return {
      complete: true,
      reason: COMPLETION_REASONS.DIMINISHING_RETURNS,
      explanation:
        state.nextStep.stopRecommendation?.explanation ||
        `Best next step yields less than ${Math.round(minGain * 100)}% expected information gain`,
    };
  }
  if (state.coveragePct >= coverageThreshold && state.openUnknownCount === 0) {
    return {
      complete: true,
      reason: COMPLETION_REASONS.COVERAGE_COMPLETE,
      explanation: `Coverage ${Math.round(state.coveragePct * 100)}% with all resolvable unknowns addressed`,
    };
  }
  if (
    state.coveragePct >= coverageThreshold &&
    state.overallConfidence >= threshold &&
    state.keyGapsSatisfied
  ) {
    return {
      complete: true,
      reason: COMPLETION_REASONS.COVERAGE_COMPLETE,
      explanation: `Coverage ${Math.round(state.coveragePct * 100)}%, decision maker and buying signals satisfied`,
    };
  }
  if (state.overallConfidence >= threshold && (state.missingEvidence.missing || []).length === 0) {
    return {
      complete: true,
      reason: COMPLETION_REASONS.CONFIDENCE_THRESHOLD,
      explanation: `Confidence threshold ${threshold} reached with no open gaps`,
    };
  }
  if (state.coverageFinished) {
    return {
      complete: true,
      reason: COMPLETION_REASONS.COVERAGE_COMPLETE,
      explanation: 'Market coverage analysis marked investigation finished',
    };
  }
  if (!state.nextStep && state.iteration > 0) {
    const onlyPersistent =
      state.openUnknownCount === 0 && (state.persistentUnknownCount || 0) > 0;
    return {
      complete: true,
      reason: onlyPersistent
        ? COMPLETION_REASONS.PERSISTENT_UNKNOWNS
        : COMPLETION_REASONS.NO_HIGHER_VALUE_EVIDENCE,
      explanation: onlyPersistent
        ? 'Remaining unknowns require human conversation'
        : 'No higher-value evidence steps remain',
    };
  }
  return { complete: false, reason: null, explanation: null };
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

  const learning =
    opts.learningStore ||
    (memoryPrep.memory ? loadLearningFromMemory(memoryPrep.memory) : createProviderLearningStore());

  let board = createInvestigationBoard({
    known: (startingPoint.known || []).map((k) => ({
      gap: k.text ? null : k.gap,
      label: k.text,
      confidence: k.effectiveConfidence,
    })),
    missing: [],
    coverageThreshold: opts.coverageThreshold || DEFAULT_COVERAGE_THRESHOLD,
  });

  const journal = createInvestigationJournal(missionId);
  recordJournalStart(journal, {
    startedWith: 'Understand market and identify highest-value unknowns',
    priorUnknowns: [],
    rationale: memoryPrep.hasPriorKnowledge
      ? 'Loaded prior intelligence from memory'
      : 'Fresh investigation from market definition',
  });

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

  let completionMeta = { reason: null, explanation: null };

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

    board = createInvestigationBoard({
      known: board.known,
      persistent: board.persistent,
      missing: missingEvidence.missing,
      coverageThreshold: opts.coverageThreshold || DEFAULT_COVERAGE_THRESHOLD,
    });
    for (const k of board.known) {
      if (k.gap && missingEvidence.missing.includes(k.gap)) {
        board.known = board.known.filter((x) => x.gap !== k.gap);
      }
    }

    const coveragePct = computeCoverage(board);
    const boardSummary = summarizeBoard(board);

    const nextStep = selectNextInvestigation({
      missing: missingEvidence.missing,
      attempted: [...attempted],
      resolvedGaps: [...resolvedGaps],
      entityId: workingCandidates[0] && workingCandidates[0].id,
      registry: opts.registry,
      board,
      learning,
      memory: memoryPrep.memory,
      minExpectedGain: opts.minExpectedGain,
      adaptivePlanning: opts.adaptivePlanning,
    });

    const openUnknownCount = boardSummary.unknownCount;
    const persistentUnknownCount = boardSummary.persistentCount;
    const knownGaps = new Set((board.known || []).map((k) => k.gap));
    const keyGapsSatisfied =
      knownGaps.has('decision_maker') &&
      (knownGaps.has('buying_signals') || resolvedGaps.has('buying_signals'));

    const completion = isInvestigationComplete(
      {
        iteration,
        totalCost,
        overallConfidence,
        missingEvidence,
        nextStep,
        coverageFinished: false,
        coveragePct,
        openUnknownCount,
        persistentUnknownCount,
        keyGapsSatisfied,
      },
      opts
    );

    const stepSelection = nextStep ? explainStepSelection(nextStep, board) : null;

    iterations.push({
      iteration: iteration + 1,
      phase: completion.complete ? 'complete' : 'investigate',
      overallConfidence,
      missing: missingEvidence.missing,
      claimsCount: allClaims.length,
      conflictsCount: allConflicts.length,
      nextStep: nextStep || null,
      coveragePct,
      board: boardSummary,
      stepSelection,
    });

    emitInvestigationIteration({
      missionId,
      iteration: iteration + 1,
      overallConfidence,
      missingCount: missingEvidence.missing.length,
    });

    if (completion.complete) {
      completionMeta = { reason: completion.reason, explanation: completion.explanation };
      recordJournalStop(journal, {
        stopReason: completion.reason,
        stopExplanation: completion.explanation,
        coveragePct: Math.round(coveragePct * 100),
        remainingUnknowns: boardSummary.unknown.map((u) => u.label),
      });
      break;
    }
    if (!nextStep) break;
    if (nextStep.belowGainThreshold) {
      completionMeta = {
        reason: COMPLETION_REASONS.DIMINISHING_RETURNS,
        explanation: nextStep.stopRecommendation?.explanation,
      };
      recordJournalStop(journal, {
        stopReason: COMPLETION_REASONS.DIMINISHING_RETURNS,
        stopExplanation: nextStep.stopRecommendation?.explanation,
        coveragePct: Math.round(coveragePct * 100),
        remainingUnknowns: boardSummary.unknown.map((u) => u.label),
      });
      break;
    }

    const targetCandidate =
      workingCandidates.find((c) => c.id === nextStep.entityId) || workingCandidates[0];
    if (!targetCandidate) break;

    const stepKey = `${nextStep.entityId || 'global'}:${nextStep.gap}:${nextStep.providerId}:${nextStep.capability}`;
    attempted.add(stepKey);

    const priorUnknowns = boardSummary.unknown.map((u) => u.label);
    const result = await executeInvestigationStep(nextStep, targetCandidate, opts);
    emitInvestigationStep({ missionId, step: nextStep, collected: result.collected.length });

    const stepFailed = !result.skipped && (result.collected || []).length === 0;
    learning.recordOutcome(nextStep.providerId, nextStep.gap, {
      resolved: (result.resolvedGaps || []).includes(nextStep.gap),
      partial: (result.collected || []).length > 0 && !(result.resolvedGaps || []).includes(nextStep.gap),
    });

    board = updateBoardAfterStep(board, {
      gap: nextStep.gap,
      providerId: nextStep.providerId,
      resolvedGaps: result.resolvedGaps,
      collected: result.collected,
      failed: stepFailed,
      confidence: result.resolvedGaps?.includes(nextStep.gap) ? 0.85 : null,
    });

    const postBoardSummary = summarizeBoard(board);
    const nextTop = postBoardSummary.topPriorityUnknown;
    recordJournalStep(journal, {
      question: nextStep.question || `Need ${nextStep.gap}`,
      priorUnknowns,
      selectedProvider: nextStep.providerId,
      providerLabel: nextStep.providerLabel,
      gap: nextStep.gap,
      rationale: nextStep.rationale,
      expectedInformationGain: nextStep.expectedInformationGain,
      outcome: stepFailed ? 'failed' : (result.resolvedGaps || []).includes(nextStep.gap) ? 'resolved' : 'partial',
      evidenceCollected: result.collected,
      resolvedGaps: result.resolvedGaps,
      failed: stepFailed,
      nextQuestion: nextTop ? `Need ${nextTop.label}` : null,
      coveragePct: postBoardSummary.coveragePct,
      boardSnapshot: postBoardSummary,
    });

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
  const finalBoardSummary = summarizeBoard(board);
  const serializedJournal = serializeJournal(journal);
  const providerLearningSummary = learning.summarize();

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
    investigationBoard: finalBoardSummary,
    investigationJournal: serializedJournal,
  });

  const lastIteration = iterations[iterations.length - 1];
  const completionReason =
    completionMeta.reason ||
    (lastIteration && lastIteration.phase === 'complete'
      ? COMPLETION_REASONS.CONFIDENCE_THRESHOLD
      : overallConfidence >= (opts.confidenceThreshold || DEFAULT_CONFIDENCE_THRESHOLD)
        ? COMPLETION_REASONS.CONFIDENCE_THRESHOLD
        : COMPLETION_REASONS.NO_HIGHER_VALUE_EVIDENCE);
  const stopExplanation = completionMeta.explanation || null;

  emitInvestigationCompleted({
    missionId,
    outcome: 'completed',
    overallConfidence,
    claims: allClaims.length,
  });

  const investigationResult = buildInvestigationResult({
    outcome: candidates.length > 0 ? 'completed' : 'partial',
    completionReason,
    stopExplanation,
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
    investigationBoard: finalBoardSummary,
    investigationJournal: serializedJournal,
    providerLearning: providerLearningSummary,
    stepSelection: lastIteration?.stepSelection || null,
  });

  let memoryPersist = null;
  if (opts.persistMemory !== false) {
    memoryPersist = await persistInvestigationKnowledge(investigationResult, {
      tenantId: mission.tenantId || mission.clientId,
      missionId,
      mission,
      store: opts.memoryStore,
      completedAt: new Date().toISOString(),
      opts: {
        ...opts,
        providerLearning: exportLearningForMemory(learning),
      },
    });
  }

  return {
    ...investigationResult,
    startingPoint,
    memoryLoaded: memoryPrep.hasPriorKnowledge,
    memoryPersist,
    investigationBoard: finalBoardSummary,
    investigationJournal: serializedJournal,
  };
}

module.exports = {
  runInvestigationEngine,
  computeOverallConfidence,
  isInvestigationComplete,
};
