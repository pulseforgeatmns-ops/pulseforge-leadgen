'use strict';

/**
 * SPEC-195 — Candidate-Scoped Investigative Continuation.
 *
 * Connects post-qualification uncertainty to executable investigation tasks.
 * Investigation unit: Candidate + Hypothesis + Evidence Gap.
 */

const { asText, READINESS_STATES, QUALIFICATION_STATUSES, PROSPECT_BUCKETS } = require('../../max/scoutAcquisition/Types');
const { INVESTIGATIVE_EVIDENCE } = require('../coverage/EvidenceRequirements');
const {
  assignProvidersForRequirements,
  explainProviderForOperator,
} = require('../coverage/EvidenceProviderAssignment');
const { createDefaultUnifiedRegistry } = require('../coverage/ProviderCapabilityRegistry');
const {
  INVESTIGATION_PHASES,
} = require('../coverage/HypothesisInvestigationPlanner');
const { executeInvestigationTask } = require('../coverage/HypothesisDrivenDiscoveryEngine');
const { attachFitToClassified } = require('../../max/scoutAcquisition/FitEvaluation');
const { buildProspectEvaluation } = require('../../max/scoutAcquisition/ProspectEvaluation');
const {
  mapMissingEvidenceToGap,
  mapHypothesisToGap,
  candidateNeedsInvestigation,
} = require('./EntityInvestigationContinuation');
const {
  readTraceMetricsFromExecutionState,
  buildInvestigationExecutionTrace,
} = require('./InvestigationExecutionTrace');

const TASK_STATUS = Object.freeze({
  PENDING: 'pending',
  RUNNING: 'running',
  COMPLETED: 'completed',
  BLOCKED: 'blocked',
  EXHAUSTED: 'exhausted',
});

const HYPOTHESIS_STATUS = Object.freeze({
  UNRESOLVED: 'unresolved',
  SUPPORTED: 'supported',
  REJECTED: 'rejected',
  UNKNOWN: 'unknown',
});

const STOP_REASONS = Object.freeze({
  PRIORITIZATION_READY: 'prioritization_ready',
  INFORMATION_EXHAUSTED: 'information_exhausted',
  ECONOMIC_STOP: 'economic_stop',
  OPERATOR_DECISION_REQUIRED: 'operator_decision_required',
  QUEUE_EMPTY: 'queue_empty',
});

/** Canonical hypothesis ids per candidate (SPEC-195 §3). */
const CANDIDATE_HYPOTHESIS_IDS = Object.freeze({
  MANAGES_STRS: 'manages_strs',
  PORTFOLIO_SIZE: 'portfolio_size',
  OUTSOURCES_CLEANING: 'outsources_cleaning',
  DECISION_MAKER: 'decision_maker',
  BUYING_READINESS: 'buying_readiness',
  BUSINESS_FIT: 'business_fit',
  CLEANING_RESPONSIBILITY: 'cleaning_responsibility',
  CONTACT_PATH: 'contact_path',
});

const GAP_TO_HYPOTHESIS_ID = Object.freeze({
  portfolio_size: CANDIDATE_HYPOTHESIS_IDS.PORTFOLIO_SIZE,
  decision_maker: CANDIDATE_HYPOTHESIS_IDS.DECISION_MAKER,
  cleaning_responsibility: CANDIDATE_HYPOTHESIS_IDS.OUTSOURCES_CLEANING,
  buying_signals: CANDIDATE_HYPOTHESIS_IDS.BUYING_READINESS,
  business_fit: CANDIDATE_HYPOTHESIS_IDS.BUSINESS_FIT,
  contact_path: CANDIDATE_HYPOTHESIS_IDS.CONTACT_PATH,
});

function hypothesisIdForGap(gap) {
  return GAP_TO_HYPOTHESIS_ID[gap] || gap || 'unknown';
}

function clamp01(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

function isExecutableProvider(assignment = {}, registry = createDefaultUnifiedRegistry()) {
  if (!assignment || assignment.status === 'unavailable') return false;
  const meta = registry.get(assignment.providerId);
  if (!meta) return false;
  const availability = registry.resolveAvailability(meta);
  if (availability !== 'available') return false;
  return registry.isAvailable(assignment.providerId);
}

function filterExecutableProviders(providers = [], opts = {}) {
  const registry = opts.registry || createDefaultUnifiedRegistry();
  const executable = [];
  const unavailable = [];

  for (const row of providers || []) {
    if (isExecutableProvider(row, registry)) {
      executable.push({ ...row, desiredProvider: row.providerId, availableProvider: row.providerId });
    } else {
      unavailable.push({
        ...row,
        desiredProvider: row.providerId,
        availableProvider: null,
        status: 'unavailable',
      });
    }
  }

  return { executable, unavailable };
}

function buildCandidateHypothesisState(candidate = {}, evaluation = {}) {
  const investigation = evaluation.investigation || {};
  const gaps = deriveCanonicalGaps(evaluation, candidate);
  const state = {};

  for (const row of gaps) {
    const hypothesisId = row.hypothesisId || hypothesisIdForGap(row.gap);
    state[hypothesisId] = {
      hypothesisId,
      gap: row.gap,
      evidenceType: row.evidenceType,
      status: HYPOTHESIS_STATUS.UNRESOLVED,
      confidence: 0,
      evidenceRefs: [],
    };
  }

  if (!state[CANDIDATE_HYPOTHESIS_IDS.BUYING_READINESS]) {
    state[CANDIDATE_HYPOTHESIS_IDS.BUYING_READINESS] = {
      hypothesisId: CANDIDATE_HYPOTHESIS_IDS.BUYING_READINESS,
      gap: 'buying_signals',
      evidenceType: INVESTIGATIVE_EVIDENCE.BUYING,
      status:
        evaluation.readiness && evaluation.readiness.status === READINESS_STATES.UNKNOWN
          ? HYPOTHESIS_STATUS.UNKNOWN
          : HYPOTHESIS_STATUS.UNRESOLVED,
      confidence: evaluation.readiness ? evaluation.readiness.confidence || 0 : 0,
      evidenceRefs: [],
    };
  }

  for (const text of investigation.unresolvedHypotheses || []) {
    const mapped = mapHypothesisToGap(text);
    if (!mapped) continue;
    const hypothesisId = hypothesisIdForGap(mapped.gap);
    if (!state[hypothesisId]) {
      state[hypothesisId] = {
        hypothesisId,
        gap: mapped.gap,
        evidenceType: mapped.evidenceType,
        status: HYPOTHESIS_STATUS.UNRESOLVED,
        confidence: 0,
        evidenceRefs: [],
        label: text,
      };
    }
  }

  return state;
}

/**
 * Structured canonical gaps from evaluation investigation metadata.
 * Presentation strings are derived from these — not the other way around.
 */
function deriveCanonicalGaps(evaluation = {}, candidate = {}) {
  const gaps = new Map();
  const investigation = evaluation.investigation || {};

  for (const text of investigation.missingEvidence || []) {
    const mapped = mapMissingEvidenceToGap(text);
    if (!mapped) continue;
    const hypothesisId = hypothesisIdForGap(mapped.gap);
    gaps.set(`${mapped.gap}:${mapped.evidenceType}`, {
      gap: mapped.gap,
      evidenceType: mapped.evidenceType,
      hypothesisId,
      label: text,
      source: 'missingEvidence',
    });
  }

  for (const text of investigation.unresolvedHypotheses || []) {
    const mapped = mapHypothesisToGap(text);
    if (!mapped) continue;
    const hypothesisId = hypothesisIdForGap(mapped.gap);
    gaps.set(`${mapped.gap}:${mapped.evidenceType}`, {
      gap: mapped.gap,
      evidenceType: mapped.evidenceType,
      hypothesisId,
      openQuestion: text,
      source: 'unresolvedHypothesis',
    });
  }

  for (const unknown of candidate.unknowns || []) {
    const text = typeof unknown === 'object' ? unknown.text || unknown.unknown : String(unknown || '');
    const mapped = mapHypothesisToGap(text);
    if (!mapped) continue;
    const hypothesisId = hypothesisIdForGap(mapped.gap);
    gaps.set(`${mapped.gap}:${mapped.evidenceType}`, {
      gap: mapped.gap,
      evidenceType: mapped.evidenceType,
      hypothesisId,
      openQuestion: text,
      source: 'unknown',
    });
  }

  const rec = candidate.recommendedNextInvestigation;
  if (rec && rec.action && !/no further investigation required/i.test(rec.action)) {
    const mapped = mapHypothesisToGap(rec.action);
    if (mapped) {
      const hypothesisId = hypothesisIdForGap(mapped.gap);
      gaps.set(`${mapped.gap}:${mapped.evidenceType}`, {
        gap: mapped.gap,
        evidenceType: mapped.evidenceType,
        hypothesisId,
        openQuestion: rec.action,
        source: 'recommendedNextInvestigation',
        impact: rec.impact || 'high',
      });
    }
  }

  return [...gaps.values()];
}

function estimateInformationGain(gap = {}, candidate = {}, evaluation = {}) {
  let gain = 0.35;
  if (gap.impact === 'high' || gap.source === 'recommendedNextInvestigation') gain += 0.25;
  if (evaluation.qualification && evaluation.qualification.status === QUALIFICATION_STATUSES.QUALIFIED) {
    gain += 0.15;
  }
  const fit = Number(candidate.fit || evaluation.qualification?.confidence || 0);
  gain += clamp01(fit) * 0.2;
  if (evaluation.readiness && evaluation.readiness.status === READINESS_STATES.UNKNOWN) gain += 0.1;
  if (candidate.website) gain += 0.05;
  return Number(Math.min(0.99, gain).toFixed(2));
}

function buildCandidateInvestigationTask(candidate = {}, gap = {}, opts = {}) {
  const entityId = asText(candidate.id || candidate.companyId) || `entity-${Date.now()}`;
  const entityName = asText(candidate.name) || entityId;
  const hypothesisId = gap.hypothesisId || hypothesisIdForGap(gap.gap);
  const evaluation = candidate.evaluation || {};

  const requirements = [
    {
      evidenceType: gap.evidenceType,
      questionIds: [`${entityId}:${gap.gap}`],
      entityId,
      candidateId: entityId,
      hypothesisId,
      required: true,
      satisfied: false,
      confidence: 0,
      sources: [],
    },
  ];

  const assignments = assignProvidersForRequirements(requirements, opts);
  const { executable, unavailable } = filterExecutableProviders(assignments, opts);

  let status = TASK_STATUS.PENDING;
  if (!executable.length && unavailable.length) status = TASK_STATUS.BLOCKED;
  if (!executable.length && !unavailable.length) status = TASK_STATUS.EXHAUSTED;

  const expectedInformationGain = estimateInformationGain(gap, candidate, evaluation);
  const rank = Number(candidate.rank || candidate.prospectRank || 999);

  return {
    id: `task:${entityId}:${gap.evidenceType}`,
    missionId: asText(opts.missionId) || null,
    candidateId: entityId,
    entityId,
    entityName,
    hypothesisId,
    gap: gap.gap,
    evidenceType: gap.evidenceType,
    label: `${entityName}: ${gap.openQuestion || gap.label || gap.gap}`,
    hypothesis: gap.openQuestion || gap.label || null,
    providers: executable,
    desiredProviders: assignments.map((a) => a.providerId),
    unavailableProviders: unavailable,
    scope: 'entity',
    phase: phaseForEvidenceType(gap.evidenceType),
    mergeStrategy: 'evidence_fusion',
    rationale: `Candidate investigation (SPEC-195): ${gap.source || 'gap'} → ${gap.gap}`,
    status,
    attempts: 0,
    evidenceProduced: [],
    expectedInformationGain,
    priority: Number((expectedInformationGain - rank * 0.001).toFixed(4)),
    createdAt: new Date().toISOString(),
    completedAt: null,
    candidateContext: {
      businessName: entityName,
      website: asText(candidate.website || candidate.url) || null,
      placeId: asText(candidate.placeId || candidate.place_id) || null,
      address: asText(candidate.address || candidate.location) || null,
    },
  };
}

function phaseForEvidenceType(evidenceType) {
  if (evidenceType === INVESTIGATIVE_EVIDENCE.IDENTITY) return INVESTIGATION_PHASES.IDENTITY;
  if (evidenceType === INVESTIGATIVE_EVIDENCE.DECISION_MAKERS) return INVESTIGATION_PHASES.DECISION_MAKERS;
  if (evidenceType === INVESTIGATIVE_EVIDENCE.GROWTH || evidenceType === INVESTIGATIVE_EVIDENCE.BUYING) {
    return INVESTIGATION_PHASES.GROWTH;
  }
  if (evidenceType === INVESTIGATIVE_EVIDENCE.CLEANING) return INVESTIGATION_PHASES.CLEANING;
  return INVESTIGATION_PHASES.IDENTITY;
}

function buildCandidateInvestigationTasks(candidate = {}, opts = {}) {
  const gaps = deriveCanonicalGaps(candidate.evaluation || {}, candidate);
  if (!gaps.length && candidateNeedsInvestigation(candidate)) {
    gaps.push({
      gap: 'decision_maker',
      evidenceType: INVESTIGATIVE_EVIDENCE.DECISION_MAKERS,
      hypothesisId: CANDIDATE_HYPOTHESIS_IDS.DECISION_MAKER,
      openQuestion: `Resolve remaining uncertainty for ${candidate.name || candidate.id}`,
      source: 'default',
    });
  }
  return gaps.map((gap) => buildCandidateInvestigationTask(candidate, gap, opts));
}

function buildCandidateInvestigationQueue(candidates = [], opts = {}) {
  const tasks = candidates.flatMap((candidate) => buildCandidateInvestigationTasks(candidate, opts));
  return tasks.sort((a, b) => b.priority - a.priority || b.expectedInformationGain - a.expectedInformationGain);
}

function selectNextInvestigation(queue = [], state = {}) {
  const completed = new Set(state.completedTaskIds || []);
  const running = state.runningTaskId || null;

  for (const task of queue) {
    if (completed.has(task.id)) continue;
    if (running && task.id === running) continue;
    if (task.status === TASK_STATUS.COMPLETED || task.status === TASK_STATUS.EXHAUSTED) continue;
    if (task.status === TASK_STATUS.BLOCKED && !(task.providers || []).length) continue;
    if ((task.providers || []).length === 0 && task.status !== TASK_STATUS.PENDING) continue;
    return task;
  }
  return null;
}

function shouldStopInvestigation(state = {}, queue = [], opts = {}) {
  const pending = queue.filter(
    (task) =>
      task.status === TASK_STATUS.PENDING ||
      task.status === TASK_STATUS.RUNNING ||
      (task.status === TASK_STATUS.BLOCKED && (task.providers || []).length > 0)
  );
  const executablePending = pending.filter((task) => (task.providers || []).length > 0);

  if (!executablePending.length) {
    const blocked = queue.filter((task) => task.status === TASK_STATUS.BLOCKED);
    if (blocked.length && !pending.length) {
      return {
        stop: true,
        reason: STOP_REASONS.INFORMATION_EXHAUSTED,
        explanation: 'Remaining providers cannot resolve material uncertainty.',
        blockedTasks: blocked.map((t) => t.id),
      };
    }
    return {
      stop: true,
      reason: STOP_REASONS.QUEUE_EMPTY,
      explanation: 'No pending candidate investigation tasks remain.',
    };
  }

  const minGain = opts.minExpectedGain != null ? opts.minExpectedGain : 0.08;
  const topGain = Math.max(...executablePending.map((t) => t.expectedInformationGain || 0));
  if (topGain < minGain) {
    return {
      stop: true,
      reason: STOP_REASONS.ECONOMIC_STOP,
      explanation: `Expected information gain (${topGain}) below economic threshold (${minGain}).`,
    };
  }

  if (state.prioritizationReady === true) {
    return {
      stop: true,
      reason: STOP_REASONS.PRIORITIZATION_READY,
      explanation: 'Enough comparative evidence exists for prioritization.',
    };
  }

  if (opts.maxTasks != null && (state.executedCount || 0) >= opts.maxTasks) {
    return {
      stop: true,
      reason: STOP_REASONS.PRIORITIZATION_READY,
      explanation: `Investigation budget exhausted (${opts.maxTasks} tasks).`,
    };
  }

  return { stop: false, reason: null, explanation: null };
}

function applyEvidenceToCandidateHypotheses(hypothesisState = {}, task = {}, evidence = {}) {
  const next = { ...hypothesisState };
  const hypothesisId = task.hypothesisId || hypothesisIdForGap(task.gap);
  const current = next[hypothesisId] || {
    hypothesisId,
    gap: task.gap,
    evidenceType: task.evidenceType,
    status: HYPOTHESIS_STATUS.UNRESOLVED,
    confidence: 0,
    evidenceRefs: [],
  };

  const produced = evidence.evidenceProduced || evidence.types || [];
  const hasEvidence = produced.length > 0 || evidence.confidence > 0 || evidence.candidateUpdated === true;

  next[hypothesisId] = {
    ...current,
    status: hasEvidence ? HYPOTHESIS_STATUS.SUPPORTED : current.status,
    confidence: Math.max(current.confidence, Number(evidence.confidence || (hasEvidence ? 0.65 : 0))),
    evidenceRefs: [
      ...(current.evidenceRefs || []),
      {
        taskId: task.id,
        providerId: evidence.providerId || null,
        evidenceType: task.evidenceType,
        produced,
        collectedAt: new Date().toISOString(),
      },
    ],
  };

  return next;
}

function mergeInvestigationEvidenceIntoCompany(company = {}, classified = {}, taskResult = {}) {
  const nextCompany = { ...company };
  const nextClassified = { ...classified };
  const candidates = taskResult.candidates || [];

  for (const row of candidates) {
    if (row.people && row.people.length) {
      nextCompany.people = [...(nextCompany.people || []), ...row.people];
    }
    if (row.signals && row.signals.length) {
      nextCompany.signals = [...(nextCompany.signals || []), ...row.signals];
      nextClassified.signals = [...(nextClassified.signals || []), ...row.signals];
    }
    if (row.website && !nextCompany.website) nextCompany.website = row.website;
  }

  if (taskResult.evidenceProduced && taskResult.evidenceProduced.length) {
    nextClassified.evidenceRefs = [
      ...(nextClassified.evidenceRefs || []),
      ...taskResult.evidenceProduced.map((label, index) => ({
        id: `ev-inv-${taskResult.taskId || 'task'}-${index}`,
        kind: 'investigation',
        sourceKind: 'observed_fact',
        label,
        snapshot: { taskId: taskResult.taskId, source: 'candidate_investigation' },
      })),
    ];
  }

  return { company: nextCompany, classified: nextClassified };
}

function buildInvestigationCandidateRows(companies = [], classified = [], opts = {}) {
  const rows = [];

  function pushCandidate(company, classRow, rank) {
    if (!company || !classRow) return;
    const evaluation = classRow.evaluation || buildProspectEvaluation({
      candidate: company,
      classified: classRow,
      fit: company.fitEvaluation || {},
      qualification: {},
      searchDefinition: opts.searchDefinition || {},
    });

    const candidate = {
      id: company.id,
      companyId: company.id,
      name: company.name || classRow.name,
      rank,
      fit: classRow.fit,
      website: company.website,
      address: company.address || company.location,
      placeId: company.placeId || company.place_id,
      unknowns: classRow.unknowns || [],
      evaluation,
      qualificationStatus: evaluation.qualification?.status,
      readinessState: evaluation.readiness?.status || evaluation.readinessState,
      prospectBucket: evaluation.bucket,
      recommendedNextInvestigation: classRow.recommendedNextInvestigation || null,
      hypothesisState: buildCandidateHypothesisState({ id: company.id, name: company.name }, evaluation),
    };

    if (candidateNeedsInvestigation(candidate)) {
      rows.push(candidate);
    }
  }

  companies.forEach((company, index) => {
    pushCandidate(company, classified[index], index + 1);
  });

  return rows;
}

function explainCandidateInvestigation(candidate = {}, queue = []) {
  const evaluation = candidate.evaluation || {};
  const hypothesisState = candidate.hypothesisState || buildCandidateHypothesisState(candidate, evaluation);
  const unresolved = Object.values(hypothesisState).filter(
    (row) => row.status === HYPOTHESIS_STATUS.UNRESOLVED || row.status === HYPOTHESIS_STATUS.UNKNOWN
  );
  const nextTask = queue.find((task) => task.candidateId === (candidate.id || candidate.companyId));

  return {
    candidateId: candidate.id || candidate.companyId,
    name: candidate.name,
    whyUnderInvestigation: {
      businessFit: evaluation.qualification?.status === QUALIFICATION_STATUSES.QUALIFIED ? 'strong' : 'uncertain',
      readiness: evaluation.readiness?.status || READINESS_STATES.UNKNOWN,
      unresolvedHypotheses: unresolved.map((row) => row.gap || row.hypothesisId),
    },
    nextInvestigation: nextTask
      ? {
          target: nextTask.candidateContext?.website || nextTask.entityName,
          gap: nextTask.gap,
          reason: `Expected information gain ${nextTask.expectedInformationGain} for ${nextTask.gap}.`,
          taskId: nextTask.id,
        }
      : null,
  };
}

/**
 * Production candidate investigation loop (SPEC-195 §6).
 */
async function runCandidateInvestigationLoop(input = {}) {
  const {
    companies = [],
    classified = [],
    searchDefinition = {},
    marketDefinition = {},
    mission = {},
    adapters = [],
    opts = {},
  } = input;

  const candidates = input.candidates || buildInvestigationCandidateRows(companies, classified, {
    searchDefinition,
  });

  if (!candidates.length) {
    return {
      candidates: [],
      queue: [],
      executedTasks: [],
      executionTraces: [],
      companies,
      classified,
      stop: { stop: true, reason: STOP_REASONS.QUEUE_EMPTY, explanation: 'No investigable candidates.' },
      explainability: [],
    };
  }

  let queue = buildCandidateInvestigationQueue(candidates, {
    ...opts,
    missionId: mission.id || opts.missionId,
  });

  const state = {
    completedTaskIds: [],
    executedCount: 0,
    prioritizationReady: false,
    collectedEvidence: [],
  };

  const executedTasks = [];
  const executionTraces = [];
  const explainability = candidates.map((candidate) =>
    explainCandidateInvestigation(candidate, queue)
  );

  let workingCompanies = companies.slice();
  let workingClassified = classified.slice();

  const maxIterations = opts.maxCandidateInvestigationIterations != null ? opts.maxCandidateInvestigationIterations : 15;

  for (let iteration = 0; iteration < maxIterations; iteration += 1) {
    const stop = shouldStopInvestigation(state, queue, opts);
    if (stop.stop) {
      return {
        candidates,
        queue,
        executedTasks,
        executionTraces,
        companies: workingCompanies,
        classified: workingClassified,
        stop,
        explainability,
        hypothesisStates: candidates.reduce((acc, row) => {
          acc[row.id] = row.hypothesisState;
          return acc;
        }, {}),
      };
    }

    const task = selectNextInvestigation(queue, state);
    if (!task) break;

    task.status = TASK_STATUS.RUNNING;
    task.attempts = (task.attempts || 0) + 1;

    const candidateIndex = candidates.findIndex((row) => String(row.id) === String(task.candidateId));
    const candidate = candidateIndex >= 0 ? candidates[candidateIndex] : null;
    const hypothesisId = task.hypothesisId;
    const traceStartedAt = new Date().toISOString();
    const metricsBefore = readTraceMetricsFromExecutionState(
      candidate && candidate.evaluation,
      candidate && candidate.hypothesisState && candidate.hypothesisState[hypothesisId],
      candidate && candidate.rank
    );

    const executeFn = opts.executeInvestigationTask || executeInvestigationTask;
    let result;
    try {
      result = await executeFn(task, searchDefinition, adapters, marketDefinition, opts);
    } catch (err) {
      result = {
        taskId: task.id,
        evidenceType: task.evidenceType,
        status: 'failed',
        errors: [{ message: err.message || String(err) }],
        candidates: [],
      };
    }

    const produced = (result.mergedReport && result.mergedReport.evidenceProduced) || [];
    task.evidenceProduced = produced;
    task.status =
      result.status === 'completed' || result.status === 'partial'
        ? TASK_STATUS.COMPLETED
        : (task.providers || []).length
          ? TASK_STATUS.EXHAUSTED
          : TASK_STATUS.BLOCKED;
    task.completedAt = new Date().toISOString();

    executedTasks.push({ ...result, task });
    state.completedTaskIds.push(task.id);
    state.executedCount += 1;
    state.collectedEvidence.push({
      evidenceType: task.evidenceType,
      candidateId: task.candidateId,
      entityId: task.entityId,
      hypothesisId: task.hypothesisId,
      evidenceProduced: produced,
      providerId: (result.reports && result.reports[0] && result.reports[0].providerId) || null,
    });

    if (candidateIndex >= 0) {
      candidates[candidateIndex].hypothesisState = applyEvidenceToCandidateHypotheses(
        candidates[candidateIndex].hypothesisState || {},
        task,
        {
          evidenceProduced: produced,
          confidence: result.status === 'completed' ? 0.7 : 0.2,
          candidateUpdated: (result.candidates || []).length > 0,
        }
      );
    }

    const companyIndex = workingCompanies.findIndex((row) => String(row.id) === String(task.candidateId));
    if (companyIndex >= 0) {
      const merged = mergeInvestigationEvidenceIntoCompany(
        workingCompanies[companyIndex],
        workingClassified[companyIndex],
        { ...result, taskId: task.id, evidenceProduced: produced }
      );
      workingCompanies[companyIndex] = merged.company;
      workingClassified[companyIndex] = merged.classified;

      const attached = attachFitToClassified(
        merged.classified,
        merged.company,
        searchDefinition,
        opts.now
      );
      workingClassified[companyIndex] = attached.classified;
      workingCompanies[companyIndex].prospectEvaluation = attached.evaluation;
      workingClassified[companyIndex].evaluation = attached.evaluation;

      if (candidateIndex >= 0) {
        candidates[candidateIndex].evaluation = attached.evaluation;
      }
    }

    const metricsAfter = readTraceMetricsFromExecutionState(
      candidate && candidate.evaluation,
      candidate && candidate.hypothesisState && candidate.hypothesisState[hypothesisId],
      candidate && candidate.rank
    );
    executionTraces.push(
      buildInvestigationExecutionTrace({
        task,
        result,
        before: metricsBefore,
        after: metricsAfter,
        startedAt: traceStartedAt,
        completedAt: task.completedAt,
        evidenceProduced: produced,
      })
    );

    queue = buildCandidateInvestigationQueue(candidates, {
      ...opts,
      missionId: mission.id || opts.missionId,
      completedTaskIds: state.completedTaskIds,
    });
  }

  const finalStop = shouldStopInvestigation(state, queue, opts);
  return {
    candidates,
    queue,
    executedTasks,
    executionTraces,
    companies: workingCompanies,
    classified: workingClassified,
    stop: finalStop,
    explainability,
    hypothesisStates: candidates.reduce((acc, row) => {
      acc[row.id] = row.hypothesisState;
      return acc;
    }, {}),
  };
}

function assertNoProseOnlyInvestigation(candidate = {}, tasks = []) {
  const needs = candidateNeedsInvestigation(candidate);
  const rec = candidate.recommendedNextInvestigation;
  const hasRecommendation =
    rec && rec.action && !/no further investigation required/i.test(String(rec.action));

  if (!needs && !hasRecommendation) return { ok: true };

  const executable = tasks.filter((task) => (task.providers || []).length > 0);
  const blocked = tasks.filter((task) => task.status === TASK_STATUS.BLOCKED);

  if (executable.length > 0) return { ok: true };
  if (blocked.length > 0) {
    return {
      ok: true,
      reason: 'all_capable_providers_unavailable',
      blockedTasks: blocked.map((t) => t.id),
    };
  }

  return {
    ok: false,
    reason: 'prose_only_investigation',
    message: 'Qualified prospect has investigation recommendation but no executable task (AUDIT-070 regression).',
  };
}

module.exports = {
  TASK_STATUS,
  HYPOTHESIS_STATUS,
  STOP_REASONS,
  CANDIDATE_HYPOTHESIS_IDS,
  buildCandidateHypothesisState,
  deriveCanonicalGaps,
  buildCandidateInvestigationTask,
  buildCandidateInvestigationTasks,
  buildCandidateInvestigationQueue,
  selectNextInvestigation,
  shouldStopInvestigation,
  applyEvidenceToCandidateHypotheses,
  buildInvestigationCandidateRows,
  runCandidateInvestigationLoop,
  explainCandidateInvestigation,
  assertNoProseOnlyInvestigation,
  filterExecutableProviders,
  isExecutableProvider,
  hypothesisIdForGap,
};
