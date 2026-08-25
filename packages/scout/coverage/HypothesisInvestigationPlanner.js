'use strict';

/**
 * SPEC-177 — Hypothesis Investigation Planner.
 * Business hypotheses become the primary planning abstraction.
 * Generates InvestigationPlan with questions, evidence requirements, and provider assignments.
 */

const { generateHypotheses } = require('../investigation/HypothesisGeneration');
const {
  deriveQuestionsFromHypotheses,
  buildEvidenceRequirementsFromQuestions,
  computeOutstandingEvidence,
  computeSatisfiedEvidence,
  INVESTIGATIVE_EVIDENCE,
} = require('./EvidenceRequirements');
const {
  assignProvidersForRequirements,
  explainProviderForOperator,
} = require('./EvidenceProviderAssignment');

const INVESTIGATION_PHASES = Object.freeze({
  IDENTITY: 'identity',
  DECISION_MAKERS: 'decision_makers',
  GROWTH: 'growth',
  CLEANING: 'cleaning_signals',
  COMPLETE: 'complete',
});

/** Evidence phases execute in dependency order (identity before enrichment). */
const PHASE_EVIDENCE_ORDER = Object.freeze([
  INVESTIGATIVE_EVIDENCE.IDENTITY,
  INVESTIGATIVE_EVIDENCE.PORTFOLIO,
  INVESTIGATIVE_EVIDENCE.DECISION_MAKERS,
  INVESTIGATIVE_EVIDENCE.GROWTH,
  INVESTIGATIVE_EVIDENCE.CLEANING,
  INVESTIGATIVE_EVIDENCE.REVIEWS,
  INVESTIGATIVE_EVIDENCE.LICENSING,
  INVESTIGATIVE_EVIDENCE.SOCIAL,
  INVESTIGATIVE_EVIDENCE.CONTACT,
  INVESTIGATIVE_EVIDENCE.BUYING,
]);

function buildHypothesisInvestigationPlan(partial = {}) {
  return {
    version: 'SPEC-177',
    mission: partial.mission || null,
    objective: partial.objective || '',
    marketDefinition: partial.marketDefinition || null,
    hypotheses: Array.isArray(partial.hypotheses) ? partial.hypotheses : [],
    questions: Array.isArray(partial.questions) ? partial.questions : [],
    evidenceRequirements: Array.isArray(partial.evidenceRequirements)
      ? partial.evidenceRequirements
      : [],
    assignedProviders: Array.isArray(partial.assignedProviders) ? partial.assignedProviders : [],
    satisfiedEvidence: Array.isArray(partial.satisfiedEvidence) ? partial.satisfiedEvidence : [],
    outstandingEvidence: Array.isArray(partial.outstandingEvidence)
      ? partial.outstandingEvidence
      : [],
    tasks: Array.isArray(partial.tasks) ? partial.tasks : [],
    currentPhase: partial.currentPhase || INVESTIGATION_PHASES.IDENTITY,
    sufficientlyInvestigated: partial.sufficientlyInvestigated === true,
    rationale: partial.rationale || '',
    createdAt: partial.createdAt || new Date().toISOString(),
    updatedAt: partial.updatedAt || new Date().toISOString(),
  };
}

function buildInvestigationTask(partial = {}) {
  return {
    id: partial.id || `task-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    evidenceType: partial.evidenceType || '',
    label: partial.label || partial.task || '',
    providers: Array.isArray(partial.providers) ? partial.providers : [],
    status: partial.status || 'pending',
    mergeStrategy: partial.mergeStrategy || 'identity_resolution',
    rationale: partial.rationale || '',
    phase: partial.phase || INVESTIGATION_PHASES.IDENTITY,
  };
}

function deriveObjective(mission = {}, marketDefinition = {}) {
  const text = mission.objectiveText || mission.objective || '';
  if (text) return text;
  const segment = marketDefinition.market || marketDefinition.segment || 'target market';
  const geo = marketDefinition.geography || '';
  return `Investigate ${segment}${geo ? ` in ${geo}` : ''} through hypothesis-driven evidence collection.`;
}

/**
 * Build investigation tasks from provider assignments (evidence-first, not query strings).
 * @param {object[]} assignments
 * @returns {object[]}
 */
function buildTasksFromAssignments(assignments = []) {
  const byEvidence = new Map();

  for (const row of assignments) {
    const existing = byEvidence.get(row.evidenceType) || {
      evidenceType: row.evidenceType,
      providers: [],
      label: row.task,
      phase: phaseForEvidence(row.evidenceType),
    };
    if (!existing.providers.some((p) => p.providerId === row.providerId)) {
      existing.providers.push(row);
    }
    byEvidence.set(row.evidenceType, existing);
  }

  const tasks = [];
  for (const evidenceType of PHASE_EVIDENCE_ORDER) {
    const group = byEvidence.get(evidenceType);
    if (!group) continue;
    tasks.push(
      buildInvestigationTask({
        id: `task:${evidenceType}`,
        evidenceType,
        label: group.label || `Collect ${evidenceType}`,
        providers: group.providers,
        phase: group.phase,
        mergeStrategy:
          evidenceType === INVESTIGATIVE_EVIDENCE.IDENTITY ? 'identity_resolution' : 'evidence_fusion',
        rationale: `Investigation task for ${evidenceType}; providers assigned by evidence requirement, not search keywords.`,
      })
    );
  }

  return tasks;
}

function phaseForEvidence(evidenceType) {
  if (evidenceType === INVESTIGATIVE_EVIDENCE.IDENTITY) return INVESTIGATION_PHASES.IDENTITY;
  if (evidenceType === INVESTIGATIVE_EVIDENCE.DECISION_MAKERS) return INVESTIGATION_PHASES.DECISION_MAKERS;
  if (evidenceType === INVESTIGATIVE_EVIDENCE.GROWTH) return INVESTIGATION_PHASES.GROWTH;
  if (evidenceType === INVESTIGATIVE_EVIDENCE.CLEANING) return INVESTIGATION_PHASES.CLEANING;
  return INVESTIGATION_PHASES.IDENTITY;
}

/**
 * Create a hypothesis-driven investigation plan.
 * @param {object} input
 * @returns {object}
 */
function createHypothesisInvestigationPlan(input = {}) {
  const { mission = {}, marketDefinition = {}, opts = {} } = input;

  const hypotheses = input.hypotheses || generateHypotheses(marketDefinition, mission, opts);
  const questions = deriveQuestionsFromHypotheses(hypotheses, marketDefinition);
  const evidenceRequirements = buildEvidenceRequirementsFromQuestions(questions);
  const assignedProviders = assignProvidersForRequirements(evidenceRequirements, opts);
  const tasks = buildTasksFromAssignments(assignedProviders);

  return buildHypothesisInvestigationPlan({
    mission,
    objective: deriveObjective(mission, marketDefinition),
    marketDefinition,
    hypotheses,
    questions,
    evidenceRequirements,
    assignedProviders,
    satisfiedEvidence: [],
    outstandingEvidence: evidenceRequirements,
    tasks,
    currentPhase: INVESTIGATION_PHASES.IDENTITY,
    sufficientlyInvestigated: false,
    rationale:
      'Investigation plan constructed from business hypotheses (SPEC-177). Providers collect evidence; they do not define search strategy.',
  });
}

/**
 * Update plan after evidence collection.
 * @param {object} plan
 * @param {object[]} collectedEvidence
 * @returns {object}
 */
function updatePlanAfterEvidence(plan, collectedEvidence = []) {
  const satisfied = computeSatisfiedEvidence(plan.evidenceRequirements, collectedEvidence);
  const outstanding = computeOutstandingEvidence(plan.evidenceRequirements, collectedEvidence);

  const updatedQuestions = plan.questions.map((q) => {
    const reqTypes = q.requiredEvidence || [];
    const allSatisfied = reqTypes.every((ev) =>
      satisfied.some((s) => s.evidenceType === ev)
    );
    return { ...q, satisfied: allSatisfied };
  });

  const sufficientlyInvestigated =
    outstanding.length === 0 ||
    (satisfied.some((s) => s.evidenceType === INVESTIGATIVE_EVIDENCE.IDENTITY) &&
      updatedQuestions.filter((q) => !q.satisfied).length === 0);

  let currentPhase = plan.currentPhase;
  if (satisfied.some((s) => s.evidenceType === INVESTIGATIVE_EVIDENCE.IDENTITY)) {
    currentPhase = INVESTIGATION_PHASES.DECISION_MAKERS;
  }
  if (sufficientlyInvestigated) {
    currentPhase = INVESTIGATION_PHASES.COMPLETE;
  }

  return buildHypothesisInvestigationPlan({
    ...plan,
    questions: updatedQuestions,
    satisfiedEvidence: satisfied,
    outstandingEvidence: outstanding,
    currentPhase,
    sufficientlyInvestigated,
    updatedAt: new Date().toISOString(),
  });
}

/**
 * Get next executable tasks given current evidence state.
 * Respects phase ordering: identity before decision makers (Scenario 2).
 * @param {object} plan
 * @param {object} [opts]
 * @returns {object[]}
 */
function getNextInvestigationTasks(plan, opts = {}) {
  if (plan.sufficientlyInvestigated) return [];

  const identitySatisfied = (plan.satisfiedEvidence || []).some(
    (s) => s.evidenceType === INVESTIGATIVE_EVIDENCE.IDENTITY
  );

  return (plan.tasks || []).filter((task) => {
    if (task.status === 'completed' || task.status === 'skipped') return false;

    // Scenario 2: decision-maker tasks only after identity established.
    if (
      !identitySatisfied &&
      task.evidenceType !== INVESTIGATIVE_EVIDENCE.IDENTITY &&
      opts.requireIdentityFirst !== false
    ) {
      return false;
    }

    // Skip tasks whose evidence is already satisfied (Scenario 6).
    const alreadySatisfied = (plan.satisfiedEvidence || []).some(
      (s) => s.evidenceType === task.evidenceType
    );
    if (alreadySatisfied) return false;

    return true;
  });
}

/**
 * Mark tasks complete and skip remaining when investigation is sufficient (Scenario 6).
 * @param {object} plan
 * @returns {object}
 */
function markInvestigationComplete(plan) {
  const tasks = (plan.tasks || []).map((task) => ({
    ...task,
    status: task.status === 'pending' ? 'skipped' : task.status,
    skipReason: task.status === 'pending' ? 'hypothesis_sufficiently_investigated' : null,
  }));

  return buildHypothesisInvestigationPlan({
    ...plan,
    tasks,
    currentPhase: INVESTIGATION_PHASES.COMPLETE,
    sufficientlyInvestigated: true,
    updatedAt: new Date().toISOString(),
  });
}

/**
 * Build operator explainability entries for all provider assignments.
 * @param {object} plan
 * @returns {object[]}
 */
function buildOperatorExplanations(plan) {
  return (plan.assignedProviders || []).map((assignment) => ({
    providerId: assignment.providerId,
    evidenceType: assignment.evidenceType,
    explanation: explainProviderForOperator(assignment, plan),
  }));
}

/**
 * Revise plan when providers become unavailable (Scenario 4).
 * @param {object} plan
 * @param {string[]} unavailableProviders
 * @returns {object}
 */
function revisePlanForUnavailableProviders(plan, unavailableProviders = []) {
  const unavailable = new Set(unavailableProviders.map((p) => String(p).toLowerCase()));

  const revisedAssignments = assignProvidersForRequirements(plan.evidenceRequirements, {
    unavailableProviders: [...unavailable],
    includeUnavailable: false,
  });

  const revisedTasks = buildTasksFromAssignments(revisedAssignments);

  return buildHypothesisInvestigationPlan({
    ...plan,
    assignedProviders: revisedAssignments,
    tasks: revisedTasks,
    rationale: `${plan.rationale} Revised after provider unavailability: ${[...unavailable].join(', ')}.`,
    updatedAt: new Date().toISOString(),
  });
}

module.exports = {
  INVESTIGATION_PHASES,
  PHASE_EVIDENCE_ORDER,
  buildHypothesisInvestigationPlan,
  buildInvestigationTask,
  createHypothesisInvestigationPlan,
  updatePlanAfterEvidence,
  getNextInvestigationTasks,
  markInvestigationComplete,
  buildOperatorExplanations,
  revisePlanForUnavailableProviders,
  buildTasksFromAssignments,
};
