'use strict';

/**
 * SPEC-145 — Adaptive Investigation Planning.
 * Before collecting evidence, Scout constructs an explicit Investigation Plan.
 * ADR-064: Investigation Before Execution — providers execute the plan; they do not define it.
 */

const { generateHypotheses } = require('./HypothesisGeneration');
const { buildEvidencePlan } = require('../intelligence/EvidencePlanning');
const { createDefaultProviderRegistry } = require('../intelligence/ProviderCapabilityRegistry');
const { planInvestigationChain, costScoreForTier } = require('./InvestigationPlanner');
const {
  createProviderLearningStore,
  loadLearningFromMemory,
} = require('./ProviderLearning');
const { DEFAULT_COVERAGE_THRESHOLD } = require('./InvestigationBoard');
const { DEFAULT_CONFIDENCE_THRESHOLD, DEFAULT_MAX_COST_BUDGET } = require('./types');

const COST_TIER_ESTIMATE = Object.freeze({
  free: 0,
  cached: 0,
  local: 2,
  paid: 8,
});

const REQUIREMENT_TO_GAP = Object.freeze({
  candidate_universe: 'geographic_fit',
  decision_makers: 'decision_maker',
  property_count: 'portfolio_size',
  contact_path: 'contact_path',
  buying_signals: 'buying_signals',
  cleaning_responsibility: 'cleaning_responsibility',
  existing_vendors: 'vendor_relationship',
  geographic_coverage: 'geographic_fit',
  business_maturity: 'business_fit',
});

const PROVIDER_STATUS = Object.freeze({
  PENDING: 'pending',
  COMPLETED: 'completed',
  SKIPPED: 'skipped',
  FAILED: 'failed',
  UNAVAILABLE: 'unavailable',
});

function buildProviderPlan(partial = {}) {
  return {
    provider: partial.provider || partial.providerId,
    providerId: partial.providerId || partial.provider,
    providerLabel: partial.providerLabel || partial.provider || partial.providerId,
    capabilities: Array.isArray(partial.capabilities) ? partial.capabilities : [],
    evidenceExpected: Array.isArray(partial.evidenceExpected) ? partial.evidenceExpected : [],
    estimatedCost: partial.estimatedCost != null ? partial.estimatedCost : 0,
    confidenceGain: partial.confidenceGain != null ? partial.confidenceGain : 0,
    gap: partial.gap || null,
    order: partial.order != null ? partial.order : 0,
    status: partial.status || PROVIDER_STATUS.PENDING,
    rationale: partial.rationale || '',
    skipReason: partial.skipReason || null,
    unavailableReason: partial.unavailableReason || null,
  };
}

function buildInvestigationPlan(partial = {}) {
  return {
    version: 'SPEC-145',
    mission: partial.mission || null,
    objective: partial.objective || '',
    hypotheses: Array.isArray(partial.hypotheses) ? partial.hypotheses : [],
    evidenceRequired: Array.isArray(partial.evidenceRequired) ? partial.evidenceRequired : [],
    providerSequence: Array.isArray(partial.providerSequence) ? partial.providerSequence : [],
    stoppingConditions: partial.stoppingConditions || {},
    estimatedCoverage: partial.estimatedCoverage || {},
    estimatedConfidence: partial.estimatedConfidence != null ? partial.estimatedConfidence : 0,
    estimatedCost: partial.estimatedCost != null ? partial.estimatedCost : 0,
    createdAt: partial.createdAt || new Date().toISOString(),
    revisions: Array.isArray(partial.revisions) ? partial.revisions : [],
    rationale: partial.rationale || '',
  };
}

function buildInvestigationStatus(partial = {}) {
  return {
    completedSteps: Array.isArray(partial.completedSteps) ? partial.completedSteps : [],
    remainingSteps: Array.isArray(partial.remainingSteps) ? partial.remainingSteps : [],
    confidence: partial.confidence != null ? partial.confidence : 0,
    coverage: partial.coverage != null ? partial.coverage : 0,
    cost: partial.cost != null ? partial.cost : 0,
    blockers: Array.isArray(partial.blockers) ? partial.blockers : [],
    remainingUnknowns: Array.isArray(partial.remainingUnknowns) ? partial.remainingUnknowns : [],
    recommendedNextProvider: partial.recommendedNextProvider || null,
    recommendedNextInvestigation: partial.recommendedNextInvestigation || null,
  };
}

function deriveObjective(mission = {}, marketDefinition = {}) {
  const text = mission.objectiveText || mission.objective || '';
  const segment = marketDefinition.segment || marketDefinition.segments?.[0] || 'target market';
  const geo = marketDefinition.geography || '';
  if (text) return text;
  return `Identify highest-probability opportunities among ${segment}${geo ? ` in ${geo}` : ''}`;
}

function buildStoppingConditions(opts = {}) {
  const confidenceTarget =
    opts.confidenceThreshold != null ? opts.confidenceThreshold : DEFAULT_CONFIDENCE_THRESHOLD;
  const coverageTarget =
    opts.coverageThreshold != null ? opts.coverageThreshold : DEFAULT_COVERAGE_THRESHOLD;
  const budgetLimit = opts.maxCostBudget != null ? opts.maxCostBudget : DEFAULT_MAX_COST_BUDGET;

  return {
    confidenceTarget,
    coverageTarget,
    budgetLimit,
    minExpectedGain: opts.minExpectedGain != null ? opts.minExpectedGain : 0.02,
    stopWhen: [
      'confidence_target_achieved',
      'coverage_target_achieved',
      'budget_exhausted',
      'evidence_exhausted',
      'operator_interruption',
    ],
    expression: `Confidence ≥ ${confidenceTarget} OR Coverage ≥ ${Math.round(coverageTarget * 100)}% OR budget exhausted`,
  };
}

function gapsFromHypotheses(hypotheses = []) {
  const gaps = new Set();
  for (const hyp of hypotheses) {
    if (hyp.gap) gaps.add(hyp.gap);
  }
  return [...gaps];
}

function gapsFromEvidencePlan(evidencePlan = {}) {
  return (evidencePlan.required || [])
    .map((req) => REQUIREMENT_TO_GAP[req] || req)
    .filter(Boolean);
}

function estimateCostForTier(tier) {
  return COST_TIER_ESTIMATE[tier] != null ? COST_TIER_ESTIMATE[tier] : costScoreForTier(tier);
}

function buildProviderSequence(hypotheses, evidencePlan, opts = {}) {
  const registry = opts.registry || createDefaultProviderRegistry();
  const learning = opts.learning || createProviderLearningStore();
  const allGaps = [...new Set([...gapsFromHypotheses(hypotheses), ...gapsFromEvidencePlan(evidencePlan)])];

  const providerPlans = [];
  const seen = new Set();

  for (const gap of allGaps) {
    const chain = planInvestigationChain(gap, {
      registry,
      learning,
      adaptivePlanning: opts.adaptivePlanning !== false,
    });

    for (const step of chain) {
      const key = `${step.providerId}:${step.gap}:${step.capability}`;
      if (seen.has(key)) continue;
      seen.add(key);

      providerPlans.push(
        buildProviderPlan({
          provider: step.providerId,
          providerId: step.providerId,
          providerLabel: step.providerLabel,
          capabilities: [step.capability],
          evidenceExpected: [step.gap],
          estimatedCost: estimateCostForTier(step.costTier),
          confidenceGain: step.expectedInformationGain,
          gap: step.gap,
          order: providerPlans.length + 1,
          rationale: step.rationale,
        })
      );
    }
  }

  providerPlans.sort((a, b) => {
    const ratioA = (a.confidenceGain || 0) / Math.max(a.estimatedCost, 1);
    const ratioB = (b.confidenceGain || 0) / Math.max(b.estimatedCost, 1);
    if (ratioB !== ratioA) return ratioB - ratioA;
    return a.order - b.order;
  });

  providerPlans.forEach((p, i) => {
    p.order = i + 1;
  });

  return providerPlans;
}

function estimatePlanMetrics(providerSequence, opts = {}) {
  const universeEstimate =
    opts.universeEstimate && typeof opts.universeEstimate === 'object'
      ? opts.universeEstimate
      : null;
  const estimatedUniverse =
    (universeEstimate && universeEstimate.expected) ||
    opts.estimatedMarket ||
    opts.estimatedUniverse ||
    0;
  const targetCoverage = opts.coverageThreshold || DEFAULT_COVERAGE_THRESHOLD;
  const targetConfidence = opts.confidenceThreshold || DEFAULT_CONFIDENCE_THRESHOLD;

  const pendingProviders = providerSequence.filter((p) => p.status === PROVIDER_STATUS.PENDING);
  const totalCost = pendingProviders.reduce((s, p) => s + (p.estimatedCost || 0), 0);
  const cumulativeGain = pendingProviders.reduce((s, p) => s + (p.confidenceGain || 0), 0);
  const estimatedConfidence = Number(Math.min(0.99, 0.2 + cumulativeGain * 0.15).toFixed(2));

  return {
    estimatedCoverage: {
      estimatedUniverse,
      targetCoverage,
      requiredSample: estimatedUniverse > 0 ? Math.ceil(estimatedUniverse * targetCoverage) : 0,
      confidenceTarget: targetConfidence,
    },
    estimatedConfidence,
    estimatedCost: totalCost,
  };
}

/**
 * Construct an explicit Investigation Plan before any provider executes.
 * @param {object} input
 * @returns {object}
 */
function createInvestigationPlan(input = {}) {
  const { mission = {}, marketDefinition = {}, opts = {}, learning, memory } = input;
  const hypotheses = generateHypotheses(marketDefinition, mission, opts);
  const evidencePlan = buildEvidencePlan(marketDefinition, opts);

  const learningStore =
    learning || (memory ? loadLearningFromMemory(memory) : createProviderLearningStore());

  const providerSequence = buildProviderSequence(hypotheses, evidencePlan, {
    ...opts,
    learning: learningStore,
  });

  const metrics = estimatePlanMetrics(providerSequence, opts);
  const stoppingConditions = buildStoppingConditions(opts);

  return buildInvestigationPlan({
    mission,
    objective: deriveObjective(mission, marketDefinition),
    hypotheses,
    evidenceRequired: evidencePlan.required,
    providerSequence,
    stoppingConditions,
    estimatedCoverage: metrics.estimatedCoverage,
    estimatedConfidence: metrics.estimatedConfidence,
    estimatedCost: metrics.estimatedCost,
    rationale:
      'Investigation plan constructed before any provider execution (ADR-064). Hypotheses drive evidence requirements; provider sequence optimizes confidence per cost.',
  });
}

function findReplacementProviders(gap, unavailableProvider, opts = {}) {
  const registry = opts.registry || createDefaultProviderRegistry();
  const learning = opts.learning || createProviderLearningStore();
  const chain = planInvestigationChain(gap, { registry, learning, adaptivePlanning: true });

  return chain
    .filter((s) => s.providerId !== unavailableProvider)
    .slice(0, 2)
    .map((s) =>
      buildProviderPlan({
        provider: s.providerId,
        providerId: s.providerId,
        providerLabel: s.providerLabel,
        capabilities: [s.capability],
        evidenceExpected: [s.gap],
        estimatedCost: estimateCostForTier(s.costTier),
        confidenceGain: Number(((s.expectedInformationGain || 0) * 0.9).toFixed(3)),
        gap: s.gap,
        rationale: `Replacement for unavailable ${unavailableProvider}`,
      })
    );
}

/**
 * Revise plan when providers are unavailable (Test 2).
 * @param {object} plan
 * @param {object} revision
 * @returns {object}
 */
function reviseInvestigationPlan(plan, revision = {}) {
  const unavailable = new Set(
    (revision.unavailableProviders || []).map((p) => String(p).toLowerCase())
  );
  const reason = revision.reason || 'Provider unavailable';
  const replacements = revision.replacements || {};

  let revisedSequence = plan.providerSequence.map((entry) => {
    const pid = String(entry.providerId || entry.provider).toLowerCase();
    if (unavailable.has(pid) && entry.status === PROVIDER_STATUS.PENDING) {
      return { ...entry, status: PROVIDER_STATUS.UNAVAILABLE, unavailableReason: reason };
    }
    return entry;
  });

  for (const [oldProvider, newSteps] of Object.entries(replacements)) {
    const idx = revisedSequence.findIndex(
      (e) =>
        String(e.providerId).toLowerCase() === oldProvider.toLowerCase() &&
        e.status === PROVIDER_STATUS.UNAVAILABLE
    );
    if (idx >= 0 && Array.isArray(newSteps)) {
      const inserts = newSteps.map((s, i) =>
        buildProviderPlan({
          ...s,
          status: PROVIDER_STATUS.PENDING,
          order: idx + i + 2,
        })
      );
      revisedSequence = [
        ...revisedSequence.slice(0, idx + 1),
        ...inserts,
        ...revisedSequence.slice(idx + 1),
      ];
    }
  }

  for (const pid of unavailable) {
    const failedEntries = revisedSequence.filter(
      (e) => String(e.providerId).toLowerCase() === pid && e.status === PROVIDER_STATUS.UNAVAILABLE
    );
    for (const entry of failedEntries) {
      if (!replacements[pid] && entry.gap) {
        const altSteps = findReplacementProviders(entry.gap, pid, revision.opts || {});
        if (altSteps.length) {
          const idx = revisedSequence.indexOf(entry);
          revisedSequence = [
            ...revisedSequence.slice(0, idx + 1),
            ...altSteps.map((s, i) => ({ ...s, order: idx + i + 2 })),
            ...revisedSequence.slice(idx + 1),
          ];
        }
      }
    }
  }

  const confidencePenalty = unavailable.size * 0.05;
  const newConfidence = Math.max(0.1, (plan.estimatedConfidence || 0) - confidencePenalty);

  const revisionRecord = {
    at: new Date().toISOString(),
    reason,
    unavailableProviders: [...unavailable],
    confidenceBefore: plan.estimatedConfidence,
    confidenceAfter: newConfidence,
  };

  return buildInvestigationPlan({
    ...plan,
    providerSequence: revisedSequence,
    estimatedConfidence: Number(newConfidence.toFixed(2)),
    revisions: [...(plan.revisions || []), revisionRecord],
    rationale: `${plan.rationale || ''} Revised: ${reason}.`.trim(),
  });
}

function updatePlanAfterStep(plan, step, outcome = {}) {
  const sequence = plan.providerSequence.map((entry) => {
    if (
      entry.providerId === step.providerId &&
      entry.gap === step.gap &&
      (entry.status === PROVIDER_STATUS.PENDING || entry.status === PROVIDER_STATUS.FAILED)
    ) {
      let status = PROVIDER_STATUS.COMPLETED;
      if (outcome.failed) status = PROVIDER_STATUS.FAILED;
      else if (outcome.skipped) status = PROVIDER_STATUS.SKIPPED;
      return { ...entry, status };
    }
    return entry;
  });

  return { ...plan, providerSequence: sequence };
}

function skipRemainingProviders(plan, reason = 'confidence_target_achieved') {
  const sequence = plan.providerSequence.map((entry) => {
    if (entry.status === PROVIDER_STATUS.PENDING) {
      return { ...entry, status: PROVIDER_STATUS.SKIPPED, skipReason: reason };
    }
    return entry;
  });
  return { ...plan, providerSequence: sequence };
}

function isStepInPlan(step, plan) {
  if (!plan || !plan.providerSequence || !plan.providerSequence.length) return false;
  return plan.providerSequence.some(
    (e) =>
      e.providerId === step.providerId &&
      (e.gap === step.gap || !e.gap) &&
      e.status !== PROVIDER_STATUS.UNAVAILABLE
  );
}

function buildInvestigationStatusFromPlan(plan, executionState = {}) {
  const sequence = plan.providerSequence || [];
  const completed = sequence.filter((e) => e.status === PROVIDER_STATUS.COMPLETED);
  const remaining = sequence.filter((e) => e.status === PROVIDER_STATUS.PENDING);
  const blocked = sequence.filter(
    (e) => e.status === PROVIDER_STATUS.FAILED || e.status === PROVIDER_STATUS.UNAVAILABLE
  );

  const recommendedNext =
    remaining.sort(
      (a, b) =>
        (b.confidenceGain || 0) / Math.max(b.estimatedCost, 1) -
        (a.confidenceGain || 0) / Math.max(a.estimatedCost, 1)
    )[0] || null;

  return buildInvestigationStatus({
    completedSteps: completed,
    remainingSteps: remaining,
    confidence:
      executionState.confidence != null ? executionState.confidence : plan.estimatedConfidence,
    coverage: executionState.coverage != null ? executionState.coverage : 0,
    cost: executionState.cost != null ? executionState.cost : 0,
    blockers: blocked.map((f) => ({
      provider: f.providerId,
      gap: f.gap,
      reason: f.unavailableReason || f.skipReason || 'Provider failed',
    })),
    remainingUnknowns: executionState.remainingUnknowns || [],
    recommendedNextProvider: recommendedNext ? recommendedNext.providerId : null,
    recommendedNextInvestigation: recommendedNext
      ? `${recommendedNext.providerLabel} → ${recommendedNext.gap}`
      : null,
  });
}

/**
 * Build a plan that incorporates prior provider learning (Test 5).
 * @param {object} input
 * @param {object} priorMemory
 * @returns {object}
 */
function createInvestigationPlanWithLearning(input = {}, priorMemory = null) {
  const learning =
    priorMemory && priorMemory.investigation
      ? loadLearningFromMemory(priorMemory)
      : input.learning || createProviderLearningStore();

  return createInvestigationPlan({
    ...input,
    learning,
    memory: priorMemory || input.memory,
  });
}

module.exports = {
  PROVIDER_STATUS,
  COST_TIER_ESTIMATE,
  buildInvestigationPlan,
  buildProviderPlan,
  buildInvestigationStatus,
  createInvestigationPlan,
  createInvestigationPlanWithLearning,
  reviseInvestigationPlan,
  updatePlanAfterStep,
  skipRemainingProviders,
  isStepInPlan,
  buildInvestigationStatusFromPlan,
  findReplacementProviders,
  buildStoppingConditions,
  estimatePlanMetrics,
  buildProviderSequence,
  deriveObjective,
};
