'use strict';

/**
 * SPEC-142 — Investigation Planner.
 * SPEC-145 — Adaptive value-of-information planning; question chooses provider.
 */

const { buildInvestigationStep } = require('./types');
const { GAP_TO_EVIDENCE_TYPES } = require('./MissingEvidence');
const {
  createDefaultProviderRegistry,
  COST_TIER_ORDER,
} = require('../intelligence/ProviderCapabilityRegistry');
const { GAP_TO_CAPABILITY, EVIDENCE_TYPE_TO_CAPABILITY, resolveCapability } = require('./GapCapabilities');

const COST_TIER_SCORE = Object.freeze({
  free: 1,
  cached: 2,
  local: 4,
  paid: 8,
});

const { getGapProfile, getTopPriorityUnknown } = require('./InvestigationBoard');
const {
  createProviderLearningStore,
  estimateInformationGain,
  loadLearningFromMemory,
} = require('./ProviderLearning');

const DEFAULT_MIN_EXPECTED_GAIN = 0.02;

function costScoreForTier(tier) {
  return COST_TIER_SCORE[tier] != null ? COST_TIER_SCORE[tier] : 10;
}

/**
 * Map a gap to the provider best suited to answer it (question-driven, not pipeline order).
 * @param {string} gap
 * @param {object} [opts]
 * @returns {string[]}
 */
function providersForGap(gap, opts = {}) {
  const learning = opts.learning || createProviderLearningStore();
  const ranked = learning.getBestProvidersForGap(gap, 5).map((r) => r.providerId);
  const evidenceTypes = GAP_TO_EVIDENCE_TYPES[gap] || [gap];
  const fromEvidence = evidenceTypes.map((et) => {
    const cap = resolveCapability(et);
    const registry = opts.registry || createDefaultProviderRegistry();
    const match = registry.selectForCapabilities([cap], { allowMultiplePerCapability: true });
    return match[0] && match[0].providerId;
  }).filter(Boolean);

  const merged = [...new Set([...ranked, ...fromEvidence])];
  return merged;
}

/**
 * Build investigation chain for a gap (adaptive: highest expected gain first).
 * @param {string} gap
 * @param {object} [opts]
 * @returns {object[]}
 */
function planInvestigationChain(gap, opts = {}) {
  const registry = opts.registry || createDefaultProviderRegistry();
  const learning = opts.learning || createProviderLearningStore();
  const profile = getGapProfile(gap);
  const evidenceTypes = GAP_TO_EVIDENCE_TYPES[gap] || [gap];
  const steps = [];
  const seen = new Set();

  for (const evidenceType of evidenceTypes) {
    const capability = resolveCapability(evidenceType);
    const providers = registry
      .selectForCapabilities([capability], { allowMultiplePerCapability: true })
      .filter((p) => !seen.has(`${p.providerId}:${capability}`));

    for (const provider of providers) {
      seen.add(`${provider.providerId}:${capability}`);
      const providerMeta = registry.get(provider.providerId) || {};
      const effectiveness = learning.getEffectiveness(provider.providerId, gap);
      const expectedGain = estimateInformationGain({
        gapImpact: profile.impact,
        providerEffectiveness: effectiveness,
        providerCoverage: providerMeta.coverage || 0.5,
        providerReliability: providerMeta.reliability || provider.reliability || 0.7,
      });

      steps.push(
        buildInvestigationStep({
          gap,
          capability,
          providerId: provider.providerId,
          providerLabel: provider.label,
          costTier: provider.costTier,
          costScore: costScoreForTier(provider.costTier),
          entityId: opts.entityId || null,
          expectedInformationGain: expectedGain,
          rationale: `Resolve ${profile.label} via ${provider.label} — expected gain ${Math.round(expectedGain * 100)}%`,
          question: `Need ${profile.label}`,
          gapImpact: profile.impact,
          gapDifficulty: profile.difficulty,
        })
      );
    }
  }

  if (opts.adaptivePlanning === false) {
    return steps.sort((a, b) => a.costScore - b.costScore);
  }

  return steps.sort((a, b) => {
    const gainA = a.expectedInformationGain || 0;
    const gainB = b.expectedInformationGain || 0;
    if (gainB !== gainA) return gainB - gainA;
    if (a.costScore !== b.costScore) return a.costScore - b.costScore;
    return COST_TIER_ORDER.indexOf(a.costTier) - COST_TIER_ORDER.indexOf(b.costTier);
  });
}

/**
 * Select the next best investigation step using value-of-information.
 * @param {object} input
 * @returns {object|null}
 */
function selectNextInvestigation(input = {}) {
  const attempted = new Set(input.attempted || []);
  const resolvedGaps = new Set(input.resolvedGaps || []);
  const registry = input.registry || createDefaultProviderRegistry();
  const learning =
    input.learning ||
    (input.memory ? loadLearningFromMemory(input.memory) : createProviderLearningStore());
  const minGain = input.minExpectedGain != null ? input.minExpectedGain : DEFAULT_MIN_EXPECTED_GAIN;
  const adaptivePlanning = input.adaptivePlanning !== false;

  let priorityGaps = input.missing || [];
  if (input.board) {
    const top = getTopPriorityUnknown(input.board);
    if (top) {
      priorityGaps = [
        top.gap,
        ...priorityGaps.filter((g) => g !== top.gap),
      ];
    }
  }

  priorityGaps = priorityGaps.filter((g) => !resolvedGaps.has(g));
  if (priorityGaps.length === 0) return null;

  const chains = [];
  for (const gap of priorityGaps) {
    chains.push(
      ...planInvestigationChain(gap, {
        registry,
        learning,
        entityId: input.entityId,
        adaptivePlanning,
      })
    );
  }

  const available = chains.filter((step) => {
    const key = `${step.entityId || 'global'}:${step.gap}:${step.providerId}:${step.capability}`;
    return !attempted.has(key);
  });

  if (available.length === 0) return null;

  if (adaptivePlanning) {
    available.sort((a, b) => {
      const gainA = a.expectedInformationGain || 0;
      const gainB = b.expectedInformationGain || 0;
      if (gainB !== gainA) return gainB - gainA;
      if (a.costScore !== b.costScore) return a.costScore - b.costScore;
      return COST_TIER_ORDER.indexOf(a.costTier) - COST_TIER_ORDER.indexOf(b.costTier);
    });

    const best = available[0];
    if ((best.expectedInformationGain || 0) < minGain) {
      return {
        ...best,
        belowGainThreshold: true,
        stopRecommendation: {
          reason: 'diminishing_returns',
          expectedGain: best.expectedInformationGain,
          minGain,
          explanation: `Best next step (${best.providerLabel}) yields only ${Math.round((best.expectedInformationGain || 0) * 100)}% expected gain`,
        },
      };
    }
    return best;
  }

  available.sort((a, b) => {
    if (a.costScore !== b.costScore) return a.costScore - b.costScore;
    return COST_TIER_ORDER.indexOf(a.costTier) - COST_TIER_ORDER.indexOf(b.costTier);
  });
  return available[0];
}

/**
 * Compare candidate steps by diminishing returns — pick highest gain per cost unit.
 * @param {object[]} candidates
 * @returns {object|null}
 */
function selectByDiminishingReturns(candidates = []) {
  if (!candidates.length) return null;
  const scored = candidates.map((step) => ({
    ...step,
    gainPerCost: (step.expectedInformationGain || 0) / Math.max(step.costScore || 1, 1),
  }));
  scored.sort((a, b) => b.gainPerCost - a.gainPerCost);
  return scored[0];
}

/**
 * Mark steps skipped when earlier evidence resolved the gap.
 * @param {object[]} chain
 * @param {Set<string>} resolvedGaps
 * @returns {object[]}
 */
function applyDynamicReplanning(chain, resolvedGaps) {
  const resolved = new Set(resolvedGaps);
  let skipRemainingForGap = null;

  return chain.map((step) => {
    if (resolved.has(step.gap)) {
      return { ...step, skipped: true, skipReason: `${step.gap} already resolved` };
    }
    if (skipRemainingForGap === step.gap) {
      return { ...step, skipped: true, skipReason: `Earlier step resolved ${step.gap}` };
    }
    if (!step.skipped && resolved.has(step.gap)) skipRemainingForGap = step.gap;
    return step;
  });
}

/**
 * Explain why a step was chosen (acceptance criteria helper).
 * @param {object} step
 * @param {object} board
 * @returns {object}
 */
function explainStepSelection(step, board = null) {
  const top = board ? getTopPriorityUnknown(board) : null;
  return {
    mostImportantUnknown: top ? top.gap : step.gap,
    whyHighestPriority: top
      ? `Impact ${top.impact}, difficulty ${top.difficulty}, expected value ${top.expectedValue}`
      : `Gap ${step.gap} has impact ${step.gapImpact}`,
    chosenProvider: step.providerId,
    providerLabel: step.providerLabel,
    whyThisProvider: step.rationale,
    expectedInformationGain: step.expectedInformationGain,
    question: step.question || `Need ${step.gap}`,
  };
}

module.exports = {
  GAP_TO_CAPABILITY,
  EVIDENCE_TYPE_TO_CAPABILITY,
  COST_TIER_SCORE,
  DEFAULT_MIN_EXPECTED_GAIN,
  providersForGap,
  planInvestigationChain,
  selectNextInvestigation,
  selectByDiminishingReturns,
  applyDynamicReplanning,
  costScoreForTier,
  explainStepSelection,
};
