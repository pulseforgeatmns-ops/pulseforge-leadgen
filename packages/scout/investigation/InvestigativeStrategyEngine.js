'use strict';

/**
 * SPEC-163 — Investigative Strategy Engine.
 * ADR-083 — Investigate what reduces uncertainty most.
 *
 * Scout selects investigations by expected information gain, not checklist order.
 * Every investigation must have documented expected information gain.
 */

const { COMPLETION_REASONS } = require('./types');
const {
  getGapProfile,
  buildUnknownEntry,
  sortUnknownsByValue,
} = require('./InvestigationBoard');
const { GAP_TO_EVIDENCE_TYPES } = require('./MissingEvidence');
const { createDefaultProviderRegistry } = require('../intelligence/ProviderCapabilityRegistry');
const {
  createProviderLearningStore,
  estimateInformationGain,
  loadLearningFromMemory,
} = require('./ProviderLearning');
const { planInvestigationChain } = require('./InvestigationPlanner');

const DEFAULT_MIN_EXPECTED_GAIN = 0.02;
const DEFAULT_UNCERTAINTY_THRESHOLD = 0.15;

/** Heuristic activation shifts investigation priorities (SPEC-163 Scenario 3). */
const HEURISTIC_INVESTIGATION_PRIORITIES = Object.freeze({
  vendor_instability: {
    priorityGaps: ['current_vendor', 'cleaning_responsibility', 'vendor_relationship', 'contract_timing'],
    focusSources: ['google_reviews', 'linkedin', 'news', 'website'],
    boost: 0.12,
    reason: 'Vendor Instability active — test vendor change signals',
  },
  growth_market: {
    priorityGaps: ['portfolio_size', 'expansion_plans', 'buying_signals'],
    focusSources: ['news', 'website', 'county_records'],
    boost: 0.08,
    reason: 'Growth Market active — validate expansion opportunity',
  },
  buying_readiness: {
    priorityGaps: ['decision_maker', 'buying_signals', 'contract_timing'],
    focusSources: ['linkedin', 'news', 'prospeo'],
    boost: 0.1,
    reason: 'Buying Readiness active — identify decision window',
  },
  expansion_hiring: {
    priorityGaps: ['decision_maker', 'vendor_relationship', 'buying_signals'],
    focusSources: ['linkedin', 'news'],
    boost: 0.08,
    reason: 'Expansion Hiring active — leadership change may precede vendor evaluation',
  },
  operational_maturity: {
    priorityGaps: ['decision_maker', 'company_size'],
    focusSources: ['linkedin', 'website'],
    boost: 0.05,
    reason: 'Operational Maturity active — map formal decision structure',
  },
  relationship_leverage: {
    priorityGaps: ['contact_path', 'decision_maker'],
    focusSources: ['linkedin', 'prospeo'],
    boost: 0.06,
    reason: 'Relationship Leverage active — find warm introduction paths',
  },
});

/** Operator-facing investigation sources mapped to provider capabilities. */
const INVESTIGATION_SOURCE_CATALOG = Object.freeze({
  google_reviews: {
    source: 'google_reviews',
    label: 'Google Reviews',
    providerId: 'google_maps',
    costTier: 'paid',
    costScore: 8,
    gapEffectiveness: {
      current_vendor: 0.85,
      cleaning_responsibility: 0.8,
      vendor_relationship: 0.75,
      buying_signals: 0.55,
    },
  },
  facebook: {
    source: 'facebook',
    label: 'Facebook',
    providerId: 'facebook',
    costTier: 'free',
    costScore: 1,
    gapEffectiveness: {
      decision_maker: 0.25,
      buying_signals: 0.45,
      vendor_relationship: 0.4,
      current_vendor: 0.35,
    },
  },
  website: {
    source: 'website',
    label: 'Website',
    providerId: 'website',
    costTier: 'free',
    costScore: 1,
    gapEffectiveness: {
      portfolio_size: 0.4,
      cleaning_responsibility: 0.55,
      business_fit: 0.7,
      decision_maker: 0.3,
      current_vendor: 0.35,
    },
  },
  linkedin: {
    source: 'linkedin',
    label: 'LinkedIn',
    providerId: 'linkedin',
    costTier: 'paid',
    costScore: 8,
    gapEffectiveness: {
      decision_maker: 0.9,
      ownership: 0.75,
      buying_signals: 0.55,
      vendor_relationship: 0.35,
      expansion_plans: 0.5,
    },
  },
  news: {
    source: 'news',
    label: 'News',
    providerId: 'news',
    costTier: 'paid',
    costScore: 8,
    gapEffectiveness: {
      buying_signals: 0.75,
      expansion_plans: 0.7,
      vendor_relationship: 0.45,
      portfolio_size: 0.3,
    },
  },
  county_records: {
    source: 'county_records',
    label: 'County Records',
    providerId: 'county_records',
    costTier: 'local',
    costScore: 4,
    gapEffectiveness: {
      portfolio_size: 0.85,
      ownership: 0.7,
      property_count: 0.9,
    },
  },
  prospeo: {
    source: 'prospeo',
    label: 'Prospeo',
    providerId: 'prospeo',
    costTier: 'paid',
    costScore: 8,
    gapEffectiveness: {
      contact_path: 0.85,
      decision_maker: 0.55,
    },
  },
  secretary_of_state: {
    source: 'secretary_of_state',
    label: 'Secretary of State',
    providerId: 'county_records',
    costTier: 'local',
    costScore: 4,
    gapEffectiveness: {
      ownership: 0.8,
      company_size: 0.5,
    },
  },
  industry_directory: {
    source: 'industry_directory',
    label: 'Industry Directory',
    providerId: 'existing_pf',
    costTier: 'cached',
    costScore: 2,
    gapEffectiveness: {
      business_fit: 0.6,
      portfolio_size: 0.35,
      decision_maker: 0.2,
    },
  },
});

const UNKNOWN_TEXT_TO_GAP = Object.freeze([
  { pattern: /decision\s*maker|who\s+(owns|runs|leads)/i, gap: 'decision_maker' },
  { pattern: /clean(ing)?\s*vendor|current\s*vendor|outsource\s*clean/i, gap: 'current_vendor' },
  { pattern: /clean(ing)?\s*responsib|who\s+cleans/i, gap: 'cleaning_responsibility' },
  { pattern: /portfolio|property\s*count|how\s+many\s+propert/i, gap: 'portfolio_size' },
  { pattern: /expansion|growth|portfolio\s*growth/i, gap: 'expansion_plans' },
  { pattern: /contact|reach|email|phone/i, gap: 'contact_path' },
  { pattern: /buying|procurement|evaluate/i, gap: 'buying_signals' },
  { pattern: /vendor\s*rel/i, gap: 'vendor_relationship' },
  { pattern: /contract|renewal/i, gap: 'contract_timing' },
  { pattern: /ownership|owner/i, gap: 'ownership' },
  { pattern: /company\s*size|employee/i, gap: 'company_size' },
  { pattern: /facebook/i, gap: 'buying_signals' },
  { pattern: /coverage\s*incomplete/i, gap: 'portfolio_size' },
]);

function buildInvestigationOption(partial = {}) {
  return {
    id: partial.id || `inv-${partial.source || 'unknown'}-${partial.gap || 'gap'}`,
    source: partial.source,
    sourceLabel: partial.sourceLabel || partial.label || partial.source,
    providerId: partial.providerId || null,
    gap: partial.gap,
    unknownId: partial.unknownId || null,
    objective: partial.objective || `Resolve ${partial.gap || 'unknown'}`,
    expectedInformationGain:
      partial.expectedInformationGain != null ? Number(partial.expectedInformationGain) : 0,
    expectedConfidenceIncrease:
      partial.expectedConfidenceIncrease != null
        ? Number(partial.expectedConfidenceIncrease)
        : partial.expectedInformationGain != null
          ? Number((partial.expectedInformationGain * 0.5).toFixed(3))
          : 0,
    estimatedCost: partial.estimatedCost != null ? partial.estimatedCost : partial.costScore || 1,
    costTier: partial.costTier || 'free',
    reasoning: partial.reasoning || '',
    heuristicBoost: partial.heuristicBoost || 0,
    heuristicReasons: partial.heuristicReasons || [],
    status: partial.status || 'candidate',
    completedAt: partial.completedAt || null,
    documentedGain: partial.expectedInformationGain != null,
  };
}

function buildInvestigationStrategy(partial = {}) {
  return {
    kind: 'investigation_strategy',
    spec: 'SPEC-163',
    adr: 'ADR-083',
    knowns: partial.knowns || [],
    unknowns: partial.unknowns || [],
    assumptions: partial.assumptions || [],
    hypotheses: partial.hypotheses || [],
    candidateInvestigations: partial.candidateInvestigations || [],
    investigationQueue: partial.investigationQueue || [],
    selectedInvestigation: partial.selectedInvestigation || null,
    expectedInformationGain: partial.expectedInformationGain ?? null,
    completedInvestigations: partial.completedInvestigations || [],
    informationGainHistory: partial.informationGainHistory || [],
    reasoning: partial.reasoning || '',
    stoppingCondition: partial.stoppingCondition || null,
    activatedHeuristics: partial.activatedHeuristics || [],
    recalculatedAt: partial.recalculatedAt || new Date().toISOString(),
  };
}

function inferGapFromUnknown(unknown = {}) {
  if (unknown.gap) return unknown.gap;
  const text = String(unknown.label || unknown.question || unknown.text || unknown);
  for (const entry of UNKNOWN_TEXT_TO_GAP) {
    if (entry.pattern.test(text)) return entry.gap;
  }
  return 'business_fit';
}

function normalizeUnknown(entry, index = 0) {
  if (typeof entry === 'string') {
    const gap = inferGapFromUnknown({ text: entry });
    const profile = getGapProfile(gap);
    return buildUnknownEntry({
      gap,
      label: entry,
      impact: profile.impact,
      difficulty: profile.difficulty,
    });
  }

  const gap = entry.gap || inferGapFromUnknown(entry);
  const profile = getGapProfile(gap);
  return {
    ...buildUnknownEntry({
      gap,
      label: entry.label || entry.question || entry.text || profile.label,
      impact: entry.impact ?? profile.impact,
      difficulty: entry.difficulty ?? profile.difficulty,
      status: entry.status,
      attemptCount: entry.attemptCount,
      failedProviders: entry.failedProviders,
    }),
    id: entry.id || `unknown-${gap}-${index}`,
    question: entry.question || entry.label || profile.label,
    source: entry.source || 'uncertainty_tracking',
    priority: entry.priority || 'medium',
    hypothesisId: entry.hypothesisId || null,
  };
}

function extractKnownsFromState(state = {}, board = null) {
  const knowns = [];

  for (const resolved of state.uncertainty?.resolved || []) {
    knowns.push({
      kind: 'resolved_unknown',
      label: typeof resolved === 'string' ? resolved : resolved.label || resolved.gap,
      gap: typeof resolved === 'object' ? resolved.gap : inferGapFromUnknown({ text: resolved }),
      source: 'uncertainty_resolved',
    });
  }

  for (const node of state.evidenceGraph?.nodes || []) {
    if (node.type === 'claim' || node.type === 'confidence') {
      knowns.push({
        kind: 'evidence_graph',
        label: node.label || node.id,
        gap: node.data?.gap || null,
        confidence: node.data?.confidence,
        source: node.data?.source || 'evidence',
      });
    }
  }

  if (board?.known) {
    for (const k of board.known) {
      knowns.push({
        kind: 'board_known',
        gap: k.gap,
        label: k.label,
        confidence: k.confidence,
        source: k.source,
      });
    }
  }

  return knowns;
}

function extractUnknownsFromState(state = {}, board = null, judgmentResult = null) {
  const unknownMap = new Map();
  const resolvedGaps = new Set();

  for (const resolved of state.uncertainty?.resolved || []) {
    resolvedGaps.add(inferGapFromUnknown({ text: typeof resolved === 'string' ? resolved : resolved.label }));
  }

  const addUnknown = (entry) => {
    const normalized = normalizeUnknown(entry, unknownMap.size);
    if (resolvedGaps.has(normalized.gap)) return;
    const key = normalized.gap;
    if (!unknownMap.has(key) || (normalized.impact || 0) > (unknownMap.get(key).impact || 0)) {
      unknownMap.set(key, normalized);
    }
  };

  for (const open of state.uncertainty?.open || []) addUnknown(open);
  for (const persistent of state.uncertainty?.persistent || []) {
    addUnknown({
      ...(typeof persistent === 'string' ? { label: persistent } : persistent),
      source: 'persistent_unknown',
      priority: 'medium',
    });
  }

  for (const hyp of (state.activeHypotheses || []).filter((h) => h.lifecycle === 'generated')) {
    addUnknown({
      gap: 'buying_signals',
      label: `Test hypothesis: ${hyp.text}`,
      question: `Test hypothesis: ${hyp.text}`,
      source: 'hypothesis',
      priority: 'high',
      hypothesisId: hyp.id,
      impact: 0.85,
    });
  }

  if (board?.unknown) {
    for (const u of board.unknown) addUnknown(u);
  }

  const hasTrackedUncertainty =
    (state.uncertainty?.open || []).length > 0 ||
    (state.uncertainty?.persistent || []).length > 0;

  if (!hasTrackedUncertainty && !state.coverage?.complete && unknownMap.size === 0) {
    const defaultUnknowns = [
      { gap: 'decision_maker', label: 'Decision maker unknown' },
      { gap: 'current_vendor', label: 'Current cleaning vendor unknown' },
      { gap: 'portfolio_size', label: 'Portfolio size unknown' },
      { gap: 'expansion_plans', label: 'Portfolio growth trajectory unknown' },
      { gap: 'buying_signals', label: 'Buying window signals unknown' },
    ];
    for (const u of defaultUnknowns) {
      if (unknownMap.size >= 5) break;
      if (!unknownMap.has(u.gap) && !resolvedGaps.has(u.gap)) addUnknown(u);
    }
  }

  return sortUnknownsByValue([...unknownMap.values()]);
}

function extractAssumptionsFromState(state = {}) {
  const assumptions = [];
  const market = state.marketDefinition || {};

  if (market.terminology?.length) {
    assumptions.push({
      kind: 'terminology',
      label: `Market terminology: ${market.terminology.slice(0, 2).join(', ')}`,
      confidence: state.confidence,
    });
  }

  if (state.universeEstimate) {
    assumptions.push({
      kind: 'universe_estimate',
      label: `Estimated universe: ${state.universeEstimate.expected ?? state.universeEstimate.minimum}-${state.universeEstimate.maximum}`,
      confidence: state.universeEstimate.confidence,
    });
  }

  return assumptions;
}

function getHeuristicBoosts(activatedHeuristics = []) {
  const boosts = {
    gapBoosts: {},
    sourceBoosts: {},
    reasons: [],
  };

  for (const activated of activatedHeuristics) {
    const config = HEURISTIC_INVESTIGATION_PRIORITIES[activated.heuristicId];
    if (!config) continue;

    boosts.reasons.push(config.reason);
    for (const gap of config.priorityGaps) {
      boosts.gapBoosts[gap] = Math.max(boosts.gapBoosts[gap] || 0, config.boost);
    }
    for (const source of config.focusSources) {
      boosts.sourceBoosts[source] = Math.max(boosts.sourceBoosts[source] || 0, config.boost);
    }
  }

  return boosts;
}

function sourcesForGap(gap) {
  const evidenceTypes = GAP_TO_EVIDENCE_TYPES[gap] || [gap];
  const sources = new Set();

  for (const [sourceKey, config] of Object.entries(INVESTIGATION_SOURCE_CATALOG)) {
    if ((config.gapEffectiveness[gap] || 0) > 0.1) {
      sources.add(sourceKey);
    }
  }

  for (const evidenceType of evidenceTypes) {
    if (evidenceType === 'reviews') sources.add('google_reviews');
    if (evidenceType === 'linkedin' || evidenceType === 'people') sources.add('linkedin');
    if (evidenceType === 'website') sources.add('website');
    if (evidenceType === 'news') sources.add('news');
    if (evidenceType === 'county_records' || evidenceType === 'property_portfolio') {
      sources.add('county_records');
    }
    if (evidenceType === 'contacts' || evidenceType === 'emails') sources.add('prospeo');
    if (evidenceType === 'vendor_references') sources.add('google_reviews');
    if (evidenceType === 'hiring_activity') sources.add('linkedin');
  }

  return [...sources];
}

function buildCandidateInvestigationsForUnknown(unknown, context = {}) {
  const gap = unknown.gap || inferGapFromUnknown(unknown);
  const profile = getGapProfile(gap);
  const learning = context.learning || createProviderLearningStore();
  const heuristicBoosts = context.heuristicBoosts || { gapBoosts: {}, sourceBoosts: {}, reasons: [] };
  const gapHeuristicBoost = heuristicBoosts.gapBoosts[gap] || 0;
  const options = [];

  for (const sourceKey of sourcesForGap(gap)) {
    const catalog = INVESTIGATION_SOURCE_CATALOG[sourceKey];
    if (!catalog) continue;

    const providerEffectiveness =
      catalog.gapEffectiveness[gap] ||
      learning.getEffectiveness(catalog.providerId, gap) ||
      0.35;

    const sourceHeuristicBoost = heuristicBoosts.sourceBoosts[sourceKey] || 0;
    const totalHeuristicBoost = Math.max(gapHeuristicBoost, sourceHeuristicBoost);

    let expectedGain = estimateInformationGain({
      gapImpact: unknown.impact ?? profile.impact,
      providerEffectiveness,
      providerCoverage: 0.7,
      providerReliability: 0.75,
      diminishingFactor: 1 - (unknown.difficulty ?? profile.difficulty) * 0.3,
    });

    if (totalHeuristicBoost > 0) {
      expectedGain = Number(Math.min(0.99, expectedGain + totalHeuristicBoost).toFixed(3));
    }

    const heuristicReasons = [];
    if (gapHeuristicBoost > 0) {
      heuristicReasons.push(...heuristicBoosts.reasons.filter((r) => r.includes(gap.replace(/_/g, ' ')) || true));
    }
    if (sourceHeuristicBoost > 0 && gapHeuristicBoost === 0) {
      heuristicReasons.push(...heuristicBoosts.reasons);
    }

    options.push(
      buildInvestigationOption({
        id: `inv-${sourceKey}-${gap}`,
        source: sourceKey,
        sourceLabel: catalog.label,
        providerId: catalog.providerId,
        gap,
        unknownId: unknown.id,
        objective: `Resolve ${profile.label}: ${unknown.label || unknown.question || profile.label}`,
        expectedInformationGain: expectedGain,
        expectedConfidenceIncrease: Number((expectedGain * 0.5).toFixed(3)),
        estimatedCost: catalog.costScore,
        costTier: catalog.costTier,
        heuristicBoost: totalHeuristicBoost,
        heuristicReasons: [...new Set(heuristicReasons)].slice(0, 2),
        reasoning: buildOptionReasoning({
          unknown,
          sourceLabel: catalog.label,
          expectedGain,
          heuristicBoost: totalHeuristicBoost,
          heuristicReasons,
        }),
      })
    );
  }

  if (context.registry && context.adaptivePlanning !== false) {
    const chain = planInvestigationChain(gap, {
      registry: context.registry,
      learning,
      adaptivePlanning: true,
    });
    for (const step of chain.slice(0, 3)) {
      const existing = options.find((o) => o.providerId === step.providerId);
      if (existing) {
        existing.expectedInformationGain = Math.max(
          existing.expectedInformationGain,
          step.expectedInformationGain || 0
        );
        continue;
      }
      options.push(
        buildInvestigationOption({
          source: step.providerId,
          sourceLabel: step.providerLabel,
          providerId: step.providerId,
          gap,
          unknownId: unknown.id,
          objective: step.question || step.rationale,
          expectedInformationGain: step.expectedInformationGain || 0,
          estimatedCost: step.costScore,
          costTier: step.costTier,
          reasoning: step.rationale,
        })
      );
    }
  }

  return options.sort((a, b) => b.expectedInformationGain - a.expectedInformationGain);
}

function buildOptionReasoning({ unknown, sourceLabel, expectedGain, heuristicBoost, heuristicReasons }) {
  const parts = [
    `${sourceLabel} expected to reduce uncertainty on "${unknown.label || unknown.gap}" (+${expectedGain.toFixed(2)} information gain)`,
  ];
  if (heuristicBoost > 0 && heuristicReasons.length) {
    parts.push(heuristicReasons[0]);
  }
  return parts.join('. ');
}

function buildInvestigationQueue(unknowns, allCandidates, heuristicBoosts) {
  const queue = [];
  let priority = 1;

  for (const unknown of unknowns) {
    const candidates = allCandidates.filter((c) => c.gap === unknown.gap);
    const best = candidates[0];
    if (!best) continue;

    const gapBoost = heuristicBoosts.gapBoosts[unknown.gap] || 0;
    queue.push({
      priority,
      unknown: unknown.label || unknown.question || unknown.gap,
      gap: unknown.gap,
      expectedGain: best.expectedInformationGain,
      selectedSource: best.sourceLabel,
      source: best.source,
      reason:
        gapBoost > 0
          ? heuristicBoosts.reasons.find((r) => r) || `${unknown.gap} prioritized by business heuristics`
          : `High-value opportunity — impact ${unknown.impact}, difficulty ${unknown.difficulty}`,
      investigationId: best.id,
    });
    priority += 1;
  }

  return queue.sort((a, b) => b.expectedGain - a.expectedGain).map((entry, idx) => ({
    ...entry,
    priority: idx + 1,
  }));
}

function extractCompletedInvestigations(state = {}, inputCompleted = []) {
  const completed = [...inputCompleted];

  for (const node of state.evidenceGraph?.nodes || []) {
    if (node.type !== 'evidence') continue;
    const source = String(node.data?.source || node.source || '').toLowerCase();
    completed.push({
      source: source || 'evidence',
      sourceLabel: node.label || source,
      gap: node.data?.gap || inferGapFromUnknown({ text: node.label }),
      completedAt: node.data?.collectedAt || null,
      expectedInformationGain: node.data?.expectedInformationGain ?? null,
      documentedGain: node.data?.expectedInformationGain != null,
    });
  }

  return completed;
}

function buildInformationGainHistory(state = {}, candidates = []) {
  const history = (state.confidenceEvolution || []).map((step) => ({
    step: step.confidence,
    reason: step.reason,
    source: step.source,
    timestamp: step.at || step.timestamp,
    expectedInformationGain: step.expectedInformationGain ?? null,
  }));

  for (const candidate of candidates.filter((c) => c.status === 'completed')) {
    history.push({
      source: candidate.sourceLabel,
      gap: candidate.gap,
      expectedInformationGain: candidate.expectedInformationGain,
      actualConfidenceIncrease: candidate.expectedConfidenceIncrease,
      timestamp: candidate.completedAt,
    });
  }

  return history;
}

function evaluateStrategyStoppingConditions(strategy, state = {}, opts = {}) {
  const minGain = opts.minExpectedGain ?? DEFAULT_MIN_EXPECTED_GAIN;
  const uncertaintyThreshold = opts.uncertaintyThreshold ?? DEFAULT_UNCERTAINTY_THRESHOLD;
  const coverageThreshold = opts.coverageThreshold ?? 0.8;

  const best = strategy.selectedInvestigation;
  const openCount = strategy.unknowns.length;
  const remainingUncertainty = openCount / Math.max(openCount + strategy.knowns.length + 1, 1);
  const coveragePct = state.coverage?.searches?.ratio ?? (state.coverage?.complete ? 1 : 0);
  const coverageSufficient = state.coverage?.complete === true || coveragePct >= coverageThreshold;

  if (best && (best.expectedInformationGain || 0) < minGain) {
    return {
      stop: true,
      reason: COMPLETION_REASONS.DIMINISHING_RETURNS,
      explanation: `Remaining investigations provide minimal value — best option (${best.sourceLabel}) yields only +${(best.expectedInformationGain || 0).toFixed(2)} expected information gain.`,
      expectedInformationGain: best.expectedInformationGain,
    };
  }

  if (coverageSufficient && (remainingUncertainty <= uncertaintyThreshold || openCount === 0)) {
    return {
      stop: true,
      reason: COMPLETION_REASONS.COVERAGE_COMPLETE,
      explanation:
        openCount === 0
          ? 'Coverage threshold reached. Remaining investigations provide negligible value.'
          : `Coverage threshold reached. Uncertainty ${Math.round(remainingUncertainty * 100)}% — remaining investigations provide negligible value.`,
    };
  }

  if (opts.missionEvidenceSatisfied && openCount === 0) {
    return {
      stop: true,
      reason: COMPLETION_REASONS.COVERAGE_COMPLETE,
      explanation: 'Mission evidence requirements satisfied.',
    };
  }

  if (opts.operatorPause) {
    return {
      stop: true,
      reason: COMPLETION_REASONS.BLOCKED,
      explanation: 'Operator policy requires investigation pause.',
    };
  }

  return { stop: false, reason: null, explanation: null };
}

/**
 * Build investigative strategy from current understanding state.
 * @param {object} input
 * @returns {object}
 */
function buildInvestigativeStrategy(input = {}) {
  const state = input.state || {};
  const board = input.board || null;
  const judgmentResult = input.judgmentResult || state.businessJudgment || null;
  const activatedHeuristics = judgmentResult?.activatedHeuristics || [];
  const heuristicBoosts = getHeuristicBoosts(activatedHeuristics);

  const knowns = extractKnownsFromState(state, board);
  const unknowns = extractUnknownsFromState(state, board, judgmentResult);
  const assumptions = extractAssumptionsFromState(state);
  const hypotheses = (state.activeHypotheses || []).map((h) => ({
    id: h.id,
    text: h.text,
    lifecycle: h.lifecycle,
    confidence: h.confidence,
  }));

  const registry = input.registry || createDefaultProviderRegistry();
  const learning =
    input.learning ||
    (input.memory ? loadLearningFromMemory(input.memory) : createProviderLearningStore());

  const candidateInvestigations = [];
  for (const unknown of unknowns) {
    candidateInvestigations.push(
      ...buildCandidateInvestigationsForUnknown(unknown, {
        registry,
        learning,
        heuristicBoosts,
        adaptivePlanning: input.adaptivePlanning !== false,
      })
    );
  }

  candidateInvestigations.sort((a, b) => b.expectedInformationGain - a.expectedInformationGain);

  const investigationQueue = buildInvestigationQueue(unknowns, candidateInvestigations, heuristicBoosts);
  const selectedInvestigation = candidateInvestigations[0] || null;

  const completedInvestigations = extractCompletedInvestigations(state, input.completedInvestigations);
  const informationGainHistory = buildInformationGainHistory(state, candidateInvestigations);

  const strategy = buildInvestigationStrategy({
    knowns,
    unknowns,
    assumptions,
    hypotheses,
    candidateInvestigations,
    investigationQueue,
    selectedInvestigation,
    expectedInformationGain: selectedInvestigation?.expectedInformationGain ?? null,
    completedInvestigations,
    informationGainHistory,
    activatedHeuristics: activatedHeuristics.map((h) => ({
      id: h.heuristicId,
      name: h.name,
      score: h.score,
    })),
    reasoning: selectedInvestigation
      ? `Selected ${selectedInvestigation.sourceLabel} to resolve "${selectedInvestigation.gap}" — highest expected information gain (+${selectedInvestigation.expectedInformationGain.toFixed(2)}).`
      : 'No candidate investigations remain.',
    stoppingCondition: null,
  });

  strategy.stoppingCondition = evaluateStrategyStoppingConditions(strategy, state, input.opts || {});

  return strategy;
}

/**
 * Recalculate strategy after an unknown is resolved (SPEC-163 Scenario 2).
 * @param {object} input
 * @returns {object}
 */
function recalculateStrategyAfterResolution(input = {}) {
  const state = input.state || {};
  const resolvedGap = input.resolvedGap || input.gap;
  const resolvedLabel = input.resolvedLabel || resolvedGap;

  const nextState = {
    ...state,
    uncertainty: {
      ...state.uncertainty,
      open: (state.uncertainty?.open || []).filter((u) => {
        const gap = typeof u === 'object' ? u.gap : inferGapFromUnknown({ text: u });
        return gap !== resolvedGap;
      }),
      resolved: [...new Set([...(state.uncertainty?.resolved || []), resolvedLabel])],
    },
  };

  const strategy = buildInvestigativeStrategy({
    ...input,
    state: nextState,
  });

  strategy.recalculatedAt = new Date().toISOString();
  strategy.reasoning = `Resolved "${resolvedLabel}". ${strategy.reasoning}`;

  return { state: nextState, strategy };
}

function strategyToNextQuestions(strategy = {}) {
  const questions = [];

  for (const entry of strategy.investigationQueue.slice(0, 8)) {
    questions.push({
      question: entry.unknown,
      gap: entry.gap,
      priority: entry.priority === 1 ? 'high' : entry.priority <= 3 ? 'medium' : 'low',
      source: 'investigative_strategy',
      expectedInformationGain: entry.expectedGain,
      recommendedSource: entry.selectedSource,
      recommendedSourceId: entry.source,
      investigationId: entry.investigationId,
      rationale: entry.reason,
      documentedGain: true,
    });
  }

  if (strategy.selectedInvestigation && !questions.length) {
    const sel = strategy.selectedInvestigation;
    questions.push({
      question: sel.objective,
      gap: sel.gap,
      priority: 'high',
      source: 'investigative_strategy',
      expectedInformationGain: sel.expectedInformationGain,
      recommendedSource: sel.sourceLabel,
      recommendedSourceId: sel.source,
      investigationId: sel.id,
      rationale: sel.reasoning,
      documentedGain: sel.documentedGain === true,
    });
  }

  return questions;
}

function applyInvestigativeStrategy(state, strategy) {
  return {
    ...state,
    investigativeStrategy: strategy,
    nextQuestions: strategyToNextQuestions(strategy),
    phase: 'investigate',
    updatedAt: new Date().toISOString(),
  };
}

/**
 * Explain why a specific investigation was chosen (SPEC-163 Scenario 4).
 * @param {object} strategy
 * @param {string} sourceOrId — source key, provider id, or investigation id
 * @returns {object}
 */
function explainInvestigationChoice(strategy = {}, sourceOrId = null) {
  const target =
    strategy.candidateInvestigations.find(
      (c) => c.id === sourceOrId || c.source === sourceOrId || c.providerId === sourceOrId
    ) ||
    strategy.selectedInvestigation ||
    strategy.candidateInvestigations[0];

  if (!target) {
    return {
      currentUncertainty: 'No open uncertainties requiring investigation.',
      expectedGain: 0,
      explanation: 'No investigations recommended.',
    };
  }

  const unknown = strategy.unknowns.find((u) => u.gap === target.gap);
  const alternatives = strategy.candidateInvestigations
    .filter((c) => c.gap === target.gap && c.id !== target.id)
    .slice(0, 4)
    .map((c) => ({
      source: c.sourceLabel,
      expectedInformationGain: c.expectedInformationGain,
    }));

  const higherRemaining = strategy.candidateInvestigations.filter(
    (c) => c.expectedInformationGain > target.expectedInformationGain
  );

  return {
    currentUncertainty: unknown
      ? `${unknown.label || unknown.gap} unknown.`
      : `${target.gap} unresolved.`,
    selectedInvestigation: {
      source: target.sourceLabel,
      sourceId: target.source,
      gap: target.gap,
      objective: target.objective,
    },
    expectedGain: target.expectedInformationGain,
    expectedConfidenceIncrease: target.expectedConfidenceIncrease,
    reasoning: target.reasoning,
    heuristicInfluence: target.heuristicReasons?.length ? target.heuristicReasons : null,
    alternativesOnSameUnknown: alternatives,
    higherThanRemainingInvestigations: higherRemaining.length === 0,
    explanation: [
      `Current uncertainty: ${unknown?.label || target.gap} unknown.`,
      `Expected gain: +${target.expectedInformationGain.toFixed(2)} confidence.`,
      higherRemaining.length === 0
        ? 'Higher than all remaining investigations.'
        : `${higherRemaining.length} investigation(s) rank higher globally.`,
    ].join(' '),
    adr: 'ADR-083',
    spec: 'SPEC-163',
  };
}

function buildInvestigativeStrategyReport(strategy = {}, stop = null) {
  return {
    spec: 'SPEC-163',
    adr: 'ADR-083',
    knowns: strategy.knowns,
    remainingUnknowns: strategy.unknowns.map((u) => ({
      gap: u.gap,
      label: u.label || u.question,
      impact: u.impact,
      difficulty: u.difficulty,
      expectedValue: u.expectedValue,
    })),
    completedInvestigations: strategy.completedInvestigations,
    investigationQueue: strategy.investigationQueue,
    candidateInvestigations: strategy.candidateInvestigations.slice(0, 15).map((c) => ({
      source: c.sourceLabel,
      gap: c.gap,
      expectedInformationGain: c.expectedInformationGain,
      expectedConfidenceIncrease: c.expectedConfidenceIncrease,
      reasoning: c.reasoning,
      documentedGain: c.documentedGain === true,
    })),
    informationGainHistory: strategy.informationGainHistory,
    selectedInvestigation: strategy.selectedInvestigation
      ? {
          source: strategy.selectedInvestigation.sourceLabel,
          gap: strategy.selectedInvestigation.gap,
          expectedInformationGain: strategy.selectedInvestigation.expectedInformationGain,
          reasoning: strategy.selectedInvestigation.reasoning,
        }
      : null,
    whyInvestigationStopped: stop?.explanation || strategy.stoppingCondition?.explanation || null,
    stopCondition: stop || strategy.stoppingCondition,
    recommendedNextInvestigation: strategy.selectedInvestigation
      ? {
          action: 'investigate',
          source: strategy.selectedInvestigation.sourceLabel,
          sourceId: strategy.selectedInvestigation.source,
          gap: strategy.selectedInvestigation.gap,
          question: strategy.selectedInvestigation.objective,
          expectedInformationGain: strategy.selectedInvestigation.expectedInformationGain,
          rationale: strategy.selectedInvestigation.reasoning,
          documentedGain: true,
        }
      : {
          action: 'none',
          reason: strategy.stoppingCondition?.explanation || 'No higher-value investigations remain.',
        },
    activatedHeuristicInfluence: strategy.activatedHeuristics,
    reasoning: strategy.reasoning,
    everyInvestigationDocumented: strategy.candidateInvestigations.every(
      (c) => c.documentedGain === true
    ),
  };
}

module.exports = {
  HEURISTIC_INVESTIGATION_PRIORITIES,
  INVESTIGATION_SOURCE_CATALOG,
  DEFAULT_MIN_EXPECTED_GAIN,
  DEFAULT_UNCERTAINTY_THRESHOLD,
  buildInvestigationOption,
  buildInvestigationStrategy,
  buildInvestigativeStrategy,
  recalculateStrategyAfterResolution,
  applyInvestigativeStrategy,
  strategyToNextQuestions,
  explainInvestigationChoice,
  evaluateStrategyStoppingConditions,
  buildInvestigativeStrategyReport,
  extractKnownsFromState,
  extractUnknownsFromState,
  buildCandidateInvestigationsForUnknown,
  getHeuristicBoosts,
  inferGapFromUnknown,
  normalizeUnknown,
};
