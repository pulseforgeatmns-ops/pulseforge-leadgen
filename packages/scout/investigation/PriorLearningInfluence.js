'use strict';

/**
 * Advisory prior OutcomeLearning influence for Scout investigation (SPEC-077).
 *
 * Historical learning informs investigation but never replaces current mission evidence.
 * autoApplied is always false — no heuristic library mutation occurs here.
 */

const { asText } = require('../../max/scoutAcquisition/Types');

const LEARNING_TOPIC_PATTERNS = Object.freeze([
  {
    id: 'operations_hiring',
    pattern: /office[- ]?manager|operations.*hiring|hiring.*operations|operations manager/i,
    gaps: ['decision_maker', 'buying_signals'],
    signalMatchers: [/hiring/i, /operations/i, /office manager/i, /coordinator/i],
    description: 'operations-hiring signals',
  },
  {
    id: 'vendor_instability',
    pattern: /vendor.*instability|vendor.*change|cleaning vendor|vendor switch/i,
    gaps: ['current_vendor', 'vendor_relationship', 'cleaning_responsibility'],
    signalMatchers: [/vendor/i, /cleaning/i, /review/i, /complaint/i],
    description: 'vendor-instability signals',
  },
  {
    id: 'expansion_growth',
    pattern: /expansion|growth|portfolio|new location|hiring/i,
    gaps: ['portfolio_size', 'expansion_plans', 'buying_signals'],
    signalMatchers: [/expansion/i, /growth/i, /hiring/i, /new office/i],
    description: 'expansion or growth signals',
  },
]);

function mapSecPriorLearningToOutcomeLearnings(priorLearning = []) {
  if (!Array.isArray(priorLearning)) return [];
  return priorLearning.map((item) => ({
    id: item.id,
    kind: item.kind,
    statement: item.statement,
    sourceMissionId: item.sourceMissionId,
    evaluationId: item.evaluationId || null,
    predictionId: item.predictionId || null,
    evidence: Array.isArray(item.evidence) ? item.evidence.slice() : [],
    relevance: item.relevance || null,
    autoApplied: item.autoApplied === true,
    direction: item.direction || null,
    subject: item.subject || null,
    accuracy: item.accuracy || null,
    at: item.at || null,
    source: 'outcome_learning',
  }));
}

function collectCandidateSignalTexts(candidate = {}) {
  const texts = [];
  for (const signal of candidate.signals || []) {
    texts.push(asText(signal.label || signal.type));
    texts.push(asText(signal.type));
  }
  for (const row of candidate.evidence || []) {
    texts.push(asText(row.label || row.kind));
  }
  return texts.filter(Boolean);
}

function countCurrentEvidenceMatches(candidates = [], topic) {
  let matchCount = 0;
  const matchedCandidateIds = [];

  for (const candidate of candidates) {
    const texts = collectCandidateSignalTexts(candidate);
    const hasSignalMatch = texts.some((text) =>
      topic.signalMatchers.some((matcher) => matcher.test(text))
    );
    if (hasSignalMatch) {
      matchCount += 1;
      if (candidate.id) matchedCandidateIds.push(candidate.id);
    }
  }

  return { matchCount, matchedCandidateIds };
}

function matchLearningTopic(statement = '') {
  const text = asText(statement);
  if (!text) return null;
  for (const topic of LEARNING_TOPIC_PATTERNS) {
    if (topic.pattern.test(text)) return topic;
  }
  return null;
}

/**
 * Evaluate whether advisory prior learning materially influences investigation.
 * Influence requires BOTH relevant learning AND matching current mission evidence.
 */
function evaluatePriorLearningInfluence(input = {}) {
  const priorOutcomeLearnings = input.priorOutcomeLearnings || [];
  const candidates = input.candidates || [];
  const learningInfluence = [];
  const strategyAdjustments = [];

  for (const learning of priorOutcomeLearnings) {
    if (learning.autoApplied === true) continue;

    const topic = matchLearningTopic(learning.statement);
    if (!topic) continue;

    const currentEvidence = countCurrentEvidenceMatches(candidates, topic);
    if (currentEvidence.matchCount <= 0) continue;

    learningInfluence.push({
      learningId: learning.id,
      sourceMissionId: learning.sourceMissionId,
      evaluationId: learning.evaluationId || null,
      kind: learning.kind,
      reasonUsed: `Used to prioritize verification of ${topic.description} based on ${currentEvidence.matchCount} current candidate(s) with matching mission evidence.`,
      currentEvidenceCount: currentEvidence.matchCount,
      matchedCandidateIds: currentEvidence.matchedCandidateIds,
      advisoryOnly: true,
      autoApplied: false,
    });

    strategyAdjustments.push({
      learningId: learning.id,
      topicId: topic.id,
      gaps: topic.gaps.slice(),
      boost: 0.08,
      reason: learning.statement,
    });
  }

  return { learningInfluence, strategyAdjustments };
}

function applyPriorLearningToStrategy(strategy = {}, adjustments = []) {
  if (!adjustments.length) {
    return { strategy, applied: false };
  }

  const gapBoosts = new Map();
  for (const adj of adjustments) {
    for (const gap of adj.gaps || []) {
      gapBoosts.set(gap, (gapBoosts.get(gap) || 0) + (adj.boost || 0));
    }
  }

  const boostOption = (option) => {
    const gap = option.gap;
    const boost = gapBoosts.get(gap);
    if (!boost) return option;
    return {
      ...option,
      expectedInformationGain:
        option.expectedInformationGain != null
          ? Number((option.expectedInformationGain + boost).toFixed(3))
          : boost,
      heuristicBoost: (option.heuristicBoost || 0) + boost,
      heuristicReasons: [
        ...(option.heuristicReasons || []),
        'Prior OutcomeLearning advisory boost (current evidence supports historical pattern).',
      ],
      priorLearningBoost: true,
    };
  };

  const candidateInvestigations = (strategy.candidateInvestigations || []).map(boostOption);
  const investigationQueue = (strategy.investigationQueue || []).map(boostOption);

  let selectedInvestigation = strategy.selectedInvestigation;
  if (selectedInvestigation && gapBoosts.has(selectedInvestigation.gap)) {
    selectedInvestigation = boostOption(selectedInvestigation);
  } else if (candidateInvestigations.length) {
    const reranked = [...candidateInvestigations].sort(
      (a, b) => (b.expectedInformationGain || 0) - (a.expectedInformationGain || 0)
    );
    selectedInvestigation = reranked[0] || selectedInvestigation;
  }

  return {
    strategy: {
      ...strategy,
      candidateInvestigations,
      investigationQueue,
      selectedInvestigation,
      priorLearningApplied: true,
      reasoning: strategy.reasoning
        ? `${strategy.reasoning} Prior OutcomeLearning informed investigation priorities without skipping evidence collection.`
        : 'Prior OutcomeLearning informed investigation priorities without skipping evidence collection.',
    },
    applied: true,
  };
}

module.exports = {
  LEARNING_TOPIC_PATTERNS,
  mapSecPriorLearningToOutcomeLearnings,
  evaluatePriorLearningInfluence,
  applyPriorLearningToStrategy,
  matchLearningTopic,
  countCurrentEvidenceMatches,
};
