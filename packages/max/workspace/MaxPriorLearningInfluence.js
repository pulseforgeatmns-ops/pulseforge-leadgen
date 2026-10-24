'use strict';

/**
 * Advisory prior OutcomeLearning influence for Max prioritization (AUDIT-078).
 *
 * Historical learning informs business judgment but never replaces current Scout evidence.
 * autoApplied is always false — no configuration or heuristic library mutation occurs here.
 */

const { LEARNING_OBJECT_KINDS } = require('../../acquisition-mission/OutcomeLearning');
const { asText } = require('../../acquisition-mission/types');

const MAX_ALLOWED_KINDS = Object.freeze(new Set([
  LEARNING_OBJECT_KINDS.STRATEGY,
  LEARNING_OBJECT_KINDS.HEURISTIC,
  LEARNING_OBJECT_KINDS.MARKET_UNDERSTANDING,
  LEARNING_OBJECT_KINDS.OPPORTUNITY_RULE,
  LEARNING_OBJECT_KINDS.ORGANIZATIONAL,
]));

const CONFIDENCE_BOOST = 0.06;
const STRONG_EVIDENCE_FIT = 0.7;
const STRONG_EVIDENCE_CONFIDENCE = 0.7;

const MAX_LEARNING_TOPIC_PATTERNS = Object.freeze([
  {
    id: 'operations_hiring',
    pattern: /office[- ]?manager|operations.*hiring|hiring.*operations|operations manager/i,
    signalMatchers: [/hiring/i, /operations/i, /office manager/i, /coordinator/i],
    influenceKind: 'priority_confidence',
    description: 'operations-hiring signals',
    polarity: 'positive',
  },
  {
    id: 'vendor_instability',
    pattern: /vendor.*instability|vendor.*change|cleaning vendor|vendor switch/i,
    signalMatchers: [/vendor/i, /cleaning/i, /review/i, /complaint/i],
    influenceKind: 'strategic_constraint',
    description: 'vendor-instability signals',
    polarity: 'positive',
  },
  {
    id: 'expansion_growth',
    pattern: /expansion|growth|portfolio|new location|hiring/i,
    signalMatchers: [/expansion/i, /growth/i, /hiring/i, /new office/i],
    influenceKind: 'timing_caution',
    description: 'expansion or growth signals',
    polarity: 'positive',
  },
  {
    id: 'vertical_underperformance',
    pattern: /performed poorly|underperform|low conversion|weak conversion|poor results|underperformed/i,
    segmentMatchers: [/law firm/i, /accounting/i, /restaurant/i, /salon/i, /fitness/i],
    influenceKind: 'known_risk_warning',
    description: 'historical segment underperformance',
    polarity: 'negative',
  },
]);

function maxAllowsLearningKind(kind) {
  const normalized = asText(kind).toLowerCase();
  if (!normalized) return false;
  return MAX_ALLOWED_KINDS.has(normalized);
}

function isNegativeLearning(learning = {}) {
  const direction = asText(learning.direction).toLowerCase();
  if (direction === 'weakened' || direction === 'needs_review') return true;
  const statement = asText(learning.statement);
  return /performed poorly|underperform|low conversion|weak conversion|poor results|underperformed/i.test(statement);
}

function matchMaxLearningTopic(learning = {}) {
  const text = [learning.statement, learning.subject].filter(Boolean).join(' ');
  if (!text) return null;
  for (const topic of MAX_LEARNING_TOPIC_PATTERNS) {
    if (topic.pattern.test(text)) return topic;
  }
  return null;
}

function collectProspectEvidenceTexts(prospect = {}) {
  const texts = [];
  texts.push(asText(prospect.name));
  texts.push(asText(prospect.rationale));
  if (prospect.intelligenceBrief && prospect.intelligenceBrief.summary) {
    texts.push(asText(prospect.intelligenceBrief.summary));
  }
  for (const signal of prospect.signals || []) {
    texts.push(asText(signal.label || signal.type));
    texts.push(asText(signal.type));
  }
  for (const row of prospect.evidenceRefs || prospect.evidence || []) {
    texts.push(asText(row.label || row.kind));
    texts.push(asText(row.id));
  }
  return texts.filter(Boolean);
}

function buildDiscoveryEvidenceContext(discovery = {}) {
  const ranked = Array.isArray(discovery.rankedProspects) ? discovery.rankedProspects : [];
  const opportunities = Array.isArray(discovery.opportunities) ? discovery.opportunities : [];
  const source = ranked.length ? ranked : opportunities;

  const prospects = source.map((row, index) => ({
    id: row.id || row.companyId || `prospect_${index + 1}`,
    companyId: row.companyId || row.id || null,
    name: row.name || null,
    fit: row.fit != null ? Number(row.fit) : null,
    confidence: row.confidence != null ? Number(row.confidence) : null,
    texts: collectProspectEvidenceTexts(row),
    evidenceRefs: (row.evidenceRefs || row.evidence || []).map((ref, refIndex) => ({
      id: ref.id || `${row.id || row.companyId || 'prospect'}_ev_${refIndex + 1}`,
      label: asText(ref.label || ref.kind),
      companyId: row.companyId || row.id || null,
      companyName: row.name || null,
    })),
  }));

  const globalEvidence = [];
  for (const row of discovery.evidence || []) {
    globalEvidence.push({
      id: row.id || asText(row.label),
      label: asText(row.label),
      source: asText(row.source),
    });
  }
  for (const signal of discovery.buyingSignals || []) {
    globalEvidence.push({
      id: asText(typeof signal === 'string' ? signal : signal.label),
      label: asText(typeof signal === 'string' ? signal : signal.label),
      source: asText(signal && signal.source),
    });
  }

  return { prospects, globalEvidence };
}

function prospectHasStrongEvidence(prospect = {}) {
  if (prospect.fit != null && prospect.fit >= STRONG_EVIDENCE_FIT) return true;
  if (prospect.confidence != null && prospect.confidence >= STRONG_EVIDENCE_CONFIDENCE) return true;
  const signalCount = (prospect.texts || []).filter((text) =>
    /hiring|vendor|expansion|growth|buying|verified|operations/i.test(text)
  ).length;
  return signalCount >= 2;
}

function prospectMatchesTopic(prospect = {}, topic = {}) {
  const texts = prospect.texts || [];
  if (topic.signalMatchers) {
    return texts.some((text) => topic.signalMatchers.some((matcher) => matcher.test(text)));
  }
  if (topic.segmentMatchers) {
    return texts.some((text) => topic.segmentMatchers.some((matcher) => matcher.test(text)));
  }
  return false;
}

function findMatchingCurrentEvidence(context = {}, topic = {}, learning = {}) {
  const refs = [];
  const matchedProspects = [];

  for (const prospect of context.prospects || []) {
    if (!prospectMatchesTopic(prospect, topic)) continue;
    matchedProspects.push(prospect);

    for (const ref of prospect.evidenceRefs || []) {
      if (ref.id || ref.label) refs.push({ ...ref, prospectId: prospect.id });
    }
    if (prospect.name) {
      refs.push({
        id: prospect.id,
        label: prospect.name,
        companyId: prospect.companyId,
        prospectId: prospect.id,
      });
    }
  }

  for (const row of context.globalEvidence || []) {
    const label = asText(row.label);
    if (!label) continue;
    const matchesTopic = topic.signalMatchers
      ? topic.signalMatchers.some((matcher) => matcher.test(label))
      : topic.segmentMatchers
        ? topic.segmentMatchers.some((matcher) => matcher.test(label))
        : false;
    if (matchesTopic) refs.push(row);
  }

  const strongEvidence = matchedProspects.some((prospect) => prospectHasStrongEvidence(prospect));

  return {
    refs,
    matchedProspects,
    strongEvidence,
    matchCount: matchedProspects.length,
  };
}

function buildReasonUsed(topic, currentEvidence, learning, { conflict = false } = {}) {
  if (conflict) {
    return `Historical ${topic.description} noted (${asText(learning.statement)}); current Scout evidence remains authoritative for ${currentEvidence.matchCount} prioritized prospect(s).`;
  }
  return `Used to strengthen prioritization rationale for ${topic.description} based on ${currentEvidence.matchCount} current prospect(s) with matching Scout evidence.`;
}

/**
 * Evaluate whether advisory prior learning materially influences Max prioritization.
 * Influence requires BOTH relevant learning AND matching current mission evidence.
 */
function evaluateMaxPriorLearningInfluence(input = {}) {
  const priorLearning = Array.isArray(input.priorLearning) ? input.priorLearning : [];
  const discovery = input.discovery || {};
  const evidenceContext = buildDiscoveryEvidenceContext(discovery);

  const learningInfluence = [];
  const adjustments = {
    priorityBoosts: new Map(),
    recommendations: [],
    constraints: [],
    objectiveReasonSuffix: '',
    timingCaution: '',
    verificationRecommendations: [],
  };

  for (const learning of priorLearning) {
    if (learning.autoApplied === true) continue;
    if (!maxAllowsLearningKind(learning.kind)) continue;

    const topic = matchMaxLearningTopic(learning);
    if (!topic) continue;

    const currentEvidence = findMatchingCurrentEvidence(evidenceContext, topic, learning);
    if (currentEvidence.refs.length <= 0 || currentEvidence.matchCount <= 0) continue;

    const negative = isNegativeLearning(learning) || topic.polarity === 'negative';

    if (negative) {
      if (!currentEvidence.strongEvidence) continue;

      const names = currentEvidence.matchedProspects.map((row) => row.name).filter(Boolean);
      const label = names[0] || 'prioritized prospect';
      adjustments.constraints.push(
        `Historical risk: ${topic.description} — monitor ${label} closely despite current buying signals.`
      );
      adjustments.recommendations.push(
        `Keep ${label} prioritized based on current Scout evidence; note historical ${topic.description} as an advisory risk.`
      );

      learningInfluence.push({
        learningId: learning.id,
        sourceMissionId: learning.sourceMissionId,
        evaluationId: learning.evaluationId || null,
        kind: topic.influenceKind,
        reasonUsed: buildReasonUsed(topic, currentEvidence, learning, { conflict: true }),
        currentEvidenceRefs: currentEvidence.refs.slice(0, 8),
        advisoryOnly: true,
        autoApplied: false,
      });
      continue;
    }

    for (const prospect of currentEvidence.matchedProspects) {
      const key = prospect.companyId || prospect.id || prospect.name;
      if (!key) continue;
      adjustments.priorityBoosts.set(key, {
        amount: CONFIDENCE_BOOST,
        rationale: `Prior OutcomeLearning supports ${topic.description} (advisory; current Scout evidence confirmed).`,
      });
    }

    adjustments.recommendations.push(
      `Increase strategic confidence for prospects showing ${topic.description} — historical learning aligns with current Scout evidence.`
    );
    if (topic.influenceKind === 'timing_caution') {
      adjustments.timingCaution = `Watch timing on ${topic.description}`;
    }
    if (topic.id === 'operations_hiring') {
      adjustments.verificationRecommendations.push(
        'Verify operations-hiring signals before first outreach wave.'
      );
    }

    learningInfluence.push({
      learningId: learning.id,
      sourceMissionId: learning.sourceMissionId,
      evaluationId: learning.evaluationId || null,
      kind: topic.influenceKind,
      reasonUsed: buildReasonUsed(topic, currentEvidence, learning),
      currentEvidenceRefs: currentEvidence.refs.slice(0, 8),
      advisoryOnly: true,
      autoApplied: false,
    });
  }

  if (adjustments.verificationRecommendations.length) {
    adjustments.recommendations.push(...adjustments.verificationRecommendations);
  }

  return { learningInfluence, adjustments };
}

function applyMaxPriorLearningAdjustments(prioritizationPayload = {}, evaluation = {}) {
  const { learningInfluence = [], adjustments = {} } = evaluation;
  if (!learningInfluence.length) return prioritizationPayload;

  const result = { ...prioritizationPayload };

  if (adjustments.priorityBoosts && adjustments.priorityBoosts.size) {
    result.priorities = (result.priorities || []).map((row) => {
      const key = row.companyId || row.id || row.name;
      const boost = adjustments.priorityBoosts.get(key);
      if (!boost) return row;

      const next = { ...row };
      if (next.confidence != null) {
        next.confidence = Number(Math.min(1, next.confidence + boost.amount).toFixed(3));
      }
      if (boost.rationale) {
        next.rationale = [next.rationale, boost.rationale].filter(Boolean).join(' ');
      }
      return next;
    });
  }

  if (adjustments.recommendations && adjustments.recommendations.length) {
    result.recommendations = [
      ...(result.recommendations || []),
      ...adjustments.recommendations,
    ];
  }

  if (adjustments.constraints && adjustments.constraints.length) {
    result.constraints = [
      ...(result.constraints || []),
      ...adjustments.constraints,
    ];
  }

  if (adjustments.timingCaution) {
    result.timing = `${result.timing} — ${adjustments.timingCaution}`;
  }

  if (adjustments.objectiveReasonSuffix) {
    result.objectiveReason = [result.objectiveReason, adjustments.objectiveReasonSuffix]
      .filter(Boolean)
      .join(' ');
  }

  return result;
}

module.exports = {
  MAX_ALLOWED_KINDS,
  MAX_LEARNING_TOPIC_PATTERNS,
  maxAllowsLearningKind,
  evaluateMaxPriorLearningInfluence,
  applyMaxPriorLearningAdjustments,
  buildDiscoveryEvidenceContext,
  findMatchingCurrentEvidence,
  matchMaxLearningTopic,
};
