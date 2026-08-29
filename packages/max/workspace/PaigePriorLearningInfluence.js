'use strict';

/**
 * Advisory prior OutcomeLearning influence for Paige messaging (AUDIT-079).
 *
 * Historical messaging learning informs VARIANTS output when grounded in current
 * mission context. Max strategy remains authoritative. autoApplied is always false.
 */

const { LEARNING_OBJECT_KINDS } = require('../../acquisition-mission/OutcomeLearning');
const { asText } = require('../../acquisition-mission/types');

const COMMUNICATION_KEYWORDS =
  /messaging|message|subject|hook|cta|copy|tone|communication|email copy|outreach copy/i;
const DELIVERABILITY_PATTERN =
  /deliverability|inbox health|bounce rate|spam|warmup|send capacity|dmarc|spf|sender reputation/i;
const MAX_OWNED_STRATEGY_PATTERN =
  /geography expansion|market timing|prioritization|icp score|segment expansion|portfolio timing/i;

const PAIGE_ALLOWED_KINDS = Object.freeze(new Set([
  LEARNING_OBJECT_KINDS.MESSAGING,
  LEARNING_OBJECT_KINDS.STRATEGY,
]));

const PAIGE_MESSAGING_TOPIC_PATTERNS = Object.freeze([
  {
    id: 'generic_subject',
    pattern: /generic.*subject|subject line.*generic|generic.*underperform|vague subject|broad subject line/i,
    segmentMatchers: [/law firm/i, /accounting/i, /legal/i, /cpa/i, /professional/i],
    influenceKind: 'subject_specificity',
    polarity: 'negative',
  },
  {
    id: 'specific_subject',
    pattern: /company[- ]specific subject|personalized subject|specific subject|name in subject/i,
    segmentMatchers: [/law firm/i, /accounting/i, /legal/i, /professional/i],
    influenceKind: 'subject_hypothesis',
    polarity: 'positive',
  },
  {
    id: 'tone_mismatch',
    pattern: /tone|messaging tone|voice mismatch|too formal|too casual|tone adjustment/i,
    segmentMatchers: [/law firm/i, /owner/i, /professional/i],
    influenceKind: 'tone_adjustment',
    polarity: 'negative',
  },
  {
    id: 'hook_underperformance',
    pattern: /hook.*underperform|opening line|first sentence|weak hook|intro.*underperform/i,
    influenceKind: 'body_angle',
    polarity: 'negative',
  },
  {
    id: 'cta_framing',
    pattern: /cta|call.to.action|walkthrough.*underperform|reply rate|scheduling language/i,
    influenceKind: 'cta_framing',
    polarity: 'negative',
  },
]);

function paigeAllowsLearningKind(kind, learning = {}) {
  const normalized = asText(kind).toLowerCase();
  if (!normalized || !PAIGE_ALLOWED_KINDS.has(normalized)) return false;

  const text = [
    learning.statement,
    learning.subject,
    learning.primaryCause,
  ].filter(Boolean).join(' ');

  if (DELIVERABILITY_PATTERN.test(text)) return false;

  if (normalized === LEARNING_OBJECT_KINDS.MESSAGING) return true;

  if (normalized === LEARNING_OBJECT_KINDS.STRATEGY) {
    if (MAX_OWNED_STRATEGY_PATTERN.test(text) && !COMMUNICATION_KEYWORDS.test(text)) {
      return false;
    }
    return COMMUNICATION_KEYWORDS.test(text);
  }

  return false;
}

function isPositiveLearning(learning = {}) {
  const direction = asText(learning.direction).toLowerCase();
  if (direction === 'validated' || direction === 'strengthened') return true;
  if (direction === 'needs_review' || direction === 'weakened') return false;
  const statement = asText(learning.statement);
  return /perform|outperform|improved|lift|increase|worked|effective/i.test(statement)
    && !/underperform|poor|weak|generic.*under/i.test(statement);
}

function isNegativeLearning(learning = {}) {
  return !isPositiveLearning(learning);
}

function matchPaigeMessagingTopic(learning = {}) {
  const text = [learning.statement, learning.subject].filter(Boolean).join(' ');
  if (!text) return null;
  for (const topic of PAIGE_MESSAGING_TOPIC_PATTERNS) {
    if (topic.pattern.test(text)) return topic;
  }
  return null;
}

function buildPaigeMessagingContext(input = {}) {
  const max = input.max || {};
  const scout = input.scout || {};
  const plan = input.plan || {};

  const segment = asText(plan.market?.segment || plan.market?.label || plan.market?.vertical);
  const topTarget = max.rankedTargets?.[0]?.name
    || max.priorities?.[0]?.name
    || scout.companies?.[0]?.name
    || scout.rankedProspects?.[0]?.name
    || null;
  const objective = asText(max.objectives?.[0]?.text || plan.objective);
  const maxRecommendations = (max.recommendations || []).map((row) => asText(row)).filter(Boolean);
  const buyingSignals = (scout.buyingSignals || []).map((signal) =>
    (typeof signal === 'string' ? signal : asText(signal.label))
  ).filter(Boolean);

  const evidenceRefs = [];
  if (segment) {
    evidenceRefs.push({ id: 'ctx_segment', label: segment, kind: 'segment' });
  }
  if (topTarget) {
    evidenceRefs.push({ id: 'ctx_target', label: topTarget, kind: 'target_company' });
  }
  if (objective) {
    evidenceRefs.push({ id: 'ctx_objective', label: objective, kind: 'mission_objective' });
  }
  for (const rec of maxRecommendations.slice(0, 3)) {
    evidenceRefs.push({
      id: `ctx_max_rec_${evidenceRefs.length}`,
      label: rec,
      kind: 'max_recommendation',
    });
  }
  for (const signal of buyingSignals.slice(0, 3)) {
    evidenceRefs.push({
      id: `ctx_signal_${evidenceRefs.length}`,
      label: signal,
      kind: 'buying_signal',
    });
  }

  return {
    segment,
    topTarget,
    objective,
    maxRecommendations,
    buyingSignals,
    channel: asText(input.channel) || 'email',
    evidenceRefs,
    anchorTexts: [segment, topTarget, objective, ...maxRecommendations, ...buyingSignals].filter(Boolean),
  };
}

function segmentTextMatches(segment, pattern) {
  const normalized = asText(segment).replace(/_/g, ' ');
  return pattern.test(normalized) || pattern.test(asText(segment));
}

function normalizeSegment(value) {
  return asText(value).toLowerCase().replace(/[\s_-]+/g, '_');
}

function segmentsMatch(a, b) {
  const left = normalizeSegment(a);
  const right = normalizeSegment(b);
  if (!left || !right) return false;
  if (left === right) return true;
  return left.includes(right) || right.includes(left);
}

const LEARNING_SEGMENT_MENTIONS = Object.freeze([
  { pattern: /law firm|legal/i, segment: 'law_firm' },
  { pattern: /restaurant/i, segment: 'restaurant' },
  { pattern: /accounting|cpa/i, segment: 'accounting' },
  { pattern: /salon/i, segment: 'salon' },
  { pattern: /fitness|gym/i, segment: 'fitness' },
]);

function learningSegmentAlignsWithContext(learning = {}, context = {}) {
  const text = asText(learning.statement);
  const current = normalizeSegment(context.segment);
  if (!current) return true;

  let mentioned = null;
  for (const row of LEARNING_SEGMENT_MENTIONS) {
    if (!row.pattern.test(text)) continue;
    if (mentioned && mentioned !== row.segment) return false;
    mentioned = row.segment;
  }

  if (!mentioned) return true;
  return segmentsMatch(mentioned, current);
}

function findMatchingCurrentContext(topic, context = {}, learning = {}) {
  const learningText = asText(learning.statement);
  const refs = [];

  if (!learningSegmentAlignsWithContext(learning, context)) {
    return { matched: false, refs: [], matchCount: 0 };
  }

  if (topic.segmentMatchers) {
    const segmentAligned = topic.segmentMatchers.some((matcher) =>
      segmentTextMatches(context.segment, matcher)
      || matcher.test(learningText)
    );
    if (!segmentAligned) {
      return { matched: false, refs: [], matchCount: 0 };
    }
  }

  const hasSegmentAnchor = Boolean(context.segment) && (
    learningText.toLowerCase().includes(context.segment.toLowerCase().replace(/_/g, ' '))
    || learningText.toLowerCase().includes(context.segment.toLowerCase())
    || (topic.segmentMatchers && topic.segmentMatchers.some((matcher) =>
      segmentTextMatches(context.segment, matcher)
    ))
  );
  const hasTargetAnchor = Boolean(context.topTarget) && (
    /company|specific|firm|office|target/i.test(learningText)
    || topic.id === 'generic_subject'
    || topic.id === 'specific_subject'
  );
  const hasObjectiveAnchor = Boolean(context.objective);
  const hasMaxAnchor = context.maxRecommendations.length > 0;
  const hasSignalAnchor = context.buyingSignals.length > 0;

  const grounded = hasSegmentAnchor
    || (hasTargetAnchor && (hasObjectiveAnchor || hasMaxAnchor || hasSignalAnchor))
    || (hasObjectiveAnchor && hasSegmentAnchor);

  if (!grounded) {
    return { matched: false, refs: [], matchCount: 0 };
  }

  if (hasSegmentAnchor) {
    refs.push(...context.evidenceRefs.filter((row) => row.kind === 'segment'));
  }
  if (hasTargetAnchor) {
    refs.push(...context.evidenceRefs.filter((row) => row.kind === 'target_company'));
  }
  if (hasObjectiveAnchor) {
    refs.push(...context.evidenceRefs.filter((row) => row.kind === 'mission_objective'));
  }
  if (hasMaxAnchor) {
    refs.push(...context.evidenceRefs.filter((row) => row.kind === 'max_recommendation'));
  }
  if (hasSignalAnchor) {
    refs.push(...context.evidenceRefs.filter((row) => row.kind === 'buying_signal'));
  }

  const uniqueRefs = [];
  const seen = new Set();
  for (const row of refs.length ? refs : context.evidenceRefs.slice(0, 4)) {
    const key = row.id || row.label;
    if (seen.has(key)) continue;
    seen.add(key);
    uniqueRefs.push(row);
  }

  return {
    matched: true,
    refs: uniqueRefs,
    matchCount: uniqueRefs.length,
  };
}

function detectMaxConflict(topic, context = {}, max = {}, learning = {}) {
  const maxText = [
    context.objective,
    ...(context.maxRecommendations || []),
    ...((max.constraints || []).map((row) => (typeof row === 'string' ? row : row.label || row.text))),
  ].filter(Boolean).join(' ');

  if (topic.id === 'generic_subject' && isPositiveLearning(learning)) {
    if (/specific|personaliz|company|named target/i.test(maxText)) return true;
  }

  if (topic.polarity === 'positive' && isNegativeLearning(learning)) {
    if (/prioritize|first outreach|act on buying signal/i.test(maxText)) return true;
  }

  return false;
}

function buildReasonUsed(topic, currentContext, learning, messagingContext = {}, { conflict = false } = {}) {
  if (conflict) {
    return `Historical ${topic.influenceKind} noted (${asText(learning.statement)}); current Max strategy remains authoritative — messaging adjusts within Max constraints.`;
  }
  return `Used to inform ${topic.influenceKind} for current ${messagingContext.segment || 'mission'} context based on ${currentContext.matchCount} grounded anchor(s).`;
}

function buildMessagingAdjustments(topic, learning, context, { conflict = false } = {}) {
  const adjustments = {
    hypothesesAdd: [],
    experimentsAdd: [],
    subjectOverride: null,
    ctaOverride: null,
    bodyAngleNote: null,
  };

  const target = context.topTarget || 'your office';
  const segmentLabel = asText(context.segment).replace(/_/g, ' ') || 'target buyers';
  const positive = isPositiveLearning(learning);
  const negative = isNegativeLearning(learning) || topic.polarity === 'negative';

  if (topic.id === 'generic_subject') {
    if (negative || !positive) {
      adjustments.subjectOverride = `Workspace walkthrough for ${target}`;
      adjustments.hypothesesAdd.push(
        `Company-specific subject framing should outperform generic office-cleaning templates for ${segmentLabel}.`
      );
      adjustments.experimentsAdd.push({
        name: 'subject_specificity',
        variant: 'firm_name_and_service_context',
        hypothesis: 'Subject lines naming the firm and service context lift opens for professional-services buyers.',
      });
      if (conflict) {
        adjustments.experimentsAdd.push({
          name: 'max_strategy_alignment',
          variant: 'advisory_conflict_test',
          hypothesis: 'Test messaging variant aligned with current Max objective while noting historical generic-subject caution.',
        });
      }
    } else if (positive) {
      adjustments.hypothesesAdd.push(
        `Reinforce company-specific subject hypothesis for ${segmentLabel} outreach.`
      );
    }
  }

  if (topic.id === 'specific_subject' && positive) {
    adjustments.hypothesesAdd.push(
      `Validated prior learning supports personalized subject lines for ${segmentLabel} when a named target is available.`
    );
    adjustments.experimentsAdd.push({
      name: 'subject_personalization',
      variant: 'company_name_in_subject',
      hypothesis: 'Company-specific subject lines increase open rates for prioritized targets.',
    });
  }

  if (topic.id === 'tone_mismatch' && negative) {
    adjustments.bodyAngleNote = 'operator_voice';
    adjustments.hypothesesAdd.push(
      `Adjust tone toward operator voice for ${segmentLabel}; prior tone mismatch underperformed.`
    );
    adjustments.experimentsAdd.push({
      name: 'tone_operator_voice',
      variant: 'direct_operator_framing',
      hypothesis: 'Direct operator voice outperforms mismatched formal tone for local professional-services buyers.',
    });
  }

  if (topic.id === 'hook_underperformance' && negative) {
    adjustments.bodyAngleNote = 'lead_with_pain_point';
    adjustments.hypothesesAdd.push(
      'Lead with a concrete workspace pain point instead of a generic service introduction.'
    );
    adjustments.experimentsAdd.push({
      name: 'opening_angle',
      variant: 'pain_point_first',
      hypothesis: 'Pain-point-first openings outperform generic intros for cold outreach.',
    });
  }

  if (topic.id === 'cta_framing' && negative) {
    adjustments.ctaOverride = 'Reply with a good time for a brief walkthrough';
    adjustments.hypothesesAdd.push(
      'Low-friction scheduling language should outperform prior CTA framing.'
    );
    adjustments.experimentsAdd.push({
      name: 'cta_framing',
      variant: 'scheduling_flexibility',
      hypothesis: 'Flexible scheduling CTA increases reply rates versus rigid walkthrough asks.',
    });
  }

  if (negative && !adjustments.experimentsAdd.length) {
    adjustments.experimentsAdd.push({
      name: `avoid_${topic.id}`,
      variant: 'reframe_against_prior_weakness',
      hypothesis: `Experiment against prior weak ${topic.influenceKind} approach for ${segmentLabel}.`,
    });
  }

  return adjustments;
}

/**
 * Evaluate whether advisory prior learning materially influences Paige messaging.
 * Influence requires BOTH relevant learning AND matching current mission context.
 */
function evaluatePaigePriorLearningInfluence(input = {}) {
  const priorLearning = Array.isArray(input.priorLearning) ? input.priorLearning : [];
  const context = buildPaigeMessagingContext(input);
  const max = input.max || {};

  const learningInfluence = [];
  const adjustments = {
    hypothesesAdd: [],
    experimentsAdd: [],
    subjectOverride: null,
    ctaOverride: null,
    bodyAngleNote: null,
  };

  for (const learning of priorLearning) {
    if (learning.autoApplied === true) continue;
    if (!paigeAllowsLearningKind(learning.kind, learning)) continue;

    const topic = matchPaigeMessagingTopic(learning);
    if (!topic) continue;

    const currentContext = findMatchingCurrentContext(topic, context, learning);
    if (!currentContext.matched || currentContext.refs.length <= 0) continue;

    const conflict = detectMaxConflict(topic, context, max, learning);
    const topicAdjustments = buildMessagingAdjustments(topic, learning, context, { conflict });

    if (topicAdjustments.subjectOverride && !adjustments.subjectOverride) {
      adjustments.subjectOverride = topicAdjustments.subjectOverride;
    }
    if (topicAdjustments.ctaOverride && !adjustments.ctaOverride) {
      adjustments.ctaOverride = topicAdjustments.ctaOverride;
    }
    if (topicAdjustments.bodyAngleNote && !adjustments.bodyAngleNote) {
      adjustments.bodyAngleNote = topicAdjustments.bodyAngleNote;
    }
    adjustments.hypothesesAdd.push(...(topicAdjustments.hypothesesAdd || []));
    adjustments.experimentsAdd.push(...(topicAdjustments.experimentsAdd || []));

    learningInfluence.push({
      learningId: learning.id,
      sourceMissionId: learning.sourceMissionId,
      evaluationId: learning.evaluationId || null,
      kind: topic.influenceKind,
      direction: learning.direction || null,
      reasonUsed: buildReasonUsed(topic, currentContext, learning, context, { conflict }),
      currentEvidenceRefs: currentContext.refs.slice(0, 8),
      advisoryOnly: true,
      autoApplied: false,
    });
  }

  return { learningInfluence, adjustments, context };
}

function applyBodyAngleNote(body, note, plan = {}) {
  const segmentLabel = asText(plan.market?.label || plan.market?.segment).replace(/_/g, ' ')
    || 'local offices';
  if (note === 'lead_with_pain_point') {
    return [
      `Many ${segmentLabel} struggle with inconsistent cleaning between high-traffic client areas.`,
      body,
    ].filter(Boolean).join('\n\n');
  }
  if (note === 'operator_voice') {
    return body.replace(
      /^Hi — we help/i,
      'Hi — I help'
    );
  }
  return body;
}

function applyPaigePriorLearningAdjustments(payload = {}, evaluation = {}, plan = {}) {
  const { learningInfluence = [], adjustments = {} } = evaluation;
  if (!learningInfluence.length) return payload;

  const result = {
    ...payload,
    variants: (payload.variants || []).map((row) => ({ ...row })),
    subjects: [...(payload.subjects || [])],
    hypotheses: [...(payload.hypotheses || [])],
    experiments: [...(payload.experiments || [])],
  };

  if (adjustments.subjectOverride && result.variants[0]) {
    result.variants[0].subject = adjustments.subjectOverride;
    result.subjects = [adjustments.subjectOverride, ...result.subjects.slice(1)];
  }

  if (adjustments.ctaOverride && result.variants[0]) {
    result.variants[0].cta = adjustments.ctaOverride;
    result.cta = adjustments.ctaOverride;
  }

  if (adjustments.bodyAngleNote && result.variants[0]) {
    result.variants[0].body = applyBodyAngleNote(
      result.variants[0].body,
      adjustments.bodyAngleNote,
      plan
    );
    result.messaging = result.variants[0].body;
  }

  if (adjustments.hypothesesAdd?.length) {
    result.hypotheses = [...result.hypotheses, ...adjustments.hypothesesAdd];
  }

  if (adjustments.experimentsAdd?.length) {
    const existingNames = new Set((result.experiments || []).map((row) => row.name));
    for (const experiment of adjustments.experimentsAdd) {
      if (existingNames.has(experiment.name)) continue;
      result.experiments.push(experiment);
      existingNames.add(experiment.name);
    }
  }

  return result;
}

module.exports = {
  PAIGE_ALLOWED_KINDS,
  PAIGE_MESSAGING_TOPIC_PATTERNS,
  paigeAllowsLearningKind,
  evaluatePaigePriorLearningInfluence,
  applyPaigePriorLearningAdjustments,
  buildPaigeMessagingContext,
  matchPaigeMessagingTopic,
  findMatchingCurrentContext,
};
