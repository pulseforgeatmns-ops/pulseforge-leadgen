'use strict';

/**
 * SPEC-166 — Outcome Learning Engine.
 * ADR-086 — Every decision must teach.
 *
 * Closes the loop: Decision → Execution → Outcome → Learning → Future Decisions.
 * Every recommendation becomes a prediction. Every completed outcome updates
 * Scout heuristics, Max strategy, Paige messaging, and organizational knowledge.
 *
 * Invariants:
 * - Never auto-apply learnings to live missions (ADR-055)
 * - Deterministic evaluation — no LLM confidence mutation (ADR-008)
 * - No recommendation disappears without eventual outcome resolution
 */

const { asText, nowIso, newId, round2, clone } = require('./types');
const { learnFromOutcome } = require('../scout/heuristics/BusinessHeuristicsEngine');
const { OUTCOME_KINDS } = require('../scout/heuristics/types');

const SPEC = 'SPEC-166';
const ADR = 'ADR-086';

const PREDICTION_STATUS = Object.freeze({
  PENDING: 'pending',
  RESOLVED: 'resolved',
  INCONCLUSIVE: 'inconclusive',
});

const ACCURACY_LABELS = Object.freeze({
  CORRECT: 'correct',
  INCORRECT: 'incorrect',
  PARTIAL: 'partial',
  INCONCLUSIVE: 'inconclusive',
});

const LEARNING_OBJECT_KINDS = Object.freeze({
  HEURISTIC: 'business_heuristic',
  OPPORTUNITY_RULE: 'opportunity_rule',
  MESSAGING: 'messaging',
  MARKET_UNDERSTANDING: 'market_understanding',
  STRATEGY: 'strategy',
  ORGANIZATIONAL: 'organizational',
});

/** Outcome types that resolve a prediction as positive (won). */
const POSITIVE_OUTCOME_TYPES = Object.freeze([
  'walkthrough_booked',
  'meeting_booked',
  'won',
  'closed_won',
  'reply',
  'interested',
]);

/** Outcome types that resolve a prediction as negative (lost). */
const NEGATIVE_OUTCOME_TYPES = Object.freeze([
  'lost',
  'closed_lost',
  'no_answer',
  'not_interested',
  'unsubscribe',
  'bounce',
  'wrong_person',
  'negative',
  'declined',
]);

/** Outcome types that are inconclusive for prediction accuracy. */
const INCONCLUSIVE_OUTCOME_TYPES = Object.freeze([
  'open',
  'sent',
  'queued',
  'out_of_office',
]);

function normalizeOutcomeType(type) {
  return asText(type).toLowerCase().replace(/\s+/g, '_');
}

function outcomeKindFromType(type) {
  const normalized = normalizeOutcomeType(type);
  if (POSITIVE_OUTCOME_TYPES.includes(normalized)) return OUTCOME_KINDS.WON;
  if (NEGATIVE_OUTCOME_TYPES.includes(normalized)) return OUTCOME_KINDS.LOST;
  return null;
}

function expectedOutcomeKind(expectedOutcome = {}) {
  const kind = asText(expectedOutcome.kind || expectedOutcome.outcome || expectedOutcome.label).toLowerCase();
  if (/walkthrough|meeting|booked|won|reply|positive|high/i.test(kind)) return OUTCOME_KINDS.WON;
  if (/lost|no.?answer|decline|negative|low/i.test(kind)) return OUTCOME_KINDS.LOST;
  const probability = Number(expectedOutcome.probability ?? expectedOutcome.confidence);
  if (Number.isFinite(probability) && probability >= 0.6) return OUTCOME_KINDS.WON;
  return null;
}

/**
 * Capture a prediction when PulseForge makes a recommendation.
 * Every recommendation becomes a measurable prediction.
 * @param {object} input
 * @returns {object}
 */
function capturePrediction(input = {}) {
  const recommendation = input.recommendation || {};
  const strategicDecision = input.strategicDecision || {};
  const opportunity = input.opportunity || input.topOpportunity || {};
  const expectedOutcome =
    input.expectedOutcome ||
    strategicDecision.expectedBusinessOutcome ||
    recommendation.expectedOutcome ||
    null;

  const confidence =
    input.confidence != null
      ? round2(Number(input.confidence))
      : round2(
          Number(
            recommendation.confidence ??
              strategicDecision.tradeoff?.confidencePercent ??
              opportunity.confidence ??
              0
          ) / (Number(recommendation.confidence) > 1 ? 100 : 1)
        );

  const probability =
    input.probability != null
      ? round2(Number(input.probability))
      : confidence;

  return {
    id: asText(input.id) || newId('pred'),
    spec: SPEC,
    adr: ADR,
    tenantId: asText(input.tenantId),
    missionId: asText(input.missionId),
    opportunityId: asText(input.opportunityId || opportunity.id || opportunity.entity?.id),
    opportunityName: asText(
      input.opportunityName || opportunity.entity?.name || opportunity.name || opportunity.entity
    ),
    recommendation: {
      kind: asText(recommendation.kind) || 'outreach',
      summary: asText(recommendation.summary || input.summary),
      confidence,
      probability,
      businessValue: asText(
        input.businessValue ||
          recommendation.businessValue ||
          opportunity.expectedBusinessValue?.level ||
          opportunity.category
      ),
      basedOnStrategicDecision: recommendation.basedOnStrategicDecision === true,
      basedOnUnderstanding: recommendation.basedOnUnderstanding === true,
    },
    expectedOutcome: expectedOutcome
      ? {
          kind: asText(expectedOutcome.kind || expectedOutcome.label || 'walkthrough'),
          label: asText(expectedOutcome.label || expectedOutcome.kind || 'Positive business outcome'),
          probability,
          businessValue: asText(expectedOutcome.businessValue || input.businessValue),
          arr: expectedOutcome.arr ?? expectedOutcome.expectedArr ?? null,
        }
      : {
          kind: 'walkthrough',
          label: asText(input.expectedLabel) || 'Walkthrough booked',
          probability,
          businessValue: asText(input.businessValue),
        },
    contributingHeuristicIds: (
      input.contributingHeuristicIds ||
      input.heuristicIds ||
      (input.judgmentResult?.activatedHeuristics || []).map((h) => h.id)
    ).filter(Boolean),
    operatorAction: null,
    status: PREDICTION_STATUS.PENDING,
    capturedAt: nowIso(input.at || input.now),
    payload: clone(input.payload || {}),
  };
}

/**
 * Record what the operator actually did in response to the recommendation.
 * @param {object} prediction
 * @param {object} input
 * @returns {object}
 */
function recordOperatorAction(prediction, input = {}) {
  const copy = clone(prediction);
  copy.operatorAction = {
    action: asText(input.action || input.operatorAction),
    taken: input.taken !== false,
    at: nowIso(input.at || input.now),
    notes: asText(input.notes),
    deferred: input.deferred === true,
  };
  return copy;
}

/**
 * Compare prediction with observed business outcome.
 * @param {object} prediction
 * @param {object} input
 * @returns {object}
 */
function evaluatePrediction(prediction, input = {}) {
  const actualType = normalizeOutcomeType(input.actualOutcome || input.type || input.outcomeType);
  const actualKind = outcomeKindFromType(actualType);
  const expectedKind = expectedOutcomeKind(prediction.expectedOutcome);

  let accuracy = ACCURACY_LABELS.INCONCLUSIVE;
  let correct = null;

  if (actualKind && expectedKind) {
    correct = actualKind === expectedKind;
    accuracy = correct ? ACCURACY_LABELS.CORRECT : ACCURACY_LABELS.INCORRECT;
  } else if (actualKind === OUTCOME_KINDS.WON && !expectedKind) {
    correct = true;
    accuracy = ACCURACY_LABELS.CORRECT;
  } else if (actualKind === OUTCOME_KINDS.LOST && expectedKind === OUTCOME_KINDS.WON) {
    correct = false;
    accuracy = ACCURACY_LABELS.INCORRECT;
  } else if (INCONCLUSIVE_OUTCOME_TYPES.includes(actualType)) {
    accuracy = ACCURACY_LABELS.INCONCLUSIVE;
  } else if (actualKind === OUTCOME_KINDS.WON && expectedKind === OUTCOME_KINDS.LOST) {
    correct = false;
    accuracy = ACCURACY_LABELS.PARTIAL;
  }

  const expectedProbability = Number(prediction.expectedOutcome?.probability ?? prediction.recommendation?.probability ?? 0);
  const outcomeDelta = actualKind === OUTCOME_KINDS.WON
    ? round2(expectedProbability)
    : actualKind === OUTCOME_KINDS.LOST
      ? round2(-expectedProbability)
      : 0;

  const confidenceAdjustment = accuracy === ACCURACY_LABELS.CORRECT
    ? round2(Math.min(0.15, expectedProbability * 0.1))
    : accuracy === ACCURACY_LABELS.INCORRECT
      ? round2(-Math.min(0.15, expectedProbability * 0.1))
      : 0;

  const evaluation = {
    id: asText(input.id) || newId('eval'),
    spec: SPEC,
    adr: ADR,
    tenantId: prediction.tenantId,
    missionId: prediction.missionId,
    predictionId: prediction.id,
    opportunityId: prediction.opportunityId,
    recommendation: prediction.recommendation,
    operatorAction: prediction.operatorAction,
    expectedOutcome: prediction.expectedOutcome,
    actualOutcome: {
      type: actualType,
      kind: actualKind,
      label: asText(input.label) || formatOutcomeLabel(actualType),
      at: nowIso(input.at || input.now),
      prospectId: input.prospectId || null,
    },
    accuracy,
    correct,
    outcomeDelta,
    confidenceAdjustment,
    rootCause: buildRootCauseAnalysis({
      prediction,
      actualType,
      actualKind,
      accuracy,
      input,
    }),
    evaluatedAt: nowIso(input.at || input.now),
    autoApplied: false,
  };

  return evaluation;
}

function formatOutcomeLabel(type) {
  const labels = {
    walkthrough_booked: 'Walkthrough booked',
    meeting_booked: 'Meeting booked',
    reply: 'Reply received',
    no_answer: 'No answer',
    lost: 'Lost deal',
    not_interested: 'Not interested',
    unsubscribe: 'Unsubscribed',
    bounce: 'Bounced',
  };
  return labels[type] || type.replace(/_/g, ' ');
}

/**
 * Root cause analysis when prediction fails or partially succeeds.
 * @param {object} input
 * @returns {object}
 */
function buildRootCauseAnalysis(input = {}) {
  const { prediction, actualType, actualKind, accuracy, input: ctx } = input;
  if (accuracy === ACCURACY_LABELS.CORRECT || accuracy === ACCURACY_LABELS.INCONCLUSIVE) {
    return {
      primaryCause: null,
      secondaryCause: null,
      lesson: accuracy === ACCURACY_LABELS.CORRECT
        ? 'Prediction matched observed outcome.'
        : 'Insufficient signal to evaluate prediction accuracy.',
      surprised: false,
    };
  }

  const causes = [];
  if (/contract|vendor|stability/i.test(ctx.notes || '')) {
    causes.push('Already under contract');
  }
  if (/unavailable|wrong_person|no_answer/i.test(actualType)) {
    causes.push('Decision maker unavailable');
  }
  if (/relationship|leverage/i.test((prediction.contributingHeuristicIds || []).join(' '))) {
    causes.push('Relationship leverage overestimated');
  }
  if (/vendor|stability/i.test((prediction.recommendation?.summary || ''))) {
    causes.push('Vendor stability heuristic over-weighted');
  }
  if (/seasonal|timing/i.test(ctx.notes || '')) {
    causes.push('Market timing misread');
  }
  if (!causes.length) {
    if (actualKind === OUTCOME_KINDS.LOST) causes.push('Buying signals overestimated');
    else causes.push('Expected outcome not achieved');
  }

  const primaryCause = asText(ctx.primaryCause) || causes[0] || 'Prediction did not match outcome';
  const secondaryCause = asText(ctx.secondaryCause) || causes[1] || null;

  return {
    primaryCause,
    secondaryCause,
    lesson: asText(ctx.lesson) || `${primaryCause}. ${secondaryCause ? `${secondaryCause}. ` : ''}Review contributing heuristics and opportunity reasoning.`,
    surprised: ctx.surprised === true || accuracy === ACCURACY_LABELS.INCORRECT,
    heuristicIds: prediction.contributingHeuristicIds || [],
  };
}

/**
 * Produce learning objects from an evaluation. Never auto-applied.
 * @param {object} evaluation
 * @param {object} [opts]
 * @returns {object}
 */
function produceLearningUpdates(evaluation, opts = {}) {
  const heuristicLibrary = opts.heuristicLibrary || [];
  const prediction = opts.prediction || {};
  const actualKind = evaluation.actualOutcome?.kind;
  const heuristicOutcome = actualKind === OUTCOME_KINDS.WON
    ? OUTCOME_KINDS.WON
    : actualKind === OUTCOME_KINDS.LOST
      ? OUTCOME_KINDS.LOST
      : null;

  const heuristicIds = prediction.contributingHeuristicIds || evaluation.rootCause?.heuristicIds || [];

  const heuristicResult = heuristicOutcome && heuristicIds.length
    ? learnFromOutcome(heuristicLibrary, {
        outcome: heuristicOutcome,
        contributingHeuristicIds: heuristicIds,
      })
    : { library: heuristicLibrary, updated: [] };

  const learnings = [];
  const at = evaluation.evaluatedAt || nowIso();

  if (heuristicResult.updated.length) {
    for (const row of heuristicResult.updated) {
      learnings.push({
        id: newId('olearn'),
        spec: SPEC,
        tenantId: evaluation.tenantId,
        missionId: evaluation.missionId,
        evaluationId: evaluation.id,
        kind: LEARNING_OBJECT_KINDS.HEURISTIC,
        subject: row.name,
        subjectId: row.id,
        direction: row.outcome === OUTCOME_KINDS.WON ? 'strengthened' : 'weakened',
        previousStrength: row.previousStrength,
        nextStrength: row.nextStrength,
        statement: `Heuristic "${row.name}" ${row.outcome === OUTCOME_KINDS.WON ? 'strengthened' : 'weakened'} after ${evaluation.accuracy} prediction.`,
        accuracy: evaluation.accuracy,
        autoApplied: false,
        at,
      });
    }
  }

  if (evaluation.accuracy === ACCURACY_LABELS.INCORRECT && evaluation.rootCause?.lesson) {
    learnings.push({
      id: newId('olearn'),
      spec: SPEC,
      tenantId: evaluation.tenantId,
      missionId: evaluation.missionId,
      evaluationId: evaluation.id,
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      subject: evaluation.opportunityId || 'strategy',
      direction: 'updated',
      statement: evaluation.rootCause.lesson,
      primaryCause: evaluation.rootCause.primaryCause,
      secondaryCause: evaluation.rootCause.secondaryCause,
      accuracy: evaluation.accuracy,
      autoApplied: false,
      at,
    });
  }

  if (evaluation.accuracy === ACCURACY_LABELS.CORRECT && evaluation.opportunityId) {
    learnings.push({
      id: newId('olearn'),
      spec: SPEC,
      tenantId: evaluation.tenantId,
      missionId: evaluation.missionId,
      evaluationId: evaluation.id,
      kind: LEARNING_OBJECT_KINDS.OPPORTUNITY_RULE,
      subject: evaluation.opportunityId,
      direction: 'improved',
      statement: `Opportunity prediction accuracy improved for ${evaluation.recommendation?.summary || evaluation.opportunityId}.`,
      accuracy: evaluation.accuracy,
      autoApplied: false,
      at,
    });
  }

  if (evaluation.rootCause?.primaryCause && /messaging|subject|hook|cta/i.test(evaluation.rootCause.primaryCause)) {
    learnings.push({
      id: newId('olearn'),
      spec: SPEC,
      tenantId: evaluation.tenantId,
      missionId: evaluation.missionId,
      evaluationId: evaluation.id,
      kind: LEARNING_OBJECT_KINDS.MESSAGING,
      subject: 'messaging',
      direction: evaluation.accuracy === ACCURACY_LABELS.CORRECT ? 'validated' : 'needs_review',
      statement: evaluation.rootCause.lesson,
      accuracy: evaluation.accuracy,
      autoApplied: false,
      at,
    });
  }

  return {
    evaluation,
    heuristicUpdates: heuristicResult.updated,
    heuristicLibrary: heuristicResult.library,
    learnings,
    autoApplied: false,
    spec: SPEC,
  };
}

/**
 * Resolve a pending prediction with observed outcome and produce learnings.
 * @param {object} prediction
 * @param {object} input
 * @param {object} [opts]
 * @returns {object}
 */
function resolvePrediction(prediction, input = {}, opts = {}) {
  const evaluation = evaluatePrediction(prediction, input);
  const result = produceLearningUpdates(evaluation, { ...opts, prediction });
  const resolvedPrediction = {
    ...clone(prediction),
    status: evaluation.accuracy === ACCURACY_LABELS.INCONCLUSIVE
      ? PREDICTION_STATUS.INCONCLUSIVE
      : PREDICTION_STATUS.RESOLVED,
    resolvedAt: evaluation.evaluatedAt,
    evaluationId: evaluation.id,
    accuracy: evaluation.accuracy,
  };
  return {
    prediction: resolvedPrediction,
    evaluation,
    learnings: result.learnings,
    heuristicUpdates: result.heuristicUpdates,
    heuristicLibrary: result.heuristicLibrary,
    autoApplied: false,
    spec: SPEC,
  };
}

/**
 * Build Outcome Review section for Mission Intelligence Report.
 * @param {object} input
 * @returns {object}
 */
function buildOutcomeReviewSection(input = {}) {
  const predictions = input.predictions || [];
  const evaluations = input.evaluations || [];
  const learnings = input.outcomeLearnings || input.learnings || [];

  const resolved = evaluations.filter((e) => e.accuracy !== ACCURACY_LABELS.INCONCLUSIVE);
  const correct = resolved.filter((e) => e.accuracy === ACCURACY_LABELS.CORRECT);
  const accuracyRate = resolved.length ? round2(correct.length / resolved.length) : null;

  const heuristicUpdates = learnings.filter((l) => l.kind === LEARNING_OBJECT_KINDS.HEURISTIC);
  const strategyUpdates = learnings.filter((l) => l.kind === LEARNING_OBJECT_KINDS.STRATEGY);
  const opportunityUpdates = learnings.filter((l) => l.kind === LEARNING_OBJECT_KINDS.OPPORTUNITY_RULE);

  const recentEvaluations = evaluations
    .slice()
    .sort((a, b) => String(b.evaluatedAt).localeCompare(String(a.evaluatedAt)))
    .slice(0, 10)
    .map((row) => ({
      prediction: row.recommendation?.summary,
      expectedOutcome: row.expectedOutcome?.label,
      actualOutcome: row.actualOutcome?.label,
      accuracy: row.accuracy,
      primaryCause: row.rootCause?.primaryCause,
      lesson: row.rootCause?.lesson,
      confidenceAdjustment: row.confidenceAdjustment,
    }));

  return {
    kind: 'outcome_review',
    spec: SPEC,
    adr: ADR,
    predictions: {
      total: predictions.length,
      pending: predictions.filter((p) => p.status === PREDICTION_STATUS.PENDING).length,
      resolved: predictions.filter((p) => p.status === PREDICTION_STATUS.RESOLVED).length,
    },
    accuracy: {
      rate: accuracyRate,
      label: accuracyRate != null ? `${Math.round(accuracyRate * 100)}%` : 'N/A',
      correct: correct.length,
      evaluated: resolved.length,
    },
    lessons: learnings.map((l) => ({
      kind: l.kind,
      subject: l.subject,
      direction: l.direction,
      statement: l.statement,
    })),
    heuristicUpdates: heuristicUpdates.map((l) => ({
      name: l.subject,
      direction: l.direction,
      previousStrength: l.previousStrength,
      nextStrength: l.nextStrength,
    })),
    strategyUpdates: strategyUpdates.map((l) => l.statement),
    opportunityReasoningUpdates: opportunityUpdates.map((l) => l.statement),
    recentEvaluations,
    organizationalLearning: summarizeOrganizationalLearning(evaluations, learnings),
    noRecommendationDisappears: predictions.every(
      (p) => p.status !== PREDICTION_STATUS.PENDING || input.allowPending === true
    ),
    autoApplied: false,
  };
}

/**
 * Tenant-level organizational learning summary.
 * @param {object[]} evaluations
 * @param {object[]} learnings
 * @param {object} [opts]
 * @returns {object}
 */
function summarizeOrganizationalLearning(evaluations = [], learnings = [], opts = {}) {
  const since = opts.since ? new Date(opts.since).getTime() : null;
  const filteredEvals = since
    ? evaluations.filter((e) => new Date(e.evaluatedAt).getTime() >= since)
    : evaluations;
  const filteredLearnings = since
    ? learnings.filter((l) => new Date(l.at).getTime() >= since)
    : learnings;

  const resolved = filteredEvals.filter((e) => e.accuracy !== ACCURACY_LABELS.INCONCLUSIVE);
  const succeeded = resolved.filter((e) => e.accuracy === ACCURACY_LABELS.CORRECT);
  const failed = resolved.filter((e) => e.accuracy === ACCURACY_LABELS.INCORRECT);

  const surprised = failed
    .filter((e) => e.rootCause?.surprised)
    .map((e) => ({
      prediction: e.recommendation?.summary,
      expected: e.expectedOutcome?.label,
      actual: e.actualOutcome?.label,
      cause: e.rootCause?.primaryCause,
    }));

  const succeededPatterns = succeeded.map((e) => ({
    prediction: e.recommendation?.summary,
    lesson: e.rootCause?.lesson,
  }));

  const whatShouldChange = filteredLearnings
    .filter((l) => l.direction === 'weakened' || l.direction === 'needs_review' || l.direction === 'updated')
    .map((l) => l.statement);

  const whatShouldNeverHappenAgain = failed
    .map((e) => e.rootCause?.lesson)
    .filter(Boolean);

  return {
    spec: SPEC,
    period: opts.period || 'all',
    evaluated: resolved.length,
    succeeded: succeeded.length,
    failed: failed.length,
    accuracyRate: resolved.length ? round2(succeeded.length / resolved.length) : null,
    whatSurprisedUs: surprised,
    whatPredictionFailed: failed.map((e) => ({
      prediction: e.recommendation?.summary,
      expected: e.expectedOutcome?.label,
      actual: e.actualOutcome?.label,
      primaryCause: e.rootCause?.primaryCause,
    })),
    whatPredictionSucceeded: succeededPatterns,
    whatShouldChange,
    whatShouldNeverHappenAgain: [...new Set(whatShouldNeverHappenAgain)],
    heuristicChanges: filteredLearnings
      .filter((l) => l.kind === LEARNING_OBJECT_KINDS.HEURISTIC)
      .map((l) => ({ name: l.subject, direction: l.direction })),
    autoApplied: false,
    summary: buildOrganizationalSummary({
      evaluated: resolved.length,
      succeeded: succeeded.length,
      failed: failed.length,
      surprised: surprised.length,
      learnings: filteredLearnings.length,
    }),
  };
}

function buildOrganizationalSummary({ evaluated, succeeded, failed, surprised, learnings }) {
  if (!evaluated) return 'No completed outcome evaluations yet. Predictions will be compared once business outcomes are observed.';
  const parts = [
    `${evaluated} prediction${evaluated === 1 ? '' : 's'} evaluated.`,
    `${succeeded} succeeded, ${failed} failed.`,
  ];
  if (surprised) parts.push(`${surprised} surprised us.`);
  if (learnings) parts.push(`${learnings} learning record${learnings === 1 ? '' : 's'} captured (not auto-applied).`);
  return parts.join(' ');
}

function formatOutcomeLearningReport(report = {}) {
  const lines = ['Outcome Learning', ''];
  if (report.accuracy?.label) {
    lines.push('Prediction Accuracy', report.accuracy.label, '');
  }
  for (const row of report.recentEvaluations || []) {
    lines.push('Prediction', row.prediction || '—', '');
    lines.push('Expected', row.expectedOutcome || '—', '');
    lines.push('Actual', row.actualOutcome || '—', '');
    lines.push('Accuracy', row.accuracy || '—', '');
    if (row.lesson) lines.push('Lesson', row.lesson, '');
  }
  if (report.organizationalLearning?.summary) {
    lines.push('Organizational Learning', report.organizationalLearning.summary);
  }
  return lines.join('\n').trim();
}

function isTerminalOutcomeType(type) {
  const normalized = normalizeOutcomeType(type);
  return POSITIVE_OUTCOME_TYPES.includes(normalized) || NEGATIVE_OUTCOME_TYPES.includes(normalized);
}

function pendingPredictionsForMission(predictions = [], missionId) {
  return predictions.filter(
    (p) => p.missionId === missionId && p.status === PREDICTION_STATUS.PENDING
  );
}

module.exports = {
  SPEC,
  ADR,
  PREDICTION_STATUS,
  ACCURACY_LABELS,
  LEARNING_OBJECT_KINDS,
  POSITIVE_OUTCOME_TYPES,
  NEGATIVE_OUTCOME_TYPES,
  capturePrediction,
  recordOperatorAction,
  evaluatePrediction,
  buildRootCauseAnalysis,
  produceLearningUpdates,
  resolvePrediction,
  buildOutcomeReviewSection,
  summarizeOrganizationalLearning,
  formatOutcomeLearningReport,
  isTerminalOutcomeType,
  pendingPredictionsForMission,
  outcomeKindFromType,
};
