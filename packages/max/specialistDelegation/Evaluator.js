'use strict';

/**
 * SPEC-098 — Max evaluates SpecialistResult. It does not become ground truth.
 * Specialists cannot mutate Command Deck priority from this module.
 */

const { asText, clone, isPlainObject, normalizeStringRecords } = require('./Types');

const PRIORITY_STATES = Object.freeze(['monitored', 'normal', 'elevated', 'urgent']);

function textBlob(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value.toLowerCase();
  try {
    return JSON.stringify(value).toLowerCase();
  } catch (_) {
    return String(value).toLowerCase();
  }
}

function normalizeOperatorDirection(raw) {
  if (!raw) return null;
  if (typeof raw === 'string') {
    const text = asText(raw);
    return text
      ? {
          text,
          excludedSegments: [],
          focusSegments: [],
          authoritative: true,
        }
      : null;
  }
  if (!isPlainObject(raw)) return null;
  const text = asText(raw.text || raw.direction || raw.objectiveText);
  const excluded = Array.isArray(raw.excludedSegments)
    ? raw.excludedSegments.map(asText).filter(Boolean)
    : [];
  const focus = Array.isArray(raw.focusSegments)
    ? raw.focusSegments.map(asText).filter(Boolean)
    : [];
  if (!text && !excluded.length && !focus.length) return null;
  return {
    text: text || '',
    excludedSegments: excluded,
    focusSegments: focus,
    authoritative: raw.authoritative !== false,
  };
}

function mentionsSegment(haystack, segment) {
  const needle = String(segment || '')
    .toLowerCase()
    .replace(/[_-]+/g, ' ')
    .trim();
  if (!needle) return false;
  const compact = haystack.replace(/[_-]+/g, ' ');
  return compact.includes(needle);
}

/**
 * Operator direction remains authoritative (SPEC-095/096).
 * A prior specialist result cannot silently restore an excluded segment.
 */
function assessOperatorDirection(result, operatorDirection) {
  const direction = normalizeOperatorDirection(operatorDirection);
  if (!direction) {
    return { honored: true, challenge: null, direction: null };
  }

  const haystack = [
    result.summary,
    result.recommendedNextAction,
    ...(result.observations || []),
  ]
    .map(textBlob)
    .join(' ');

  const restored = direction.excludedSegments.filter((seg) =>
    mentionsSegment(haystack, seg)
  );

  if (restored.length && direction.authoritative) {
    return {
      honored: false,
      challenge:
        `Operator direction remains authoritative: do not restore ${restored.join(', ')}. ` +
        (direction.focusSegments.length
          ? `Continue focusing on ${direction.focusSegments.join(', ')}.`
          : direction.text),
      direction,
      restoredSegments: restored,
    };
  }

  return { honored: true, challenge: null, direction };
}

function expectedReturnSatisfied(delegation, result) {
  const expected = delegation.expectedReturn || {};
  const missing = [];
  if (expected.requireEvidence && !(result.evidenceRefs || []).length) {
    missing.push('evidence');
  }
  if (expected.requireConfidence && result.confidence == null) {
    missing.push('confidence');
  }
  if (expected.requireRecommendation && !result.recommendedNextAction) {
    missing.push('recommendation');
  }
  return { satisfied: missing.length === 0, missing };
}

function suggestPriorityChange(delegation, result, evaluationHints) {
  if (evaluationHints && evaluationHints.suggestedPriorityChange) {
    const hint = evaluationHints.suggestedPriorityChange;
    if (PRIORITY_STATES.includes(hint.to)) {
      return {
        domain: hint.domain || 'acquisition',
        from: hint.from || 'normal',
        to: hint.to,
        reason: hint.reason || result.summary || 'Material specialist intelligence.',
      };
    }
  }

  if (result.status !== 'completed') return null;
  if (result.confidence == null || Number(result.confidence) < 0.75) return null;

  const objective = String(delegation.objective || '').toLowerCase();
  const looksAcquisition =
    delegation.capability === 'acquisition_assessment' ||
    delegation.specialist === 'scout' ||
    /\bacquisition\b|\bopportunit/.test(objective);

  if (!looksAcquisition) return null;
  if (!(result.evidenceRefs || []).length) return null;

  return {
    domain: 'acquisition',
    from: 'normal',
    to: 'elevated',
    reason:
      result.summary ||
      'Specialist returned timely acquisition opportunities worth Max review.',
  };
}

/**
 * Evaluate a specialist result. Never mutates Command Deck priority.
 *
 * @param {object} input
 * @returns {object}
 */
function evaluateSpecialistResult(input = {}) {
  const delegation = input.delegation;
  const result = input.result;
  if (!delegation || !result) {
    throw new Error('evaluateSpecialistResult requires delegation and result');
  }

  const contract = expectedReturnSatisfied(delegation, result);
  const operator = assessOperatorDirection(
    result,
    input.operatorDirection ||
      (delegation.businessContext && delegation.businessContext.operatorDirection)
  );

  const objectiveSatisfied =
    result.status === 'completed' &&
    contract.satisfied &&
    operator.honored;

  const materialChange =
    objectiveSatisfied &&
    (result.evidenceRefs || []).length > 0 &&
    Number(result.confidence) >= 0.7 &&
    operator.honored;

  const suggestedPriorityChange = operator.honored
    ? suggestPriorityChange(delegation, result, input)
    : null;

  const warrantsOperatorAttention =
    !operator.honored ||
    result.status === 'blocked' ||
    result.status === 'declined_policy' ||
    materialChange;

  const warrantsAnotherDelegation =
    result.status === 'partial' ||
    result.status === 'blocked' ||
    (result.recommendedNextAction &&
      result.recommendedNextAction.type === 'retry');

  const explanation = buildEvaluationExplanation({
    delegation,
    result,
    objectiveSatisfied,
    materialChange,
    operator,
    suggestedPriorityChange,
    contract,
  });

  const provenance = {
    evaluation: { kind: 'max_evaluation' },
    result: {
      id: result.id,
      status: result.status,
      specialist: result.specialist,
      capability: result.capability,
    },
    delegation: {
      id: delegation.id,
      objective: delegation.objective,
      reason: delegation.reason,
      authority: delegation.authority,
      specialist: delegation.specialist,
      capability: delegation.capability,
    },
    evidence: (result.evidenceRefs || []).map((e) => ({
      id: e.id,
      kind: e.kind,
      sourceKind: e.sourceKind,
      label: e.label || null,
    })),
    inputEvidence: (delegation.evidenceRefs || []).map((e) => ({
      id: e.id,
      kind: e.kind,
      sourceKind: e.sourceKind,
    })),
  };

  return {
    delegationId: delegation.id,
    resultId: result.id,
    tenantId: delegation.tenantId,
    objectiveSatisfied,
    materialChange,
    warrantsOperatorAttention,
    warrantsAnotherDelegation,
    suggestedPriorityChange,
    priorityApplied: false,
    operatorDirectionHonored: operator.honored,
    operatorChallenge: operator.challenge,
    acceptedAsGroundTruth: false,
    explanation,
    provenance,
    payload: {
      contract,
      resultStatus: result.status,
      confidence: result.confidence,
      uncertainties: result.uncertainties || [],
      restoredSegments: operator.restoredSegments || [],
    },
  };
}

function buildEvaluationExplanation(input) {
  const { delegation, result, objectiveSatisfied, materialChange, operator, suggestedPriorityChange, contract } =
    input;

  if (!operator.honored) {
    return (
      `I did not accept the ${result.specialist} result as a change of direction. ` +
      (operator.challenge || 'Operator direction remains authoritative.')
    );
  }

  if (result.status === 'declined_policy') {
    return `I declined to act on this delegation because it violated the effective authority or policy boundary.`;
  }

  if (result.status === 'blocked') {
    return (
      `I could not finish "${delegation.objective}" because the specialist was blocked. ` +
      (result.summary || '')
    );
  }

  if (result.status === 'partial') {
    return (
      `I received a partial result for "${delegation.objective}". ` +
      `Useful evidence was preserved. ${result.summary || ''} ` +
      `Missing return fields: ${(contract.missing || []).join(', ') || 'none'}.`
    );
  }

  if (result.status === 'failed') {
    return (
      `The specialist failed while working on "${delegation.objective}". ` +
      `Any evidence already gathered remains available.`
    );
  }

  const priorityClause = suggestedPriorityChange
    ? ` That may warrant moving ${suggestedPriorityChange.domain} from ${suggestedPriorityChange.from} to ${suggestedPriorityChange.to}.`
    : '';

  if (objectiveSatisfied && materialChange) {
    return (
      `I evaluated the ${result.specialist} result for "${delegation.objective}". ` +
      `${result.summary || ''} Confidence ${result.confidence}. ` +
      `This is evidence for my reasoning, not automatic ground truth.` +
      priorityClause
    );
  }

  return (
    `I evaluated the ${result.specialist} result for "${delegation.objective}". ` +
    `${result.summary || ''} The objective was ${objectiveSatisfied ? 'satisfied' : 'not fully satisfied'}.`
  );
}

function formatOperatorExplanation(evaluation, extras = {}) {
  const ev = extras.evidence || (evaluation.provenance && evaluation.provenance.evidence) || [];
  const evidenceIds = ev.map((e) => e.id).filter(Boolean);
  const evidenceClause = evidenceIds.length
    ? ` Evidence: ${evidenceIds.join(', ')}.`
    : '';
  return `${evaluation.explanation}${evidenceClause}`;
}

module.exports = {
  PRIORITY_STATES,
  evaluateSpecialistResult,
  assessOperatorDirection,
  normalizeOperatorDirection,
  expectedReturnSatisfied,
  formatOperatorExplanation,
  clone,
  normalizeStringRecords,
};
