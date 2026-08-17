'use strict';

/**
 * SPEC-101 — SpecialistCognitiveTrace projection.
 *
 * Composes inspectable history from existing SPEC-098/099A/100 records.
 * Does not invent a parallel store. Missing layers stay unknown.
 */

const { asText, clone, isPlainObject } = require('./Types');
const {
  projectAvailableContext,
  projectSuppliedContext,
  projectConsumedContext,
  contextFieldPresent,
  geographyLabel,
} = require('./ContextLayers');

const FAILURE_BOUNDARIES = Object.freeze({
  CONTEXT_RETRIEVAL: 'context_retrieval_failure',
  DELEGATION: 'delegation_failure',
  SPECIALIST_INTERPRETATION: 'specialist_interpretation_failure',
  CAPABILITY: 'capability_failure',
  EXTERNAL_DEPENDENCY: 'external_dependency_failure',
  EVIDENCE_INSUFFICIENCY: 'evidence_insufficiency',
  UNKNOWN: 'unknown',
});

const EVIDENCE_LAYERS = Object.freeze({
  BUSINESS: 'business_evidence',
  INVESTIGATION: 'investigation_provenance',
  SYSTEM: 'system_provenance',
  MAX_EVALUATION: 'max_evaluation',
});

function asTextList(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => {
      if (typeof item === 'string') return asText(item);
      if (!item || typeof item !== 'object') return null;
      return asText(item.text || item.summary || item.message || item.label);
    })
    .filter(Boolean);
}

function investigationFrom(result) {
  if (!result) return null;
  if (result.payload && isPlainObject(result.payload.investigation)) {
    return result.payload.investigation;
  }
  if (isPlainObject(result.investigation)) return result.investigation;
  return null;
}

function projectExecution(result) {
  const investigation = investigationFrom(result);
  const actionsTaken = asTextList(result && result.actionsTaken);
  const errors = Array.isArray(result && result.errors) ? result.errors : [];
  const limitations = investigation && Array.isArray(investigation.limitations)
    ? investigation.limitations.slice()
    : [];
  const unavailable =
    (investigation &&
      investigation.sources &&
      investigation.sources.sourceTypesUnavailable) ||
    [];
  const checked =
    (investigation &&
      investigation.sources &&
      investigation.sources.sourceTypesChecked) ||
    [];
  const failedActions = errors
    .map((err) => asText(err && (err.message || err.code)))
    .filter(Boolean);
  const attempted = actionsTaken.slice();
  if (!attempted.length && result && result.status) {
    attempted.push(`Specialist run ended ${result.status}.`);
  }
  return {
    attemptedActions: attempted,
    successfulActions: result && ['completed', 'partial'].includes(result.status)
      ? actionsTaken
      : actionsTaken.filter((line) => !/fail|could not|unable/i.test(line)),
    failedActions,
    unavailableCapabilities: unavailable.slice(),
    limitations,
    sourcesChecked: checked.slice(),
  };
}

function projectResultLayer(result) {
  if (!result) {
    return {
      status: null,
      findings: [],
      evidenceRefs: [],
      confidence: null,
      coverageConfidence: null,
      uncertainties: [],
    };
  }
  const investigation = investigationFrom(result);
  return {
    status: asText(result.status),
    findings: asTextList(result.observations),
    evidenceRefs: Array.isArray(result.evidenceRefs) ? clone(result.evidenceRefs) : [],
    confidence: result.confidence == null ? null : Number(result.confidence),
    coverageConfidence:
      investigation && investigation.coverageConfidence != null
        ? Number(investigation.coverageConfidence)
        : result.payload && result.payload.coverageConfidence != null
          ? Number(result.payload.coverageConfidence)
          : null,
    uncertainties: asTextList(result.uncertainties),
    summary: asText(result.summary),
  };
}

function projectMaxEvaluation(evaluation) {
  if (!evaluation) return null;
  const payload = isPlainObject(evaluation.payload) ? evaluation.payload : {};
  return {
    acceptedFindings: Array.isArray(evaluation.acceptedClaims)
      ? clone(evaluation.acceptedClaims)
      : Array.isArray(payload.acceptedClaims)
        ? clone(payload.acceptedClaims)
        : [],
    rejectedFindings: Array.isArray(evaluation.rejectedClaims)
      ? clone(evaluation.rejectedClaims)
      : Array.isArray(payload.rejectedClaims)
        ? clone(payload.rejectedClaims)
        : [],
    interpretation: asText(evaluation.explanation),
    materiality: asText(evaluation.materiality || payload.materiality),
    materialChange: evaluation.materialChange === true,
    priorityEffect: evaluation.suggestedPriorityChange
      ? clone(evaluation.suggestedPriorityChange)
      : null,
    priorityApplied: evaluation.priorityApplied === true,
    reasoning: asTextList(evaluation.reasoning).concat(
      evaluation.explanation ? [evaluation.explanation] : []
    ),
    conclusionTrust: asText(evaluation.conclusionTrust || payload.conclusionTrust),
    coverageBand: asText(evaluation.coverageBand || payload.coverageBand),
    coverageConfidence:
      evaluation.coverageConfidence != null
        ? Number(evaluation.coverageConfidence)
        : payload.coverageConfidence != null
          ? Number(payload.coverageConfidence)
          : null,
    marketAbsenceJustified: evaluation.marketAbsenceJustified === true,
    acceptedAsGroundTruth: evaluation.acceptedAsGroundTruth === true,
    evaluatedAt: asText(evaluation.createdAt || evaluation.evaluatedAt),
  };
}

function classifyGeographyBoundary(available, supplied, consumed) {
  const availableKnown = available && available.recorded === true;
  const availableGeo = contextFieldPresent(available, 'geography');
  const suppliedGeo = contextFieldPresent(supplied, 'geography');
  const consumedGeo =
    consumed &&
    consumed.geographyResolved === true &&
    contextFieldPresent(consumed, 'geography');
  const consumedRecorded = consumed && consumed.recorded === true;
  const unresolved =
    consumed &&
    (/geography could not be resolved/i.test(String(consumed.invalidReason || '')) ||
      (consumed.geographyResolved === false && !consumedGeo));

  if (availableKnown && availableGeo && !suppliedGeo) {
    return {
      boundary: FAILURE_BOUNDARIES.DELEGATION,
      field: 'geography',
      known: true,
    };
  }
  if (suppliedGeo && consumedRecorded && !consumedGeo) {
    return {
      boundary: FAILURE_BOUNDARIES.SPECIALIST_INTERPRETATION,
      field: 'geography',
      known: true,
    };
  }
  if (availableKnown && !availableGeo && !suppliedGeo) {
    return {
      boundary: FAILURE_BOUNDARIES.CONTEXT_RETRIEVAL,
      field: 'geography',
      known: true,
    };
  }
  if (!availableKnown && suppliedGeo && consumedRecorded && !consumedGeo) {
    return {
      boundary: FAILURE_BOUNDARIES.SPECIALIST_INTERPRETATION,
      field: 'geography',
      known: true,
    };
  }
  if (!availableKnown && !suppliedGeo && (unresolved || consumedRecorded)) {
    return {
      boundary: FAILURE_BOUNDARIES.UNKNOWN,
      field: 'geography',
      known: false,
    };
  }
  return null;
}

function classifyFailureBoundary(trace) {
  const result = trace.result || {};
  const execution = trace.execution || {};
  const errors = (trace.raw && trace.raw.result && trace.raw.result.errors) || [];
  const providerFailed = errors.some((err) =>
    /provider|repository|timeout|network/i.test(
      String((err && (err.code || err.message)) || '')
    )
  );
  const adapterUnavailable = errors.some((err) =>
    /adapter_unavailable|unknown_capability/i.test(String((err && err.code) || ''))
  );

  const geo = classifyGeographyBoundary(
    trace.availableContext,
    trace.suppliedContext,
    trace.consumedContext
  );
  if (geo) return geo;

  if (adapterUnavailable) {
    return { boundary: FAILURE_BOUNDARIES.CAPABILITY, field: null, known: true };
  }
  if (providerFailed) {
    return {
      boundary: FAILURE_BOUNDARIES.EXTERNAL_DEPENDENCY,
      field: null,
      known: true,
    };
  }

  const evaluated =
    (trace.investigation &&
      trace.investigation.coverage &&
      Number(trace.investigation.coverage.candidatesEvaluated || 0)) ||
    0;
  if (
    result.status &&
    ['completed', 'partial'].includes(result.status) &&
    evaluated > 0 &&
    (result.findings || []).length === 0
  ) {
    return {
      boundary: FAILURE_BOUNDARIES.EVIDENCE_INSUFFICIENCY,
      field: null,
      known: true,
    };
  }

  if (execution.failedActions && execution.failedActions.length && !result.status) {
    return { boundary: FAILURE_BOUNDARIES.UNKNOWN, field: null, known: false };
  }

  if (result.status === 'failed' || result.status === 'blocked') {
    return { boundary: FAILURE_BOUNDARIES.UNKNOWN, field: null, known: false };
  }

  return { boundary: null, field: null, known: true };
}

/**
 * Compose a SpecialistCognitiveTrace from persisted contract records.
 *
 * @param {object} input
 * @returns {object}
 */
function composeCognitiveTrace(input = {}) {
  const delegation = input.delegation || null;
  const result = input.result || null;
  const evaluation = input.evaluation || null;
  const investigation = investigationFrom(result);
  const availableContext = projectAvailableContext(input);
  const suppliedContext = delegation
    ? projectSuppliedContext(delegation)
    : {
        recorded: false,
        geography: null,
        serviceArea: null,
        segments: [],
      };
  const consumedContext = projectConsumedContext(result, delegation || {});
  const resultLayer = projectResultLayer(result);
  const maxEvaluation = projectMaxEvaluation(evaluation);
  const execution = projectExecution(result || {});

  const trace = {
    traceId:
      asText(input.traceId) ||
      asText(evaluation && evaluation.id) ||
      asText(result && result.id) ||
      asText(delegation && delegation.id),
    tenantId: asText(
      (delegation && delegation.tenantId) ||
        (result && result.tenantId) ||
        (evaluation && evaluation.tenantId) ||
        input.tenantId
    ),
    sessionId: asText(input.sessionId),
    specialist: asText(
      (delegation && delegation.specialist) || (result && result.specialist)
    ),
    capability: asText(
      (delegation && delegation.capability) || (result && result.capability)
    ),
    operatorObjective: asText(
      input.operatorObjective || (delegation && delegation.objective)
    ),
    operatorQuestion: asText(input.operatorQuestion),
    delegation: delegation
      ? {
          specialist: delegation.specialist,
          capability: delegation.capability,
          reason: delegation.reason,
          requestedTask: delegation.objective,
          suppliedContext,
          constraints: delegation.constraints || {},
          requestedEvidence: suppliedContext.requestedEvidence,
          createdAt: delegation.createdAt,
          id: delegation.id,
        }
      : null,
    availableContext,
    suppliedContext,
    consumedContext,
    execution,
    result: resultLayer,
    investigation,
    maxEvaluation,
    evidenceLayers: {
      [EVIDENCE_LAYERS.BUSINESS]: (resultLayer.evidenceRefs || []).filter(
        (ref) => ref && ref.kind !== 'specialist_result' && ref.sourceKind !== 'system'
      ),
      [EVIDENCE_LAYERS.INVESTIGATION]: investigation,
      [EVIDENCE_LAYERS.SYSTEM]: {
        specialist: (delegation && delegation.specialist) || (result && result.specialist),
        capability: (delegation && delegation.capability) || (result && result.capability),
        delegationId: delegation && delegation.id,
        resultId: result && result.id,
        evaluationId: evaluation && evaluation.id,
      },
      [EVIDENCE_LAYERS.MAX_EVALUATION]: maxEvaluation,
    },
    raw: {
      delegation: delegation ? { id: delegation.id } : null,
      result: result
        ? { id: result.id, status: result.status, errors: result.errors || [] }
        : null,
      evaluation: evaluation ? { id: evaluation.id } : null,
    },
  };

  trace.failure = classifyFailureBoundary(trace);
  return trace;
}

function tracesShareObjective(a, b) {
  const left = String((a && a.operatorObjective) || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
  const right = String((b && b.operatorObjective) || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
  if (!left || !right) return false;
  if (left === right) return true;
  const tokens = (s) => new Set(s.split(/\s+/).filter((t) => t.length > 3));
  const aSet = tokens(left);
  const bSet = tokens(right);
  let overlap = 0;
  for (const t of aSet) {
    if (bSet.has(t)) overlap += 1;
  }
  return overlap / Math.max(aSet.size, bSet.size, 1) >= 0.45;
}

function inspectionSummary(trace) {
  if (!trace) return null;
  const specialist = trace.specialist || 'the specialist';
  const asked = (trace.delegation && trace.delegation.requestedTask) || trace.operatorObjective;
  const suppliedGeo = geographyLabel(trace.suppliedContext && trace.suppliedContext.geography);
  const failure = trace.failure && trace.failure.boundary;
  const judgment =
    (trace.maxEvaluation &&
      (trace.maxEvaluation.materialChange
        ? 'Priority change was justified by accepted business evidence.'
        : 'Insufficient coverage to interpret the specialist result as market evidence.')) ||
    null;
  return {
    askedOf: asked,
    contextSupplied: {
      business: trace.suppliedContext && trace.suppliedContext.business,
      geography: suppliedGeo || 'missing',
      objective: trace.suppliedContext && trace.suppliedContext.objective,
    },
    execution:
      (trace.execution &&
        trace.execution.failedActions[0]) ||
      (trace.result && trace.result.summary) ||
      null,
    failure,
    maxJudgment: judgment,
    specialist,
  };
}

module.exports = {
  FAILURE_BOUNDARIES,
  EVIDENCE_LAYERS,
  composeCognitiveTrace,
  classifyFailureBoundary,
  projectExecution,
  projectResultLayer,
  projectMaxEvaluation,
  tracesShareObjective,
  inspectionSummary,
  investigationFrom,
};
