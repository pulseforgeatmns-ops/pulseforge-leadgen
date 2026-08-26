'use strict';

/**
 * SPEC-132 — Specialist Execution Contract (SEC).
 *
 * Universal execution envelope for every specialist.
 * Mission Planning decides what. TME decides when. SEC defines how.
 *
 * Specialists receive intent — they do not interpret intent.
 * Specialists return contributions — they do not mutate mission state.
 */

const { SPECIALISTS, asText, nowIso, newId, clone, amoError } = require('./types');
const { assertContract } = require('./Contracts');
const {
  scoutInput,
  maxInput,
  paigeInput,
  veraInput,
  rexInput,
  emmettInput,
} = require('./SpecialistInputs');
const { buildSharedContext } = require('./Context');

const EXECUTION_STATUSES = Object.freeze({
  SUCCESS: 'SUCCESS',
  PARTIAL: 'PARTIAL',
  BLOCKED: 'BLOCKED',
  FAILED: 'FAILED',
});

const RECOMMENDATION_TIERS = Object.freeze({
  REQUIRED: 'required',
  SUGGESTED: 'suggested',
  OPTIONAL: 'optional',
});

const NEXT_ACTION_KINDS = Object.freeze([
  'expand_geography',
  'request_more_evidence',
  'generate_outreach',
  'pause_mission',
  'operator_review',
  'retry',
  'advance_stage',
]);

const CONFIDENCE_DIMENSIONS = Object.freeze([
  'overall',
  'evidence',
  'fit',
  'completeness',
]);

function isPlainObject(value) {
  return value != null && typeof value === 'object' && !Array.isArray(value);
}

function clamp01(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.min(1, Math.max(0, n));
}

function round2(value) {
  return Math.round(Number(value || 0) * 100) / 100;
}

/**
 * Structured confidence — never a bare floating-point number.
 */
function normalizeConfidence(value) {
  if (value == null) {
    return {
      overall: 0,
      evidence: 0,
      fit: 0,
      completeness: 0,
    };
  }
  if (typeof value === 'number' || typeof value === 'string') {
    const overall = round2(clamp01(value));
    return {
      overall,
      evidence: overall,
      fit: overall,
      completeness: overall,
    };
  }
  if (!isPlainObject(value)) {
    throw amoError('sec_confidence_invalid', 'Confidence must be structured or numeric.');
  }
  const overall = round2(clamp01(
    value.overall != null ? value.overall : value.score != null ? value.score : value.total
  ));
  return {
    overall,
    evidence: round2(clamp01(value.evidence != null ? value.evidence : overall)),
    fit: round2(clamp01(value.fit != null ? value.fit : overall)),
    completeness: round2(clamp01(
      value.completeness != null ? value.completeness : value.completenessScore != null
        ? value.completenessScore
        : overall
    )),
  };
}

/**
 * Every evidence claim requires source, confidence, timestamp, provenance.
 */
function normalizeEvidenceItem(raw, index = 0) {
  if (raw == null) return null;
  if (typeof raw === 'string') {
    const text = raw.trim();
    if (!text) return null;
    return {
      id: `ev_${index}`,
      label: text,
      source: text,
      confidence: 0.5,
      timestamp: nowIso(),
      provenance: { kind: 'inferred', source: 'legacy_string' },
    };
  }
  if (!isPlainObject(raw)) return null;

  const snapshot = isPlainObject(raw.snapshot) ? raw.snapshot : {};
  const source =
    asText(raw.source)
    || asText(snapshot.source)
    || asText(raw.sourceKind)
    || asText(raw.label)
    || 'unknown';
  const label = asText(raw.label) || asText(raw.text) || source;
  const timestamp =
    asText(raw.timestamp)
    || asText(raw.observedAt)
    || asText(raw.observed_at)
    || asText(snapshot.observedAt)
    || nowIso();

  return {
    id: asText(raw.id) || `ev_${index}`,
    label,
    source,
    confidence: round2(clamp01(raw.confidence != null ? raw.confidence : 0.5)),
    timestamp,
    provenance: isPlainObject(raw.provenance)
      ? clone(raw.provenance)
      : {
        kind: asText(raw.sourceKind) || asText(raw.kind) || 'observed',
        source,
        ...(snapshot.companyId ? { entityId: snapshot.companyId } : {}),
      },
  };
}

function normalizeEvidence(value) {
  if (!Array.isArray(value)) {
    if (isPlainObject(value)) return [normalizeEvidenceItem(value, 0)].filter(Boolean);
    return [];
  }
  return value
    .map((item, index) => normalizeEvidenceItem(item, index))
    .filter(Boolean);
}

function normalizeUnknown(raw, index = 0) {
  if (raw == null) return null;
  if (typeof raw === 'string') {
    const text = raw.trim();
    if (!text) return null;
    return { unknown: text, reason: 'Not determined during execution.' };
  }
  if (!isPlainObject(raw)) return null;
  const unknown = asText(raw.unknown || raw.text || raw.label);
  if (!unknown) return null;
  return {
    id: asText(raw.id) || `unk_${index}`,
    unknown,
    reason: asText(raw.reason) || asText(raw.detail) || 'Not determined during execution.',
  };
}

function normalizeUnknowns(value) {
  if (!Array.isArray(value)) return [];
  return value.map(normalizeUnknown).filter(Boolean);
}

function normalizeRecommendation(raw, index = 0) {
  if (raw == null) return null;
  if (typeof raw === 'string') {
    const text = raw.trim();
    if (!text) return null;
    return { tier: RECOMMENDATION_TIERS.SUGGESTED, text };
  }
  if (!isPlainObject(raw)) return null;
  const text = asText(raw.text || raw.label || raw.recommendation);
  if (!text) return null;
  let tier = asText(raw.tier || raw.level) || RECOMMENDATION_TIERS.SUGGESTED;
  if (!Object.values(RECOMMENDATION_TIERS).includes(tier)) {
    tier = RECOMMENDATION_TIERS.SUGGESTED;
  }
  return {
    id: asText(raw.id) || `rec_${index}`,
    tier,
    text,
    reason: asText(raw.reason) || null,
  };
}

function normalizeRecommendations(value) {
  if (!Array.isArray(value)) return [];
  return value.map(normalizeRecommendation).filter(Boolean);
}

function normalizeNextAction(raw, index = 0) {
  if (raw == null) return null;
  if (typeof raw === 'string') {
    const kind = asText(raw);
    if (!kind) return null;
    return { kind, label: kind.replace(/_/g, ' ') };
  }
  if (!isPlainObject(raw)) return null;
  const kind = asText(raw.kind || raw.action || raw.type);
  if (!kind) return null;
  return {
    id: asText(raw.id) || `act_${index}`,
    kind: NEXT_ACTION_KINDS.includes(kind) ? kind : kind,
    label: asText(raw.label) || kind.replace(/_/g, ' '),
    reason: asText(raw.reason) || null,
    payload: isPlainObject(raw.payload) ? clone(raw.payload) : null,
  };
}

function normalizeNextActions(value) {
  if (!Array.isArray(value)) return [];
  return value.map(normalizeNextAction).filter(Boolean);
}

function normalizeContributions(value, specialist, options = {}) {
  if (!value || (isPlainObject(value) && !Object.keys(value).length)) {
    return {};
  }
  const payload = isPlainObject(value) ? clone(value) : { result: value };
  const skipContract = options.skipContract === true;
  if (specialist && !skipContract) {
    try {
      assertContract(specialist, payload);
    } catch (err) {
      err.code = err.code || 'sec_contribution_contract';
      throw err;
    }
  }
  return payload;
}

function specialistInputFor(specialist, mission, extras = {}) {
  const who = asText(specialist).toLowerCase();
  switch (who) {
    case SPECIALISTS.SCOUT:
      return scoutInput(mission);
    case SPECIALISTS.MAX:
      return maxInput(mission, extras);
    case SPECIALISTS.PAIGE:
      return paigeInput(mission);
    case SPECIALISTS.VERA:
      return veraInput(mission, extras.companies || []);
    case SPECIALISTS.REX:
      return rexInput(mission, extras.progress || {});
    case SPECIALISTS.EMMETT:
      return emmettInput(mission, extras);
    default:
      throw amoError('sec_unknown_specialist', `No SEC input contract for specialist: ${specialist}`);
  }
}

/**
 * Build the universal specialist input envelope.
 * No specialist parses workspace messages or retrieves Blueprint directly.
 */
function buildExecutionInput(input = {}) {
  const mission = input.mission;
  const specialist = asText(input.specialist);
  if (!mission) throw amoError('sec_mission_required', 'Mission is required.');
  if (!specialist) throw amoError('sec_specialist_required', 'Specialist is required.');

  const transactionId = asText(input.transactionId) || newId('sec');
  const plan = mission.structuredMission || mission.missionPlanDraft || null;
  const contributions = Array.isArray(input.contributions) ? input.contributions : [];
  const sharedContext = buildSharedContext(mission, contributions);

  return Object.freeze({
    spec: 'SPEC-132',
    transactionId,
    specialist,
    missionPlan: plan ? clone(plan) : null,
    executionContext: clone(input.executionContext || {
      stage: mission.stage,
      status: mission.status,
      missionId: mission.id,
      tenantId: mission.tenantId || mission.clientId,
    }),
    workspaceContext: clone(input.workspaceContext || sharedContext),
    blueprintContext: clone(input.blueprintContext || input.businessContext || {}),
    evidencePolicy: clone(
      input.evidencePolicy
      || (plan && (plan.evidence || plan.evidencePolicy))
      || {}
    ),
    memoryContext: clone(input.memoryContext || {
      observations: input.observations || [],
    }),
    operatorPreferences: clone(input.operatorPreferences || {}),
    specialistInput: specialistInputFor(specialist, mission, input),
    structuredOnly: true,
    missionBound: true,
  });
}

function normalizeStatus(value) {
  const status = asText(value).toUpperCase();
  if (Object.values(EXECUTION_STATUSES).includes(status)) return status;
  const legacy = asText(value).toLowerCase();
  if (legacy === 'completed') return EXECUTION_STATUSES.SUCCESS;
  if (legacy === 'partial') return EXECUTION_STATUSES.PARTIAL;
  if (legacy === 'blocked') return EXECUTION_STATUSES.BLOCKED;
  if (legacy === 'failed') return EXECUTION_STATUSES.FAILED;
  return null;
}

/**
 * Create a normalized Execution Result — the only object specialists may return.
 */
function createExecutionResult(input = {}) {
  const specialist = asText(input.specialist);
  const transactionId = asText(input.transactionId) || newId('sec');
  let status = normalizeStatus(input.status);

  if (!status && input.blocked === true) status = EXECUTION_STATUSES.BLOCKED;
  if (!status) status = EXECUTION_STATUSES.SUCCESS;

  const confidence = normalizeConfidence(input.confidence);
  const evidence = normalizeEvidence(input.evidence || input.evidenceRefs);
  const unknowns = normalizeUnknowns(input.unknowns);
  const recommendations = normalizeRecommendations(input.recommendations);
  const nextActions = normalizeNextActions(input.nextActions || input.next_actions);
  const skipContributionContract =
    status === EXECUTION_STATUSES.BLOCKED || status === EXECUTION_STATUSES.FAILED;
  const contributions = input.contributions != null
    ? normalizeContributions(input.contributions, specialist, { skipContract: skipContributionContract })
    : (input.payload != null
      ? normalizeContributions(input.payload, specialist, { skipContract: skipContributionContract })
      : {});

  const audit = {
    specialist,
    transactionId,
    executedAt: asText(input.executedAt) || nowIso(),
    durationMs: input.durationMs != null ? Number(input.durationMs) : null,
    spec: 'SPEC-132',
    ...(isPlainObject(input.audit) ? clone(input.audit) : {}),
  };

  const result = {
    spec: 'SPEC-132',
    status,
    confidence,
    evidence,
    contributions,
    recommendations,
    unknowns,
    nextActions,
    audit,
    explainability: buildExplainability({
      status,
      confidence,
      evidence,
      recommendations,
      unknowns,
      contributions,
      ...(isPlainObject(input.explainability) ? input.explainability : {}),
    }),
  };

  if (status === EXECUTION_STATUSES.BLOCKED) {
    result.blocked = {
      reason: asText(input.reason) || asText(input.blocked && input.blocked.reason) || 'Execution blocked.',
      requiredPrecondition:
        asText(input.requiredPrecondition)
        || asText(input.required_precondition)
        || asText(input.blocked && input.blocked.requiredPrecondition)
        || null,
      recommendedAction:
        asText(input.recommendedAction)
        || asText(input.recommended_action)
        || asText(input.blocked && input.blocked.recommendedAction)
        || null,
    };
  }

  return Object.freeze(result);
}

/**
 * Specialists never throw user-facing errors — they return BLOCKED.
 */
function blockedExecutionResult(input = {}) {
  return createExecutionResult({
    ...input,
    status: EXECUTION_STATUSES.BLOCKED,
    blocked: true,
    confidence: input.confidence || { overall: 0, evidence: 0, fit: 0, completeness: 0 },
    evidence: input.evidence || [],
    contributions: input.contributions || {},
    recommendations: input.recommendations || [{
      tier: RECOMMENDATION_TIERS.REQUIRED,
      text: input.recommendedAction || input.recommended_action || 'Resolve blocking precondition.',
    }],
    unknowns: input.unknowns || [],
    nextActions: input.nextActions || [{
      kind: 'operator_review',
      label: 'Operator review required',
      reason: input.reason,
    }],
  });
}

function failedExecutionResult(input = {}) {
  return createExecutionResult({
    ...input,
    status: EXECUTION_STATUSES.FAILED,
    confidence: input.confidence || { overall: 0, evidence: 0, fit: 0, completeness: 0 },
    evidence: input.evidence || [],
    contributions: input.contributions || {},
    recommendations: input.recommendations || [],
    unknowns: input.unknowns || [],
    nextActions: input.nextActions || [{ kind: 'retry', label: 'Retry execution' }],
  });
}

/**
 * Automatic explainability — every specialist answers the four questions.
 */
function buildExplainability(input = {}) {
  const recommendations = normalizeRecommendations(input.recommendations);
  const unknowns = normalizeUnknowns(input.unknowns);
  const evidence = normalizeEvidence(input.evidence);
  const contributions = isPlainObject(input.contributions) ? input.contributions : {};

  const whyRecommended = recommendations
    .filter((r) => r.tier === RECOMMENDATION_TIERS.REQUIRED || r.tier === RECOMMENDATION_TIERS.SUGGESTED)
    .map((r) => (r.reason ? `${r.text} — ${r.reason}` : r.text));

  if (!whyRecommended.length && contributions && Object.keys(contributions).length) {
    whyRecommended.push('Contributions satisfy the specialist output contract for this stage.');
  }

  const whyNotRecommended = [];
  if (input.status === EXECUTION_STATUSES.BLOCKED || input.blocked) {
    whyNotRecommended.push(
      input.reason
      || (input.blocked && input.blocked.reason)
      || 'Execution blocked by missing precondition.'
    );
  }
  const optionalOnly = recommendations.filter((r) => r.tier === RECOMMENDATION_TIERS.OPTIONAL);
  if (optionalOnly.length) {
    whyNotRecommended.push(
      ...optionalOnly.map((r) => `Deferred optional action: ${r.text}`)
    );
  }

  const confidence = normalizeConfidence(input.confidence);
  const evidenceConfidenceChanges = evidence.map((ev) =>
    `${ev.label}: source=${ev.source}, confidence=${ev.confidence}`
  );
  if (confidence.overall != null) {
    evidenceConfidenceChanges.unshift(
      `Overall=${confidence.overall}, evidence=${confidence.evidence}, fit=${confidence.fit}, completeness=${confidence.completeness}`
    );
  }

  return {
    whyRecommended,
    whyNotRecommended,
    evidenceConfidenceChanges,
    remainsUnknown: unknowns.map((u) => `${u.unknown} — ${u.reason}`),
  };
}

function assertExecutionStatus(status) {
  const normalized = normalizeStatus(status);
  if (!normalized) {
    throw amoError('sec_status_invalid', `Status must be one of: ${Object.values(EXECUTION_STATUSES).join(', ')}.`);
  }
  return normalized;
}

function assertConfidenceStructure(confidence) {
  const normalized = normalizeConfidence(confidence);
  for (const dim of CONFIDENCE_DIMENSIONS) {
    const value = normalized[dim];
    if (!Number.isFinite(value) || value < 0 || value > 1) {
      throw amoError('sec_confidence_invalid', `Confidence.${dim} must be a number between 0 and 1.`);
    }
  }
  return normalized;
}

function assertEvidenceProvenance(evidence) {
  const items = normalizeEvidence(evidence);
  for (const item of items) {
    if (!asText(item.source)) {
      throw amoError('sec_evidence_source_missing', 'Every evidence claim requires a source.');
    }
    if (!asText(item.timestamp)) {
      throw amoError('sec_evidence_timestamp_missing', 'Every evidence claim requires a timestamp.');
    }
    if (!item.provenance || typeof item.provenance !== 'object') {
      throw amoError('sec_evidence_provenance_missing', 'Every evidence claim requires provenance.');
    }
  }
  return items;
}

function assertRecommendationsTiered(recommendations) {
  const items = normalizeRecommendations(recommendations);
  for (const item of items) {
    if (!Object.values(RECOMMENDATION_TIERS).includes(item.tier)) {
      throw amoError(
        'sec_recommendation_tier_invalid',
        'Recommendations must distinguish required, suggested, and optional.'
      );
    }
  }
  return items;
}

/**
 * Validate an Execution Result before TME commit.
 */
function validateExecutionResult(result, options = {}) {
  if (!result || typeof result !== 'object') {
    throw amoError('sec_result_missing', 'Execution Result is required.');
  }

  const specialist = asText(options.specialist || result.audit && result.audit.specialist);
  const status = assertExecutionStatus(result.status);
  const confidence = assertConfidenceStructure(result.confidence);
  const evidence = assertEvidenceProvenance(result.evidence);
  const recommendations = assertRecommendationsTiered(result.recommendations);
  const unknowns = normalizeUnknowns(result.unknowns);
  const nextActions = normalizeNextActions(result.nextActions);

  const requireEvidence = options.requireEvidence !== false
    && status !== EXECUTION_STATUSES.BLOCKED
    && status !== EXECUTION_STATUSES.FAILED;
  if (requireEvidence && !evidence.length) {
    throw amoError('sec_evidence_missing', 'Execution Result must attach evidence.');
  }

  const requireContributions = options.requireContributions !== false
    && status === EXECUTION_STATUSES.SUCCESS;
  const contributions = result.contributions || {};
  if (requireContributions && specialist) {
    if (!contributions || !Object.keys(contributions).length) {
      throw amoError('sec_contributions_missing', 'Successful execution must include contributions.');
    }
    assertContract(specialist, contributions);
  }

  if (status === EXECUTION_STATUSES.BLOCKED) {
    const blocked = result.blocked || {};
    if (!asText(blocked.reason)) {
      throw amoError('sec_blocked_reason_missing', 'BLOCKED results require a reason.');
    }
  }

  if (!result.explainability || !isPlainObject(result.explainability)) {
    throw amoError('sec_explainability_missing', 'Execution Result must include explainability.');
  }

  if (!result.audit || !asText(result.audit.transactionId)) {
    throw amoError('sec_audit_missing', 'Execution Result must include audit with transactionId.');
  }

  return {
    ok: true,
    status,
    specialist,
    confidence,
    evidence,
    contributions,
    recommendations,
    unknowns,
    nextActions,
  };
}

/**
 * Wrap specialist execution — converts thrown errors to FAILED/BLOCKED results.
 */
async function executeSpecialist(input = {}) {
  const startedAt = Date.now();
  const specialist = asText(input.specialist);
  const transactionId = asText(input.transactionId) || newId('sec');

  let executionInput;
  try {
    executionInput = buildExecutionInput({
      ...input,
      specialist,
      transactionId,
    });
  } catch (err) {
    return blockedExecutionResult({
      specialist,
      transactionId,
      reason: err.message,
      requiredPrecondition: err.code || 'sec_input_invalid',
      recommendedAction: 'Fix mission plan or preconditions before retrying.',
      durationMs: Date.now() - startedAt,
    });
  }

  if (typeof input.run !== 'function') {
    return blockedExecutionResult({
      specialist,
      transactionId,
      reason: 'Specialist executor is not available.',
      requiredPrecondition: 'registered_specialist',
      recommendedAction: 'Register a specialist adapter before execution.',
      durationMs: Date.now() - startedAt,
    });
  }

  try {
    const raw = await input.run(executionInput);
    if (raw && raw.spec === 'SPEC-132' && raw.status) {
      return createExecutionResult({
        ...raw,
        specialist,
        transactionId,
        durationMs: Date.now() - startedAt,
      });
    }
    return fromLegacyOutput(specialist, raw, {
      transactionId,
      durationMs: Date.now() - startedAt,
    });
  } catch (err) {
    if (input.treatErrorsAsBlocked) {
      return blockedExecutionResult({
        specialist,
        transactionId,
        reason: err.message || 'Specialist blocked.',
        requiredPrecondition: err.code || 'specialist_error',
        recommendedAction: 'Review specialist preconditions and retry.',
        durationMs: Date.now() - startedAt,
      });
    }
    return failedExecutionResult({
      specialist,
      transactionId,
      reason: err.message || 'Specialist failed.',
      durationMs: Date.now() - startedAt,
    });
  }
}

/**
 * Map legacy specialist outputs (pre-SEC) into Execution Result shape.
 */
function fromLegacyOutput(specialist, raw = {}, ctx = {}) {
  const who = asText(specialist).toLowerCase();
  const transactionId = asText(ctx.transactionId) || newId('sec');
  const durationMs = ctx.durationMs;

  if (who === SPECIALISTS.SCOUT) {
    return fromScoutLegacyOutput(raw, { specialist, transactionId, durationMs });
  }

  if (who === SPECIALISTS.MAX) {
    const payload = raw.prioritizationPayload || raw.payload || raw;
    const blocked = raw.blocked === true || /blocked/i.test(String(raw.status || ''));
    const status = blocked ? EXECUTION_STATUSES.BLOCKED : normalizeStatus(raw.status) || EXECUTION_STATUSES.SUCCESS;
    return createExecutionResult({
      specialist,
      transactionId,
      status,
      confidence: raw.confidence || payload.confidence,
      evidence: payload.evidence || raw.evidence || [],
      contributions: payload,
      recommendations: raw.recommendations || payload.recommendations || [],
      unknowns: raw.unknowns || payload.unknowns || [],
      nextActions: raw.nextActions || raw.next_actions || [],
      durationMs,
      reason: blocked ? (raw.reason || raw.summary) : null,
      requiredPrecondition: blocked ? 'max_prioritization' : null,
    });
  }

  const payload = raw.payload || raw;
  const status = normalizeStatus(raw.status) || EXECUTION_STATUSES.SUCCESS;
  return createExecutionResult({
    specialist,
    transactionId,
    status,
    confidence: raw.confidence || payload.confidence,
    evidence: payload.evidence || raw.evidence || raw.evidenceRefs,
    contributions: payload,
    recommendations: raw.recommendations || [],
    unknowns: raw.unknowns || payload.unknowns || [],
    nextActions: raw.nextActions || raw.next_actions || [],
    durationMs,
  });
}

function fromScoutLegacyOutput(raw = {}, ctx = {}) {
  const payload = raw.payload || raw.discoveryPayload || raw;
  const blocked = payload.blocked === true || /blocked/i.test(String(raw.status || payload.outcome || ''));
  const status = blocked ? EXECUTION_STATUSES.BLOCKED : normalizeStatus(raw.status) || EXECUTION_STATUSES.SUCCESS;
  const confidenceBreakdown = payload.confidenceBreakdown || null;
  const confidence = confidenceBreakdown || payload.confidence || raw.confidence;

  const evidence = payload.evidence
    || (payload.opportunities || []).flatMap((o) => o.evidenceRefs || [])
    || raw.evidenceRefs
    || [];

  const unknowns = (payload.opportunities || []).flatMap((o) => o.unknowns || [])
    .concat(payload.unknowns || raw.unknowns || []);

  const nextActions = [];
  if (blocked) {
    nextActions.push({
      kind: 'request_more_evidence',
      label: 'Request more evidence',
      reason: payload.blockReason || raw.summary,
    });
  } else if ((payload.qualifiedCount || 0) === 0) {
    nextActions.push({ kind: 'expand_geography', label: 'Expand geography' });
  }

  const result = createExecutionResult({
    specialist: ctx.specialist || SPECIALISTS.SCOUT,
    transactionId: ctx.transactionId,
    status,
    confidence,
    evidence,
    contributions: payload,
    recommendations: raw.recommendations || [],
    unknowns,
    nextActions,
    durationMs: ctx.durationMs,
    reason: blocked ? (payload.blockReason || raw.summary) : null,
    requiredPrecondition: blocked ? 'discovery_evidence' : null,
    recommendedAction: blocked ? 'Adjust mission criteria or expand search.' : null,
  });

  return result;
}

function executionResultFromStageOutput(output = {}, options = {}) {
  const specialist = asText(options.specialist);
  if (output.executionResult && output.executionResult.spec === 'SPEC-132') {
    return output.executionResult;
  }
  if (output.discoveryPayload) {
    return fromScoutLegacyOutput(
      { ...output.scoutResult, discoveryPayload: output.discoveryPayload, payload: output.discoveryPayload },
      { specialist: specialist || SPECIALISTS.SCOUT, transactionId: options.transactionId }
    );
  }
  if (output.prioritizationPayload) {
    return fromLegacyOutput(
      specialist || SPECIALISTS.MAX,
      { ...output.maxResult, prioritizationPayload: output.prioritizationPayload, payload: output.prioritizationPayload },
      { transactionId: options.transactionId }
    );
  }
  return fromLegacyOutput(specialist, output, { transactionId: options.transactionId });
}

module.exports = {
  EXECUTION_STATUSES,
  RECOMMENDATION_TIERS,
  NEXT_ACTION_KINDS,
  CONFIDENCE_DIMENSIONS,
  normalizeConfidence,
  normalizeEvidence,
  normalizeUnknowns,
  normalizeRecommendations,
  normalizeNextActions,
  specialistInputFor,
  buildExecutionInput,
  createExecutionResult,
  blockedExecutionResult,
  failedExecutionResult,
  buildExplainability,
  validateExecutionResult,
  assertExecutionStatus,
  assertConfidenceStructure,
  assertEvidenceProvenance,
  assertRecommendationsTiered,
  executeSpecialist,
  fromLegacyOutput,
  fromScoutLegacyOutput,
  executionResultFromStageOutput,
};
