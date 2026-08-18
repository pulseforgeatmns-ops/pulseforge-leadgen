'use strict';

/**
 * SPEC-098 — Max Specialist Delegation Contract.
 *
 * Max creates a bounded SpecialistDelegation, a specialist adapter performs
 * bounded work, a SpecialistResult returns, and Max evaluates it as evidence.
 * No new universal runtime. Specialists cannot mutate Command Deck priority.
 * Operator direction remains authoritative. No automatic recursion.
 */

const {
  AUTHORITY_LEVELS,
  DELEGATION_STATUSES,
  RESULT_STATUSES,
  SpecialistDelegationError,
  nowIso,
  asText,
  clone,
  isPlainObject,
  normalizeEvidenceRefs,
  normalizeConstraints,
  normalizeExpectedReturn,
  normalizeBusinessContext,
  normalizeTargetContext,
  normalizeStringRecords,
} = require('./Types');
const {
  createDefaultCapabilityRegistry,
  createCapabilityRegistry,
  DEFAULT_CAPABILITIES,
} = require('./CapabilityRegistry');
const {
  DEFAULT_TENANT_DELEGATION_POLICY,
  validateDelegationAuthority,
  assertAuthorizedTenant,
  normalizeTenantPolicy,
  policyEvent,
} = require('./Authority');
const { newId, createMemoryStore, createPostgresStore, resolveStore } = require('./Store');
const { runTestIntelligence, isTestIntelligence, CONTRACT_OBJECTIVE, DEFAULT_EVIDENCE } =
  require('./TestIntelligenceAdapter');
const { runScoutAcquisitionIntelligence } = require('../scoutAcquisition/ScoutAdapter');
const {
  evaluateSpecialistResult,
  formatOperatorExplanation,
  normalizeOperatorDirection,
} = require('./Evaluator');
const { buildProvenanceChain, formatProvenanceNarrative } = require('./Provenance');
const {
  composeCognitiveTrace,
  classifyFailureBoundary,
  inspectionSummary,
  FAILURE_BOUNDARIES,
  EVIDENCE_LAYERS,
} = require('./CognitiveTrace');
const {
  captureAvailableContext,
  projectAvailableContext,
  projectSuppliedContext,
  projectConsumedContext,
} = require('./ContextLayers');
const {
  classifyOperatorIntent,
  resolveRecentReferent,
  looksLikeInterrogation,
  looksLikeNewInvestigation,
  formatDisambiguation,
  INTENT,
} = require('./InterrogationIntent');
const { answerFromTrace, limitationAnswer } = require('./InterrogationAnswer');
const {
  COGNITIVE_MODES,
  NEVER_DELEGATE_MODES,
  classifyCognitiveMode,
  forbidsSpecialistDelegation,
  looksLikeInvestigation,
  looksLikeSummary,
  looksLikeCompletedRetrieval,
} = require('./CognitiveMode');
const {
  UNKNOWN_ANSWER,
  mayCreateDelegation,
  mayEnterSpecialistPath,
  meetsInvestigationThreshold,
  shouldInvokeSpecialist,
} = require('./RetrievalGate');

const ADAPTERS = Object.freeze({
  test_intelligence: runTestIntelligence,
  scout: runScoutAcquisitionIntelligence,
});

function toPublicDelegation(row) {
  if (!row) return null;
  const out = clone(row);
  if (
    !out.availableContext &&
    out.businessContext &&
    out.businessContext.maxAvailableContext
  ) {
    out.availableContext = clone(out.businessContext.maxAvailableContext);
  }
  return out;
}

function toPublicResult(row) {
  if (!row) return null;
  const out = clone(row);
  delete out._contractObjective;
  delete out._fixtureMode;
  return out;
}

function buildDelegationRecord(authorizedTenantId, input) {
  const tenantId = assertAuthorizedTenant(
    authorizedTenantId,
    input.tenantId || input.tenant_id
  );
  const specialist = asText(input.specialist);
  const capability = asText(input.capability);
  const objective = asText(input.objective);
  const reason = asText(input.reason);
  if (!specialist || !capability) {
    throw new SpecialistDelegationError(
      'invalid_delegation',
      'specialist and capability are required.'
    );
  }
  if (!objective) {
    throw new SpecialistDelegationError('invalid_delegation', 'objective is required.');
  }
  if (!reason) {
    throw new SpecialistDelegationError(
      'invalid_delegation',
      'reason is required (why this work matters now).'
    );
  }

  const now = nowIso();
  const availableContext =
    input.availableContext && typeof input.availableContext === 'object'
      ? input.availableContext
      : null;
  const businessContext = normalizeBusinessContext({
    ...(input.businessContext || {}),
    ...(availableContext ? { maxAvailableContext: availableContext } : {}),
  });
  return {
    id: asText(input.id) || newId(),
    tenantId,
    specialist,
    capability,
    objective,
    reason,
    availableContext,
    businessContext,
    targetContext: normalizeTargetContext(input.targetContext),
    evidenceRefs: normalizeEvidenceRefs(input.evidenceRefs),
    constraints: normalizeConstraints(input.constraints),
    authority: asText(input.authority),
    expectedReturn: normalizeExpectedReturn(input.expectedReturn),
    requestedBy: asText(input.requestedBy) || 'max',
    createdAt: input.createdAt || now,
    status: 'created',
    policyEvents: [],
    updatedAt: now,
    _fixtureMode: asText(input.fixtureMode) || null,
  };
}

function resolveAdapter(capability) {
  if (!capability || !capability.callable || !capability.adapter) return null;
  return ADAPTERS[capability.adapter] || null;
}

/**
 * Create a Max-owned delegation. Persists even when policy rejects it
 * so the reasoning trail survives.
 *
 * @param {object} input
 * @param {object} [opts]
 */
async function createDelegation(input = {}, opts = {}) {
  const store = resolveStore(opts);
  const registry = opts.registry || createDefaultCapabilityRegistry();
  const authorizedTenantId = input.authorizedTenantId || input.tenantId;
  const record = buildDelegationRecord(authorizedTenantId, input);
  const capability = registry.get(record.specialist, record.capability);
  const decision = validateDelegationAuthority({
    delegation: record,
    capability,
    tenantPolicy: opts.tenantPolicy || input.tenantPolicy,
  });

  if (!decision.ok) {
    record.status = decision.events.some((e) => e.kind === 'unknown_capability')
      ? 'rejected'
      : 'declined_policy';
    record.policyEvents = decision.events;
    const saved = await store.insertDelegation(record);
    const err = new SpecialistDelegationError(
      decision.events[0] && decision.events[0].kind
        ? decision.events[0].kind
        : 'declined_policy',
      (decision.events[0] && decision.events[0].message) ||
        'Delegation declined by policy.',
      403,
      { delegation: toPublicDelegation(saved), policyEvents: decision.events }
    );
    err.delegation = toPublicDelegation(saved);
    throw err;
  }

  record.status = 'authorized';
  record.policyEvents = decision.events;
  return toPublicDelegation(await store.insertDelegation(record));
}

function buildPolicyResult(delegation, events, status = 'declined_policy') {
  const now = nowIso();
  return {
    id: newId(),
    delegationId: delegation.id,
    tenantId: delegation.tenantId,
    specialist: delegation.specialist,
    capability: delegation.capability,
    status,
    summary:
      events[0] && events[0].message
        ? events[0].message
        : 'Delegation declined by policy.',
    observations: [],
    actionsTaken: [],
    evidenceRefs: [],
    artifactRefs: [],
    confidence: null,
    uncertainties: [],
    recommendedNextAction: null,
    policyEvents: events,
    errors: events.map((e) => ({ code: e.kind, message: e.message })),
    startedAt: now,
    completedAt: now,
  };
}

/**
 * Execute an authorized delegation through its adapter.
 * Does not spawn further delegations from recommendedNextAction.
 *
 * @param {object} input
 * @param {object} [opts]
 */
async function executeDelegation(input = {}, opts = {}) {
  const store = resolveStore(opts);
  const registry = opts.registry || createDefaultCapabilityRegistry();
  const tenantId = assertAuthorizedTenant(
    input.authorizedTenantId || input.tenantId,
    input.tenantId
  );
  const delegationId = asText(input.delegationId || input.id);
  if (!delegationId) {
    throw new SpecialistDelegationError('invalid_delegation', 'delegationId is required.');
  }

  const delegation = await store.getDelegation(delegationId, tenantId);
  if (!delegation) {
    throw new SpecialistDelegationError('not_found', 'Delegation not found for tenant.', 404);
  }

  if (delegation.status === 'declined_policy' || delegation.status === 'rejected') {
    const result = buildPolicyResult(delegation, delegation.policyEvents || []);
    const saved = await store.insertResult(result);
    return toPublicResult(saved);
  }

  const capability = registry.get(delegation.specialist, delegation.capability);
  if (!capability) {
    const events = [
      policyEvent(
        'unknown_capability',
        `Unknown capability ${delegation.specialist}/${delegation.capability}.`
      ),
    ];
    const result = buildPolicyResult(delegation, events, 'declined_policy');
    await store.updateDelegation({
      ...delegation,
      status: 'declined_policy',
      policyEvents: events,
      updatedAt: nowIso(),
    });
    return toPublicResult(await store.insertResult(result));
  }

  if (!capability.callable) {
    const events = [
      policyEvent(
        'adapter_unavailable',
        `No callable adapter for ${capability.specialist}/${capability.capability}.`
      ),
    ];
    const now = nowIso();
    await store.updateDelegation({
      ...delegation,
      status: 'blocked',
      policyEvents: events,
      updatedAt: now,
    });
    const result = {
      id: newId(),
      delegationId: delegation.id,
      tenantId: delegation.tenantId,
      specialist: delegation.specialist,
      capability: delegation.capability,
      status: 'blocked',
      summary: events[0].message,
      observations: [],
      actionsTaken: [],
      evidenceRefs: [],
      artifactRefs: [],
      confidence: null,
      uncertainties: ['Specialist adapter is not wired.'],
      recommendedNextAction: null,
      policyEvents: events,
      errors: [{ code: 'adapter_unavailable', message: events[0].message }],
      startedAt: now,
      completedAt: now,
    };
    return toPublicResult(await store.insertResult(result));
  }

  const adapter = resolveAdapter(capability);
  if (!adapter) {
    const events = [
      policyEvent(
        'adapter_unavailable',
        `Adapter "${capability.adapter}" is not registered.`
      ),
    ];
    const result = buildPolicyResult(delegation, events, 'blocked');
    await store.updateDelegation({
      ...delegation,
      status: 'blocked',
      policyEvents: events,
      updatedAt: nowIso(),
    });
    return toPublicResult(await store.insertResult(result));
  }

  await store.updateDelegation({
    ...delegation,
    status: 'running',
    updatedAt: nowIso(),
  });

  let raw;
  try {
    raw = adapter(delegation, {
      mode: opts.fixtureMode || delegation._fixtureMode || input.fixtureMode,
      ...(opts.adapterOpts || {}),
    });
    if (raw && typeof raw.then === 'function') raw = await raw;
  } catch (err) {
    const now = nowIso();
    const result = {
      id: newId(),
      delegationId: delegation.id,
      tenantId: delegation.tenantId,
      specialist: delegation.specialist,
      capability: delegation.capability,
      status: 'failed',
      summary: 'Specialist execution failed.',
      observations: [],
      actionsTaken: [],
      evidenceRefs: [],
      artifactRefs: [],
      confidence: null,
      uncertainties: [],
      recommendedNextAction: { type: 'retry', text: 'Retry the delegation.' },
      policyEvents: [],
      errors: [{ code: 'adapter_error', message: err.message || String(err) }],
      startedAt: now,
      completedAt: now,
    };
    await store.updateDelegation({
      ...delegation,
      status: 'failed',
      updatedAt: now,
    });
    return toPublicResult(await store.insertResult(result));
  }

  const result = {
    id: newId(),
    delegationId: delegation.id,
    tenantId: delegation.tenantId,
    specialist: delegation.specialist,
    capability: delegation.capability,
    status: RESULT_STATUSES.includes(raw.status) ? raw.status : 'failed',
    summary: asText(raw.summary),
    observations: normalizeStringRecords(raw.observations),
    actionsTaken: normalizeStringRecords(raw.actionsTaken),
    evidenceRefs: normalizeEvidenceRefs(raw.evidenceRefs),
    artifactRefs: Array.isArray(raw.artifactRefs) ? clone(raw.artifactRefs) : [],
    confidence: raw.confidence == null ? null : Number(raw.confidence),
    uncertainties: Array.isArray(raw.uncertainties)
      ? raw.uncertainties.map((u) => (typeof u === 'string' ? u : asText(u && u.text))).filter(Boolean)
      : [],
    recommendedNextAction: raw.recommendedNextAction || null,
    policyEvents: Array.isArray(raw.policyEvents) ? raw.policyEvents : [],
    errors: Array.isArray(raw.errors) ? raw.errors : [],
    startedAt: raw.startedAt || nowIso(),
    completedAt: raw.completedAt || nowIso(),
    payload: isPlainObject(raw.payload) ? clone(raw.payload) : {},
  };

  const terminal = RESULT_STATUSES.includes(result.status) ? result.status : 'failed';
  await store.updateDelegation({
    ...delegation,
    status: terminal,
    updatedAt: nowIso(),
  });

  return toPublicResult(await store.insertResult(result));
}

/**
 * Create + execute in one Max-owned step. Still does not recurse.
 */
async function delegateAndExecute(input = {}, opts = {}) {
  const store = resolveStore(opts);
  const merged = { ...opts, store };
  let delegation;
  try {
    delegation = await createDelegation(input, merged);
  } catch (err) {
    if (err instanceof SpecialistDelegationError && err.delegation) {
      const result = await executeDelegation(
        {
          authorizedTenantId: err.delegation.tenantId,
          delegationId: err.delegation.id,
        },
        merged
      );
      return { delegation: err.delegation, result, spawned: [] };
    }
    throw err;
  }
  const result = await executeDelegation(
    {
      authorizedTenantId: delegation.tenantId,
      delegationId: delegation.id,
      fixtureMode: input.fixtureMode,
    },
    merged
  );
  return { delegation, result, spawned: [] };
}

async function evaluateResult(input = {}, opts = {}) {
  const store = resolveStore(opts);
  const tenantId = assertAuthorizedTenant(
    input.authorizedTenantId || input.tenantId,
    input.tenantId
  );
  const result =
    input.result ||
    (input.resultId
      ? await store.getResult(input.resultId, tenantId)
      : input.delegationId
        ? await store.getResultByDelegation(input.delegationId, tenantId)
        : null);
  if (!result) {
    throw new SpecialistDelegationError('not_found', 'Result not found for tenant.', 404);
  }
  const delegation =
    input.delegation || (await store.getDelegation(result.delegationId, tenantId));
  if (!delegation) {
    throw new SpecialistDelegationError(
      'not_found',
      'Delegation not found for tenant.',
      404
    );
  }

  const evaluation = evaluateSpecialistResult({
    delegation,
    result,
    operatorDirection: input.operatorDirection,
    suggestedPriorityChange: input.suggestedPriorityChange,
  });
  evaluation.id = newId();
  evaluation.createdAt = nowIso();
  const saved = await store.insertEvaluation(evaluation);
  return clone(saved);
}

/**
 * Max-only explicit apply of a suggested priority change.
 * Specialists have no access to this function through adapters.
 */
async function applyEvaluationPriority(input = {}, opts = {}) {
  const store = resolveStore(opts);
  const tenantId = assertAuthorizedTenant(
    input.authorizedTenantId || input.tenantId,
    input.tenantId
  );
  const evaluation = await store.getEvaluation(input.evaluationId, tenantId);
  if (!evaluation) {
    throw new SpecialistDelegationError('not_found', 'Evaluation not found for tenant.', 404);
  }
  if (!evaluation.suggestedPriorityChange) {
    throw new SpecialistDelegationError(
      'no_priority_change',
      'Evaluation does not suggest a Command Deck priority change.'
    );
  }
  if (!opts.priorityApplier || typeof opts.priorityApplier !== 'function') {
    throw new SpecialistDelegationError(
      'priority_applier_required',
      'Max must supply a priority applier — specialists cannot mutate Command Deck priority.'
    );
  }
  const applied = await opts.priorityApplier({
    tenantId,
    domainId: evaluation.suggestedPriorityChange.domain,
    computedPriority: evaluation.suggestedPriorityChange.to,
    reason: evaluation.explanation,
    evidenceRefs:
      (evaluation.provenance && evaluation.provenance.evidence) || [],
    evaluationId: evaluation.id,
    delegationId: evaluation.delegationId,
    resultId: evaluation.resultId,
  });
  evaluation.priorityApplied = true;
  evaluation.payload = {
    ...(evaluation.payload || {}),
    priorityApplyResult: applied || { applied: true },
  };
  if (typeof store.insertEvaluation === 'function') {
    evaluation.id = newId();
    evaluation.createdAt = nowIso();
    return store.insertEvaluation(evaluation);
  }
  return evaluation;
}

async function getDelegation(id, opts = {}) {
  const store = resolveStore(opts);
  const tenantId = assertAuthorizedTenant(opts.authorizedTenantId || opts.tenantId);
  return toPublicDelegation(await store.getDelegation(id, tenantId));
}

async function getResult(id, opts = {}) {
  const store = resolveStore(opts);
  const tenantId = assertAuthorizedTenant(opts.authorizedTenantId || opts.tenantId);
  return toPublicResult(await store.getResult(id, tenantId));
}

async function getResultForDelegation(delegationId, opts = {}) {
  const store = resolveStore(opts);
  const tenantId = assertAuthorizedTenant(opts.authorizedTenantId || opts.tenantId);
  return toPublicResult(await store.getResultByDelegation(delegationId, tenantId));
}

async function getEvaluation(id, opts = {}) {
  const store = resolveStore(opts);
  const tenantId = assertAuthorizedTenant(opts.authorizedTenantId || opts.tenantId);
  return clone(await store.getEvaluation(id, tenantId));
}

async function listDelegations(filter = {}, opts = {}) {
  const store = resolveStore(opts);
  const tenantId = assertAuthorizedTenant(
    filter.authorizedTenantId || opts.authorizedTenantId || filter.tenantId
  );
  return store.listDelegations({ ...filter, tenantId });
}

async function traceProvenance(input = {}, opts = {}) {
  const store = resolveStore(opts);
  const tenantId = assertAuthorizedTenant(
    input.authorizedTenantId || opts.authorizedTenantId || input.tenantId
  );
  let evaluation = input.evaluation || null;
  let result = input.result || null;
  let delegation = input.delegation || null;

  if (!evaluation && input.evaluationId) {
    evaluation = await store.getEvaluation(input.evaluationId, tenantId);
  }
  if (!result && (input.resultId || (evaluation && evaluation.resultId))) {
    result = await store.getResult(input.resultId || evaluation.resultId, tenantId);
  }
  if (!delegation && (input.delegationId || (result && result.delegationId) || (evaluation && evaluation.delegationId))) {
    delegation = await store.getDelegation(
      input.delegationId ||
        (result && result.delegationId) ||
        (evaluation && evaluation.delegationId),
      tenantId
    );
  }
  if (!result && delegation) {
    result = await store.getResultByDelegation(delegation.id, tenantId);
  }
  if (!evaluation && result) {
    const list = await store.listEvaluations({
      tenantId,
      resultId: result.id,
    });
    evaluation = list[0] || null;
  }

  const chain = buildProvenanceChain({ evaluation, result, delegation });
  return {
    chain,
    narrative: formatProvenanceNarrative(chain),
    evaluation,
    result: toPublicResult(result),
    delegation: toPublicDelegation(delegation),
  };
}

/**
 * Compose inspectable cognitive traces from persisted delegations/results/evaluations.
 * Tenant-scoped. Does not invent missing work.
 */
async function listRecentCognitiveTraces(filter = {}, opts = {}) {
  const store = resolveStore(opts);
  const tenantId = assertAuthorizedTenant(
    filter.authorizedTenantId || opts.authorizedTenantId || filter.tenantId
  );
  const delegations = await store.listDelegations({
    tenantId,
    specialist: filter.specialist,
    capability: filter.capability,
    limit: filter.limit != null ? filter.limit : 12,
  });
  const traces = [];
  for (const row of delegations) {
    const delegation = toPublicDelegation(row);
    const result = toPublicResult(await store.getResultByDelegation(delegation.id, tenantId));
    let evaluation = null;
    if (typeof store.listEvaluations === 'function') {
      const list = await store.listEvaluations({
        tenantId,
        delegationId: delegation.id,
        resultId: result && result.id,
      });
      evaluation = list[0] || null;
    }
    traces.push(
      composeCognitiveTrace({
        tenantId,
        sessionId: filter.sessionId,
        operatorObjective: delegation.objective,
        operatorQuestion: filter.operatorQuestion,
        availableContext: delegation.availableContext,
        delegation,
        result,
        evaluation,
      })
    );
  }
  return traces;
}

async function retrieveCognitiveTrace(input = {}, opts = {}) {
  const store = resolveStore(opts);
  const tenantId = assertAuthorizedTenant(
    input.authorizedTenantId || opts.authorizedTenantId || input.tenantId
  );
  const traces = await listRecentCognitiveTraces(
    {
      authorizedTenantId: tenantId,
      tenantId,
      specialist: input.specialist,
      capability: input.capability,
      sessionId: input.sessionId,
      operatorQuestion: input.question,
      limit: input.limit != null ? input.limit : 12,
    },
    { ...opts, store }
  );
  return resolveRecentReferent({
    traces,
    question: input.question,
    specialist: input.specialist,
    domain: input.domain,
    objective: input.objective,
    recentSpecialist: input.recentSpecialist,
    conversationMentions: input.conversationMentions,
  });
}

function createSpecialistDelegationService(options = {}) {
  const store = options.store || createMemoryStore();
  const registry = options.registry || createDefaultCapabilityRegistry();
  const tenantPolicy = options.tenantPolicy || null;
  const base = { store, registry, tenantPolicy };

  return {
    store,
    registry,
    createDelegation: (input, opts) => createDelegation(input, { ...base, ...opts }),
    executeDelegation: (input, opts) => executeDelegation(input, { ...base, ...opts }),
    delegateAndExecute: (input, opts) => delegateAndExecute(input, { ...base, ...opts }),
    evaluateResult: (input, opts) => evaluateResult(input, { ...base, ...opts }),
    applyEvaluationPriority: (input, opts) =>
      applyEvaluationPriority(input, { ...base, ...opts }),
    getDelegation: (id, opts) => getDelegation(id, { ...base, ...opts }),
    getResult: (id, opts) => getResult(id, { ...base, ...opts }),
    getResultForDelegation: (id, opts) => getResultForDelegation(id, { ...base, ...opts }),
    getEvaluation: (id, opts) => getEvaluation(id, { ...base, ...opts }),
    listDelegations: (filter, opts) => listDelegations(filter, { ...base, ...opts }),
    traceProvenance: (input, opts) => traceProvenance(input, { ...base, ...opts }),
    listRecentCognitiveTraces: (filter, opts) =>
      listRecentCognitiveTraces(filter, { ...base, ...opts }),
    retrieveCognitiveTrace: (input, opts) =>
      retrieveCognitiveTrace(input, { ...base, ...opts }),
    composeCognitiveTrace,
    classifyOperatorIntent,
    resolveRecentReferent,
    looksLikeInterrogation,
    looksLikeNewInvestigation,
    formatDisambiguation,
    answerFromTrace,
    limitationAnswer,
    inspectionSummary,
    INTENT,
    FAILURE_BOUNDARIES,
    COGNITIVE_MODES,
    NEVER_DELEGATE_MODES,
    classifyCognitiveMode,
    forbidsSpecialistDelegation,
    looksLikeInvestigation,
    looksLikeSummary,
    looksLikeCompletedRetrieval,
    UNKNOWN_ANSWER,
    mayCreateDelegation,
    mayEnterSpecialistPath,
    meetsInvestigationThreshold,
    shouldInvokeSpecialist,
  };
}

module.exports = {
  AUTHORITY_LEVELS,
  DELEGATION_STATUSES,
  RESULT_STATUSES,
  DEFAULT_CAPABILITIES,
  DEFAULT_TENANT_DELEGATION_POLICY,
  CONTRACT_OBJECTIVE,
  DEFAULT_EVIDENCE,
  SpecialistDelegationError,
  createMemoryStore,
  createPostgresStore,
  createDefaultCapabilityRegistry,
  createCapabilityRegistry,
  createSpecialistDelegationService,
  createDelegation,
  executeDelegation,
  delegateAndExecute,
  evaluateResult,
  applyEvaluationPriority,
  getDelegation,
  getResult,
  getResultForDelegation,
  getEvaluation,
  listDelegations,
  traceProvenance,
  listRecentCognitiveTraces,
  retrieveCognitiveTrace,
  composeCognitiveTrace,
  classifyFailureBoundary,
  inspectionSummary,
  FAILURE_BOUNDARIES,
  EVIDENCE_LAYERS,
  captureAvailableContext,
  projectAvailableContext,
  projectSuppliedContext,
  projectConsumedContext,
  classifyOperatorIntent,
  resolveRecentReferent,
  looksLikeInterrogation,
  looksLikeNewInvestigation,
  formatDisambiguation,
  INTENT,
  answerFromTrace,
  limitationAnswer,
  COGNITIVE_MODES,
  NEVER_DELEGATE_MODES,
  classifyCognitiveMode,
  forbidsSpecialistDelegation,
  looksLikeInvestigation,
  looksLikeSummary,
  looksLikeCompletedRetrieval,
  UNKNOWN_ANSWER,
  mayCreateDelegation,
  mayEnterSpecialistPath,
  meetsInvestigationThreshold,
  shouldInvokeSpecialist,
  evaluateSpecialistResult,
  formatOperatorExplanation,
  normalizeOperatorDirection,
  buildProvenanceChain,
  formatProvenanceNarrative,
  validateDelegationAuthority,
  normalizeTenantPolicy,
  isTestIntelligence,
  runTestIntelligence,
};
