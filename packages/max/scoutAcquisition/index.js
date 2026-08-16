'use strict';

/**
 * SPEC-100 — Max ↔ Scout Acquisition Intelligence Loop.
 *
 * Max decides whether Scout is needed, delegates through SPEC-098,
 * Scout investigates (intelligence only), Max evaluates, AO state may
 * update, and Command Deck priority changes only when Max applies it.
 */

const Types = require('./Types');
const { buildBoundedScoutContext, criteriaFingerprint } = require('./BoundedContext');
const {
  assessScoutNeed,
  looksLikeAcquisitionQuestion,
  looksLikeExplainPriority,
  looksLikeFollowUp,
  looksLikeFindMoreLike,
} = require('./NeedAssessment');
const { retrieveExistingIntelligence, loadRepository } = require('./ExistingIntelligence');
const {
  runScoutAcquisitionIntelligence,
  isScoutAcquisition,
} = require('./ScoutAdapter');
const {
  createMemoryAcquisitionState,
  createPostgresAcquisitionState,
  buildAcquisitionSummary,
  deriveStateFromEvaluation,
  toCommandDeckSignal,
} = require('./AcquisitionState');
const {
  evaluateScoutResult,
  persistScoutEvaluation,
  opportunitiesFromResult,
  shouldApplyPriority,
} = require('./MaxEvaluation');
const {
  formatAcquisitionExplanation,
  formatOpportunityAnswer,
} = require('./Explainability');
const specialistDelegation = require('../specialistDelegation');

function defaultDelegationService(opts = {}) {
  if (opts.delegationService) return opts.delegationService;
  return specialistDelegation.createSpecialistDelegationService(opts);
}

function extractSeedCompanyId(question, opportunities) {
  const ranked = (opportunities || []).slice();
  const num = String(question || '').match(/\bnumber (?:two|2)\b/i);
  if (num && ranked[1]) return ranked[1].companyId;
  const like = String(question || '').match(/\blike\s+(?:number\s+)?(\d+)\b/i);
  if (like && ranked[Number(like[1]) - 1]) return ranked[Number(like[1]) - 1].companyId;
  return ranked[0] ? ranked[0].companyId : null;
}

/**
 * Full Max-owned Scout loop for one operator turn.
 *
 * @param {object} input
 * @param {object} [opts]
 */
async function runAcquisitionIntelligenceLoop(input = {}, opts = {}) {
  const tenantId = Types.asText(input.authorizedTenantId || input.tenantId);
  if (!tenantId) {
    throw new specialistDelegation.SpecialistDelegationError(
      'tenant_required',
      'authorizedTenantId is required for Scout acquisition intelligence.',
      403
    );
  }

  const delegationService = defaultDelegationService(opts);
  const aoStore = opts.aoStore || createMemoryAcquisitionState();
  const priorState = await aoStore.get(tenantId);
  const bounded = buildBoundedScoutContext({
    authorizedTenantId: tenantId,
    tenantId,
    businessContext: input.businessContext,
    targetContext: input.targetContext,
    approvedUnderstanding: input.approvedUnderstanding,
    operatorDirection: input.operatorDirection,
    priorDelegationId: input.priorDelegationId,
    priorResultId: input.priorResultId,
    seedCompanyId: input.seedCompanyId,
    notes: input.notes,
  });

  const existingRepo = await loadRepository({
    authorizedTenantId: tenantId,
    tenantId,
    targetContext: bounded.targetContext,
    businessContext: bounded.businessContext,
    companies: opts.companies,
    people: opts.people,
    loadCompanies: opts.loadCompanies,
  });

  const recentDelegations = await delegationService.listDelegations(
    { authorizedTenantId: tenantId, tenantId, specialist: Types.SCOUT_SPECIALIST, limit: 12 },
    opts.delegationOpts || {}
  );
  const recentResults = [];
  for (const row of recentDelegations) {
    const result = await delegationService.getResultForDelegation(row.id, {
      authorizedTenantId: tenantId,
      ...(opts.delegationOpts || {}),
    });
    if (result) {
      recentResults.push({
        ...result,
        objective: row.objective,
        specialist: row.specialist,
        capability: row.capability,
        targetContext: row.targetContext,
        businessContext: row.businessContext,
        criteriaFingerprint: criteriaFingerprint(row.targetContext, row.businessContext),
      });
    }
  }

  const need = assessScoutNeed({
    question: input.question,
    objective: input.objective,
    reason: input.reason,
    context: input.context,
    targetContext: bounded.targetContext,
    businessContext: bounded.businessContext,
    existingIntelligence: priorState
      ? { ...priorState, sufficient: priorState.opportunityCount > 0 }
      : { ...existingRepo, sufficient: false },
    recentResults,
    freshnessMs: opts.freshnessMs,
    now: opts.now,
    force: input.force === true,
  });

  if (need.kind === 'explain') {
    const trail = await delegationService.traceProvenance(
      {
        authorizedTenantId: tenantId,
        evaluationId: priorState && priorState.evaluationId,
        resultId: priorState && priorState.resultId,
        delegationId: priorState && priorState.delegationId,
      },
      opts.delegationOpts || {}
    );
    const explained = formatAcquisitionExplanation({
      evaluation: trail.evaluation,
      result: trail.result,
      delegation: trail.delegation,
      state: priorState,
    });
    return {
      delegated: false,
      kind: 'explain',
      need,
      prose: explained.narrative,
      trail,
      state: priorState,
      outboundInvoked: [],
    };
  }

  if (!need.needed && (need.kind === 'followup' || need.kind === 'reuse')) {
    const opportunities =
      (priorState && priorState.opportunities) ||
      opportunitiesFromResult(need.reuse) ||
      [];
    const prose = looksLikeFollowUp(input.question)
      ? formatOpportunityAnswer({ question: input.question, opportunities })
      : (need.reuse && need.reuse.summary) ||
        (priorState && priorState.summary) ||
        'Current acquisition intelligence is already sufficient.';
    return {
      delegated: false,
      kind: need.kind,
      need,
      prose,
      state: priorState,
      reuse: need.reuse,
      outboundInvoked: [],
    };
  }

  if (!need.needed) {
    return {
      delegated: false,
      kind: need.kind,
      need,
      prose: null,
      state: priorState,
      outboundInvoked: [],
    };
  }

  if (looksLikeFindMoreLike(input.question) && priorState) {
    bounded.targetContext.seedCompanyId =
      bounded.targetContext.seedCompanyId ||
      extractSeedCompanyId(input.question, priorState.opportunities);
    bounded.targetContext.priorResultId = priorState.resultId;
    bounded.targetContext.priorDelegationId = priorState.delegationId;
  }

  const { delegation, result } = await delegationService.delegateAndExecute(
    {
      authorizedTenantId: tenantId,
      tenantId,
      specialist: Types.SCOUT_SPECIALIST,
      capability: Types.SCOUT_CAPABILITY,
      objective:
        Types.asText(input.objective) ||
        String(input.question || 'Identify acquisition opportunities for the current objective.'),
      reason: need.reason,
      authority: input.authority || 'observe',
      expectedReturn: {
        type: 'acquisition_intelligence',
        requireEvidence: true,
        requireConfidence: true,
        requireRecommendation: true,
      },
      businessContext: bounded.businessContext,
      targetContext: bounded.targetContext,
      evidenceRefs: input.evidenceRefs || [],
      constraints: {
        geography: bounded.targetContext.geography,
        targetSegments: bounded.targetContext.segments,
        requiredDeterminate: bounded.targetContext.geography ? ['geography'] : [],
      },
      requestedBy: 'max',
      fixtureMode: input.fixtureMode,
    },
    {
      ...(opts.delegationOpts || {}),
      store: delegationService.store,
      registry: delegationService.registry,
      adapterOpts: {
        companies: opts.companies,
        people: opts.people,
        loadCompanies: opts.loadCompanies,
        discover: opts.discover,
        mode: input.fixtureMode,
      },
    }
  );

  const evaluation = evaluateScoutResult({
    delegation,
    result,
    priorState,
    operatorDirection:
      input.operatorDirection ||
      (bounded.businessContext && bounded.businessContext.operatorDirection),
  });
  evaluation.id = specialistDelegation.createMemoryStore ? require('crypto').randomUUID() : evaluation.id;
  if (!evaluation.id) evaluation.id = require('crypto').randomUUID();
  evaluation.createdAt = new Date().toISOString();
  if (delegationService.store && typeof delegationService.store.insertEvaluation === 'function') {
    await delegationService.store.insertEvaluation(evaluation);
  }

  let state = priorState;
  if (evaluation.materialChange || result.status === 'completed' || result.status === 'partial') {
    const next = deriveStateFromEvaluation({
      evaluation,
      result,
      opportunities: opportunitiesFromResult(result),
    });
    if (evaluation.materialChange) {
      state = await aoStore.put(next);
    } else if (!priorState) {
      state = await aoStore.put({ ...next, priorityImpact: null, materiality: evaluation.materiality });
    }
  }

  let priorityApply = null;
  if (input.applyPriority === true && shouldApplyPriority(evaluation) && opts.priorityApplier) {
    priorityApply = await specialistDelegation.applyEvaluationPriority(
      { authorizedTenantId: tenantId, evaluationId: evaluation.id },
      {
        store: delegationService.store,
        priorityApplier: opts.priorityApplier,
      }
    );
    evaluation.priorityApplied = true;
  }

  const explained = formatAcquisitionExplanation({
    evaluation,
    result,
    delegation,
    state,
  });

  return {
    delegated: true,
    kind: 'investigated',
    need,
    delegation,
    result,
    evaluation,
    state,
    priorityApply,
    prose: explained.narrative,
    trail: explained,
    outboundInvoked: (result.payload && result.payload.outboundInvoked) || [],
    bounded,
  };
}

module.exports = {
  ...Types,
  buildBoundedScoutContext,
  criteriaFingerprint,
  assessScoutNeed,
  looksLikeAcquisitionQuestion,
  looksLikeExplainPriority,
  looksLikeFollowUp,
  looksLikeFindMoreLike,
  retrieveExistingIntelligence,
  loadRepository,
  runScoutAcquisitionIntelligence,
  isScoutAcquisition,
  createMemoryAcquisitionState,
  createPostgresAcquisitionState,
  buildAcquisitionSummary,
  deriveStateFromEvaluation,
  toCommandDeckSignal,
  evaluateScoutResult,
  persistScoutEvaluation,
  opportunitiesFromResult,
  shouldApplyPriority,
  formatAcquisitionExplanation,
  formatOpportunityAnswer,
  runAcquisitionIntelligenceLoop,
};
