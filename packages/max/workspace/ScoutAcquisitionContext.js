'use strict';

/**
 * SPEC-100 — Max Workspace adapter for the Scout acquisition intelligence loop.
 * Max remains the operator-facing responder. Scout is intelligence-only.
 */

const scoutAcquisition = require('../../../services/scoutAcquisitionIntelligence');
const { buildStructuredResponse } = require('./WorkspaceTypes');

function defaultLoop(input = {}) {
  return input.runLoop || scoutAcquisition.runAcquisitionIntelligenceLoop;
}

function resolveTenantId(input = {}) {
  const session = input.session || null;
  const sessionCtx =
    session && session.context && typeof session.context === 'object'
      ? session.context
      : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};
  return String(
    input.authorizedTenantId ||
      envelope.tenantId ||
      sessionCtx.tenantId ||
      envelope.clientId ||
      sessionCtx.clientId ||
      ''
  ).trim();
}

function shouldHandleScoutAcquisition(input = {}) {
  const question = String(input.question || '').trim();
  if (!question) return false;
  const context = {
    ...(input.session && input.session.context),
    ...(input.context || {}),
    action: input.action || (input.context && input.context.action),
  };
  return scoutAcquisition.looksLikeAcquisitionQuestion(question, context);
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleScoutAcquisitionTurn(input = {}) {
  const question = String(input.question || '').trim();
  if (!question) return null;
  if (!shouldHandleScoutAcquisition(input)) return null;

  const tenantId = resolveTenantId(input);
  if (!tenantId) return null;

  const session = input.session || null;
  const sessionCtx =
    session && session.context && typeof session.context === 'object'
      ? session.context
      : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};

  const runLoop = defaultLoop(input);
  const result = await runLoop(
    {
      authorizedTenantId: tenantId,
      tenantId,
      question,
      objective: envelope.objective || sessionCtx.objective || question,
      reason: envelope.reason,
      authority: envelope.authority || 'observe',
      businessContext: envelope.businessContext || sessionCtx.businessContext,
      targetContext: envelope.targetContext || sessionCtx.targetContext,
      approvedUnderstanding:
        envelope.approvedUnderstanding ||
        (sessionCtx.businessBlueprint && sessionCtx.businessBlueprint.approved) ||
        sessionCtx.approvedUnderstanding,
      operatorDirection: envelope.operatorDirection || sessionCtx.operatorDirection,
      context: {
        ...sessionCtx,
        ...envelope,
        domainId: envelope.domainId || sessionCtx.domainId,
        action: input.action || envelope.action || sessionCtx.action,
        acquisitionLoop: true,
      },
      applyPriority: envelope.applyPriority === true || input.applyPriority === true,
      fixtureMode: input.fixtureMode || envelope.fixtureMode,
    },
    {
      delegationService: input.delegationService,
      delegationOpts: input.delegationOpts,
      aoStore: input.aoStore,
      companies: input.companies,
      people: input.people,
      loadCompanies: input.loadCompanies,
      discover: input.discover,
      priorityApplier: input.priorityApplier,
      freshnessMs: input.freshnessMs,
    }
  );

  if (!result || result.kind === 'unrelated') return null;

  if (session && session.context && typeof session.context === 'object') {
    session.context.acquisitionLoop = true;
    session.context.domainId = session.context.domainId || 'acquisition';
    if (result.evaluation) {
      session.context.lastScoutEvaluation = {
        id: result.evaluation.id,
        materiality: result.evaluation.materiality,
        materialChange: result.evaluation.materialChange,
        delegationId: result.evaluation.delegationId,
        resultId: result.evaluation.resultId,
      };
      session.context.lastSpecialistEvaluation = session.context.lastScoutEvaluation;
    }
    if (result.state) {
      session.context.acquisitionIntelligence = {
        summary: result.state.summary,
        opportunityCount: result.state.opportunityCount,
        timelyCount: result.state.timelyCount,
      };
    }
  }

  const prose =
    result.prose ||
    'I checked current acquisition intelligence before deciding whether Scout needed to investigate.';

  const structured = buildStructuredResponse({
    answer: prose,
    reasoning: [
      result.need && result.need.reason,
      result.delegated
        ? 'Delegated a bounded Scout acquisition_intelligence investigation.'
        : 'Answered from existing durable acquisition intelligence.',
      result.evaluation
        ? `Max evaluation materiality: ${result.evaluation.materiality}.`
        : null,
    ].filter(Boolean),
    supportingEvidence:
      (result.result && result.result.evidenceRefs) ||
      (result.trail && result.trail.chain && result.trail.chain.evidence) ||
      [],
    contradictingEvidence: [],
    confidence:
      result.evaluation && result.result && result.result.confidence != null
        ? result.result.confidence
        : 0.8,
    nextInvestigations: [],
    recommendedActions: [
      { id: 'acknowledge', type: 'review', label: 'Continue' },
    ],
    confidenceContributors: ['scout_acquisition', 'spec_100'],
    timelineReferences: [],
    relatedEntities: [],
    metadata: {
      sourcesUsed: {
        briefing: false,
        reasoning: true,
        memory: true,
        policy: true,
        knowledge: false,
      },
      evidenceCount:
        (result.result && result.result.evidenceRefs && result.result.evidenceRefs.length) ||
        0,
      asOf: new Date().toISOString(),
      unavailable: [],
      delegationId: result.delegation && result.delegation.id,
      resultId: result.result && result.result.id,
      evaluationId: result.evaluation && result.evaluation.id,
      scoutDelegated: result.delegated === true,
      acquisitionLoop: true,
    },
  });

  return {
    reason: result.delegated
      ? 'scout_acquisition_intelligence'
      : `scout_acquisition_${result.kind}`,
    structured,
    prose,
    loop: result,
  };
}

module.exports = {
  shouldHandleScoutAcquisition,
  maybeHandleScoutAcquisitionTurn,
};
