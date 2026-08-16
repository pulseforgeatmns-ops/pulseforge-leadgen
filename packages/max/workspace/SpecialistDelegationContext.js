'use strict';

/**
 * SPEC-098 — Max Workspace adapter for specialist delegation.
 *
 * Max-side creation and consumption only. Specialists do not receive the
 * full workspace session. Command Deck priority is never mutated here.
 */

const specialistDelegation = require('../../../services/specialistDelegation');
const { buildStructuredResponse } = require('./WorkspaceTypes');

function defaultService(input = {}) {
  return input.delegationService || specialistDelegation;
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
      ''
  ).trim();
}

function resolveOpts(input = {}) {
  return input.delegationOpts || {};
}

/**
 * Max creates a bounded delegation from workspace context.
 * Only the supplied envelope is forwarded — never the full session.
 *
 * @param {object} input
 */
async function createBoundedDelegation(input = {}) {
  const service = defaultService(input);
  const tenantId = resolveTenantId(input);
  if (!tenantId) {
    throw new specialistDelegation.SpecialistDelegationError(
      'tenant_required',
      'Workspace tenant is required to create a specialist delegation.',
      403
    );
  }

  const envelope = {
    authorizedTenantId: tenantId,
    tenantId,
    specialist: input.specialist,
    capability: input.capability,
    objective: input.objective,
    reason: input.reason,
    businessContext: input.businessContext || {},
    targetContext: input.targetContext || null,
    evidenceRefs: input.evidenceRefs || [],
    constraints: input.constraints || {},
    authority: input.authority,
    expectedReturn: input.expectedReturn,
    requestedBy: input.requestedBy || 'max',
    fixtureMode: input.fixtureMode,
    tenantPolicy: input.tenantPolicy,
  };

  return service.createDelegation(envelope, resolveOpts(input));
}

/**
 * Max executes a previously created delegation and persists the result.
 */
async function executeBoundedDelegation(input = {}) {
  const service = defaultService(input);
  const tenantId = resolveTenantId(input);
  return service.executeDelegation(
    {
      authorizedTenantId: tenantId,
      tenantId,
      delegationId: input.delegationId,
      fixtureMode: input.fixtureMode,
    },
    resolveOpts(input)
  );
}

/**
 * Max consumes a specialist result as evidence — never as ground truth.
 * Does not apply Command Deck priority.
 */
async function consumeSpecialistResult(input = {}) {
  const service = defaultService(input);
  const tenantId = resolveTenantId(input);
  const evaluation = await service.evaluateResult(
    {
      authorizedTenantId: tenantId,
      tenantId,
      resultId: input.resultId,
      delegationId: input.delegationId,
      result: input.result,
      delegation: input.delegation,
      operatorDirection: input.operatorDirection,
    },
    resolveOpts(input)
  );

  if (input.session && input.session.context && typeof input.session.context === 'object') {
    input.session.context.lastSpecialistEvaluation = {
      id: evaluation.id,
      delegationId: evaluation.delegationId,
      resultId: evaluation.resultId,
      objectiveSatisfied: evaluation.objectiveSatisfied,
      materialChange: evaluation.materialChange,
    };
  }

  return evaluation;
}

/**
 * Operator-facing explanation of a material conclusion.
 */
async function explainSpecialistTrail(input = {}) {
  const service = defaultService(input);
  const tenantId = resolveTenantId(input);
  return service.traceProvenance(
    {
      authorizedTenantId: tenantId,
      tenantId,
      evaluationId: input.evaluationId,
      resultId: input.resultId,
      delegationId: input.delegationId,
    },
    resolveOpts(input)
  );
}

function workspaceStructured(answer, reasoning, extras = {}) {
  return buildStructuredResponse({
    answer,
    reasoning,
    supportingEvidence: extras.supportingEvidence || [],
    contradictingEvidence: [],
    confidence: extras.confidence != null ? extras.confidence : 0.85,
    nextInvestigations: extras.nextInvestigations || [],
    recommendedActions: extras.recommendedActions || [
      { id: 'acknowledge', type: 'review', label: 'Continue' },
    ],
    confidenceContributors: ['specialist_delegation', 'spec_098'],
    timelineReferences: [],
    relatedEntities: extras.relatedEntities || [],
    metadata: {
      sourcesUsed: {
        briefing: false,
        reasoning: true,
        memory: true,
        policy: true,
        knowledge: false,
      },
      evidenceCount: extras.evidenceCount || 0,
      asOf: new Date().toISOString(),
      unavailable: [],
      delegationId: extras.delegationId || null,
      resultId: extras.resultId || null,
    },
  });
}

module.exports = {
  createBoundedDelegation,
  executeBoundedDelegation,
  consumeSpecialistResult,
  explainSpecialistTrail,
  workspaceStructured,
};
