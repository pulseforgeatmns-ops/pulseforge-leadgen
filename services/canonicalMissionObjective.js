'use strict';

/**
 * Resolve tenant acquisition mission objective from durable canonical evidence.
 */

const { loadApprovedClientIntelligence } = require('../packages/max/workspace/ClientIntelligenceContext');
const { resolveCanonicalObjective } = require('../packages/max/workspace/ResolvedObjective');
const { extractCanonicalObjectiveEvidence } = require('../packages/max/workspace/CanonicalObjectiveEvidence');
const { getActiveObjectives } = require('./operatorObjectives');
const { buildClientIntelligenceMissionEvidence } = require('../packages/max/workspace/AcquisitionOwnership');

async function loadTenantMissionObjectiveContext(input = {}) {
  const tenantId = String(input.tenantId || input.clientId || '').trim();
  const clientId = Number(input.clientId != null ? input.clientId : tenantId);
  if (!tenantId || !Number.isFinite(clientId)) {
    const err = new Error('tenantId and clientId are required.');
    err.code = 'tenant_required';
    throw err;
  }

  const pool = input.pool || null;
  const ciLoaded = await loadApprovedClientIntelligence({
    tenantId,
    clientId,
    cieOpts: pool ? { pool } : {},
    propagateLoadErrors: input.propagateLoadErrors === true,
  });

  const operatorObjectives = await getActiveObjectives(
    { tenantId, clientId },
    pool ? { pool } : {}
  );

  const strategicEvidence = buildClientIntelligenceMissionEvidence(ciLoaded.summary);

  return {
    tenantId,
    clientId,
    summary: ciLoaded.summary,
    blueprint: ciLoaded.blueprint,
    operatorObjectives,
    strategicEvidence,
  };
}

function buildResolutionContext(ctx = {}) {
  const blueprintPayload =
    ctx.blueprint && typeof ctx.blueprint === 'object' ? { ...ctx.blueprint } : {};
  if (ctx.strategicEvidence && ctx.strategicEvidence.strategicEvidence) {
    Object.assign(blueprintPayload, ctx.strategicEvidence.strategicEvidence);
  }
  return {
    tenantId: ctx.tenantId,
    clientId: ctx.clientId,
    summary: ctx.summary,
    clientIntelligence: ctx.summary,
    blueprint: blueprintPayload,
    operatorObjectives: ctx.operatorObjectives || [],
  };
}

/**
 * @param {object} input
 * @returns {Promise<{ resolvedObjective: object, evidence: object|null, context: object }>}
 */
async function resolveTenantCanonicalMissionObjective(input = {}) {
  const ctx = input.context || (await loadTenantMissionObjectiveContext(input));
  const evidence = extractCanonicalObjectiveEvidence(buildResolutionContext(ctx));
  const resolvedObjective = resolveCanonicalObjective({
    question: asText(input.question),
    context: buildResolutionContext(ctx),
    targetSegment: input.targetSegment || null,
    executionContract: input.executionContract || null,
  });

  return { resolvedObjective, evidence, context: ctx };
}

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function insufficientObjectiveError(evidence, ctx) {
  const err = new Error('Canonical objective evidence is insufficient for mission planning.');
  err.code = 'canonical_objective_insufficient';
  err.closestEvidence = evidence || extractCanonicalObjectiveEvidence(buildResolutionContext(ctx || {}));
  err.field = err.closestEvidence ? err.closestEvidence.field : 'objective';
  return err;
}

module.exports = {
  loadTenantMissionObjectiveContext,
  resolveTenantCanonicalMissionObjective,
  buildResolutionContext,
  insufficientObjectiveError,
};
