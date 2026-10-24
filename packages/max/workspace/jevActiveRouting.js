'use strict';

/**
 * SPEC-JEV-006 — Apply guarded Jev active routing promotion after production ownership.
 */

const { randomUUID } = require('node:crypto');
const {
  assessJevDecisionForActivePromotion,
  ownerToRouteVocabulary,
  buildActiveRoutingAuditRow,
} = require('../../decision-service/activeRoutingPromotion');
const { WORKSPACE_OWNERS } = require('./WorkspaceOwnershipResolver');
const { PRIMARY_OBJECTIVES } = require('./PrimaryObjective');
const {
  classifyPendingDecisionCaptureIntent,
  CAPTURE_INTENTS,
} = require('./pendingDecisionCaptureGuard');
const { resolveTenantId } = require('./WorkspaceMissionInspection');
const { resolveAcquisitionMissionRuntime } = require('../../../services/acquisitionMissionRuntime');

const PROMOTABLE_OWNERS = Object.freeze([
  WORKSPACE_OWNERS.REASONING,
  WORKSPACE_OWNERS.KNOWLEDGE_RETRIEVAL,
  WORKSPACE_OWNERS.CONVERSATION_LAYER,
  WORKSPACE_OWNERS.REFLECTION,
]);

function hasAcquisitionMissionContext(input = {}) {
  const session = input.session || null;
  const ctx = input.context || (session && session.context) || {};
  if (ctx.missionId || ctx.acquisitionMissionId || ctx.acquisitionOwner) return true;
  const tenantId = resolveTenantId(input);
  const runtime = resolveAcquisitionMissionRuntime(input);
  const engine = runtime.engine();
  if (!engine || !tenantId) return false;
  const missions = typeof engine.list === 'function' ? engine.list(tenantId) : [];
  return missions.some((row) => row && row.stage !== 'improve');
}

function mapJevRouteToWorkspaceOwner(jevRoute, hasMissionContext) {
  if (jevRoute === 'inspection' && hasMissionContext) {
    return WORKSPACE_OWNERS.MISSION_INSPECTION;
  }
  if (jevRoute === 'intelligence') {
    return WORKSPACE_OWNERS.KNOWLEDGE_RETRIEVAL;
  }
  return null;
}

function canPromoteFromProductionOwner(workspaceOwnership, { question, operatorIntent, session }) {
  const owner = workspaceOwnership?.owner;
  const reason = workspaceOwnership?.reason;
  if (!owner) return false;
  if (owner === WORKSPACE_OWNERS.MISSION_INSPECTION) return false;
  if (PROMOTABLE_OWNERS.includes(owner)) return true;
  if (
    owner === WORKSPACE_OWNERS.ACTIVE_MISSION &&
    reason === 'pending_decision_turn_ownership'
  ) {
    const mission = operatorIntent?.mission || null;
    const pendingDecision = mission?.pendingOperatorDecision || null;
    const classification = classifyPendingDecisionCaptureIntent({
      message: question,
      pendingDecision,
      shadowDecision: operatorIntent?.shadowDecision,
    });
    return classification === CAPTURE_INTENTS.INSPECTION_OR_STATUS_QUESTION;
  }
  return false;
}

function defaultActiveRoutingAudit(row) {
  if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'test') {
    console.info('[DECISION_ACTIVE_ROUTING]', JSON.stringify(row));
  }
}

/**
 * @param {object} input
 * @param {object} input.workspaceOwnership
 * @param {object|null} input.evaluation — DecisionService.evaluateActiveRouting result
 * @param {string} input.question
 * @param {object} input.session
 * @param {object} [input.operatorIntent]
 * @param {object} [input.context]
 * @param {Function} [input.audit]
 */
function applyJevActiveRoutingPromotion(input = {}) {
  const audit = input.audit || defaultActiveRoutingAudit;
  const workspaceOwnership = input.workspaceOwnership;
  const evaluation = input.evaluation;
  const productionOwner = workspaceOwnership?.owner || null;
  const productionRoute = ownerToRouteVocabulary(productionOwner);
  const decisionId = evaluation?.decision_id || randomUUID();

  if (!evaluation || !evaluation.decision) {
    const row = buildActiveRoutingAuditRow({
      decisionId,
      productionOwner,
      productionRoute,
      blockedReason: evaluation?.blockedReason || 'jev_unavailable',
      promoted: false,
      selectedRoute: productionRoute,
    });
    audit(row);
    return { applied: false, workspaceOwnership, audit: row, objectivePatch: null };
  }

  const decision = evaluation.decision;
  const assessment = evaluation.assessment || assessJevDecisionForActivePromotion(decision);
  const hasMissionContext = hasAcquisitionMissionContext(input);
  const targetOwner = assessment.eligible
    ? mapJevRouteToWorkspaceOwner(decision.recommended_route, hasMissionContext)
    : null;

  if (
    !assessment.eligible ||
    !targetOwner ||
    !canPromoteFromProductionOwner(workspaceOwnership, input)
  ) {
    const row = buildActiveRoutingAuditRow({
      decisionId,
      productionOwner,
      productionRoute,
      jevRoute: decision.recommended_route,
      jevIntent: decision.intent,
      jevConfidence: decision.confidence,
      selectedRoute: productionRoute,
      promoted: false,
      blockedReason:
        assessment.reason ||
        (targetOwner ? 'production_owner_not_promotable' : 'no_target_owner'),
    });
    audit(row);
    return { applied: false, workspaceOwnership, audit: row, objectivePatch: null };
  }

  if (productionOwner === targetOwner) {
    const row = buildActiveRoutingAuditRow({
      decisionId,
      productionOwner,
      productionRoute,
      jevRoute: decision.recommended_route,
      jevIntent: decision.intent,
      jevConfidence: decision.confidence,
      selectedRoute: ownerToRouteVocabulary(targetOwner),
      promoted: false,
      blockedReason: 'already_routed',
    });
    audit(row);
    return { applied: false, workspaceOwnership, audit: row, objectivePatch: null };
  }

  const promotedOwnership = {
    ...workspaceOwnership,
    owner: targetOwner,
    reason: 'jev_active_routing_promotion',
    confidence: decision.confidence,
    jevPromotion: true,
    previousOwner: productionOwner,
    previousReason: workspaceOwnership.reason,
  };

  const selectedRoute = ownerToRouteVocabulary(targetOwner);
  const row = buildActiveRoutingAuditRow({
    decisionId,
    productionOwner,
    productionRoute,
    jevRoute: decision.recommended_route,
    jevIntent: decision.intent,
    jevConfidence: decision.confidence,
    selectedRoute,
    promoted: true,
    blockedReason: null,
  });
  audit(row);

  const objectivePatch =
    targetOwner === WORKSPACE_OWNERS.MISSION_INSPECTION
      ? {
          primaryObjective: PRIMARY_OBJECTIVES.MISSION_INSPECTION,
          routingDecision: {
            owner: WORKSPACE_OWNERS.MISSION_INSPECTION,
            pipeline: 'MissionRuntime',
            reason: 'jev_active_routing_promotion',
          },
        }
      : null;

  return {
    applied: true,
    workspaceOwnership: promotedOwnership,
    audit: row,
    objectivePatch,
  };
}

module.exports = {
  applyJevActiveRoutingPromotion,
  canPromoteFromProductionOwner,
  hasAcquisitionMissionContext,
  mapJevRouteToWorkspaceOwner,
};
