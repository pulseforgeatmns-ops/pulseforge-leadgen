'use strict';

/**
 * SPEC-202 — Pending decision retains conversational turn ownership until resolved
 * or the operator clearly changes subject.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  RESOLUTION_OUTCOMES,
  pendingDecisionOwnsTurn,
} = require('./PendingDecisionResolver');
const { presentableOperatorDecision } = require('../../acquisition-mission/PendingOperatorDecision');
const { OPERATOR_DECISION_KINDS } = require('../../acquisition-mission/types');
const {
  buildMissionCommunication,
  applyMissionCommunication,
  formatMissionProse,
} = require('./MissionCommunication');
const {
  ensureReadyExecutionReview,
  summarizeReadyExecutionTargets,
  summarizeReadyExecutionMessage,
  summarizeReadyExecutionQueue,
  summarizeReadyExecutionSafety,
  readyExecutionApprovalPrompt,
} = require('./AcquisitionMissionExecution');
const askPathTrace = require('./audit/AskPathTrace');
const { resolveAcquisitionActiveMission } = require('./ActiveMissionGuard');
const { resolveAcquisitionMissionRuntime } = require('./WorkspaceMissionInspection');

function contextualPendingAnswer(question, resolution, snapshot) {
  const q = String(question || '').trim();
  const mission = snapshot && snapshot.mission ? snapshot.mission : null;

  if (/\bwhat(?:'s| is| are) the (?:biggest )?risks?\b/i.test(q)) {
    if (resolution.decisionKind === 'discovery_approval') {
      return (
        'Discovery runs Scout before prioritization or outreach. The main risk is committing ' +
        'downstream execution before we validate fit, timing, and evidence quality.'
      );
    }
    return 'The main risk is advancing before the pending operator decision is resolved.';
  }

  if (/\bwhat happens\b/i.test(q)) {
    if (resolution.decisionKind === 'discovery_approval') {
      return 'If you approve discovery, Scout will run against the locked mission plan and return evidence for review.';
    }
  }

  if (mission && mission.objective) {
    return `This decision applies to the active mission: ${mission.objective}`;
  }

  return 'I can answer follow-up questions, but I still need your decision below.';
}

function buildClarifyProse(question, resolution, snapshot) {
  const pending = presentableOperatorDecision(snapshot) || {};
  const prompt = resolution.prompt || pending.prompt || 'Please confirm your decision.';
  let prefix = '';
  let reviewProse = '';

  if (resolution.outcome === RESOLUTION_OUTCOMES.QUESTION) {
    prefix = `${contextualPendingAnswer(question, resolution, snapshot)}\n\n`;
  } else if (resolution.outcome === RESOLUTION_OUTCOMES.AMBIGUOUS) {
    // SPEC-211 — For execution_approval decisions, include the canonical executionReview
    // when clarifying an ambiguous response to help the operator make an informed decision.
    if (resolution.decisionKind === OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL && snapshot) {
      const review = ensureReadyExecutionReview(snapshot, snapshot.mission || null);
      if (review) {
        const blockers = Array.isArray(review.decision && review.decision.blockers)
          ? review.decision.blockers
          : [];
        reviewProse = [
          '## Execution Ready',
          '',
          '**Targets**',
          summarizeReadyExecutionTargets(review),
          '',
          '**Channel**',
          '• Email',
          '',
          '**Message**',
          summarizeReadyExecutionMessage(review),
          '',
          '**Outbound Plan**',
          summarizeReadyExecutionQueue(review),
          '',
          '**Safety / Delivery**',
          summarizeReadyExecutionSafety(review),
          '',
        ].join('\n');
      }
      prefix = "I didn't catch a clear yes or no for the pending decision.\n\n";
    } else {
      prefix = "I didn't catch a clear yes or no for the pending decision.\n\n";
    }
  }

  return `${prefix}${reviewProse}${prompt}`.trim();
}

function buildPendingDecisionStructured(prose, resolution, snapshot, mission) {
  const comm = buildMissionCommunication({
    headline: 'Operator Decision',
    mission: mission ? mission.id : resolution.missionId,
    objective: mission && mission.objective ? mission.objective : null,
    stage: mission && mission.stage ? mission.stage : null,
    status: 'Waiting for operator',
    waitingOn: 'Operator decision',
    nextStep: 'Reply with approval, modification, or cancellation.',
    operatorDecision: resolution.prompt || null,
    reasoningEvidence: {
      known: [`Pending decision kind: ${resolution.decisionKind}`],
      inference: ['Pending decision retains turn ownership until resolved (SPEC-202).'],
      unknown: [],
      evidenceNeeded: [],
      confidence: 1,
    },
    includeReasoningMarker: false,
  });

  const base = buildStructuredResponse({
    answer: prose,
    reasoning: [],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: 1,
    nextInvestigations: [],
    recommendedActions: [],
    confidenceContributors: ['spec_202', 'pending_decision_turn_ownership'],
    timelineReferences: [],
    relatedEntities: mission
      ? [{ id: mission.id, type: 'acquisition_mission', name: mission.objective || mission.id }]
      : [],
    metadata: {
      spec: 'SPEC-202',
      pendingDecisionResolution: resolution,
      missionCommunication: true,
      acquisitionMission: true,
      pendingDecisionTurnOwnership: true,
    },
  });

  const structured = applyMissionCommunication(base, comm, { includeReasoning: false });
  structured.answer = prose;
  return structured;
}

/**
 * Handle unresolved pending operator decisions before generic cognition routing.
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandlePendingDecisionTurn(input = {}) {
  const operatorIntent = input.operatorIntent || null;
  const resolution =
    operatorIntent && operatorIntent.pendingDecisionResolution
      ? operatorIntent.pendingDecisionResolution
      : null;

  if (!pendingDecisionOwnsTurn(resolution)) {
    return null;
  }

  askPathTrace.traceEnter('maybeHandlePendingDecisionTurn', {
    outcome: resolution.outcome,
    decisionKind: resolution.decisionKind,
    missionId: resolution.missionId,
  });

  const runtime = resolveAcquisitionMissionRuntime(input);
  const engine = runtime.engine();
  const amoResolution = await resolveAcquisitionActiveMission(input);
  const mission =
    amoResolution.mission ||
    (engine && resolution.missionId && engine.get
      ? engine.get(resolution.missionId, input.context && input.context.tenantId)
      : null);

  const snapshot =
    engine && mission && typeof engine.inspect === 'function'
      ? engine.inspect(mission.id, {
          tenantId:
            (input.context && input.context.tenantId) ||
            (input.session &&
              input.session.context &&
              input.session.context.tenantId) ||
            null,
        })
      : { mission: mission || { id: resolution.missionId } };

  const prose = buildClarifyProse(input.question, resolution, snapshot);
  const structured = buildPendingDecisionStructured(
    prose,
    resolution,
    snapshot,
    snapshot.mission || mission
  );

  return {
    reason: 'pending_decision_turn_ownership',
    action: 'clarify',
    prose,
    structured,
    mission: snapshot.mission || mission || null,
    pendingDecisionResolution: resolution,
  };
}

module.exports = {
  maybeHandlePendingDecisionTurn,
  buildClarifyProse,
  contextualPendingAnswer,
};
