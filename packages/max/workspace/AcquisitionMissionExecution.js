'use strict';

/**
 * SPEC-127 — Acquisition Mission execution commands from the workspace ask path.
 * Maps operator execution language (approve, begin discovery, …) to mission
 * contributions and mission-oriented responses.
 */

const amo = require('../../acquisition-mission');
const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  buildAcquisitionMissionCommunication,
  formatMissionProse,
  applyMissionCommunication,
  buildReasoningEvidence,
} = require('./MissionCommunication');
const {
  resolveTenantId,
  resolveAcquisitionEngine,
  resolveMissionId,
} = require('./WorkspaceMissionInspection');
const { isMissionExecutionCommand } = require('./ExecutionLanguageDetection');
const {
  isExplicitMissionExit,
  resolveAcquisitionActiveMission,
} = require('./ActiveMissionGuard');

const { STAGES, STAGE_LABELS, SPECIALISTS, CONTRIBUTION_KINDS } = amo;

function stageLabel(stage) {
  return STAGE_LABELS[stage] || stage || 'Active';
}

function buildExecutionMissionResponse({ mission, snapshot, action, question }) {
  const workspace = snapshot.workspace || {};
  const scout = workspace.scout || null;
  const stage = mission.stage || STAGES.DISCOVER;
  const progress = mission.progressPercent != null ? mission.progressPercent : null;

  let status = `Active mission — ${stageLabel(stage)}.`;
  let nextStep = scout && scout.state === 'waiting'
    ? 'Scout discovery is running for this mission.'
    : 'Review mission workspace for the latest specialist contributions.';
  let operatorDecision = 'Continue in mission workspace?';

  if (action === 'discovery_approved') {
    status = 'Discovery stage — operator approved. Scout is next.';
    nextStep = scout && scout.state === 'complete'
      ? 'Scout discovery complete. Review ranked prospects and approve the next stage.'
      : 'Scout discovery initiated for the active mission.';
    operatorDecision = scout && scout.state === 'complete'
      ? 'Approve prioritization?'
      : 'Review Scout results when discovery completes.';
  } else if (action === 'operator_approved') {
    status = `Operator approved — ${stageLabel(stage)}.`;
    nextStep = 'Mission advanced under operator approval.';
    operatorDecision = 'Continue in mission workspace?';
  }

  const comm = buildAcquisitionMissionCommunication({
    mission: mission.title || mission.id,
    objective: mission.objective,
    status,
    stage: stageLabel(stage),
    progress,
    health: snapshot.health && snapshot.health.label
      ? snapshot.health.label
      : null,
    waitingOn: scout && scout.label ? scout.label : null,
    confidence: mission.confidence,
    currentUnderstanding: [
      mission.targetSegment
        ? `Target segment: ${mission.targetSegment}.`
        : null,
      `Execution command received: "${String(question || '').trim()}".`,
    ].filter(Boolean),
    nextStep,
    operatorDecision,
    evidenceStatus: 'Mission state',
    sources: ['acquisition_mission'],
    reasoningEvidence: buildReasoningEvidence({
      known: [`Mission ${mission.id} is active at stage ${stageLabel(stage)}.`],
      inference: ['Execution commands bind to the active mission workspace.'],
      unknown: [],
      evidenceNeeded: [],
      confidence: mission.confidence,
    }),
  });

  const prose = formatMissionProse(comm);
  const structured = applyMissionCommunication(
    buildStructuredResponse({
      answer: prose,
      reasoning: [],
      supportingEvidence: [],
      contradictingEvidence: [],
      confidence: mission.confidence != null ? mission.confidence : 0.84,
      nextInvestigations: [],
      recommendedActions: [
        {
          id: 'open_mission',
          type: 'open_mission',
          label: 'Open mission workspace',
          payload: { missionId: mission.id },
        },
      ],
      confidenceContributors: ['spec_127', 'acquisition_mission'],
      timelineReferences: [],
      relatedEntities: [
        { id: mission.id, type: 'acquisition_mission', name: mission.title || mission.id },
      ],
      metadata: {
        sourcesUsed: {
          briefing: false,
          reasoning: false,
          memory: false,
          policy: false,
          knowledge: false,
          missionState: true,
          clientIntelligence: false,
        },
        evidenceCount: 0,
        asOf: new Date().toISOString(),
        unavailable: [],
        acquisitionMission: true,
        acquisitionMissionExecution: true,
        missionExecutionAction: action,
        strictOutputShape: true,
        missionCommunication: true,
      },
    }),
    comm
  );

  return { structured, prose, comm, action };
}

function detectExecutionAction(question) {
  const q = String(question || '').trim();
  const lower = q.toLowerCase();

  if (/\bapprov(e|al|ed)\b.*\bbegin\b/i.test(q) || /\bbegin\b.*\bdiscover/i.test(lower)) {
    return 'discovery_approved';
  }
  if (/\bapprov(e|al|ed)\b/i.test(q)) {
    return 'operator_approved';
  }
  if (/\b(?:begin|start|run|execute)\b.*\bdiscover/i.test(lower)) {
    return 'discovery_approved';
  }
  if (/\b(?:continue|proceed|resume|next)\b/i.test(q)) {
    return 'operator_approved';
  }
  if (/\b(?:prioritization|outreach|send)\b/i.test(q)) {
    return 'operator_approved';
  }
  return 'operator_approved';
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleAcquisitionMissionExecution(input = {}) {
  const question = String(input.question || '').trim();
  if (!question || !isMissionExecutionCommand(question)) return null;
  if (isExplicitMissionExit(question).explicit) return null;

  const tenantId = resolveTenantId(input);
  const engine = resolveAcquisitionEngine(input);
  if (!engine || !tenantId) return null;

  const mission =
    resolveAcquisitionActiveMission(input) ||
    (() => {
      const missions = engine.list(tenantId);
      const missionId = resolveMissionId(input, missions);
      return missionId ? missions.find((row) => row && row.id === missionId) : null;
    })();

  if (!mission || mission.stage === STAGES.IMPROVE) return null;

  const action = detectExecutionAction(question);

  if (action === 'discovery_approved' || action === 'operator_approved') {
    try {
      engine.contribute(
        mission.id,
        {
          specialist: SPECIALISTS.OPERATOR,
          kind: CONTRIBUTION_KINDS.APPROVAL,
          payload: { approved: true, command: question, action },
        },
        { tenantId }
      );
    } catch (_) {
      /* approval may already exist — mission still owns the turn */
    }
  }

  const snapshot = engine.inspect(mission.id, { tenantId });
  const response = buildExecutionMissionResponse({
    mission: snapshot.mission || mission,
    snapshot,
    action,
    question,
  });

  if (input.session && input.session.context && typeof input.session.context === 'object') {
    input.session.context.missionId = mission.id;
    input.session.context.acquisitionMissionId = mission.id;
    input.session.context.acquisitionOwner = 'MissionEngine';
  }

  return {
    reason: `acquisition_mission_${action}`,
    structured: response.structured,
    prose: response.prose,
    mission: snapshot.mission || mission,
    action,
  };
}

module.exports = {
  detectExecutionAction,
  buildExecutionMissionResponse,
  maybeHandleAcquisitionMissionExecution,
};
