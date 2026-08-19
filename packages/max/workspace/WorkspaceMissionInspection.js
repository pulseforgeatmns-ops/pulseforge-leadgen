'use strict';

/**
 * AUDIT-005 / SPEC-122 — Mission Inspection as a Workspace routing concern.
 * Runs before intent classification, response contracts, and retrieval.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  buildAcquisitionMissionCommunication,
  formatMissionProse,
  applyMissionCommunication,
  looksLikeReasoningRequest,
} = require('./MissionCommunication');
const {
  INSPECTION_PROPERTIES,
  inspectQuestion,
  classifyInspectionQuestion,
  formatInspection,
  referencesMissionState,
} = require('../../acquisition-mission/Inspection');
const { formatExplain } = require('../../acquisition-mission/Explain');
const { formatWorkspace } = require('../../acquisition-mission/Workspace');
const { formatHealth } = require('../../acquisition-mission/Health');
const {
  createWorkspaceMissionInspectionAudit,
  logWorkspaceRequest,
  logWorkspaceActiveMission,
  logWorkspaceMissionInspection,
  logMissionInspectionResult,
  logWorkspacePipeline,
  logMissionPropertyGuard,
  logWorkspaceResponse,
  buildOwnershipTrace,
} = require('./audit/WorkspaceMissionInspectionAudit');

const PIPELINE_MISSION_INSPECTION = 'MissionInspection';
const PIPELINE_RETRIEVAL = 'Retrieval';

function looksLikeAcquisitionMissionQuestion(question) {
  const q = String(question || '').trim();
  if (!q) return false;
  return /why is this mission|why (?:does|do) this mission exist|why are we (?:doing|running) this mission|how is outreach|mission health|how is (?:the )?mission\b|what(?:'s| is) blocking (?:the )?mission|mission workspace|where are we\b|mission progress|mission status/i.test(
    q
  );
}

function shouldInspectActiveMission(question, hasActiveMission) {
  if (!hasActiveMission) return looksLikeAcquisitionMissionQuestion(question);
  return Boolean(classifyInspectionQuestion(question));
}

function resolveTenantId(input = {}) {
  const session = input.session || null;
  const sessionCtx = session && session.context && typeof session.context === 'object' ? session.context : {};
  const envelope = input.context && typeof input.context === 'object' ? input.context : {};
  return String(
    input.authorizedTenantId ||
      envelope.tenantId ||
      sessionCtx.tenantId ||
      envelope.clientId ||
      sessionCtx.clientId ||
      ''
  ).trim();
}

function resolveMissionId(input = {}, missions = []) {
  return (
    (input.context && (input.context.missionId || input.context.acquisitionMissionId)) ||
    (input.session && input.session.context && input.session.context.missionId) ||
    (missions[0] && missions[0].id) ||
    null
  );
}

function resolveAcquisitionEngine(input = {}) {
  if (input.acquisitionMissionEngine) return input.acquisitionMissionEngine;
  let service = input.acquisitionMissionService;
  if (!service) {
    try {
      service = require('../../../services/acquisitionMission');
    } catch (_) {
      return null;
    }
  }
  if (!service) return null;
  return service.getEngine && service.getEngine();
}

function buildAnswerFromInspection(question, snapshot, mission, inspection) {
  if (inspection && inspection.resolved) {
    if (inspection.kind === 'explain') {
      return {
        kind: 'explain',
        prose: formatExplain(snapshot.why),
        structured: snapshot.why,
        mission,
        missionContext: inspection.missionContext,
        inspection: {
          property: inspection.property,
          pipeline: inspection.pipeline,
          resolved: true,
        },
        invented: false,
      };
    }
    if (inspection.kind === 'workspace') {
      return {
        kind: 'workspace',
        prose: formatWorkspace(snapshot.workspace),
        structured: snapshot.workspace,
        mission,
        missionContext: inspection.missionContext,
        inspection: {
          property: inspection.property,
          pipeline: inspection.pipeline,
          resolved: true,
        },
        invented: false,
      };
    }
    if (inspection.property === INSPECTION_PROPERTIES.HEALTH) {
      const healthExplain = inspection.structured;
      return {
        kind: 'health',
        prose: `${formatHealth(snapshot.health)}\n\n${formatInspection(healthExplain)}`.trim(),
        structured: { ...snapshot.health, derivation: healthExplain },
        mission,
        missionContext: inspection.missionContext,
        inspection: {
          property: inspection.property,
          pipeline: inspection.pipeline,
          resolved: true,
        },
        invented: false,
      };
    }
    if (inspection.property === INSPECTION_PROPERTIES.BLOCKER) {
      const blockerExplain = inspection.structured;
      const blocker = snapshot.blocker;
      return {
        kind: 'blocker',
        prose: blockerExplain.prose || formatInspection(blockerExplain),
        structured: blocker ? { ...blocker, derivation: blockerExplain } : blockerExplain,
        mission,
        missionContext: inspection.missionContext,
        inspection: {
          property: inspection.property,
          pipeline: inspection.pipeline,
          resolved: true,
        },
        invented: false,
      };
    }
    return {
      kind: 'inspection',
      prose: inspection.prose,
      structured: inspection.structured,
      mission,
      missionContext: inspection.missionContext,
      inspection: {
        property: inspection.property,
        pipeline: inspection.pipeline,
        resolved: true,
      },
      invented: false,
    };
  }

  if (inspection && inspection.kind === 'fallback') {
    return {
      kind: 'inspection_fallback',
      prose: null,
      structured: inspection.missionContext,
      mission,
      inspection: {
        property: null,
        pipeline: inspection.pipeline,
        resolved: false,
        reason: inspection.reason,
      },
      invented: false,
    };
  }

  return null;
}

function buildMissionInspectionResponse(question, answered) {
  const explicitReasoning = looksLikeReasoningRequest(question);
  const inspectionProperty =
    answered.inspection && answered.inspection.property
      ? answered.inspection.property
      : classifyInspectionQuestion(question);

  const missionComm = buildAcquisitionMissionCommunication(
    {
      mission: answered.mission,
      workspace: answered.kind === 'workspace' ? answered.structured : answered.missionContext,
      blocker: answered.kind === 'blocker' ? answered.structured : null,
      health: answered.kind === 'health' ? answered.structured : null,
      why: answered.kind === 'explain' ? answered.structured : null,
      inspection: answered.kind === 'inspection' ? answered.structured : null,
      missionContext: answered.missionContext || null,
    },
    {
      kind: answered.kind,
      includeReasoning: explicitReasoning,
    }
  );
  const prose = answered.prose || formatMissionProse(missionComm, {
    explicitReasoningRequest: explicitReasoning,
  });

  const base = buildStructuredResponse({
    answer: prose,
    reasoning: [],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence:
      answered.structured && answered.structured.confidence != null
        ? answered.structured.confidence
        : (answered.mission && answered.mission.confidence) ||
          (answered.missionContext && answered.missionContext.confidence) ||
          0.7,
    nextInvestigations: [],
    recommendedActions: [{ id: 'inspect_mission', type: 'review', label: 'Open mission workspace' }],
    confidenceContributors: [],
    timelineReferences: [],
    relatedEntities: answered.mission
      ? [{ id: answered.mission.id, type: 'acquisition_mission', name: answered.mission.title }]
      : [],
    metadata: {
      sourcesUsed: {
        briefing: false,
        reasoning: false,
        memory: false,
        policy: false,
        knowledge: false,
        missionState: true,
      },
      evidenceCount: 0,
      asOf: new Date().toISOString(),
      unavailable: [],
      acquisitionMission: true,
      missionInspection: true,
      inspectionProperty,
      inspectionPipeline:
        (answered.inspection && answered.inspection.pipeline) || PIPELINE_MISSION_INSPECTION,
      invented: answered.invented === true,
    },
  });

  const structured = applyMissionCommunication(base, missionComm, {
    includeReasoningInStructured: explicitReasoning,
  });

  return { structured, prose, inspectionProperty };
}

/**
 * Workspace-level mission inspection — runs before cognitive classifiers.
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleWorkspaceMissionInspection(input = {}) {
  const question = String(input.question || '').trim();
  const tenantId = resolveTenantId(input);
  const audit = input.audit || createWorkspaceMissionInspectionAudit();
  const useGlobalAudit = !input.audit;

  const emitRequest = useGlobalAudit ? logWorkspaceRequest : audit.logWorkspaceRequest.bind(audit);
  const emitActiveMission = useGlobalAudit
    ? logWorkspaceActiveMission
    : audit.logWorkspaceActiveMission.bind(audit);
  const emitInspection = useGlobalAudit
    ? logWorkspaceMissionInspection
    : audit.logWorkspaceMissionInspection.bind(audit);
  const emitInspectionResult = useGlobalAudit
    ? logMissionInspectionResult
    : audit.logMissionInspectionResult.bind(audit);
  const emitPipeline = useGlobalAudit ? logWorkspacePipeline : audit.logWorkspacePipeline.bind(audit);
  const emitPropertyGuard = useGlobalAudit
    ? logMissionPropertyGuard
    : audit.logMissionPropertyGuard.bind(audit);
  const emitResponse = useGlobalAudit ? logWorkspaceResponse : audit.logWorkspaceResponse.bind(audit);
  const traceOwnership = useGlobalAudit
    ? () => buildOwnershipTrace()
    : audit.buildOwnershipTrace.bind(audit);

  emitRequest({
    conversation: input.session && input.session.id,
    sessionId: input.session && input.session.id,
    workspace: 'max',
    operator: input.session && input.session.operator,
    question,
  });

  if (!tenantId) {
    emitActiveMission({ missionFound: false, reason: 'no_tenant' });
    emitInspection({ attempted: false, reason: 'no_tenant' });
    return null;
  }

  const engine = resolveAcquisitionEngine(input);
  if (!engine || typeof engine.inspect !== 'function' || typeof engine.list !== 'function') {
    emitActiveMission({ missionFound: false, reason: 'no_acquisition_engine' });
    emitInspection({ attempted: false, reason: 'no_acquisition_engine' });
    return null;
  }

  const missions = engine.list(tenantId);
  const hasActiveMission = missions.length > 0;
  const missionId = resolveMissionId(input, missions);
  const mission = missionId
    ? missions.find((row) => row.id === missionId) || (engine.get && engine.get(missionId))
    : missions[0];

  emitActiveMission({
    missionFound: Boolean(mission),
    missionId: mission ? mission.id : missionId,
    stage: mission ? mission.stage : null,
    status: mission ? mission.status : null,
  });

  if (!shouldInspectActiveMission(question, hasActiveMission)) {
    emitInspection({ attempted: false, reason: 'not_inspection_question', missionId: mission && mission.id });
    return null;
  }

  emitInspection({ attempted: true, missionId: mission && mission.id });

  if (!mission) {
    emitInspectionResult({
      claimed: false,
      property: null,
      confidence: null,
      missionId: null,
      reason: 'no_active_mission',
    });
    emitPipeline({
      selectedPipeline: PIPELINE_RETRIEVAL,
      reason: 'no_active_mission',
      missionId: null,
    });
    return null;
  }

  const snapshot = engine.inspect(mission.id, {
    tenantId,
    previousReplyRate: input.previousReplyRate,
  });
  const inspection = inspectQuestion(question, snapshot, {
    silent: input.silentInspection === true,
    logger: input.inspectionLogger,
  });
  const answered = buildAnswerFromInspection(question, snapshot, mission, inspection);

  if (!answered || answered.kind === 'inspection_fallback') {
    emitInspectionResult({
      claimed: false,
      property: inspection && inspection.property ? inspection.property : null,
      confidence: null,
      missionId: mission.id,
      reason: (answered && answered.inspection && answered.inspection.reason) || 'property_not_claimed',
    });
    emitPipeline({
      selectedPipeline: PIPELINE_RETRIEVAL,
      reason: 'inspection_not_claimed',
      missionId: mission.id,
    });
    return null;
  }

  const confidence =
    answered.mission && answered.mission.confidence != null
      ? answered.mission.confidence
      : answered.missionContext && answered.missionContext.confidence;

  emitInspectionResult({
    claimed: true,
    property: answered.inspection.property,
    confidence,
    missionId: mission.id,
  });
  emitPropertyGuard({
    property: answered.inspection.property,
    retrievalBlocked: true,
    missionId: mission.id,
  });
  emitPipeline({
    selectedPipeline: PIPELINE_MISSION_INSPECTION,
    reason: 'property_claimed',
    missionId: mission.id,
  });

  const { structured, prose, inspectionProperty } = buildMissionInspectionResponse(question, answered);

  emitResponse({
    selectedPipeline: PIPELINE_MISSION_INSPECTION,
    missionId: mission.id,
    inspectionProperty,
    reason: 'mission_inspection_complete',
  });

  return {
    reason: 'mission_inspection',
    structured,
    prose,
    answered,
    ownershipTrace: traceOwnership(),
    audit,
  };
}

module.exports = {
  PIPELINE_MISSION_INSPECTION,
  PIPELINE_RETRIEVAL,
  resolveMissionId,
  resolveAcquisitionEngine,
  buildAnswerFromInspection,
  buildMissionInspectionResponse,
  maybeHandleWorkspaceMissionInspection,
  looksLikeAcquisitionMissionQuestion,
  referencesMissionState,
  shouldInspectActiveMission,
  resolveTenantId,
};
