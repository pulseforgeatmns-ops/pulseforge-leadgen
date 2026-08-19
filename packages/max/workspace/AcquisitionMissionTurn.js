'use strict';

/**
 * SPEC-118 — Max answers mission questions from evidence, not opinion.
 * SPEC-121 — Mission-oriented communication with progressive reasoning disclosure.
 * SPEC-122 — Mission inspection precedes durable knowledge retrieval.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  buildAcquisitionMissionCommunication,
  formatMissionProse,
  applyMissionCommunication,
  looksLikeReasoningRequest,
} = require('./MissionCommunication');
const {
  referencesMissionState,
  classifyInspectionQuestion,
} = require('../../acquisition-mission/Inspection');

function looksLikeAcquisitionMissionQuestion(question) {
  const q = String(question || '').trim();
  if (!q) return false;
  return (
    referencesMissionState(q) ||
    /why is this mission|why (?:does|do) this mission exist|why are we (?:doing|running) this mission|how is outreach|mission health|how is (?:the )?mission\b|what(?:'s| is) blocking (?:the )?mission|mission workspace|where are we\b|mission progress|mission status/i.test(
      q
    )
  );
}

function shouldInspectActiveMission(question, hasActiveMission) {
  if (!hasActiveMission) return looksLikeAcquisitionMissionQuestion(question);
  return referencesMissionState(question) || Boolean(classifyInspectionQuestion(question));
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

async function maybeHandleAcquisitionMissionTurn(input = {}) {
  const question = String(input.question || '').trim();
  const tenantId = resolveTenantId(input);
  if (!tenantId) return null;

  let service = input.acquisitionMissionService;
  if (!service) {
    try {
      service = require('../../../services/acquisitionMission');
    } catch (_) {
      return null;
    }
  }
  if (!service || typeof service.answerOperator !== 'function') return null;

  const engine = input.acquisitionMissionEngine || (service.getEngine && service.getEngine());
  const missions = engine && typeof engine.list === 'function' ? engine.list(tenantId) : [];
  const hasActiveMission = missions.length > 0;

  if (!shouldInspectActiveMission(question, hasActiveMission)) return null;

  const missionId =
    (input.context && (input.context.missionId || input.context.acquisitionMissionId)) ||
    (input.session && input.session.context && input.session.context.missionId) ||
    null;

  const answered = await service.answerOperator(
    question,
    { tenantId, missionId, previousReplyRate: input.previousReplyRate },
    { engine, persist: input.persist }
  );
  if (!answered) return null;
  if (answered.kind === 'inspection_fallback') return null;

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
        (answered.inspection && answered.inspection.pipeline) || 'MissionInspection',
      invented: answered.invented === true,
    },
  });

  const structured = applyMissionCommunication(base, missionComm, {
    includeReasoningInStructured: explicitReasoning,
  });

  return {
    reason: 'mission_inspection',
    structured,
    prose,
    answered,
  };
}

module.exports = {
  looksLikeAcquisitionMissionQuestion,
  referencesMissionState,
  shouldInspectActiveMission,
  maybeHandleAcquisitionMissionTurn,
};
