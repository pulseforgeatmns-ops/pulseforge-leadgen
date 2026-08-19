'use strict';

/**
 * SPEC-118 — Max answers mission questions from evidence, not opinion.
 * SPEC-121 — Mission-oriented communication with progressive reasoning disclosure.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  buildAcquisitionMissionCommunication,
  formatMissionProse,
  applyMissionCommunication,
  looksLikeReasoningRequest,
} = require('./MissionCommunication');

function looksLikeAcquisitionMissionQuestion(question) {
  const q = String(question || '').trim();
  if (!q) return false;
  return /why is this mission|why (?:does|do) this mission exist|why are we (?:doing|running) this mission|how is outreach|mission health|how is (?:the )?mission\b|what(?:'s| is) blocking (?:the )?mission|mission workspace|where are we\b|mission progress|mission status/i.test(
    q
  );
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
  if (!looksLikeAcquisitionMissionQuestion(question)) return null;

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

  const explicitReasoning = looksLikeReasoningRequest(question);
  const missionComm = buildAcquisitionMissionCommunication(
    {
      mission: answered.mission,
      workspace: answered.structured,
      blocker: answered.kind === 'blocker' ? answered.structured : null,
      health: answered.kind === 'health' ? answered.structured : null,
      why: answered.kind === 'explain' ? answered.structured : null,
    },
    {
      kind: answered.kind,
      includeReasoning: explicitReasoning,
    }
  );
  const prose = formatMissionProse(missionComm, {
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
        : (answered.mission && answered.mission.confidence) || 0.7,
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
        reasoning: true,
        memory: true,
        policy: true,
        knowledge: false,
      },
      evidenceCount: 0,
      asOf: new Date().toISOString(),
      unavailable: [],
      acquisitionMission: true,
      invented: answered.invented === true,
    },
  });

  const structured = applyMissionCommunication(base, missionComm, {
    includeReasoningInStructured: explicitReasoning,
  });

  return {
    reason: 'acquisition_mission_evidence',
    structured,
    prose,
    answered,
  };
}

module.exports = {
  looksLikeAcquisitionMissionQuestion,
  maybeHandleAcquisitionMissionTurn,
};
