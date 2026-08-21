'use strict';

/**
 * SPEC-124 — Acquisition Ownership Convergence.
 * Mission Engine owns acquisition objectives. Client Intelligence contributes
 * structured evidence — never the final operator response.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  buildAcquisitionMissionCommunication,
  buildMissionCommunication,
  buildReasoningEvidence,
  formatMissionProse,
  applyMissionCommunication,
} = require('./MissionCommunication');
const {
  attachClientIntelligenceContext,
  isClientContextExecutionRequest,
} = require('./ClientIntelligenceContext');
const {
  resolveTenantId,
} = require('./WorkspaceMissionInspection');
const {
  looksLikeAcquisitionMissionQuestion,
  shouldInspectActiveMission,
} = require('./WorkspaceMissionInspection');
const {
  createAcquisitionOwnershipAudit,
  logAcquisitionOwner,
  logClientIntelligenceContribution,
  buildAcquisitionOwnershipTrace,
} = require('./audit/AcquisitionOwnershipAudit');
const { objectivesSimilar } = require('../scoutAcquisition/NeedAssessment');
const { detectAcquisitionObjective, normalizeObjectiveText } = require('./AcquisitionObjectiveDetection');
const { detectMissionExecutionLanguage } = require('./ExecutionLanguageDetection');
const {
  inferTargetSegmentFromObjective,
  deriveMissionTitle,
} = require('../../acquisition-mission/MissionNaming');
const { formatMissionUnderstandingProse } = require('../../acquisition-mission/StructuredMission');

function isAcquisitionObjectiveForMission(question) {
  const q = normalizeObjectiveText(question);
  if (!detectAcquisitionObjective(q)) return false;
  if (isClientContextExecutionRequest(q)) return false;
  if (looksLikeAcquisitionMissionQuestion(q)) return false;
  return true;
}
/**
 * Runtime guard — Mission owns acquisition requests; CIE must not respond.
 * @param {string} question
 * @param {object} [opts]
 * @returns {boolean}
 */
function missionOwnsAcquisitionRequest(question, opts = {}) {
  if (!isAcquisitionObjectiveForMission(question)) return false;
  if (opts.hasActiveMission && shouldInspectActiveMission(question, true)) {
    return false;
  }
  return true;
}

function buildClientIntelligenceMissionEvidence(summary) {
  if (!summary || !summary.approved) {
    return {
      attached: false,
      blueprintId: null,
      sectionsAttached: [],
      strategicEvidence: null,
      constraints: [],
      known: [],
    };
  }

  const sectionsAttached = [];
  const constraints = [];
  const known = [];

  if (summary.idealCustomers) {
    sectionsAttached.push('idealCustomers');
    known.push(`ICP: ${summary.idealCustomers}`);
  }
  if (summary.geography || summary.targetMarkets) {
    sectionsAttached.push('geography');
    known.push(`Geography: ${summary.geography || summary.targetMarkets}`);
  }
  if (summary.successMetrics) {
    sectionsAttached.push('successMetrics');
    known.push(`Success metrics: ${summary.successMetrics}`);
  }
  if (summary.avoidCustomers) {
    sectionsAttached.push('constraints');
    constraints.push(`Avoid: ${summary.avoidCustomers}`);
  }
  if (summary.commercialPreference) {
    sectionsAttached.push('commercialPreference');
    constraints.push('Commercial preference over residential');
  }
  if (summary.campaignGoals || summary.growthFocus) {
    sectionsAttached.push('strategy');
    known.push(
      `Strategy: ${summary.campaignGoals || summary.growthFocus}`
    );
  }
  if (summary.competitiveAdvantages) {
    sectionsAttached.push('differentiation');
    known.push(`Differentiation: ${summary.competitiveAdvantages}`);
  }

  return {
    attached: sectionsAttached.length > 0,
    blueprintId: summary.blueprintId || null,
    sectionsAttached,
    strategicEvidence: {
      icp: summary.idealCustomers || null,
      geography: summary.geography || summary.targetMarkets || null,
      successMetrics: summary.successMetrics || null,
      constraints: constraints.slice(),
      strategy: summary.campaignGoals || summary.growthFocus || null,
      commercialPreference: summary.commercialPreference || false,
      unknowns: (summary.unknowns || []).slice(0, 5),
    },
    constraints,
    known,
  };
}

function findResumableMission(missions, objective) {
  const active = missions.find((row) => row.stage !== 'improve');
  if (active && objectivesSimilar(active.objective, objective)) return active;
  return missions.find((row) => objectivesSimilar(row.objective, objective)) || null;
}

function inferTargetSegment(objective) {
  return inferTargetSegmentFromObjective(objective);
}

async function attachEvidenceToMission(missionId, objective, ciEvidence, opts = {}) {
  if (!missionId || !ciEvidence || !ciEvidence.attached) return null;

  const payload = {
    specialist: 'max',
    kind: 'constraints',
    payload: {
      source: 'client_intelligence',
      blueprintReference: true,
      constraints: ciEvidence.constraints,
      strategicContext: {
        blueprintId: ciEvidence.blueprintId,
        sectionsAttached: ciEvidence.sectionsAttached,
        ...ciEvidence.strategicEvidence,
      },
    },
  };

  if (opts.acquisitionMissionEngine && typeof opts.acquisitionMissionEngine.contribute === 'function') {
    return opts.acquisitionMissionEngine.contribute(missionId, payload, {
      tenantId: opts.tenantId,
    });
  }

  let service = opts.acquisitionMissionService;
  if (!service) {
    try {
      service = require('../../../services/acquisitionMission');
    } catch (_) {
      return null;
    }
  }
  if (!service || typeof service.contribute !== 'function') return null;

  return service.contribute(missionId, payload, {
    tenantId: opts.tenantId,
    persist: opts.persist,
    pool: opts.pool,
  });
}

function buildOwnershipMissionResponse({
  mission,
  snapshot,
  created,
  ciEvidence,
  question,
}) {
  const baseComm = buildAcquisitionMissionCommunication(
    {
      mission,
      workspace: snapshot.workspace,
      missionContext: {
        stage: mission.stage,
        stageLabel: mission.status,
        progress: mission.progressPercent,
        confidence: mission.confidence,
      },
    },
    { kind: 'workspace' }
  );

  const known = [`Objective: ${mission.objective}.`];
  if (ciEvidence && ciEvidence.known && ciEvidence.known.length) {
    known.push(...ciEvidence.known.map((row) => `${row} (Client Intelligence).`));
  }

  const inference = [];
  if (ciEvidence && ciEvidence.strategicEvidence && ciEvidence.strategicEvidence.icp) {
    inference.push(
      `Strategic basis: ${ciEvidence.strategicEvidence.icp}` +
        (ciEvidence.strategicEvidence.geography
          ? ` in ${ciEvidence.strategicEvidence.geography}`
          : '') +
        '.'
    );
  } else {
    inference.push('Mission will gather market evidence before prioritizing outreach.');
  }

  const unknown = (ciEvidence && ciEvidence.strategicEvidence && ciEvidence.strategicEvidence.unknowns) || [];
  const sources = ['Mission Engine'];
  if (ciEvidence && ciEvidence.attached) sources.push('Client Intelligence');

  const comm = buildMissionCommunication({
    ...baseComm,
    headline: created ? 'Mission Created' : 'Mission Resumed',
    mission: mission.title || 'Acquisition Mission',
    objective: mission.objective,
    status: mission.status,
    stage: mission.status,
    progress: mission.progressPercent,
    confidence: mission.confidence,
    health: 'Healthy',
    waitingOn: 'Operator direction',
    nextStep: created
      ? (mission.pendingOperatorDecision && mission.pendingOperatorDecision.kind === 'plan_clarification'
          ? 'Answer the planner question. Mission Planning will not guess.'
          : mission.missionPlanDraft
            ? 'Review Mission Understanding below, then Approve, Edit, or Cancel the mission plan before Discovery.'
            : 'Scout will identify high-probability operators matching this objective.')
      : 'Continuing the active acquisition mission from current stage.',
    operatorDecision: created
      ? (mission.pendingOperatorDecision && mission.pendingOperatorDecision.clarificationPrompt)
        || (mission.pendingOperatorDecision && mission.pendingOperatorDecision.prompt)
        || 'Approve mission plan?'
      : 'Continue in mission workspace?',
    evidenceStatus: ciEvidence && ciEvidence.attached ? '✓ Blueprint attached' : 'Mission context only',
    sources,
    reasoningEvidence: buildReasoningEvidence({
      known,
      inference,
      unknown,
      evidenceNeeded: [
        'Live campaign performance before scaling beyond the first experiment.',
      ],
      confidence: mission.confidence,
    }),
  });

  const prose = formatMissionProse(comm);
  const missionUnderstanding =
    (mission.pendingOperatorDecision && mission.pendingOperatorDecision.clarificationPrompt) ||
    (mission.missionPlanDraft && formatMissionUnderstandingProse(mission.missionPlanDraft)) ||
    (mission.pendingOperatorDecision && mission.pendingOperatorDecision.missionUnderstanding) ||
    null;
  const structured = applyMissionCommunication(
    buildStructuredResponse({
      answer: prose,
      reasoning: [],
      supportingEvidence: (ciEvidence && ciEvidence.known
        ? ciEvidence.known
        : []
      ).map((summary, index) => ({
        id: `ci:${mission.id}:${index}`,
        summary,
        sourceType: 'client_intelligence',
        kind: 'blueprint',
      })),
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
      confidenceContributors: ['spec_124', 'acquisition_mission'],
      timelineReferences: [],
      relatedEntities: [
        { id: mission.id, type: 'acquisition_mission', name: mission.title },
      ],
      metadata: {
        sourcesUsed: {
          briefing: false,
          reasoning: false,
          memory: false,
          policy: false,
          knowledge: false,
          missionState: true,
          clientIntelligence: Boolean(ciEvidence && ciEvidence.attached),
        },
        evidenceCount: ciEvidence && ciEvidence.known ? ciEvidence.known.length : 0,
        asOf: new Date().toISOString(),
        unavailable: [],
        acquisitionMission: true,
        acquisitionOwnership: true,
        missionCreated: created === true,
        missionResumed: created === false,
        missionUnderstanding,
        structuredMissionDraft: mission.missionPlanDraft || null,
        blueprintId: ciEvidence ? ciEvidence.blueprintId : null,
        clientIntelligenceContribution: ciEvidence
          ? {
              attached: ciEvidence.attached,
              sectionsAttached: ciEvidence.sectionsAttached,
            }
          : null,
        strictOutputShape: true,
        missionCommunication: true,
      },
    }),
    comm
  );

  return { structured, prose, comm };
}

/**
 * SPEC-124 — create or resume acquisition mission and return mission-owned response.
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleAcquisitionOwnershipTurn(input = {}) {
  const question = normalizeObjectiveText(input.question);
  const executionLanguage = detectMissionExecutionLanguage(question);
  const isAcquisition = isAcquisitionObjectiveForMission(question);
  const isExplicitMissionCommand =
    executionLanguage.matched &&
    (executionLanguage.reason === 'mission_create_command' ||
      executionLanguage.reason === 'mission_operate_command');
  if (!isAcquisition && !isExplicitMissionCommand) return null;

  const tenantId = resolveTenantId(input);
  const audit = input.audit || createAcquisitionOwnershipAudit();
  const useGlobalAudit = !input.audit;
  const emitOwner = useGlobalAudit
    ? logAcquisitionOwner
    : audit.logAcquisitionOwner.bind(audit);
  const emitCiContribution = useGlobalAudit
    ? logClientIntelligenceContribution
    : audit.logClientIntelligenceContribution.bind(audit);
  const traceOwnership = useGlobalAudit
    ? () => buildAcquisitionOwnershipTrace()
    : audit.buildOwnershipTrace.bind(audit);

  if (!tenantId) return null;

  let service = input.acquisitionMissionService;
  if (!service) {
    try {
      service = require('../../../services/acquisitionMission');
    } catch (_) {
      return null;
    }
  }
  if (!service || typeof service.createMission !== 'function') return null;

  const ciLoaded = await attachClientIntelligenceContext(input);
  const ciEvidence = buildClientIntelligenceMissionEvidence(ciLoaded.summary);

  const missions = await service.listMissions(tenantId, {
    persist: input.persist,
    pool: input.pool,
  });
  let mission = findResumableMission(missions, question);
  let created = false;

  if (!mission) {
    const targetSegment = inferTargetSegment(question);
    mission = await service.createMission(
      {
        tenantId,
        clientId: Number(tenantId) || tenantId,
        objective: question,
        title: deriveMissionTitle(question, targetSegment),
        targetSegment,
        createdBy: 'max',
        owner: 'Operator',
        constraints: ciEvidence.constraints.slice(),
        planningContext: {
          blueprint: ciEvidence.strategicEvidence || null,
        },
      },
      {
        persist: input.persist,
        pool: input.pool,
      }
    );
    created = true;
  }

  const engine = service.getEngine();
  if (!engine || typeof engine.inspect !== 'function') return null;

  if (ciEvidence.attached) {
    await attachEvidenceToMission(mission.id, question, ciEvidence, {
      tenantId,
      persist: input.persist,
      pool: input.pool,
      acquisitionMissionService: service,
    });
    emitCiContribution({
      missionId: mission.id,
      blueprintId: ciEvidence.blueprintId,
      sectionsAttached: ciEvidence.sectionsAttached,
      strategicEvidence: ciEvidence.strategicEvidence,
      attached: true,
    });
  } else {
    emitCiContribution({
      missionId: mission.id,
      attached: false,
      sectionsAttached: [],
    });
  }

  emitOwner({
    owner: 'MissionEngine',
    missionId: mission.id,
    action: created ? 'created' : 'resumed',
    objective: mission.objective,
    tenantId,
  });

  const snapshot = engine.inspect(mission.id, { tenantId });
  const { structured, prose } = buildOwnershipMissionResponse({
    mission: snapshot.mission || mission,
    snapshot,
    created,
    ciEvidence,
    question,
  });

  if (input.session && input.session.context && typeof input.session.context === 'object') {
    input.session.context.missionId = mission.id;
    input.session.context.acquisitionMissionId = mission.id;
    input.session.context.acquisitionOwner = 'MissionEngine';
  }

  return {
    reason: created ? 'acquisition_mission_created' : 'acquisition_mission_resumed',
    structured,
    prose,
    mission: snapshot.mission || mission,
    created,
    ciEvidence,
    ownershipTrace: traceOwnership(),
    audit,
  };
}

module.exports = {
  detectAcquisitionObjective,
  isAcquisitionObjectiveForMission,
  missionOwnsAcquisitionRequest,
  buildClientIntelligenceMissionEvidence,
  findResumableMission,
  attachEvidenceToMission,
  buildOwnershipMissionResponse,
  maybeHandleAcquisitionOwnershipTurn,
};
