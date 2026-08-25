'use strict';

/**
 * SPEC-124 — Acquisition Ownership Convergence.
 * SPEC-134 — Once the AMO runtime owns a response, presentation stays AMO.
 * Ownership selects the owner. It does not replace AMO presentation with
 * Mission Engine fields. Client Intelligence is supporting evidence only.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const { buildOpenMissionAction, MISSION_RUNTIMES } = require('./MissionActions');
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
  resolveAcquisitionMissionRuntime,
  assertRuntimeEngine,
} = require('../../../services/acquisitionMissionRuntime');
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
const { formatMissionUnderstandingProse, formatCanonicalObjectiveDisplay } = require('../../acquisition-mission/StructuredMission');
const { resolveCanonicalObjective, canonicalObjectiveText } = require('./ResolvedObjective');
const {
  presentationFromDiscoveryPayload,
  findLatestDiscoveryContribution,
} = require('../../acquisition-mission/DiscoveryPresentation');
const {
  hasSufficientEvidenceForPrioritization,
} = require('../../acquisition-mission/DiscoveryPayload');
const { presentableOperatorDecision } = require('../../acquisition-mission/PendingOperatorDecision');

const AMO_SOURCES = Object.freeze(['acquisition_mission', 'scout']);

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

  const runtime = resolveAcquisitionMissionRuntime(opts);
  return runtime.contribute(missionId, payload, {
    tenantId: opts.tenantId,
    ...runtime.persistOpts(opts),
  });
}

function discoveryFromSnapshot(snapshot = {}) {
  if (snapshot.discoveryArtifact) return snapshot.discoveryArtifact;
  const row = findLatestDiscoveryContribution(snapshot.contributions || []);
  if (!row) return null;
  return presentationFromDiscoveryPayload(row.payload || {});
}

function amoOperatorDecision(mission, snapshot, discovery) {
  const presented = presentableOperatorDecision({
    mission,
    contributions: snapshot && snapshot.contributions,
  });
  if (presented) return presented.prompt;
  if (snapshot && snapshot.blocker) return 'Resolve blocker to continue?';
  if (discovery && hasSufficientEvidenceForPrioritization(discovery)) {
    return 'Approve prioritization?';
  }
  if (discovery) return 'Request more discovery evidence?';
  return null;
}

function amoEvidenceStatus(discovery) {
  if (discovery && discovery.summary) return discovery.summary;
  if (discovery) return 'Scout discovery attached';
  return 'Mission state';
}

/**
 * SPEC-134 — fill AMO presentation contract fields on the AMO communication
 * object. Does not apply Mission Engine sources, Blueprint evidence status,
 * or legacy operator-decision copy.
 */
function applyAmoPresentationContract(baseComm, { mission, snapshot, created } = {}) {
  const discovery = discoveryFromSnapshot(snapshot);
  const operatorDecision = amoOperatorDecision(mission, snapshot, discovery);
  return buildMissionCommunication({
    ...baseComm,
    headline: created ? 'Mission Created' : 'Mission Resumed',
    mission: (mission && (mission.title || mission.id)) || baseComm.mission,
    objective: (mission && mission.objective) || baseComm.objective,
    operatorDecision: operatorDecision || baseComm.operatorDecision,
    evidenceStatus: amoEvidenceStatus(discovery),
    sources: AMO_SOURCES.slice(),
    discoveryResults: discovery || baseComm.discoveryResults || null,
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
      ...(snapshot || {}),
      mission,
      workspace: snapshot && snapshot.workspace,
      missionContext: {
        stage: mission.stage,
        stageLabel: mission.status,
        progress: mission.progressPercent,
        confidence: mission.confidence,
      },
    },
    { kind: 'workspace' }
  );

  const comm = applyAmoPresentationContract(baseComm, {
    mission,
    snapshot,
    created,
  });
  comm.reasoningEvidence = buildReasoningEvidence({
    known: [`Objective: ${mission.objective}.`],
    inference: comm.discoveryResults
      ? ['Scout discovery artifact is attached to the mission.']
      : ['AMO runtime owns this response. Presentation stays on the acquisition mission.'],
    unknown: [],
    evidenceNeeded: [],
    confidence: mission.confidence,
  });

  const prose = formatMissionProse(comm);
  const missionUnderstanding =
    (mission.pendingOperatorDecision && mission.pendingOperatorDecision.clarificationPrompt) ||
    formatCanonicalObjectiveDisplay(mission) ||
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
        buildOpenMissionAction({
          missionId: mission.id,
          runtime: mission.runtime || MISSION_RUNTIMES.AMO,
          label: 'Open mission workspace',
        }),
      ],
      confidenceContributors: ['spec_134', 'spec_124', 'acquisition_mission'],
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
        missionRuntime: 'AMO',
        presentationContract: 'amo',
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

  const runtime = resolveAcquisitionMissionRuntime(input);
  const persistOpts = runtime.persistOpts(input);

  const ciLoaded = await attachClientIntelligenceContext(input);
  const ciEvidence = buildClientIntelligenceMissionEvidence(ciLoaded.summary);

  const missions = await runtime.list(tenantId, persistOpts);
  let mission = findResumableMission(missions, question);
  let created = false;

  if (!mission) {
    const targetSegment = inferTargetSegment(question);
    const resolvedObjective =
      input.resolvedObjective ||
      (input.session && input.session.context && input.session.context.resolvedObjective) ||
      resolveCanonicalObjective({
        question,
        executionContract: input.executionContract,
        objectiveResolution: input.objectiveResolution,
        context: {
          blueprint: ciEvidence.strategicEvidence || null,
        },
        targetSegment,
      });
    const canonicalObjective = canonicalObjectiveText(resolvedObjective) || question;

    mission = await runtime.create(
      {
        tenantId,
        clientId: Number(tenantId) || tenantId,
        objective: canonicalObjective,
        resolvedObjective,
        executionPolicy: resolvedObjective.executionPolicy,
        communicationPolicy: resolvedObjective.communicationPolicy,
        evaluationPolicy: resolvedObjective.evaluationPolicy,
        title: deriveMissionTitle(canonicalObjective, targetSegment),
        targetSegment,
        createdBy: 'max',
        owner: 'Operator',
        constraints: ciEvidence.constraints.slice(),
        planningContext: {
          blueprint: ciEvidence.strategicEvidence || null,
        },
        executionContract: input.executionContract || null,
      },
      persistOpts
    );
    created = true;
  }

  const engine = runtime.engine();
  assertRuntimeEngine(engine, runtime);

  if (ciEvidence.attached) {
    await attachEvidenceToMission(mission.id, question, ciEvidence, {
      tenantId,
      ...persistOpts,
      acquisitionMissionRuntime: runtime,
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
    owner: 'AMO',
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
    input.session.context.acquisitionOwner = 'AMO';
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
  applyAmoPresentationContract,
  buildOwnershipMissionResponse,
  maybeHandleAcquisitionOwnershipTurn,
  AMO_SOURCES,
};
