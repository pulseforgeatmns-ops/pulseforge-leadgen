'use strict';

/**
 * SPEC-100 — Max Workspace adapter for the Scout acquisition intelligence loop.
 * Max remains the operator-facing responder. Scout is intelligence-only.
 */

const scoutAcquisition = require('../../../services/scoutAcquisitionIntelligence');
const { classifyCognitiveMode } = require('../specialistDelegation/CognitiveMode');
const { mayEnterSpecialistPath } = require('../specialistDelegation/RetrievalGate');
const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  toBusinessEvidenceRefs,
  buildSystemProvenance,
  investigationFromResult,
} = require('../scoutAcquisition/InvestigationProvenance');
const { inspectionSummary } = require('../specialistDelegation/CognitiveTrace');
const {
  CONTRACT_IDS,
  selectResponseContract,
  composeAccordingToContract,
  attachContractMetadata,
} = require('./ResponseContract');

function defaultLoop(input = {}) {
  return input.runLoop || scoutAcquisition.runAcquisitionIntelligenceLoop;
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
      envelope.clientId ||
      sessionCtx.clientId ||
      ''
  ).trim();
}

function shouldHandleScoutAcquisition(input = {}) {
  const question = String(input.question || '').trim();
  if (!question) return false;
  try {
    const operating = require('./OperatingEvidenceRetrieval');
    if (operating.shouldRetrieveOperatingEvidence(question)) return false;
  } catch (_) {
    /* classifier unavailable — fall through */
  }
  const context = {
    ...(input.session && input.session.context),
    ...(input.context || {}),
    action: input.action || (input.context && input.context.action),
  };
  const mode = classifyCognitiveMode(question, {
    session: input.session,
    context,
  });
  const reusePriorWork =
    scoutAcquisition.looksLikeFollowUp(question) ||
    scoutAcquisition.looksLikeExplainPriority(question) ||
    scoutAcquisition.looksLikeInvestigationInspection(question);
  if (
    !mayEnterSpecialistPath(mode, {
      question,
      session: input.session,
      context,
    }) &&
    !reusePriorWork
  ) {
    return false;
  }
  const enabled = context.enabledAgents || context.enabled_agents;
  if (Array.isArray(enabled) && !enabled.map(String).includes('scout')) {
    return false;
  }
  return scoutAcquisition.looksLikeAcquisitionQuestion(question, context);
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleScoutAcquisitionTurn(input = {}) {
  const question = String(input.question || '').trim();
  if (!question) return null;
  if (!shouldHandleScoutAcquisition(input)) return null;

  const tenantId = resolveTenantId(input);
  if (!tenantId) return null;

  const session = input.session || null;
  const sessionCtx =
    session && session.context && typeof session.context === 'object'
      ? session.context
      : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};

  const runLoop = defaultLoop(input);
  const result = await runLoop(
    {
      authorizedTenantId: tenantId,
      tenantId,
      question,
      objective: envelope.objective || sessionCtx.objective || question,
      reason: envelope.reason,
      authority: envelope.authority || 'observe',
      businessContext: envelope.businessContext || sessionCtx.businessContext,
      targetContext: envelope.targetContext || sessionCtx.targetContext,
      approvedUnderstanding:
        envelope.approvedUnderstanding ||
        (sessionCtx.businessBlueprint && sessionCtx.businessBlueprint.approved) ||
        sessionCtx.approvedUnderstanding,
      operatorDirection: envelope.operatorDirection || sessionCtx.operatorDirection,
      context: {
        ...sessionCtx,
        ...envelope,
        domainId: envelope.domainId || sessionCtx.domainId,
        action: input.action || envelope.action || sessionCtx.action,
        acquisitionLoop: true,
      },
      applyPriority: envelope.applyPriority === true || input.applyPriority === true,
      fixtureMode: input.fixtureMode || envelope.fixtureMode,
    },
    {
      delegationService: input.delegationService,
      delegationOpts: input.delegationOpts,
      aoStore: input.aoStore,
      companies: input.companies,
      people: input.people,
      loadCompanies: input.loadCompanies,
      discover: input.discover,
      discoveryAdapters: input.discoveryAdapters,
      discoveryStore: input.discoveryStore,
      persistCompanies: input.persistCompanies,
      enrichPeople: input.enrichPeople,
      enablePlaces: input.enablePlaces,
      priorityApplier: input.priorityApplier,
      freshnessMs: input.freshnessMs,
    }
  );

  if (!result || result.kind === 'unrelated' || result.kind === 'retrieve') {
    return null;
  }

  if (session && session.context && typeof session.context === 'object') {
    session.context.acquisitionLoop = true;
    session.context.domainId = session.context.domainId || 'acquisition';
    if (result.evaluation) {
      session.context.lastScoutEvaluation = {
        id: result.evaluation.id,
        materiality: result.evaluation.materiality,
        materialChange: result.evaluation.materialChange,
        delegationId: result.evaluation.delegationId,
        resultId: result.evaluation.resultId,
      };
      session.context.lastSpecialistEvaluation = session.context.lastScoutEvaluation;
    }
    if (result.state) {
      session.context.acquisitionIntelligence = {
        summary: result.state.summary,
        opportunityCount: result.state.opportunityCount,
        timelyCount: result.state.timelyCount,
      };
    }
    if (result.investigation || (result.state && result.state.investigation)) {
      session.context.lastScoutInvestigation =
        result.investigation || result.state.investigation;
    }
  }

  let prose =
    result.prose ||
    'I checked current acquisition intelligence before deciding whether Scout needed to investigate.';

  const investigation =
    result.investigation ||
    investigationFromResult(result.result) ||
    (result.state && result.state.investigation) ||
    null;
  const businessEvidence = toBusinessEvidenceRefs(
    (result.result && result.result.evidenceRefs) ||
      (result.trail && result.trail.chain && result.trail.chain.evidence) ||
      []
  );
  const provenance = buildSystemProvenance({
    delegationId: result.delegation && result.delegation.id,
    resultId: result.result && result.result.id,
    evaluationId: result.evaluation && result.evaluation.id,
  });

  const known =
    (session && session.context && session.context.investigationKnown) ||
    (result.need && result.need.reason) ||
    'Existing acquisition intelligence was inspected before deciding whether Scout needed new work.';
  const { attachPipelineLog } = require('./ReasoningPipeline');
  const contract =
    input.responseContract ||
    (session && session.context && session.context.responseContract) ||
    selectResponseContract(question, classifyCognitiveMode(question, { session, context: envelope }));
  if (contract && contract.id === CONTRACT_IDS.INVESTIGATION) {
    prose = [
      composeAccordingToContract(contract, {
        known,
        needSpecialist: result.delegated
          ? 'Yes. Scout is running a bounded commercial-prospect investigation because current records do not answer this as retrieval.'
          : result.kind === 'interrogate'
            ? 'No new specialist run. I am inspecting the existing investigation trace.'
            : 'Yes if new external coverage is required; I will not invent prospects from Blueprint memory.',
        expectedOutputs:
          'Candidate companies, coverage, and fit from the investigation — not a strategy recommendation from unsupported memory.',
      }),
      prose,
    ]
      .filter(Boolean)
      .join('\n\n');
  }

  const structured = attachPipelineLog(
    attachContractMetadata(
    buildStructuredResponse({
    answer: prose,
    reasoning: [
      result.need && result.need.reason,
      result.delegated
        ? 'Delegated a bounded Scout acquisition_intelligence investigation.'
        : result.kind === 'interrogate'
          ? 'Inspected the existing specialist cognitive trace.'
          : 'Answered from existing durable acquisition intelligence.',
      result.kind === 'interrogate'
        ? null
        : result.evaluation
          ? `Max evaluation materiality: ${result.evaluation.materiality}.`
          : null,
      investigation
        ? `Investigation coverage: ${investigation.coverageBand} (${investigation.coverageConfidence}).`
        : null,
    ].filter(Boolean),
    supportingEvidence: businessEvidence,
    contradictingEvidence: [],
    confidence:
      result.evaluation && result.result && result.result.confidence != null
        ? result.result.confidence
        : 0.8,
    nextInvestigations: [],
    recommendedActions: [
      { id: 'acknowledge', type: 'review', label: 'Continue' },
    ],
    confidenceContributors: [],
    timelineReferences: [],
    relatedEntities: [],
    investigation,
    provenance,
    inspection: result.trace ? inspectionSummary(result.trace) : null,
    metadata: {
      sourcesUsed: {
        briefing: false,
        reasoning: true,
        memory: true,
        policy: true,
        knowledge: false,
      },
      evidenceCount: businessEvidence.length,
      asOf: new Date().toISOString(),
      unavailable:
        (investigation &&
          investigation.sources &&
          investigation.sources.sourceTypesUnavailable) ||
        [],
      delegationId: result.delegation && result.delegation.id,
      resultId: result.result && result.result.id,
      evaluationId: result.evaluation && result.evaluation.id,
      scoutDelegated: result.delegated === true,
      acquisitionLoop: true,
      coverageConfidence: investigation && investigation.coverageConfidence,
      coverageBand: investigation && investigation.coverageBand,
    },
  }),
    contract
  ),
    { analysis: { intent: 'investigation', analysisMode: 'investigation', kind: 'investigation' }, contract }
  );

  return {
    reason: result.delegated
      ? 'scout_acquisition_intelligence'
      : `scout_acquisition_${result.kind}`,
    structured,
    prose,
    loop: result,
    responseContract: contract,
  };
}

module.exports = {
  shouldHandleScoutAcquisition,
  maybeHandleScoutAcquisitionTurn,
};
