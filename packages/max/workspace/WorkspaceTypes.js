'use strict';

/**
 * Max Intelligence Workspace types (SPEC-009 / ADR-005).
 * LLMs never create these — they only present them.
 */

const PAGE_TYPES = Object.freeze({
  COMMAND_DECK: 'command-deck',
  COMPANY: 'company',
  RECOMMENDATION: 'recommendation',
  TIMELINE: 'timeline',
  MARKET: 'market',
});

const PAGE_TYPE_SET = new Set(Object.values(PAGE_TYPES));

const SOURCE_KEYS = Object.freeze([
  'briefing',
  'reasoning',
  'memory',
  'policy',
  'knowledge',
  'missionState',
]);

/**
 * Empty sourcesUsed map (all false).
 * @returns {{ briefing: boolean, reasoning: boolean, memory: boolean, policy: boolean, knowledge: boolean, missionState: boolean }}
 */
function emptySourcesUsed() {
  return {
    briefing: false,
    reasoning: false,
    memory: false,
    policy: false,
    knowledge: false,
    missionState: false,
  };
}

/**
 * @param {object} [partial]
 */
function buildResponseMetadata(partial = {}) {
  const sourcesUsed = {
    ...emptySourcesUsed(),
    ...(partial.sourcesUsed && typeof partial.sourcesUsed === 'object'
      ? partial.sourcesUsed
      : {}),
  };
  const meta = {
    sourcesUsed,
    evidenceCount: Number.isFinite(Number(partial.evidenceCount))
      ? Number(partial.evidenceCount)
      : 0,
    asOf: partial.asOf || null,
    unavailable: Array.isArray(partial.unavailable)
      ? partial.unavailable.map(String)
      : [],
  };
  // SPEC-022: preserve mission routing metadata when present
  if (partial.route != null) meta.route = String(partial.route);
  if (partial.missionId != null) meta.missionId = String(partial.missionId);
  if (partial.missionStatus != null) {
    meta.missionStatus = String(partial.missionStatus);
  }
  if (partial.missionCard != null) meta.missionCard = partial.missionCard;
  // SPEC-057: execution domain / surface ownership
  if (partial.executionDomain != null) {
    meta.executionDomain = String(partial.executionDomain);
  }
  if (partial.surface != null) meta.surface = String(partial.surface);
  if (partial.activeMissionAction != null) {
    meta.activeMissionAction = String(partial.activeMissionAction);
  }
  if (partial.classification != null) {
    meta.classification = String(partial.classification);
  }
  if (partial.resolutionPath != null) {
    meta.resolutionPath = String(partial.resolutionPath);
  }
  if (partial.canaryPreparationOnly === true) {
    meta.canaryPreparationOnly = true;
  }
  if (
    partial.canaryWorkflowType != null &&
    String(partial.canaryWorkflowType).trim() !== ''
  ) {
    meta.canaryWorkflowType = String(partial.canaryWorkflowType);
  }
  if (partial.provisionalDrafts === true) {
    meta.provisionalDrafts = true;
  }
  if (partial.verificationWorkOrder === true) {
    meta.verificationWorkOrder = true;
  }
  if (partial.fillableTable === true) {
    meta.fillableTable = true;
  }
  if (partial.packetReview === true) {
    meta.packetReview = true;
  }
  if (partial.packetContentReview === true) {
    meta.packetContentReview = true;
  }
  if (partial.callScriptReview === true) {
    meta.callScriptReview = true;
  }
  if (partial.callScriptContentReview === true) {
    meta.callScriptContentReview = true;
  }
  if (partial.callScriptDecisionRecord === true) {
    meta.callScriptDecisionRecord = true;
  }
  if (partial.callScriptDecisionRecord === false) {
    meta.callScriptDecisionRecord = false;
  }
  if (partial.approvedForDial === false) {
    meta.approvedForDial = false;
  }
  if (partial.approvedForDial === true) {
    meta.approvedForDial = true;
  }
  if (
    partial.dialCallApprovalStatus != null &&
    String(partial.dialCallApprovalStatus).trim() !== ''
  ) {
    meta.dialCallApprovalStatus = String(partial.dialCallApprovalStatus);
  }
  if (
    partial.callScriptContentDecision != null &&
    String(partial.callScriptContentDecision).trim() !== ''
  ) {
    meta.callScriptContentDecision = String(partial.callScriptContentDecision);
  }
  if (
    partial.callScriptReviewStatus != null &&
    String(partial.callScriptReviewStatus).trim() !== ''
  ) {
    meta.callScriptReviewStatus = String(partial.callScriptReviewStatus);
  }
  if (partial.canarySummary === true) {
    meta.canarySummary = true;
  }
  if (partial.focusedWorkOrder === true) {
    meta.focusedWorkOrder = true;
  }
  if (partial.outputSubtype != null && String(partial.outputSubtype).trim() !== '') {
    meta.outputSubtype = String(partial.outputSubtype);
  }
  if (partial.knownCurrentState === true) {
    meta.knownCurrentState = true;
  }
  if (partial.readinessSummaryTable === true) {
    meta.readinessSummaryTable = true;
  }
  if (partial.inlineKnownFacts === true) {
    meta.inlineKnownFacts = true;
  }
  if (Array.isArray(partial.missingRequiredFields)) {
    meta.missingRequiredFields = partial.missingRequiredFields.map(String);
  }
  if (partial.tableUpdate === true) {
    meta.tableUpdate = true;
  }
  if (partial.tableUpdate === false) {
    meta.tableUpdate = false;
  }
  if (Array.isArray(partial.updatedProspectIds)) {
    meta.updatedProspectIds = partial.updatedProspectIds.map(String);
  }
  if (Array.isArray(partial.reassessedProspectIds)) {
    meta.reassessedProspectIds = partial.reassessedProspectIds.map(String);
  }
  if (partial.activeWorkContextReused === true) {
    meta.activeWorkContextReused = true;
  }
  if (partial.activeWorkContextReused === false) {
    meta.activeWorkContextReused = false;
  }
  if (partial.missingActiveWorkContext === true) {
    meta.missingActiveWorkContext = true;
  }
  if (partial.readinessTableNotIngested === true) {
    meta.readinessTableNotIngested = true;
  }
  if (
    partial.canaryReadinessIngestDiagnostics &&
    typeof partial.canaryReadinessIngestDiagnostics === 'object'
  ) {
    meta.canaryReadinessIngestDiagnostics = {
      ...partial.canaryReadinessIngestDiagnostics,
    };
  }
  if (partial.requestedProspectId != null) {
    meta.requestedProspectId = String(partial.requestedProspectId);
  }
  if (partial.prospectId != null) {
    meta.prospectId = String(partial.prospectId);
  }
  if (partial.prioritizedProspectId != null) {
    meta.prioritizedProspectId = String(partial.prioritizedProspectId);
  }
  if (partial.campaignId != null) {
    meta.campaignId = String(partial.campaignId);
  }
  if (partial.mailReadiness != null) {
    meta.mailReadiness = String(partial.mailReadiness);
  }
  if (partial.callReadiness != null) {
    meta.callReadiness = String(partial.callReadiness);
  }
  if (partial.scriptReadiness != null) {
    meta.scriptReadiness = String(partial.scriptReadiness);
  }
  if (partial.primaryReadiness != null) {
    meta.primaryReadiness = String(partial.primaryReadiness);
  }
  if (partial.executionReadiness != null) {
    meta.executionReadiness = String(partial.executionReadiness);
  }
  if (partial.draftConfidence != null) {
    meta.draftConfidence = String(partial.draftConfidence);
  }
  // Suggestion-engine routing for artifact responses (esp. inline packet review
  // that does not mutate activeWorkContext).
  if (partial.outputKind != null && String(partial.outputKind).trim() !== '') {
    meta.outputKind = String(partial.outputKind);
  }
  if (
    partial.lastOutputKind != null &&
    String(partial.lastOutputKind).trim() !== ''
  ) {
    meta.lastOutputKind = String(partial.lastOutputKind);
  }
  if (partial.contextHints && typeof partial.contextHints === 'object') {
    meta.contextHints = { ...partial.contextHints };
  }
  if (
    partial.packetReviewContext &&
    typeof partial.packetReviewContext === 'object'
  ) {
    meta.packetReviewContext = { ...partial.packetReviewContext };
  }
  if (partial.strictOutputShape === true) {
    meta.strictOutputShape = true;
  }
  if (partial.strictOutputShape === false) {
    meta.strictOutputShape = false;
  }
  if (partial.prospectCount != null && Number.isFinite(Number(partial.prospectCount))) {
    meta.prospectCount = Number(partial.prospectCount);
  }
  if (partial.investigation && typeof partial.investigation === 'object') {
    meta.investigation = partial.investigation;
  }
  if (Array.isArray(partial.provenance)) {
    meta.provenance = partial.provenance;
  }
  if (partial.coverageConfidence != null && Number.isFinite(Number(partial.coverageConfidence))) {
    meta.coverageConfidence = Number(partial.coverageConfidence);
  }
  if (partial.coverageBand != null) meta.coverageBand = String(partial.coverageBand);
  if (partial.delegationId != null) meta.delegationId = String(partial.delegationId);
  if (partial.resultId != null) meta.resultId = String(partial.resultId);
  if (partial.evaluationId != null) meta.evaluationId = String(partial.evaluationId);
  if (partial.scoutDelegated === true) meta.scoutDelegated = true;
  if (partial.acquisitionLoop === true) meta.acquisitionLoop = true;
  if (partial.retrievalBeforeDelegation === true) meta.retrievalBeforeDelegation = true;
  if (partial.cognitiveMode != null) meta.cognitiveMode = String(partial.cognitiveMode);
  if (partial.specialistDelegated === false) meta.specialistDelegated = false;
  if (partial.operatingEvidenceRetrieval === true) meta.operatingEvidenceRetrieval = true;
  if (partial.evidenceGroundedRecommendation === true) {
    meta.evidenceGroundedRecommendation = true;
  }
  if (partial.claimChallenge === true) meta.claimChallenge = true;
  if (partial.claimVerdict != null) meta.claimVerdict = String(partial.claimVerdict);
  if (partial.executed === false) meta.executed = false;
  if (partial.acquisitionMission === true) meta.acquisitionMission = true;
  if (partial.acquisitionOwnership === true) meta.acquisitionOwnership = true;
  if (partial.missionRuntime != null) meta.missionRuntime = String(partial.missionRuntime);
  if (partial.presentationContract != null) {
    meta.presentationContract = String(partial.presentationContract);
  }
  if (partial.missionCreated === true) meta.missionCreated = true;
  if (partial.missionResumed === true) meta.missionResumed = true;
  if (partial.missionInspection === true) meta.missionInspection = true;
  if (partial.inspectionProperty != null) {
    meta.inspectionProperty = String(partial.inspectionProperty);
  }
  if (partial.inspectionPipeline != null) {
    meta.inspectionPipeline = String(partial.inspectionPipeline);
  }
  if (partial.invented === true) meta.invented = true;
  if (partial.invented === false) meta.invented = false;
  if (partial.missionCommunication === true) meta.missionCommunication = true;
  if (partial.missionCommunicationPayload != null) {
    meta.missionCommunicationPayload = partial.missionCommunicationPayload;
  }
  if (partial.reasoningEvidence != null) {
    meta.reasoningEvidence = partial.reasoningEvidence;
  }
  if (partial.showReasoningDisclosure === true) {
    meta.showReasoningDisclosure = true;
  }
  if (partial.businessIntelligenceUsed === true) {
    meta.businessIntelligenceUsed = true;
  }
  if (partial.businessIntelligenceUsed === false) {
    meta.businessIntelligenceUsed = false;
  }
  if (partial.identityConversation === true) meta.identityConversation = true;
  if (partial.operatingModelReflection === true) meta.operatingModelReflection = true;
  if (partial.operatingModelReflection === false) meta.operatingModelReflection = false;
  if (partial.operatingModelReasoning != null) {
    meta.operatingModelReasoning = partial.operatingModelReasoning;
  }
  if (partial.underlyingIntent != null) {
    meta.underlyingIntent = String(partial.underlyingIntent);
  }
  if (partial.reflectiveCognition === true) meta.reflectiveCognition = true;
  if (partial.conversationLayer === true) meta.conversationLayer = true;
  if (partial.conversationSubject != null) {
    meta.conversationSubject = String(partial.conversationSubject);
  }
  if (partial.conversationIntent != null) {
    meta.conversationIntent = String(partial.conversationIntent);
  }
  if (partial.readOnlyCognition === true) meta.readOnlyCognition = true;
  return meta;
}

/**
 * @param {object} input
 */
function buildStructuredResponse(input = {}) {
  const supportingEvidence = Array.isArray(input.supportingEvidence)
    ? input.supportingEvidence.map(normalizeEvidenceRef)
    : [];
  const contradictingEvidence = Array.isArray(input.contradictingEvidence)
    ? input.contradictingEvidence.map(normalizeEvidenceRef)
    : [];
  const metadata = buildResponseMetadata({
    ...input.metadata,
    evidenceCount:
      input.metadata && input.metadata.evidenceCount != null
        ? input.metadata.evidenceCount
        : supportingEvidence.length + contradictingEvidence.length,
  });

  return {
    answer: input.answer != null ? String(input.answer) : '',
    reasoning: Array.isArray(input.reasoning)
      ? input.reasoning.map(String)
      : [],
    supportingEvidence,
    contradictingEvidence,
    confidence:
      input.confidence == null || !Number.isFinite(Number(input.confidence))
        ? null
        : Number(input.confidence),
    nextInvestigations: Array.isArray(input.nextInvestigations)
      ? input.nextInvestigations.map(String)
      : [],
    recommendedActions: Array.isArray(input.recommendedActions)
      ? input.recommendedActions.map(normalizeAction)
      : [],
    confidenceContributors: Array.isArray(input.confidenceContributors)
      ? input.confidenceContributors.map(String)
      : [],
    timelineReferences: Array.isArray(input.timelineReferences)
      ? input.timelineReferences.map(normalizeTimelineRef)
      : [],
    relatedEntities: Array.isArray(input.relatedEntities)
      ? input.relatedEntities.map(normalizeEntityRef)
      : [],
    investigation:
      input.investigation && typeof input.investigation === 'object'
        ? input.investigation
        : null,
    inspection:
      input.inspection && typeof input.inspection === 'object'
        ? input.inspection
        : null,
    provenance: Array.isArray(input.provenance) ? input.provenance.slice() : [],
    metadata,
  };
}

function normalizeEvidenceRef(ref) {
  if (!ref || typeof ref !== 'object') {
    return { id: 'unknown', summary: String(ref || ''), sourceType: null };
  }
  return {
    id: String(ref.id || 'unknown'),
    summary: String(ref.summary || ref.label || ref.statement || ref.title || ''),
    sourceType: ref.sourceType != null ? String(ref.sourceType) : null,
    sourceKind: ref.sourceKind != null ? String(ref.sourceKind) : null,
    kind: ref.kind != null ? String(ref.kind) : null,
    confidence:
      ref.confidence == null || !Number.isFinite(Number(ref.confidence))
        ? null
        : Number(ref.confidence),
  };
}

function normalizeAction(action) {
  return {
    id: String(action.id || action.type || 'action'),
    type: String(action.type || 'ask_max'),
    label: String(action.label || action.type || 'Action'),
    payload:
      action.payload && typeof action.payload === 'object' ? action.payload : null,
  };
}

function normalizeTimelineRef(ref) {
  if (!ref || typeof ref !== 'object') {
    return { id: String(ref || ''), summary: String(ref || ''), at: null };
  }
  return {
    id: String(ref.id || ''),
    summary: String(ref.summary || ref.title || ''),
    at: ref.at || ref.updatedAt || null,
  };
}

function normalizeEntityRef(ref) {
  if (!ref || typeof ref !== 'object') {
    return { id: String(ref || ''), type: 'unknown', name: String(ref || '') };
  }
  return {
    id: String(ref.id || ref.companyId || ref.recommendationId || ''),
    type: String(ref.type || 'entity'),
    name: String(ref.name || ref.companyName || ref.title || ref.id || ''),
  };
}

module.exports = {
  PAGE_TYPES,
  PAGE_TYPE_SET,
  SOURCE_KEYS,
  emptySourcesUsed,
  buildResponseMetadata,
  buildStructuredResponse,
  normalizeEvidenceRef,
  normalizeAction,
  normalizeEntityRef,
  normalizeTimelineRef,
};
