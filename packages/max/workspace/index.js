'use strict';

const {
  PAGE_TYPES,
  PAGE_TYPE_SET,
  SOURCE_KEYS,
  emptySourcesUsed,
  buildResponseMetadata,
  buildStructuredResponse,
} = require('./WorkspaceTypes');
const {
  normalizeContext,
  contextFingerprint,
  contextFocusLabel,
} = require('./ContextEnvelope');
const { buildOpeningState } = require('./OpeningStateBuilder');
const {
  resolveActiveTenantId,
  resolveMaxPromptContext,
  buildTenantGreeting,
  NO_ACTIVE_CLIENT,
} = require('./TenantContextResolver');
const {
  buildSuggestions,
  buildActiveWorkSuggestions,
  isActiveDeskWorkflow,
  resolveResponseWorkContext,
  resolveSuggestionWorkContext,
  topCompanyName,
} = require('./SuggestionEngine');
const { SessionStore } = require('./SessionStore');
const { assembleEvidence } = require('./EvidenceAssembler');
const { composeResponse, classifyIntent } = require('./ResponseComposer');
const {
  composeMissionResponse,
  composeActiveMissionResponse,
} = require('./MissionResponse');
const {
  maybeHandleMissionFirstTurn,
  evaluateMissionContinuation,
  evaluateMissionEscape,
  logMissionRouting,
  logMissionRoutingOverride,
  listMissionRoutingLog,
  clearMissionRoutingLog,
  PIPELINES,
  CONTINUATION_THRESHOLD,
} = require('./MissionFirstRouting');
const {
  PresentationEngine,
  formatDeterministicProse,
} = require('./PresentationEngine');
const {
  WorkspaceEngine,
  createWorkspaceEngine,
} = require('./WorkspaceEngine');
const {
  EXECUTION_DOMAINS,
  MISSION_DOMAINS,
  selectExecutionDomain,
  attachDomainContext,
  isMissionDomain,
  toRouteDecision,
} = require('./ExecutionDomain');
const ActiveWorkContext = require('./ActiveWorkContext');
const {
  getProspectOperatingBrief,
} = require('./ProspectOperatingBriefContext');
const {
  getServiceModeOperatorLoop,
} = require('./ServiceModeOperatorLoopContext');
const {
  maybeHandlePaigeCampaignContentDelegation,
} = require('./PaigeCampaignDelegationContext');
const {
  maybeHandleOperatorObjectiveTurn,
  attachActiveObjectiveContext,
  synthesizeObjectivePaigeResponse,
} = require('./OperatorObjectiveContext');
const {
  maybeHandleClientIntelligenceTurn,
  attachClientIntelligenceContext,
  loadApprovedClientIntelligence,
} = require('./ClientIntelligenceContext');
const {
  createBoundedDelegation,
  executeBoundedDelegation,
  consumeSpecialistResult,
  explainSpecialistTrail,
} = require('./SpecialistDelegationContext');
const {
  maybeHandleScoutAcquisitionTurn,
  shouldHandleScoutAcquisition,
} = require('./ScoutAcquisitionContext');
const {
  maybeHandleSpecialistInterrogationTurn,
} = require('./SpecialistInterrogationContext');
const {
  maybeHandleAcquisitionMissionTurn,
  looksLikeAcquisitionMissionQuestion,
} = require('./AcquisitionMissionTurn');
const {
  maybeHandleRetrievalBeforeDelegationTurn,
} = require('./RetrievalBeforeDelegationContext');
const {
  loadDurableBusinessUnderstanding,
  buildBusinessUnderstandingContract,
  KNOWLEDGE_STATES,
} = require('./BusinessUnderstandingRetrieval');
const { loadOperatorContextForSession } = require('./OperatorContextLoader');
const {
  loadOperatingEvidence,
  isOperatingEvidenceQuestion,
  shouldRetrieveOperatingEvidence,
} = require('./OperatingEvidenceRetrieval');
const {
  maybeHandleOperatorOperatingUpdate,
  isOperatorOperatingUpdate,
} = require('./OperatorOperatingUpdate');
const ResponseContract = require('./ResponseContract');
const BusinessIntelligence = require('./BusinessIntelligence');
const OperatorIntentRegistry = require('./OperatorIntentRegistry');

module.exports = {
  getProspectOperatingBrief,
  getServiceModeOperatorLoop,
  maybeHandlePaigeCampaignContentDelegation,
  maybeHandleOperatorObjectiveTurn,
  attachActiveObjectiveContext,
  synthesizeObjectivePaigeResponse,
  maybeHandleClientIntelligenceTurn,
  attachClientIntelligenceContext,
  loadApprovedClientIntelligence,
  createBoundedDelegation,
  executeBoundedDelegation,
  consumeSpecialistResult,
  explainSpecialistTrail,
  maybeHandleScoutAcquisitionTurn,
  shouldHandleScoutAcquisition,
  maybeHandleSpecialistInterrogationTurn,
  maybeHandleAcquisitionMissionTurn,
  looksLikeAcquisitionMissionQuestion,
  maybeHandleRetrievalBeforeDelegationTurn,
  loadDurableBusinessUnderstanding,
  buildBusinessUnderstandingContract,
  KNOWLEDGE_STATES,
  loadOperatorContextForSession,
  loadOperatingEvidence,
  isOperatingEvidenceQuestion,
  shouldRetrieveOperatingEvidence,
  maybeHandleOperatorOperatingUpdate,
  isOperatorOperatingUpdate,
  selectResponseContract: ResponseContract.selectResponseContract,
  listResponseContracts: ResponseContract.listResponseContracts,
  getResponseContract: ResponseContract.getResponseContract,
  CONTRACT_IDS: ResponseContract.CONTRACT_IDS,
  RetrievalContract: ResponseContract.RetrievalContract,
  SummaryContract: ResponseContract.SummaryContract,
  RecommendationContract: ResponseContract.RecommendationContract,
  DiagnosisContract: ResponseContract.DiagnosisContract,
  UnknownAnalysisContract: ResponseContract.UnknownAnalysisContract,
  RiskContract: ResponseContract.RiskContract,
  ProgressContract: ResponseContract.ProgressContract,
  ChallengeContract: ResponseContract.ChallengeContract,
  InvestigationContract: ResponseContract.InvestigationContract,
  synthesizeBusinessIntelligence: BusinessIntelligence.synthesizeBusinessIntelligence,
  serializeBusinessIntelligence: BusinessIntelligence.serializeBusinessIntelligence,
  isChannelEffectivenessQuestion: BusinessIntelligence.isChannelEffectivenessQuestion,
  CATEGORIES: BusinessIntelligence.CATEGORIES,
  CONFIDENCE: BusinessIntelligence.CONFIDENCE,
  OPERATOR_INTENTS: OperatorIntentRegistry.OPERATOR_INTENTS,
  ANALYSIS_MODES: OperatorIntentRegistry.ANALYSIS_MODES,
  classifyNewAnalysisMode: OperatorIntentRegistry.classifyNewAnalysisMode,
  looksLikeDiagnosis: OperatorIntentRegistry.looksLikeDiagnosis,
  looksLikeUnknownAnalysis: OperatorIntentRegistry.looksLikeUnknownAnalysis,
  looksLikeRisk: OperatorIntentRegistry.looksLikeRisk,
  looksLikeProgress: OperatorIntentRegistry.looksLikeProgress,
  PAGE_TYPES,
  PAGE_TYPE_SET,
  SOURCE_KEYS,
  emptySourcesUsed,
  buildResponseMetadata,
  buildStructuredResponse,
  normalizeContext,
  contextFingerprint,
  contextFocusLabel,
  buildOpeningState,
  resolveActiveTenantId,
  resolveMaxPromptContext,
  buildTenantGreeting,
  NO_ACTIVE_CLIENT,
  buildSuggestions,
  buildActiveWorkSuggestions,
  isActiveDeskWorkflow,
  resolveResponseWorkContext,
  resolveSuggestionWorkContext,
  topCompanyName,
  SessionStore,
  assembleEvidence,
  composeResponse,
  classifyIntent,
  composeMissionResponse,
  composeActiveMissionResponse,
  maybeHandleMissionFirstTurn,
  evaluateMissionContinuation,
  evaluateMissionEscape,
  logMissionRouting,
  logMissionRoutingOverride,
  listMissionRoutingLog,
  clearMissionRoutingLog,
  PIPELINES,
  CONTINUATION_THRESHOLD,
  PresentationEngine,
  formatDeterministicProse,
  WorkspaceEngine,
  createWorkspaceEngine,
  EXECUTION_DOMAINS,
  MISSION_DOMAINS,
  selectExecutionDomain,
  attachDomainContext,
  isMissionDomain,
  toRouteDecision,
  ActiveWorkContext,
  getActiveWorkContext: ActiveWorkContext.getActiveWorkContext,
  setActiveWorkContext: ActiveWorkContext.setActiveWorkContext,
  buildCanaryActiveWorkContext: ActiveWorkContext.buildCanaryActiveWorkContext,
  isActiveWorkFollowUpCue: ActiveWorkContext.isActiveWorkFollowUpCue,
  isActiveWorkReuseProspectCue: ActiveWorkContext.isActiveWorkReuseProspectCue,
  isActiveWorkTransformCue: ActiveWorkContext.isActiveWorkTransformCue,
  isPacketReviewRequest: ActiveWorkContext.isPacketReviewRequest,
  isCallScriptReviewRequest: ActiveWorkContext.isCallScriptReviewRequest,
  isCallScriptDecisionRecordRequest:
    ActiveWorkContext.isCallScriptDecisionRecordRequest,
  isCanarySummaryJudgmentRequest:
    ActiveWorkContext.isCanarySummaryJudgmentRequest,
  hasCanarySummaryJudgmentCues: ActiveWorkContext.hasCanarySummaryJudgmentCues,
  hasFocusedCanaryWorkOrderCues:
    ActiveWorkContext.hasFocusedCanaryWorkOrderCues,
  hasCanarySummaryOutputCues: ActiveWorkContext.hasCanarySummaryOutputCues,
  isFocusedCanaryWorkOrderRequest:
    ActiveWorkContext.isFocusedCanaryWorkOrderRequest,
  isProceedWithPacketContentReviewRequest:
    ActiveWorkContext.isProceedWithPacketContentReviewRequest,
  isProceedWithCallScriptReviewRequest:
    ActiveWorkContext.isProceedWithCallScriptReviewRequest,
  extractOperatorIntentProse: ActiveWorkContext.extractOperatorIntentProse,
  extractPacketReviewProspectId: ActiveWorkContext.extractPacketReviewProspectId,
  extractCallScriptReviewProspectId:
    ActiveWorkContext.extractCallScriptReviewProspectId,
  isFillableTableRequest: ActiveWorkContext.isFillableTableRequest,
  isFillableTableUpdateRequest: ActiveWorkContext.isFillableTableUpdateRequest,
  isFillableTableReadinessReassessRequest:
    ActiveWorkContext.isFillableTableReadinessReassessRequest,
  isFillableTableWholeTableReassessRequest:
    ActiveWorkContext.isFillableTableWholeTableReassessRequest,
  wantsStrictFillableTableOutputShape:
    ActiveWorkContext.wantsStrictFillableTableOutputShape,
  wantsFillableTableHeading: ActiveWorkContext.wantsFillableTableHeading,
  isExplicitNewMissionRequest: ActiveWorkContext.isExplicitNewMissionRequest,
  looksLikeFillableVerificationTablePaste:
    ActiveWorkContext.looksLikeFillableVerificationTablePaste,
  parseFillableVerificationTableFromMessage:
    ActiveWorkContext.parseFillableVerificationTableFromMessage,
  parseKnownCurrentStateBullets: ActiveWorkContext.parseKnownCurrentStateBullets,
  looksLikeReadinessSummaryTablePaste:
    ActiveWorkContext.looksLikeReadinessSummaryTablePaste,
  parseReadinessSummaryTableFromMessage:
    ActiveWorkContext.parseReadinessSummaryTableFromMessage,
  hasCanaryReadinessTableCues: ActiveWorkContext.hasCanaryReadinessTableCues,
  diagnoseCanaryReadinessTableIngestion:
    ActiveWorkContext.diagnoseCanaryReadinessTableIngestion,
  emitCanaryReadinessIngestDiagnostics:
    ActiveWorkContext.emitCanaryReadinessIngestDiagnostics,
  ingestPastedFillableVerificationTable:
    ActiveWorkContext.ingestPastedFillableVerificationTable,
  ingestPastedReadinessSummaryTable:
    ActiveWorkContext.ingestPastedReadinessSummaryTable,
};
