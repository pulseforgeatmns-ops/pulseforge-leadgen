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

module.exports = {
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
  isCanarySummaryJudgmentRequest:
    ActiveWorkContext.isCanarySummaryJudgmentRequest,
  hasCanarySummaryJudgmentCues: ActiveWorkContext.hasCanarySummaryJudgmentCues,
  extractOperatorIntentProse: ActiveWorkContext.extractOperatorIntentProse,
  extractPacketReviewProspectId: ActiveWorkContext.extractPacketReviewProspectId,
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
