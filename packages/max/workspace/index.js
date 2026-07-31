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
const { buildSuggestions, topCompanyName } = require('./SuggestionEngine');
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
  isFillableTableRequest: ActiveWorkContext.isFillableTableRequest,
};
