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
};
